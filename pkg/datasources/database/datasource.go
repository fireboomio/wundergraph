package database

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"io"
	"net/http"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/sjson"
	"go.uber.org/zap"

	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/astnormalization"
	"github.com/wundergraph/graphql-go-tools/pkg/astprinter"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/datasource/httpclient"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/pkg/operationreport"

	"github.com/wundergraph/wundergraph/pkg/pool"
)

type Planner struct {
	visitor                    *plan.Visitor
	config                     Configuration
	upstreamOperation          *ast.Document
	upstreamVariables          []byte
	nodes                      []ast.Node
	variables                  resolve.Variables
	lastFieldEnclosingTypeName string
	disallowSingleFlight       bool
	disallowFieldAlias         bool
	client                     *http.Client
	isNested                   bool   // isNested - flags that datasource is nested e.g. field with datasource is not on a query type
	rootTypeName               string // rootTypeName - holds name of top level type
	rootFieldName              string // rootFieldName - holds name of root type field
	rootFieldRef               int    // rootFieldRef - holds ref of root type field

	inlinedVariables []inlinedVariable

	engineFactory *LazyEngineFactory

	debug           bool
	testsSkipEngine bool
	log             *zap.Logger

	insideJsonField bool
	jsonFieldRef    int

	operationTypeDefinitionRef int
	isQueryRaw                 bool
	isQueryRawRow              bool
}

type inlinedVariable struct {
	name         string
	typeRef      int
	nullable     bool
	isJSON       bool
	isRaw        bool
	parentIsJson bool
	replaceFunc  func(string) string
}

// ErrResponse add for transform prisma returnError to graphqlError
type ErrResponse struct {
	Errors []*SQLErrResult `json:"errors"`
}

// SQLErrResult graphqlError use Message, so replace Message with Error if Message is empty
type SQLErrResult struct {
	Error     string        `json:"error"`
	Message   string        `json:"message"`
	Path      []string      `json:"path"`
	Locations []interface{} `json:"locations"`
}

func (p *Planner) DownstreamResponseFieldAlias(downstreamFieldRef int) (alias string, exists bool) {

	// If there's no alias but the downstream Query re-uses the same path on different root fields,
	// we rewrite the downstream Query using an alias so that we can have an aliased Query to the upstream
	// while keeping a non aliased Query to the downstream but with a path rewrite on an existing root field.

	fieldName := p.visitor.Operation.FieldNameUnsafeString(downstreamFieldRef)

	if p.visitor.Operation.FieldAliasIsDefined(downstreamFieldRef) {
		return
	}

	typeName := p.visitor.Walker.EnclosingTypeDefinition.NameString(p.visitor.Definition)
	if fieldConfig := p.visitor.Config.Fields.ForTypeField(typeName, fieldName); fieldConfig != nil && len(fieldConfig.Path) == 1 {
		if fieldConfig.Path[0] != fieldName {
			aliasBytes := p.visitor.Operation.FieldNameBytes(downstreamFieldRef)
			alias, exists = string(aliasBytes), true
		}
	}
	return
}

func (p *Planner) DataSourcePlanningBehavior() plan.DataSourcePlanningBehavior {
	return plan.DataSourcePlanningBehavior{
		MergeAliasedRootNodes:      false,
		OverrideFieldPathFromAlias: false,
	}
}

type Configuration struct {
	DatasourceName      string
	DatabaseURL         string
	CloseTimeoutSeconds int32
	JsonTypeFields      []SingleTypeField
	JsonInputVariables  []string
	WunderGraphDir      string

	PrismaSchema        string
	GraphqlSchema       string
	EnvironmentVariable string
}

type SingleTypeField struct {
	TypeName  string
	FieldName string
}

func ConfigJson(config Configuration) json.RawMessage {
	out, _ := json.Marshal(config)
	return out
}

var (
	prismaCache        map[string]string
	plannerConfigCache map[string]Configuration
)

func ResetPlannerConfigCache() {
	plannerConfigCache = make(map[string]Configuration)
}

func StorePlannerConfig(rootNodes []*wgpb.TypeField, config Configuration) {
	if len(rootNodes) == 0 {
		return
	}

	rootNode := rootNodes[0]
	rootField := []string{rootNode.TypeName}
	rootField = append(rootField, rootNode.FieldNames...)
	plannerConfigCache[strings.Join(rootField, ".")] = config
}

func loadPlannerConfig(rootNodes []plan.TypeField) (config Configuration) {
	if len(rootNodes) == 0 {
		return
	}

	rootNode := rootNodes[0]
	rootField := []string{rootNode.TypeName}
	rootField = append(rootField, rootNode.FieldNames...)
	return plannerConfigCache[strings.Join(rootField, ".")]
}

func ResetPrismaCache() {
	prismaCache = make(map[string]string)
}

func StorePrisma(id, prisma string) {
	if _, ok := prismaCache[id]; ok {
		return
	}

	prismaCache[id] = prisma
}

func (p *Planner) Register(visitor *plan.Visitor, configuration plan.DataSourceConfiguration, isNested bool) error {
	p.visitor = visitor
	p.visitor.Walker.RegisterDocumentVisitor(p)
	p.visitor.Walker.RegisterFieldVisitor(p)
	p.visitor.Walker.RegisterOperationDefinitionVisitor(p)
	p.visitor.Walker.RegisterSelectionSetVisitor(p)
	p.visitor.Walker.RegisterEnterArgumentVisitor(p)
	p.visitor.Walker.RegisterInlineFragmentVisitor(p)

	p.config = loadPlannerConfig(configuration.RootNodes)

	if p.config.CloseTimeoutSeconds == 0 {
		p.config.CloseTimeoutSeconds = 10
	}

	p.isNested = isNested

	return nil
}

type fetchInput struct {
	Query     string          `json:"query"`
	Variables json.RawMessage `json:"variables"`
}

type RawJsonVariableRenderer struct {
	parentIsJson bool
}

func (r *RawJsonVariableRenderer) GetKind() string {
	return "raw_json"
}

func (r *RawJsonVariableRenderer) RenderVariable(ctx context.Context, data []byte, out io.Writer) error {
	if !r.parentIsJson {
		// when the parent is already rendering as a JSON, we don't need to wrap the child in quotes
		// this happens when using a variable inside the parameters list, e.g.
		// query($id: String!){rawQuery(query: "select foo from bar where id = $1", parameters: [$id])}
		// in this case, there will be a 2 variable renderers, one that renders the list (as JSON)
		// and a second one to inline render the value of $id
		// which doesn't need to be quoted again
		_, _ = out.Write([]byte(`\"`))
	}
	_, _ = out.Write([]byte(strings.ReplaceAll(string(data), `"`, `\\\"`)))
	if !r.parentIsJson {
		_, _ = out.Write([]byte(`\"`))
	}
	return nil
}

func (p *Planner) ConfigureFetch() plan.FetchConfiguration {

	operation := string(p.printOperation())

	input := fetchInput{
		Query:     operation,
		Variables: p.upstreamVariables,
	}

	for i, variable := range p.inlinedVariables {
		currentName := "$" + variable.name
		var (
			renderer resolve.VariableRenderer
			err      error
		)
		if variable.isRaw {
			renderer = &RawJsonVariableRenderer{
				parentIsJson: variable.parentIsJson,
			}
		} else if variable.isJSON {
			renderer = resolve.NewGraphQLVariableRenderer(`{"type":"string"}`)
		} else {
			renderer, err = resolve.NewGraphQLVariableRendererFromTypeRefWithoutValidation(p.visitor.Operation, p.visitor.Definition, variable.typeRef)
			if err != nil {
				continue
			}
		}
		contextVariable := &resolve.ContextVariable{
			Path:     []string{variable.name},
			Renderer: renderer,
			Nullable: variable.nullable,
		}
		replacement, _ := p.variables.AddVariable(contextVariable)
		currentRegexp, err := regexp.Compile(fmt.Sprintf(`%s\b`, regexp.QuoteMeta(currentName)))
		if err != nil {
			p.log.Error("failed to compile regexp", zap.Error(err))
			continue
		}

		replaceVariableFunc := func(query string) string { return currentRegexp.ReplaceAllLiteralString(query, replacement) }
		input.Query = replaceVariableFunc(input.Query)
		p.inlinedVariables[i].replaceFunc = replaceVariableFunc
	}

	rawInput, err := json.Marshal(input)
	if err != nil {
		rawInput = []byte(`{"error":` + err.Error() + `}`)
	}

	var engine *LazyEngine
	if !p.testsSkipEngine {
		prismaSchema := prismaCache[p.config.DatabaseURL]
		engine = p.engineFactory.Engine(prismaSchema, p.config.WunderGraphDir, p.config.CloseTimeoutSeconds, p.config.EnvironmentVariable)
		engine.client = p.client
	}

	return plan.FetchConfiguration{
		Input:          string(rawInput),
		ResetInputFunc: p.resetRawInput,
		DataSource: &Source{
			engine:         engine,
			debug:          p.debug,
			log:            p.log,
			datasourceName: p.config.DatasourceName,
			rootTypeName:   p.rootTypeName,
			rootFieldName:  p.rootFieldName,
		},
		Variables:            p.variables,
		DisallowSingleFlight: p.disallowSingleFlight,
		ProcessResponseConfig: resolve.ProcessResponseConfig{
			ExtractGraphqlResponse: true,
		},
	}
}

func (p *Planner) ConfigureSubscription() plan.SubscriptionConfiguration {

	input := httpclient.SetInputBodyWithPath(nil, p.upstreamVariables, "variables")
	input = httpclient.SetInputBodyWithPath(input, p.printOperation(), "query")

	return plan.SubscriptionConfiguration{
		Input:      string(input),
		DataSource: nil,
		Variables:  p.variables,
	}
}

func (p *Planner) EnterOperationDefinition(ref int) {
	operationType := p.visitor.Operation.OperationDefinitions[ref].OperationType
	if p.isNested {
		operationType = ast.OperationTypeQuery
	}
	definition := p.upstreamOperation.AddOperationDefinitionToRootNodes(ast.OperationDefinition{
		OperationType: operationType,
	})
	p.disallowSingleFlight = operationType == ast.OperationTypeMutation
	p.nodes = append(p.nodes, definition)
	p.operationTypeDefinitionRef = ref
}

func (p *Planner) LeaveOperationDefinition(_ int) {
	p.nodes = p.nodes[:len(p.nodes)-1]
}

func (p *Planner) EnterSelectionSet(ref int) {

	if p.insideJsonField {
		return
	}

	parent := p.nodes[len(p.nodes)-1]
	set := p.upstreamOperation.AddSelectionSet()
	switch parent.Kind {
	case ast.NodeKindOperationDefinition:
		p.upstreamOperation.OperationDefinitions[parent.Ref].HasSelections = true
		p.upstreamOperation.OperationDefinitions[parent.Ref].SelectionSet = set.Ref
	case ast.NodeKindField:
		p.upstreamOperation.Fields[parent.Ref].HasSelections = true
		p.upstreamOperation.Fields[parent.Ref].SelectionSet = set.Ref
	case ast.NodeKindInlineFragment:
		p.upstreamOperation.InlineFragments[parent.Ref].HasSelections = true
		p.upstreamOperation.InlineFragments[parent.Ref].SelectionSet = set.Ref
	}
	p.nodes = append(p.nodes, set)
	for _, selectionRef := range p.visitor.Operation.SelectionSets[ref].SelectionRefs {
		if p.visitor.Operation.Selections[selectionRef].Kind == ast.SelectionKindField {
			if p.visitor.Operation.FieldNameUnsafeString(p.visitor.Operation.Selections[selectionRef].Ref) == "__typename" {
				field := p.upstreamOperation.AddField(ast.Field{
					Name: p.upstreamOperation.Input.AppendInputString("__typename"),
				})
				p.upstreamOperation.AddSelection(set.Ref, ast.Selection{
					Ref:  field.Ref,
					Kind: ast.SelectionKindField,
				})
			}
		}
	}
}

func (p *Planner) LeaveSelectionSet(ref int) {

	if p.insideJsonField {
		return
	}

	p.nodes = p.nodes[:len(p.nodes)-1]
}

func (p *Planner) EnterInlineFragment(ref int) {

	if p.insideJsonField {
		return
	}

	typeCondition := p.visitor.Operation.InlineFragmentTypeConditionName(ref)
	if typeCondition == nil {
		return
	}

	inlineFragment := p.upstreamOperation.AddInlineFragment(ast.InlineFragment{
		TypeCondition: ast.TypeCondition{
			Type: p.upstreamOperation.AddNamedType(typeCondition),
		},
	})

	selection := ast.Selection{
		Kind: ast.SelectionKindInlineFragment,
		Ref:  inlineFragment,
	}

	// add __typename field to selection set which contains typeCondition
	// so that the resolver can distinguish between the response types
	typeNameField := p.upstreamOperation.AddField(ast.Field{
		Name: p.upstreamOperation.Input.AppendInputBytes([]byte("__typename")),
	})
	p.upstreamOperation.AddSelection(p.nodes[len(p.nodes)-1].Ref, ast.Selection{
		Kind: ast.SelectionKindField,
		Ref:  typeNameField.Ref,
	})

	p.upstreamOperation.AddSelection(p.nodes[len(p.nodes)-1].Ref, selection)
	p.nodes = append(p.nodes, ast.Node{Kind: ast.NodeKindInlineFragment, Ref: inlineFragment})
}

func (p *Planner) LeaveInlineFragment(ref int) {

	if p.insideJsonField {
		return
	}

	if p.nodes[len(p.nodes)-1].Kind != ast.NodeKindInlineFragment {
		return
	}
	p.nodes = p.nodes[:len(p.nodes)-1]
}

func (p *Planner) EnterField(ref int) {

	if p.insideJsonField {
		return
	}

	fieldName := p.visitor.Operation.FieldNameString(ref)
	enclosingTypeName := p.visitor.Walker.EnclosingTypeDefinition.NameString(p.visitor.Definition)
	for i := range p.config.JsonTypeFields {
		if p.config.JsonTypeFields[i].TypeName == enclosingTypeName && p.config.JsonTypeFields[i].FieldName == fieldName {
			p.insideJsonField = true
			p.jsonFieldRef = ref
			p.addJsonField(ref)
			return
		}
	}

	if p.isQueryRawJSONField(ref) {
		p.isQueryRawRow = true
		p.insideJsonField = true
		p.jsonFieldRef = ref
	}

	if p.isQueryRawField(ref) {
		p.isQueryRaw = true
	}

	p.ensureEmptyParametersArgOnRawOperations(ref)

	// store root field name and ref
	if p.rootFieldName == "" {
		p.rootFieldName = fieldName
		p.rootFieldRef = ref
	}
	// store root type name
	if p.rootTypeName == "" {
		p.rootTypeName = p.visitor.Walker.EnclosingTypeDefinition.NameString(p.visitor.Definition)
	}

	p.lastFieldEnclosingTypeName = p.visitor.Walker.EnclosingTypeDefinition.NameString(p.visitor.Definition)

	p.addField(ref)

	upstreamFieldRef := p.nodes[len(p.nodes)-1].Ref
	typeName := p.lastFieldEnclosingTypeName

	fieldConfiguration := p.visitor.Config.Fields.ForTypeField(typeName, fieldName)
	if fieldConfiguration == nil {
		return
	}
	argumentsTypes := p.fieldDefinitionArgumentTypes(ref)
	if len(argumentsTypes) == 0 {
		return
	}
	for i := range fieldConfiguration.Arguments {
		argumentConfiguration := fieldConfiguration.Arguments[i]
		argumentType, ok := argumentsTypes[argumentConfiguration.Name]
		if !ok {
			continue
		}
		p.configureArgument(upstreamFieldRef, ref, argumentConfiguration, argumentType)
	}
}

// ensureEmptyParametersArgOnRawOperations adds an empty parameters arg to raw operations
// this is required because prisma won't execute without parameters, even if empty
// we don't want to force the user to define an empty parameters,
// so we "fix" it in the backend by modifying the AST
func (p *Planner) ensureEmptyParametersArgOnRawOperations(fieldRef int) {
	if !p.isQueryRawField(fieldRef) && !p.isQueryRawJSONField(fieldRef) && !p.isExecuteRawField(fieldRef) {
		return
	}
	_, exists := p.visitor.Operation.FieldArgument(fieldRef, []byte("parameters"))
	if exists {
		return
	}
	listRef := p.visitor.Operation.AddListValue(ast.ListValue{})
	argRef := p.visitor.Operation.AddArgument(ast.Argument{
		Name: p.visitor.Operation.Input.AppendInputString("parameters"),
		Value: ast.Value{
			Kind: ast.ValueKindList,
			Ref:  listRef,
		},
	})
	p.visitor.Operation.AddArgumentToField(fieldRef, argRef)
}

func (p *Planner) isQueryRawJSONField(field int) bool {
	name := p.visitor.Operation.FieldNameString(field)
	return name == "queryRawJSON" || strings.HasSuffix(name, "_queryRawJSON")
}

func (p *Planner) isQueryRawField(field int) bool {
	name := p.visitor.Operation.FieldNameString(field)
	return name == "queryRaw" || strings.HasSuffix(name, "_queryRaw")
}

func (p *Planner) isExecuteRawField(field int) bool {
	name := p.visitor.Operation.FieldNameString(field)
	return name == "executeRaw" || strings.HasSuffix(name, "_executeRaw")
}

func (p *Planner) addJsonField(ref int) {
	fieldName := p.visitor.Operation.FieldNameString(ref)
	nameOrAlias := p.visitor.Operation.FieldAliasOrNameString(ref)
	astField := ast.Field{
		Name: p.upstreamOperation.Input.AppendInputString(fieldName),
	}
	if nameOrAlias != fieldName && !p.disallowFieldAlias {
		astField.Alias = ast.Alias{
			IsDefined: true,
			Name:      p.upstreamOperation.Input.AppendInputString(nameOrAlias),
		}
	}
	field := p.upstreamOperation.AddField(astField)
	selection := ast.Selection{
		Kind: ast.SelectionKindField,
		Ref:  field.Ref,
	}
	p.upstreamOperation.AddSelection(p.nodes[len(p.nodes)-1].Ref, selection)
}

func (p *Planner) LeaveField(ref int) {
	if p.insideJsonField {
		if p.jsonFieldRef == ref {
			p.insideJsonField = false
			p.jsonFieldRef = 0
		}
		return
	}
	p.nodes = p.nodes[:len(p.nodes)-1]
}

func (p *Planner) EnterArgument(ref int) {
	if p.insideJsonField {
		return
	}
}

func (p *Planner) EnterDocument(operation, definition *ast.Document) {
	if p.upstreamOperation == nil {
		p.upstreamOperation = ast.NewDocument()
	} else {
		p.upstreamOperation.Reset()
	}
	p.nodes = p.nodes[:0]
	p.upstreamVariables = nil
	p.variables = p.variables[:0]
	p.disallowSingleFlight = false
	p.disallowFieldAlias = true
	p.isQueryRaw = false
	p.isQueryRawRow = false

	// reset information about root type
	p.rootTypeName = ""
	p.rootFieldName = ""
	p.rootFieldRef = -1
}

func (p *Planner) LeaveDocument(operation, definition *ast.Document) {

}

func (p *Planner) isNestedRequest() bool {
	for i := range p.nodes {
		if p.nodes[i].Kind == ast.NodeKindField {
			return false
		}
	}
	selectionSetAncestors := 0
	for i := range p.visitor.Walker.Ancestors {
		if p.visitor.Walker.Ancestors[i].Kind == ast.NodeKindSelectionSet {
			selectionSetAncestors++
			if selectionSetAncestors == 2 {
				return true
			}
		}
	}
	return false
}

func (p *Planner) configureArgument(upstreamFieldRef, downstreamFieldRef int, argumentConfiguration plan.ArgumentConfiguration, argumentType int) {
	switch argumentConfiguration.SourceType {
	case plan.FieldArgumentSource:
		p.configureFieldArgumentSource(upstreamFieldRef, downstreamFieldRef, argumentConfiguration.Name, argumentType)
	case plan.ObjectFieldSource:
		p.configureObjectFieldSource(upstreamFieldRef, argumentConfiguration, argumentType)
	}
}

func (p *Planner) configureFieldArgumentSource(upstreamFieldRef, downstreamFieldRef int, argumentName string, argumentType int) {
	fieldArgument, ok := p.visitor.Operation.FieldArgument(downstreamFieldRef, []byte(argumentName))
	if !ok {
		return
	}
	argumentValue := p.visitor.Operation.ArgumentValue(fieldArgument)
	if argumentValue.Kind != ast.ValueKindVariable {
		p.applyInlineFieldArgument(upstreamFieldRef, downstreamFieldRef, argumentValue, argumentName, argumentType)
		return
	}
	variableName := p.visitor.Operation.VariableValueNameBytes(argumentValue.Ref)
	variableNameStr := p.visitor.Operation.VariableValueNameString(argumentValue.Ref)

	variableDefinition, ok := p.visitor.Operation.VariableDefinitionByNameAndOperation(p.visitor.Walker.Ancestors[0].Ref, variableName)
	if !ok {
		return
	}

	variableDefinitionType := p.visitor.Operation.VariableDefinitions[variableDefinition].Type

	_, argRef := p.upstreamOperation.AddVariableValueArgument([]byte(argumentName), variableName) // add the argument to the field, but don't redefine it
	p.upstreamOperation.AddArgumentToField(upstreamFieldRef, argRef)

	variableTypeName := p.visitor.Operation.ResolveTypeNameString(variableDefinitionType)
	isJSON := false
	for i := range p.config.JsonInputVariables {
		if variableTypeName == p.config.JsonInputVariables[i] {
			isJSON = true
		}
	}

	p.inlinedVariables = append(p.inlinedVariables, inlinedVariable{
		name:    variableNameStr,
		typeRef: variableDefinitionType,
		isJSON:  isJSON,
		isRaw:   p.isRawArgument(downstreamFieldRef, argumentName),
	})
}

const nullableKey = "Nullable"

func (p *Planner) containsNullableKey(name string) bool {
	return strings.Contains(name, nullableKey)
}

func (p *Planner) applyInlineFieldArgument(upstreamField, downstreamField int, argumentValue ast.Value, argumentName string, argumentType int) {
	importedValue := p.visitor.Importer.ImportValue(argumentValue, p.visitor.Operation, p.upstreamOperation)
	arg := ast.Argument{
		Name:  p.upstreamOperation.Input.AppendInputString(argumentName),
		Value: importedValue,
	}
	isRaw := p.isRawArgument(downstreamField, argumentName)
	if isRaw && argumentValue.Kind == ast.ValueKindList {
		// prisma requires the "parameters" arg to be a JSON (string) instead of a list
		// to turn the list into a JSON, we print quotes before and after the list
		// additionally, we indicate via "isRaw" to "addVariableDefinitionsRecursively" that we're inside a JSON
		// this is because nested args need to be triple quoted, otherwise they'd close the parent JSON quotes
		arg.PrintBeforeValue = []byte(`"`)
		arg.PrintAfterValue = []byte(`"`)
	}
	argRef := p.upstreamOperation.AddArgument(arg)
	p.upstreamOperation.AddArgumentToField(upstreamField, argRef)
	p.addVariableDefinitionsRecursively(argumentValue, argumentName, argumentType, isRaw, false)
}

func (p *Planner) addVariableDefinitionsRecursively(value ast.Value, argumentName string, argumentType int, parentIsJson, parentNullable bool) {
	argumentTypeName := p.visitor.Definition.ResolveTypeNameString(argumentType)
	switch value.Kind {
	case ast.ValueKindObject:
		nextParentNullable := p.containsNullableKey(argumentTypeName)
		inputArgumentTypes := p.inputDefinitionArgumentTypes(argumentTypeName)
		for _, i := range p.visitor.Operation.ObjectValues[value.Ref].Refs {
			nextArgumentName := p.visitor.Operation.ObjectFieldNameString(i)
			nextArgumentType, ok := inputArgumentTypes[nextArgumentName]
			if !ok {
				continue
			}
			p.addVariableDefinitionsRecursively(p.visitor.Operation.ObjectFields[i].Value, nextArgumentName, nextArgumentType, parentIsJson, nextParentNullable)
		}
		return
	case ast.ValueKindList:
		for _, i := range p.visitor.Operation.ListValues[value.Ref].Refs {
			p.addVariableDefinitionsRecursively(p.visitor.Operation.Values[i], argumentName, argumentType, parentIsJson, parentNullable)
		}
		return
	case ast.ValueKindVariable:
		// continue after switch
	default:
		return
	}

	variableName := p.visitor.Operation.VariableValueNameBytes(value.Ref)
	variableNameStr := p.visitor.Operation.VariableValueNameString(value.Ref)
	variableDefinition, exists := p.visitor.Operation.VariableDefinitionByNameAndOperation(p.visitor.Walker.Ancestors[0].Ref, variableName)
	if !exists {
		return
	}

	variableDefinitionType := p.visitor.Operation.VariableDefinitions[variableDefinition].Type

	typeName := p.visitor.Operation.ResolveTypeNameString(variableDefinitionType)
	isJSON := false
	for i := range p.config.JsonInputVariables {
		if typeName == p.config.JsonInputVariables[i] {
			isJSON = true
		}
	}

	p.inlinedVariables = append(p.inlinedVariables, inlinedVariable{
		name:         variableNameStr,
		typeRef:      variableDefinitionType,
		isJSON:       isJSON,
		isRaw:        parentIsJson,
		parentIsJson: parentIsJson,
		nullable:     parentNullable || p.containsNullableKey(argumentTypeName),
	})
}

// isRawArgument searches for a queryRaw/executeRaw field and the parameters arg
// which needs to be encoded as a JSON (string) so that prisma understands it
func (p *Planner) isRawArgument(fieldRef int, argumentName string) bool {
	if p.isQueryRawJSONField(fieldRef) || p.isQueryRawField(fieldRef) || p.isExecuteRawField(fieldRef) {
		return argumentName == "parameters"
	}
	return false
}

func (p *Planner) fieldDefinitionArgumentTypes(fieldRef int) map[string]int {
	fieldNameBytes := p.visitor.Operation.FieldNameBytes(fieldRef)
	var fieldArgumentRefs []int
	if p.rootFieldRef == fieldRef {
		var typeName ast.ByteSlice
		switch "OperationType" + p.rootTypeName {
		case ast.OperationTypeQuery.String():
			typeName = p.visitor.Definition.Index.QueryTypeName
		case ast.OperationTypeMutation.String():
			typeName = p.visitor.Definition.Index.MutationTypeName
		case ast.OperationTypeSubscription.String():
			typeName = p.visitor.Definition.Index.SubscriptionTypeName
		default:
			return nil
		}
		typeDefinition, exists := p.visitor.Definition.Index.FirstNodeByNameBytes(typeName)
		if !exists {
			return nil
		}

		fieldDefinition, exists := p.visitor.Definition.NodeFieldDefinitionByName(typeDefinition, fieldNameBytes)
		if !exists {
			return nil
		}
		fieldArgumentRefs = p.visitor.Definition.FieldDefinitionArgumentsDefinitions(fieldDefinition)
	} else {
		for _, objectTypeDef := range p.visitor.Definition.ObjectTypeDefinitions {
			if bytes.Equal(p.visitor.Definition.Input.ByteSlice(objectTypeDef.Name), []byte(p.lastFieldEnclosingTypeName)) {
				for _, fieldDefRef := range objectTypeDef.FieldsDefinition.Refs {
					fieldDef := p.visitor.Definition.FieldDefinitions[fieldDefRef]
					if bytes.Equal(p.visitor.Definition.Input.ByteSlice(fieldDef.Name), fieldNameBytes) {
						fieldArgumentRefs = fieldDef.ArgumentsDefinition.Refs
					}
				}
				break
			}
		}
	}
	if len(fieldArgumentRefs) == 0 {
		return nil
	}

	fieldArgumentTypes := make(map[string]int, len(fieldArgumentRefs))
	for _, i := range fieldArgumentRefs {
		argument := p.visitor.Definition.InputValueDefinitions[i]
		argumentName := p.visitor.Definition.Input.ByteSliceString(argument.Name)
		fieldArgumentTypes[argumentName] = argument.Type
	}
	return fieldArgumentTypes
}

func (p *Planner) inputDefinitionArgumentTypes(argumentTypeName string) map[string]int {
	var inputArgumentTypes map[string]int
	for _, inputObjectTypeDef := range p.visitor.Definition.InputObjectTypeDefinitions {
		if p.visitor.Definition.Input.ByteSliceString(inputObjectTypeDef.Name) == argumentTypeName {
			inputArgumentTypes = make(map[string]int, len(inputObjectTypeDef.InputFieldsDefinition.Refs))
			for _, inputRef := range inputObjectTypeDef.InputFieldsDefinition.Refs {
				inputDef := p.visitor.Definition.InputValueDefinitions[inputRef]
				inputArgumentTypes[p.visitor.Definition.Input.ByteSliceString(inputDef.Name)] = inputDef.Type
			}
			break
		}
	}
	return inputArgumentTypes
}

func (p *Planner) configureObjectFieldSource(upstreamFieldRef int, argumentConfiguration plan.ArgumentConfiguration, argumentType int) {
	if len(argumentConfiguration.SourcePath) < 1 || argumentType == -1 {
		return
	}

	variableName := p.upstreamOperation.GenerateUnusedVariableDefinitionName(p.nodes[0].Ref)
	variableValue, argument := p.upstreamOperation.AddVariableValueArgument([]byte(argumentConfiguration.Name), variableName)
	p.upstreamOperation.AddArgumentToField(upstreamFieldRef, argument)
	importedType := p.visitor.Importer.ImportType(argumentType, p.visitor.Definition, p.upstreamOperation)
	p.upstreamOperation.AddVariableDefinitionToOperationDefinition(p.nodes[0].Ref, variableValue, importedType)

	renderer, err := resolve.NewGraphQLVariableRendererFromTypeRef(p.visitor.Operation, p.visitor.Definition, argumentType)
	if err != nil {
		return
	}

	variable := &resolve.ObjectVariable{
		Path:     argumentConfiguration.SourcePath,
		Renderer: renderer,
	}

	objectVariableName, exists := p.variables.AddVariable(variable)
	if !exists {
		p.upstreamVariables, _ = sjson.SetRawBytes(p.upstreamVariables, string(variableName), []byte(objectVariableName))
	}
}

// printOperation - prints normalized upstream operation
func (p *Planner) printOperation() []byte {

	buf := &bytes.Buffer{}

	if p.isQueryRaw || p.isQueryRawRow {
		// we've added rawQuery and rawQueryRow to the Query type for better ergonomics,
		// but prisma expects them to be on the Mutation type
		// so we simply rewrite the AST if we have a rawQuery root field
		p.upstreamOperation.OperationDefinitions[p.operationTypeDefinitionRef].OperationType = ast.OperationTypeMutation
	}

	err := astprinter.Print(p.upstreamOperation, nil, buf)
	if err != nil {
		p.stopWithError("printOperation: printing operation failed")
		return nil
	}

	return buf.Bytes()
}

func (p *Planner) stopWithError(msg string, args ...interface{}) {
	p.visitor.Walker.StopWithInternalErr(fmt.Errorf(msg, args...))
}

/*
replaceQueryType - sets definition query type to a current root type.
Helps to do a normalization of the upstream query for a nested datasource.
Skips replace when:
1. datasource is not nested;
2. federation is enabled;
3. query type contains an operation field;

Example transformation:
Original schema definition:

	type Query {
		serviceOne(serviceOneArg: String): ServiceOneResponse
		serviceTwo(serviceTwoArg: Boolean): ServiceTwoResponse
	}

	type ServiceOneResponse {
		fieldOne: String!
		countries: [Country!]! # nested datasource without explicit field path
	}

	type ServiceTwoResponse {
		fieldTwo: String
		serviceOneField: String
		serviceOneResponse: ServiceOneResponse # nested datasource with implicit field path "serviceOne"
	}

	type Country {
		name: String!
	}

`serviceOneResponse` field of a `ServiceTwoResponse` is nested but has a field path that exists on the Query type
- In this case definition will not be modified

`countries` field of a `ServiceOneResponse` is nested and not present on the Query type
- In this case query type of definition will be replaced with a `ServiceOneResponse`

Modified schema definition:

	schema {
	   query: ServiceOneResponse
	}

	type ServiceOneResponse {
	   fieldOne: String!
	   countries: [Country!]!
	}

	type ServiceTwoResponse {
	   fieldTwo: String
	   serviceOneField: String
	   serviceOneResponse: ServiceOneResponse
	}

	type Country {
	   name: String!
	}

Refer to pkg/engine/datasource/graphql_datasource/graphql_datasource_test.go:632
Case name: TestGraphQLDataSource/nested_graphql_engines

If we didn't do this transformation, the normalization would fail because it's not possible
to traverse the AST as there's a mismatch between the upstream Operation and the schema.

If the nested Query can be rewritten so that it's a valid Query against the existing schema, fine.
However, when rewriting the nested Query onto the schema's Query type,
it might be the case that no FieldDefinition exists for the rewritten root field.
In that case, we transform the schema so that normalization and printing of the upstream Query succeeds.
*/
func (p *Planner) replaceQueryType(definition *ast.Document) {
	if !p.isNested {
		return
	}

	queryTypeName := definition.Index.QueryTypeName
	queryNode, exists := definition.Index.FirstNodeByNameBytes(queryTypeName)
	if !exists || queryNode.Kind != ast.NodeKindObjectTypeDefinition {
		return
	}

	// check that query type has rootFieldName within its fields
	hasField := definition.FieldDefinitionsContainField(definition.ObjectTypeDefinitions[queryNode.Ref].FieldsDefinition.Refs, []byte(p.rootFieldName))
	if hasField {
		return
	}

	definition.RemoveObjectTypeDefinition(definition.Index.QueryTypeName)
	definition.ReplaceRootOperationTypeDefinition(p.rootTypeName, ast.OperationTypeQuery)
}

// normalizeOperation - normalizes operation against definition.
func (p *Planner) normalizeOperation(operation, definition *ast.Document, report *operationreport.Report) (ok bool) {

	report.Reset()
	normalizer := astnormalization.NewWithOpts(
		astnormalization.WithExtractVariables(),
		astnormalization.WithRemoveFragmentDefinitions(),
		astnormalization.WithRemoveUnusedVariables(),
	)
	normalizer.NormalizeOperation(operation, definition, report)

	return !report.HasErrors()
}

// addField - add a field to an upstream operation
func (p *Planner) addField(ref int) {

	/*alias := ast.Alias{IsDefined: p.visitor.Operation.FieldAliasIsDefined(ref)}
	if alias.IsDefined {
		aliasBytes := p.visitor.Operation.FieldAliasBytes(ref)
		alias.Name = p.upstreamOperation.Input.AppendInputBytes(aliasBytes)
	}*/

	fieldName := p.visitor.Operation.FieldNameString(ref)
	typeName := p.visitor.Walker.EnclosingTypeDefinition.NameString(p.visitor.Definition)
	if fieldConfig := p.visitor.Config.Fields.ForTypeField(typeName, fieldName); fieldConfig != nil && len(fieldConfig.Path) == 1 {
		/*if fieldConfig.Path[0] != fieldName && !alias.IsDefined {
			alias.IsDefined = true
			aliasBytes := p.visitor.Operation.FieldNameBytes(ref)
			alias.Name = p.upstreamOperation.Input.AppendInputBytes(aliasBytes)
		}*/

		// override fieldName with mapping path value
		fieldName = fieldConfig.Path[0]

		// when provided field is a root type field save new field name
		if ref == p.rootFieldRef {
			p.rootFieldName = fieldName
		}
	}

	if p.isQueryRawJSONField(ref) {
		// queryRawRow doesn't exist in the prisma schema, only queryRaw
		// so we rewrite it
		fieldName = strings.Replace(fieldName, "queryRawJSON", "queryRaw", 1)
	}

	astField := ast.Field{
		Name: p.upstreamOperation.Input.AppendInputString(fieldName),
	}

	downstreamFieldName := p.visitor.Operation.FieldAliasOrNameString(ref)
	if downstreamFieldName != fieldName && !p.disallowFieldAlias {
		astField.Alias = ast.Alias{
			IsDefined: true,
			Name:      p.upstreamOperation.Input.AppendInputString(downstreamFieldName),
		}
	}

	field := p.upstreamOperation.AddField(astField)
	selection := ast.Selection{
		Kind: ast.SelectionKindField,
		Ref:  field.Ref,
	}

	p.upstreamOperation.AddSelection(p.nodes[len(p.nodes)-1].Ref, selection)
	p.nodes = append(p.nodes, field)
}

type Factory struct {
	Client          *http.Client
	engineFactory   LazyEngineFactory
	Debug           bool
	Log             *zap.Logger
	testsSkipEngine bool
}

func (f *Factory) WithHTTPClient(client *http.Client) *Factory {
	return &Factory{
		Client:        client,
		engineFactory: f.engineFactory,
		Debug:         f.Debug,
		Log:           f.Log,
	}
}

func (f *Factory) Planner(ctx context.Context) plan.DataSourcePlanner {
	f.engineFactory.closer = ctx.Done()
	return &Planner{
		client:          f.Client,
		engineFactory:   &f.engineFactory,
		debug:           f.Debug,
		log:             f.Log,
		testsSkipEngine: f.testsSkipEngine,
	}
}

type LazyEngineFactory struct {
	closer  <-chan struct{}
	engines map[string]*LazyEngine
}

func (f *LazyEngineFactory) Engine(prismaSchema, wundergraphDir string, closeTimeoutSeconds int32, environmentVariable string) *LazyEngine {
	if f.engines == nil {
		f.engines = map[string]*LazyEngine{}
	}
	engine, exists := f.engines[prismaSchema]
	if exists {
		return engine
	}
	engine = newLazyEngine(prismaSchema, wundergraphDir, closeTimeoutSeconds, environmentVariable)
	go engine.start(f.closer)
	runtime.SetFinalizer(engine, finalizeEngine)
	f.engines[prismaSchema] = engine
	return engine
}

func finalizeEngine(e *LazyEngine) {
	if e.engine != nil {
		panic("engine must be closed and set to nil before handing it over to gc")
	}
}

type LazyEngine struct {
	mutex *sync.Mutex

	environmentVariable string
	prismaSchema        string
	wundergraphDir      string
	closeTimeoutSeconds int32

	engine          HybridEngine
	engineCloser    *time.Timer
	engineExecuting int
	client          *http.Client
	closed          bool
}

func newLazyEngine(prismaSchema string, wundergraphDir string, closeTimeoutSeconds int32, environmentVariable string) *LazyEngine {
	return &LazyEngine{
		mutex:               &sync.Mutex{},
		environmentVariable: environmentVariable,
		prismaSchema:        prismaSchema,
		wundergraphDir:      wundergraphDir,
		closeTimeoutSeconds: closeTimeoutSeconds,
	}
}

func (e *LazyEngine) start(closer <-chan struct{}) {
	<-closer
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.engine != nil {
		e.engine.Close()
		e.engine = nil
	}
	e.closed = true
	return
}

func (e *LazyEngine) close() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	// 仅当引擎存在且定时关闭任务未被取消时关闭引擎
	if engine := e.engine; engine != nil && e.engineCloser != nil {
		e.engine, e.engineCloser = nil, nil
		go engine.Close()
	}
}

func (e *LazyEngine) Execute(ctx context.Context, request []byte, out io.Writer, extendCancelFunc func()) (err error) {
	if err = e.initEngine(ctx); err != nil {
		return
	}

	extendCancelFunc()
	err = e.engine.Execute(ctx, request, out)
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.engineExecuting--; e.engineExecuting == 0 {
		// 引擎顺利执行完成后设置定时关闭任务
		e.engineCloser = time.AfterFunc(time.Duration(e.closeTimeoutSeconds)*time.Second, e.close)
	}
	return err
}

func (e *LazyEngine) initEngine(ctx context.Context) (err error) {
	e.mutex.Lock()
	defer func() {
		if err == nil {
			e.engineExecuting++
		} else if engine := e.engine; engine != nil {
			// [初始化/健康检查]失败关闭引擎，防止后续被错误引用
			e.engine = nil
			go engine.Close()
		}
		e.mutex.Unlock()
	}()
	if e.closed {
		err = errors.New("engine closed")
		return
	}
	// 如果定时关闭任务存在则取消并置空
	if e.engineCloser != nil {
		e.engineCloser.Stop()
		e.engineCloser = nil
	}
	if e.engine != nil {
		err = e.engine.HealthCheck()
		return
	}

	if e.engine, err = NewHybridEngine(e.client, e.prismaSchema, e.wundergraphDir, zap.NewNop(), e.environmentVariable); err != nil {
		return
	}
	if err = e.engine.WaitUntilReady(ctx); err != nil {
		return
	}
	return
}

type Source struct {
	engine         *LazyEngine
	debug          bool
	log            *zap.Logger
	datasourceName string
	rootTypeName   string
	rootFieldName  string
}

func (s *Source) Load(ctx context.Context, input []byte, w io.Writer) (err error) {
	ctx, followCtx, reportError, err := s.fetchTransactionCtx(ctx)
	if err != nil {
		return
	}

	var spanFuncs []func(opentracing.Span)
	ctx, callback := logging.StartTraceContext(ctx, followCtx, s.spanOperationName(), s.spanWithDatasource(), logging.SpanWithLogOriginInput(input))
	defer func() {
		reportError(err)
		callback(append(spanFuncs, logging.SpanWithLogError(err))...)
	}()

	request := input
	if unrenderVariables, ok := resolve.GetUnrenderVariables(ctx, input); ok {
		if request, err = clearUnrenderVariables(ctx, unrenderVariables, request); err != nil {
			return
		}
	}
	request, _ = jsonparser.Set(request, []byte("{}"), "variables")
	request = bytes.ReplaceAll(request, []byte(`\\\\\"`), []byte(`\\\"`))
	request = s.ensureMutationPrefix(request)
	spanFuncs = append(spanFuncs, logging.SpanWithLogInput(request))

	buf, err := s.execute(ctx, request, time.Second*5)
	defer pool.PutBytesBuffer(buf)
	if err != nil {
		return
	}

	var resultErr ErrResponse
	_ = json.Unmarshal(buf.Bytes(), &resultErr)
	if len(resultErr.Errors) > 0 {
		for _, errItem := range resultErr.Errors {
			if errItem.Message == "" {
				errItem.Message = errItem.Error
				errItem.Error = ""
			}
		}
		buf.Reset()
		errBytes, _ := json.Marshal(resultErr)
		buf.Write(errBytes)
		reportError(errors.New(string(errBytes)))
		spanFuncs = append(spanFuncs, func(span opentracing.Span) { ext.Error.Set(span, true) })
	}

	spanFuncs = append(spanFuncs, logging.SpanWithLogOutput(buf.Bytes()))
	_, err = buf.WriteTo(w)
	return
}

func (s *Source) execute(ctx context.Context, request []byte, timeout time.Duration) (buf *bytes.Buffer, err error) {
	var (
		retryTimes int
		stderr     stderrMsg
	)
	buf = pool.GetBytesBuffer()
	ctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(timeout, cancel)
	extendCancelFunc := func() { timer.Reset(timeout) }
	for {
		if retryTimes == 0 {
			s.logExecuteIfDebug("Start", zap.ByteString("request", request))
		} else {
			s.logExecuteIfDebug("Retry", zap.ByteString("request", request), zap.Int("times", retryTimes))
		}
		if err = s.engine.Execute(ctx, request, buf, extendCancelFunc); err == nil {
			s.logExecuteIfDebug("Succeed", zap.String("response", buf.String()))
			return
		}

		s.logExecuteIfDebug("Error", zap.Error(err))
		if errors.Is(err, &stderr) || ctx.Err() != nil || retryTimes >= 5 {
			return
		}

		err = nil
		buf.Reset()
		retryTimes++
		time.Sleep(time.Millisecond * 500)
	}
}

func (s *Source) logExecuteIfDebug(suffix string, fields ...zap.Field) {
	if s.debug {
		s.log.Debug("database.Source.Execute."+suffix, fields...)
	}
}
