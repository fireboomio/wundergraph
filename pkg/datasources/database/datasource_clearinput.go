package database

import (
	"bytes"
	"context"
	"fmt"
	"github.com/buger/jsonparser"
	json "github.com/json-iterator/go"
	"github.com/tidwall/sjson"
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/astprinter"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"github.com/wundergraph/wundergraph/internal/unsafebytes"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"golang.org/x/exp/slices"
	"io"
	"regexp"
	"strings"
)

const (
	charLBRACE   = '{'
	charRBRACE   = '}'
	charLBRACK   = '['
	charRBRACK   = ']'
	charLPAREN   = '('
	charRPAREN   = ')'
	charCOMMA    = ','
	charSPACE    = ' '
	charsKeyword = "({,"
)

var (
	rawJsonVariableEmptyValue = []byte{charLBRACK, charRBRACK}
	nullBytesLength           = len(literal.NULL)
)

func getKeywordStartIndex(data []byte, jointIndexes []int) int {
	startIndex := bytes.LastIndexAny(data, charsKeyword)
	if data[startIndex] != charCOMMA {
		startIndex++
	}
	if jointLength := len(jointIndexes); jointLength > 1 {
		if lastJointIndex := jointIndexes[jointLength-1]; startIndex < lastJointIndex {
			startIndex = lastJointIndex
		}
	}
	return startIndex
}

func getEndIndexOffset(data []byte, startChar byte, endIndex int) (offset int) {
	if startChar == charCOMMA {
		return
	}

	if data[endIndex] == charCOMMA {
		offset++
	}
	if data[endIndex+1] == charSPACE {
		offset++
	}
	return
}

func clearUnrenderVariables(ctx context.Context, unrenderVariables []resolve.UnrenderVariable, input []byte) ([]byte, error) {
	var inputOffset int
	jointIndexes := make([]int, 0, len(unrenderVariables)*2+2)
	clearedScope := make([]string, 0, len(unrenderVariables))
	for _, variable := range unrenderVariables {
		if variable.ValueType == jsonparser.Null {
			if variable.Nullable {
				continue
			}

			return nil, fmt.Errorf("null value is not allowed for variable [%s]", variable.Name)
		}

		variableIndex := variable.ValueIndex + inputOffset
		if renderer, ok := variable.Renderer.(*RawJsonVariableRenderer); ok {
			rendererBuf := pool.GetBytesBuffer()
			rendererBuf.Write(input[:variableIndex])
			if err := renderer.RenderVariable(ctx, rawJsonVariableEmptyValue, rendererBuf); err != nil {
				return nil, err
			}
			rendererBuf.Write(input[variableIndex+nullBytesLength:])
			inputOffset += rendererBuf.Len() - len(input)
			input = make([]byte, rendererBuf.Len())
			copy(input, rendererBuf.Bytes())
			pool.PutBytesBuffer(rendererBuf)
			continue
		}

		startIndex, endIndex := variableIndex, variableIndex+nullBytesLength
		if input[variableIndex-1] != charLBRACK {
			startIndex = getKeywordStartIndex(input[:variableIndex], jointIndexes)
		}
		if input[variableIndex-1] == charLBRACK && input[variableIndex+nullBytesLength] == charRBRACK {
			endIndex++
		} else {
			endIndex += getEndIndexOffset(input, input[startIndex], endIndex)
		}
		if len(jointIndexes) == 0 {
			jointIndexes = append(jointIndexes, 0)
		}
		jointIndexes = append(jointIndexes, startIndex, endIndex)
		clearedScope = append(clearedScope, fmt.Sprintf("%d-%d", startIndex, endIndex))
	}
	if len(jointIndexes) == 0 {
		return input, nil
	}

	jointIndexes = append(jointIndexes, len(input))
	return clearInputWithScope(input, jointIndexes, clearedScope), nil
}

func clearInputWithScope(input []byte, jointIndexes []int, clearedScope []string) (clearedInput []byte) {
	clearedBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(clearedBuf)

	var nextJointIndexes []int
	var nextClearedScope []string
	for i := 0; i < len(jointIndexes)-1; i++ {
		start, end := jointIndexes[i], jointIndexes[i+1]
		if start == end || slices.Contains(clearedScope, fmt.Sprintf("%d-%d", start, end)) {
			continue
		}

		clearedLength, clearedBytes := clearedBuf.Len(), clearedBuf.Bytes()
		clearedBuf.Write(input[start:end])
		braceCleared := input[start] == charRBRACE && clearedBytes[clearedLength-1] == charLBRACE
		brackCleared := input[start] == charRBRACK && clearedBytes[clearedLength-1] == charLBRACK
		parenCleared := input[start] == charRPAREN && clearedBytes[clearedLength-1] == charLPAREN
		if !braceCleared && !brackCleared && !parenCleared {
			continue
		}

		var (
			nextClearStart int
			nextClearEnd   = clearedLength + 1
		)
		if parenCleared {
			nextClearStart = clearedLength - 1
		} else {
			if braceCleared && clearedBytes[clearedLength-2] == charLBRACK {
				nextClearStart = clearedLength - 1
				if input[start+1] == charRBRACK {
					start++
					nextClearEnd++
				}
			} else {
				nextClearStart = getKeywordStartIndex(clearedBytes[:clearedLength-1], nextJointIndexes)
			}
			if clearedBuf.Len() > nextClearEnd {
				nextClearEnd += getEndIndexOffset(input, clearedBytes[nextClearStart], start+1)
			}
		}
		if len(nextJointIndexes) == 0 {
			nextJointIndexes = append(nextJointIndexes, 0)
		}
		nextJointIndexes = append(nextJointIndexes, nextClearStart, nextClearEnd)
		nextClearedScope = append(nextClearedScope, fmt.Sprintf("%d-%d", nextClearStart, nextClearEnd))
	}
	clearedInput = clearedBuf.Bytes()
	if len(nextJointIndexes) == 0 {
		return
	}

	nextJointIndexes = append(nextJointIndexes, clearedBuf.Len())
	return clearInputWithScope(clearedInput, nextJointIndexes, nextClearedScope)
}

func (p *Planner) resetRawInput(_ *resolve.Context, skipFieldJsonPaths map[string]bool) (clearedInput string) {
	document := *p.upstreamOperation
	document.SelectionSets = make([]ast.SelectionSet, len(document.SelectionSets))
	copy(document.SelectionSets, p.upstreamOperation.SelectionSets)
	clearHelper := &clearDocumentHelper{
		document:              &document,
		skipFieldJsonPaths:    skipFieldJsonPaths,
		clearedFieldJsonPaths: make(map[string]bool, len(skipFieldJsonPaths)),
	}
	for _, operation := range document.OperationDefinitions {
		if operation.HasSelections {
			clearHelper.clearSelectionRefs(operation.SelectionSet)
		}
	}
	clearedQuery, err := astprinter.PrintString(&document, nil)
	if err != nil {
		return
	}

	for i, variable := range p.inlinedVariables {
		if !slices.Contains(clearHelper.skipVariableRefs, i) {
			clearedQuery = variable.replaceFunc(clearedQuery)
		}
	}
	rawInput, err := json.Marshal(fetchInput{Query: clearedQuery, Variables: p.upstreamVariables})
	if err != nil {
		return
	}
	clearedInput = string(rawInput)
	return
}

type clearDocumentHelper struct {
	document              *ast.Document
	clearedFieldJsonPaths map[string]bool
	skipFieldJsonPaths    map[string]bool
	skipVariableRefs      []int
}

func (c *clearDocumentHelper) clearSelectionRefs(selectionSet int, parent ...string) (clearOvered, clearedAll bool) {
	var (
		itemClearedAll bool
		itemCleared    bool
		itemWalkIndex  int
	)
	parentLength := len(parent)
	originSelectionRefs := c.document.SelectionSets[selectionSet].SelectionRefs
	savedSelectionRefs := make([]int, 0, len(originSelectionRefs))
	for i, selectionRef := range originSelectionRefs {
		if clearOvered {
			break
		}
		itemWalkIndex = i
		selection := c.document.Selections[selectionRef]
		if selection.Kind != ast.SelectionKindField {
			savedSelectionRefs = append(savedSelectionRefs, selectionRef)
			continue
		}

		fieldJsonPath := make([]string, parentLength+1)
		copy(fieldJsonPath, parent)
		fieldJsonPath[parentLength] = c.document.FieldNameString(selection.Ref)
		fieldJsonPathStr := strings.Join(fieldJsonPath, ".")
		if _, itemCleared = c.skipFieldJsonPaths[fieldJsonPathStr]; itemCleared {
			c.clearedFieldJsonPaths[fieldJsonPathStr] = true
			clearOvered = len(c.skipFieldJsonPaths) == len(c.clearedFieldJsonPaths)
		}
		if field := c.document.Fields[selection.Ref]; field.HasSelections {
			clearOvered, itemClearedAll = c.clearSelectionRefs(field.SelectionSet, fieldJsonPath...)
		}
		if itemCleared || itemClearedAll {
			for _, argumentRef := range c.document.FieldArguments(selection.Ref) {
				c.skipVariableRefs = append(c.skipVariableRefs, c.document.SearchVariableRefs(c.document.ArgumentValue(argumentRef))...)
			}
			continue
		}
		savedSelectionRefs = append(savedSelectionRefs, selectionRef)
	}
	savedSelectionRefs = append(savedSelectionRefs, originSelectionRefs[itemWalkIndex+1:]...)
	if !slices.Equal(originSelectionRefs, savedSelectionRefs) {
		c.document.SelectionSets[selectionSet].SelectionRefs = savedSelectionRefs
		clearedAll = len(savedSelectionRefs) == 0
	}
	return
}

type OptionalQueryRenderer struct{}

func (r *OptionalQueryRenderer) GetKind() string {
	return "optional_query"
}

func (r *OptionalQueryRenderer) RenderVariable(_ context.Context, data []byte, out io.Writer) error {
	_, _ = out.Write(literal.BACKSLASH)
	_, _ = out.Write(literal.QUOTE)
	for i := range data {
		switch data[i] {
		case '"':
			_, _ = out.Write(literal.BACKSLASH)
			_, _ = out.Write(literal.BACKSLASH)
			_, _ = out.Write(literal.QUOTE)
		default:
			_, _ = out.Write(data[i : i+1])
		}
	}
	_, _ = out.Write(literal.BACKSLASH)
	_, _ = out.Write(literal.QUOTE)
	return nil
}

var (
	stringArraySplit              = []byte(`","`)
	dollarTemplateRegexp          = regexp.MustCompile(`\${\w+}`)
	dollarParameterRegexp         = regexp.MustCompile(`\$\w+\b`)
	optionalParameterReplaceFuncs = map[wgpb.DataSourceKind]optionalParameterReplaceFunc{
		wgpb.DataSourceKind_SQLITE:     func(int, []byte) []byte { return []byte(`?`) },
		wgpb.DataSourceKind_MYSQL:      func(int, []byte) []byte { return []byte(`?`) },
		wgpb.DataSourceKind_POSTGRESQL: func(i int, _ []byte) []byte { return []byte(fmt.Sprintf(`$%d`, i+1)) },
	}
)

type optionalParameterReplaceFunc func(int, []byte) []byte

func SupportOptionalRaw(kind wgpb.DataSourceKind) bool {
	_, ok := optionalParameterReplaceFuncs[kind]
	return ok
}

func (p *Planner) isOptionalRawField(fieldRef int) bool {
	name := p.visitor.Operation.FieldNameString(fieldRef)
	return name == "optional_queryRaw" || strings.HasSuffix(name, "_optional_queryRaw") ||
		name == "optional_executeRaw" || strings.HasSuffix(name, "_optional_executeRaw")
}

func (p *Planner) addOptionalParameters(upstreamFieldRef int) {
	variableNameBytes := p.visitor.Operation.GenerateUnusedVariableDefinitionName(p.nodes[0].Ref)
	p.optionalParametersKey = unsafebytes.BytesToString(variableNameBytes)
	p.visitor.Operation.Input.Variables, _ = sjson.SetRawBytes(
		p.visitor.Operation.Input.Variables,
		p.optionalParametersKey, literal.ZeroArrayValue)
	variableValue, argRef := p.upstreamOperation.AddVariableValueArgument([]byte("parameters"), variableNameBytes) // add the argument to the field, but don't redefine it
	p.upstreamOperation.VariableValues[variableValue].Generated = true
	p.upstreamOperation.AddArgumentToField(upstreamFieldRef, argRef)
	p.inlinedVariables = append(p.inlinedVariables, inlinedVariable{
		generated: true,
		name:      p.optionalParametersKey,
		isRaw:     true,
	})
}

func (p *Planner) rewriteVariable(ctx *resolve.Context, key string, value []byte, valueType jsonparser.ValueType) ([]byte, error) {
	if !p.isOptionalRaw || len(p.optionalParametersKey) == 0 || valueType != jsonparser.Array {
		return value, nil
	}
	parameterReplaceFunc, ok := optionalParameterReplaceFuncs[p.getRealDatasourceKind()]
	if !ok {
		return value, nil
	}
	if p.optionalParametersKey == key {
		if v, _ok := p.optionalParameters.LoadAndDelete(ctx); _ok {
			return v.([]byte), nil
		}
		return value, nil
	}

	sqlArrayCount := bytes.Count(value, stringArraySplit) + 1
	var (
		savedParameterIndex int
		itemParameterBytes  = make([][]byte, 0, 8)
		savedParameterBytes = make([][]byte, 0, sqlArrayCount*2)
		savedSqlBytes       = make([][]byte, 0, sqlArrayCount)
	)
	_, _ = jsonparser.ArrayEach(value, func(sqlBytes []byte, _ jsonparser.ValueType, _ int, _ error) {
		itemParameterBytes = itemParameterBytes[:0]
		itemParameterIndex := savedParameterIndex
		itemSqlBytesRequired := true
		sqlBytes = dollarParameterRegexp.ReplaceAllFunc(sqlBytes, func(matchBytes []byte) []byte {
			if !itemSqlBytesRequired {
				return nil
			}
			paramKey := matchBytes[1:]
			paramValue, paramValueType, paramOffset, _ := jsonparser.Get(ctx.Variables, string(paramKey))
			if paramValueType == jsonparser.NotExist {
				itemSqlBytesRequired = false
				return nil
			}
			if paramValueType == jsonparser.String {
				paramValue = ctx.Variables[paramOffset-len(paramValue)-2 : paramOffset]
			}
			itemParameterBytes = append(itemParameterBytes, paramValue)
			modifiedBytes := parameterReplaceFunc(itemParameterIndex, paramKey)
			itemParameterIndex++
			return modifiedBytes
		})
		if itemSqlBytesRequired {
			savedSqlBytes = append(savedSqlBytes, sqlBytes)
			savedParameterIndex = itemParameterIndex
			savedParameterBytes = append(savedParameterBytes, itemParameterBytes...)
		}
	})

	finalSqlBytes := bytes.Join(savedSqlBytes, literal.SPACE)
	finalSqlBytes = dollarTemplateRegexp.ReplaceAllFunc(finalSqlBytes, func(matchBytes []byte) []byte {
		templateValue, _, _, _err := jsonparser.Get(ctx.Variables, string(matchBytes[2:len(matchBytes)-1]))
		if _err != nil {
			return matchBytes
		}
		return templateValue
	})
	finalParameterBytes := makeArrayBytes(savedParameterBytes)
	p.optionalParameters.Store(ctx, finalParameterBytes)
	return finalSqlBytes, nil
}

func (p *Planner) getRealDatasourceKind() wgpb.DataSourceKind {
	switch kind := p.config.DatasourceKind; kind {
	case wgpb.DataSourceKind_PRISMA:
		return p.config.DatasourceKindForPrisma
	default:
		return kind
	}
}

func makeArrayBytes(savedBytes [][]byte) []byte {
	finalBytes := bytes.Join(savedBytes, literal.COMMA)
	finalResult := make([]byte, len(finalBytes)+2)
	copy(finalResult[1:], finalBytes)
	finalResult[0], finalResult[len(finalBytes)+1] = charLBRACK, charRBRACK
	return finalResult
}
