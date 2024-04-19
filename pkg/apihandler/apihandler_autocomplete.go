package apihandler

import (
	"bytes"
	"context"
	"errors"
	"github.com/spf13/cast"
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/astprinter"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"strings"
)

var buildFieldDirectiveFuncs []func(string, string) (string, []DirectiveArgument)

func AddBuildFieldDirectiveFunc(buildDirectiveFunc func(string, string) (string, []DirectiveArgument)) {
	buildFieldDirectiveFuncs = append(buildFieldDirectiveFuncs, buildDirectiveFunc)
}

type (
	graphqlAutoComplete struct {
		doc                    *ast.Document
		recoverFuncs           []func()
		queryRawExpected       int
		executeRawExpected     int
		queryRawFieldIndexes   map[int]bool
		executeRawFieldIndexes map[int]bool
		queryRawTypes          map[string]*resolve.QueryRawType
		executeRawTypes        map[string]*resolve.ExecuteRawType
	}
	DirectiveArgument struct {
		Name      string
		Value     string
		ValueKind ast.ValueKind
	}
)

func newGraphqlAutoComplete(doc *ast.Document, autoCompleteRequired bool) *graphqlAutoComplete {
	autoComplete := &graphqlAutoComplete{doc: doc}
	if autoCompleteRequired {
		autoComplete.queryRawFieldIndexes = make(map[int]bool)
		autoComplete.executeRawFieldIndexes = make(map[int]bool)
	}
	return autoComplete
}

func (c *graphqlAutoComplete) ensureAllowed(ctx *resolve.Context) (allowed bool) {
	queryRawExpected, executeRawExpected := len(c.queryRawFieldIndexes), len(c.executeRawFieldIndexes)
	if allowed = queryRawExpected > 0 || executeRawExpected > 0; allowed {
		c.queryRawExpected, c.executeRawExpected = queryRawExpected, executeRawExpected
		c.queryRawTypes = make(map[string]*resolve.QueryRawType, c.queryRawExpected)
		c.executeRawTypes = make(map[string]*resolve.ExecuteRawType, c.executeRawExpected)
		for _, item := range c.doc.OperationDefinitions {
			c.walkSelections(item.HasSelections, item.SelectionSet)
		}
		if queryRawExpected > 0 {
			ctx.Context = context.WithValue(ctx.Context, resolve.QueryRawResp{}, c.queryRawTypes)
		}
		if executeRawExpected > 0 {
			ctx.Context = context.WithValue(ctx.Context, resolve.ExecuteRawResp{}, c.executeRawTypes)
		}
	}
	return
}

func (c *graphqlAutoComplete) walkSelections(hasSelections bool, selectionSet int, path ...string) {
	if !hasSelections || c.queryRawExpected == 0 && c.executeRawExpected == 0 {
		return
	}
	selectionRefs := c.doc.SelectionSets[selectionSet].SelectionRefs
	for _, selectionRef := range selectionRefs {
		selection := c.doc.Selections[selectionRef]
		if selection.Kind != ast.SelectionKindField {
			continue
		}
		nextPath := append(path, c.doc.FieldAliasOrNameString(selection.Ref))
		if _, found := c.queryRawFieldIndexes[selection.Ref]; found {
			c.queryRawTypes[strings.Join(nextPath, ".")] = &resolve.QueryRawType{FieldIndex: selection.Ref}
			c.queryRawExpected--
		}
		if _, found := c.executeRawFieldIndexes[selection.Ref]; found {
			c.executeRawTypes[strings.Join(nextPath, ".")] = &resolve.ExecuteRawType{FieldIndex: selection.Ref}
			c.executeRawExpected--
		}
		field := c.doc.Fields[selection.Ref]
		c.walkSelections(field.HasSelections, field.SelectionSet, nextPath...)
	}
}

func (c *graphqlAutoComplete) appendRecoverFunc(recoverFunc ...func()) {
	c.recoverFuncs = append(c.recoverFuncs, recoverFunc...)
}

func (c *graphqlAutoComplete) prependRecoverFunc(recoverFunc ...func()) {
	c.recoverFuncs = append(recoverFunc, c.recoverFuncs...)
}

func (c *graphqlAutoComplete) autoComplete(buf *bytes.Buffer) error {
	for _, recoverFunc := range c.recoverFuncs {
		recoverFunc()
	}

	var modified bool
	for _, item := range c.queryRawTypes {
		if item.ValueTypes == nil {
			continue
		}
		var itemSelectionSet ast.SelectionSet
		itemField := c.doc.Fields[item.FieldIndex]
		if itemField.HasSelections {
			itemSelectionSet = c.doc.SelectionSets[itemField.SelectionSet]
			originSelectionRefs := itemSelectionSet.SelectionRefs
			savedSelectionRefs := make([]int, 0, len(originSelectionRefs))
			for _, selectionRef := range originSelectionRefs {
				selection := c.doc.Selections[selectionRef]
				if selection.Kind == ast.SelectionKindField {
					fieldName := c.doc.FieldNameString(selection.Ref)
					if _, allowed := item.ValueTypes[fieldName]; allowed {
						delete(item.ValueTypes, fieldName)
						savedSelectionRefs = append(savedSelectionRefs, selectionRef)
					}
				}
			}
			if !slices.Equal(originSelectionRefs, savedSelectionRefs) {
				modified = true
				itemSelectionSet.SelectionRefs = savedSelectionRefs
				c.doc.SelectionSets[itemField.SelectionSet].SelectionRefs = savedSelectionRefs
			}
		}
		if len(item.ValueTypes) == 0 {
			continue
		}

		modified = true
		subKeys := maps.Keys(item.ValueTypes)
		slices.Sort(subKeys)
		for _, k := range subKeys {
			subField := c.buildAstField(k, item.ValueTypes[k])
			subSelection := ast.Selection{Kind: ast.SelectionKindField, Ref: c.doc.AddField(subField).Ref}
			itemSelectionSet.SelectionRefs = append(itemSelectionSet.SelectionRefs, c.doc.AddSelectionToDocument(subSelection))
		}
		if itemField.HasSelections {
			c.doc.SelectionSets[itemField.SelectionSet].SelectionRefs = itemSelectionSet.SelectionRefs
		} else {
			c.doc.Fields[item.FieldIndex].HasSelections = true
			c.doc.Fields[item.FieldIndex].SelectionSet = c.doc.AddSelectionSetToDocument(itemSelectionSet)
		}
	}

	for _, item := range c.executeRawTypes {
		modified = true
		c.doc.Fields[item.FieldIndex].Directives.Refs, c.doc.Fields[item.FieldIndex].HasDirectives = c.buildAstFieldDirectives("", "int")
	}

	if !modified {
		return errors.New("none modified")
	}
	return astprinter.PrintIndent(c.doc, nil, []byte(" "), buf)
}

func (c *graphqlAutoComplete) buildAstField(name, prismaType string) ast.Field {
	field := ast.Field{Name: c.buildByteSliceReference(name)}
	field.Directives.Refs, field.HasDirectives = c.buildAstFieldDirectives(name, prismaType)
	return field
}

func (c *graphqlAutoComplete) buildAstFieldDirectives(name, prismaType string) (directiveRefs []int, hasDirectives bool) {
	hasDirectives = len(buildFieldDirectiveFuncs) > 0
	for _, addDirectiveFunc := range buildFieldDirectiveFuncs {
		directiveName, directiveArgs := addDirectiveFunc(name, prismaType)
		directive := ast.Directive{Name: c.buildByteSliceReference(directiveName), HasArguments: len(directiveArgs) > 0}
		for _, arg := range directiveArgs {
			argRef := c.doc.AddArgument(ast.Argument{
				Name:  c.buildByteSliceReference(arg.Name),
				Value: c.buildAstValue(arg),
			})
			directive.Arguments.Refs = append(directive.Arguments.Refs, argRef)
		}
		directiveRefs = append(directiveRefs, c.doc.AddDirective(directive))
	}
	return
}

func (c *graphqlAutoComplete) buildAstValue(arg DirectiveArgument) (value ast.Value) {
	switch value.Kind = arg.ValueKind; value.Kind {
	case ast.ValueKindBoolean:
		value.Ref = cast.ToInt(cast.ToBool(arg.Value))
	case ast.ValueKindInteger:
		value.Ref = c.doc.AddIntValue(ast.IntValue{Raw: c.buildByteSliceReference(arg.Value)})
	case ast.ValueKindFloat:
		value.Ref = c.doc.AddFloatValue(ast.FloatValue{Raw: c.buildByteSliceReference(arg.Value)})
	case ast.ValueKindEnum:
		value.Ref = c.doc.AddEnumValue(ast.EnumValue{Name: c.buildByteSliceReference(arg.Value)})
	default:
		value.Kind = ast.ValueKindString
		value.Ref = c.doc.AddStringValue(ast.StringValue{Content: c.buildByteSliceReference(arg.Value)})
	}
	return
}

func (c *graphqlAutoComplete) buildByteSliceReference(name string) ast.ByteSliceReference {
	return c.doc.Input.AppendInputString(name)
}
