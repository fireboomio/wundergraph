package apihandler

import (
	"bytes"
	"errors"
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/astvisitor"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"strings"
)

var (
	clearVariableDirectiveNames []string
	clearFieldDirectiveNames    []string
	queryRawFieldSuffix         = "_queryRaw"
	executeRawFieldSuffix       = "_executeRaw"
)

func AddClearVariableDirectiveName(name string) {
	clearVariableDirectiveNames = append(clearVariableDirectiveNames, name)
}

func AddClearFieldDirectiveName(name string) {
	clearFieldDirectiveNames = append(clearFieldDirectiveNames, name)
}

func isIntrospectionQuery(doc *ast.Document) bool {
	return doc.OperationNameExists("IntrospectionQuery")
}

func (h *GraphQLHandler) clearDocumentForClearRequired(ctx *resolve.Context, doc *ast.Document, autoCompleteRequired bool) (autoComplete *graphqlAutoComplete, err error) {
	if isIntrospectionQuery(doc) {
		return
	}

	clearRequiredForVariableFunc := func(variableDef ast.VariableDefinition) bool {
		return slices.ContainsFunc(clearVariableDirectiveNames, func(name string) bool {
			return variableDef.Directives.HasDirectiveByName(doc, name)
		})
	}
	clearRequiredForFieldFunc := func(field ast.Field) bool {
		return slices.ContainsFunc(clearFieldDirectiveNames, func(name string) bool {
			return field.Directives.HasDirectiveByName(doc, name)
		})
	}
	clearRawFieldFunc := func(fieldIndex int, isQueryRawField, isExecuteRawField, clearRequired bool) {
		field := doc.Fields[fieldIndex]
		isRawField := isQueryRawField || isExecuteRawField
		if isRawField && field.HasSelections {
			originSelectionRefs := doc.SelectionSets[field.SelectionSet].SelectionRefs
			doc.Fields[fieldIndex].HasSelections = false
			doc.SelectionSets[field.SelectionSet].SelectionRefs = nil
			if autoCompleteRequired && isQueryRawField {
				recoverIndex, recoverSelectionSet := fieldIndex, field.SelectionSet
				autoComplete.appendRecoverFunc(func() {
					doc.Fields[recoverIndex].HasSelections = true
					doc.SelectionSets[recoverSelectionSet].SelectionRefs = originSelectionRefs
				})
			}
		}
		if isQueryRawField && autoCompleteRequired {
			autoComplete.queryRawFieldIndexes[fieldIndex] = true
		}
		if isExecuteRawField && autoCompleteRequired && !clearRequired {
			autoComplete.executeRawFieldIndexes[fieldIndex] = true
		}
	}
	autoComplete = newGraphqlAutoComplete(doc, autoCompleteRequired)
	recoverVariablesFuncs := h.clearDocumentForVariables(doc, autoCompleteRequired, clearRequiredForVariableFunc)
	clearFieldsFuncs, recoverFieldsFuncs := h.clearDocumentForFields(doc, autoCompleteRequired, clearRequiredForFieldFunc, clearRawFieldFunc)
	defer func() {
		for _, clearFunc := range clearFieldsFuncs {
			clearFunc()
		}
	}()
	if autoCompleteRequired {
		if !autoComplete.ensureAllowed(ctx) {
			err = errors.New("graphqlAutoComplete not allowed")
			return
		}
		autoComplete.appendRecoverFunc(recoverVariablesFuncs...)
		autoComplete.appendRecoverFunc(recoverFieldsFuncs...)
	}
	return
}

func (h *GraphQLHandler) clearDocumentForVariables(doc *ast.Document, autoCompleteRequired bool, clearRequiredCheck func(ast.VariableDefinition) bool) (recoverFuncs []func()) {
	savedVariableDefs := make(map[int]ast.VariableDefinition, len(doc.VariableDefinitions))
	touchedVariableRefs := make([]int, 0, len(doc.Arguments))
	for _, item := range doc.Arguments {
		touchedVariableRefs = append(touchedVariableRefs, doc.SearchVariableRefs(item.Value)...)
	}
	for index, item := range doc.VariableDefinitions {
		clearRequired := clearRequiredCheck(item)
		if clearRequired && !slices.ContainsFunc(touchedVariableRefs, func(i int) bool {
			return doc.VariableValueNameString(i) == doc.VariableValueNameString(item.VariableValue.Ref)
		}) {
			continue
		}
		savedVariableDefs[index] = item
	}
	if savedLength := len(savedVariableDefs); savedLength != len(doc.VariableDefinitions) {
		originVariableDefs := doc.VariableDefinitions
		doc.VariableDefinitions = make([]ast.VariableDefinition, savedLength)
		movedVariableDefs := make(map[int]int)
		savedVariableDefIndexes := maps.Keys(savedVariableDefs)
		slices.Sort(savedVariableDefIndexes)
		for targetIndex, originIndex := range savedVariableDefIndexes {
			if originIndex != targetIndex {
				movedVariableDefs[originIndex] = targetIndex
			}
			doc.VariableDefinitions[targetIndex] = savedVariableDefs[originIndex]
		}
		if autoCompleteRequired {
			recoverFuncs = append(recoverFuncs, func() {
				doc.VariableDefinitions = originVariableDefs
			})
		}

		for index, operation := range doc.OperationDefinitions {
			if !operation.HasVariableDefinitions {
				continue
			}
			originVariableRefs := operation.VariableDefinitions.Refs
			savedVariableRefs := make([]int, 0, len(originVariableRefs))
			for _, variableRef := range originVariableRefs {
				if _, saved := savedVariableDefs[variableRef]; !saved {
					continue
				}
				if targetIndex, moved := movedVariableDefs[variableRef]; moved {
					variableRef = targetIndex
				}
				savedVariableRefs = append(savedVariableRefs, variableRef)
			}
			if !slices.Equal(originVariableRefs, savedVariableRefs) {
				doc.OperationDefinitions[index].HasVariableDefinitions = len(savedVariableRefs) > 0
				doc.OperationDefinitions[index].VariableDefinitions.Refs = savedVariableRefs
				if autoCompleteRequired {
					recoverIndex := index
					recoverFuncs = append(recoverFuncs, func() {
						doc.OperationDefinitions[recoverIndex].HasVariableDefinitions = true
						doc.OperationDefinitions[recoverIndex].VariableDefinitions.Refs = originVariableRefs
					})
				}
			}
		}
	}
	return
}

func (h *GraphQLHandler) clearDocumentForFields(doc *ast.Document, autoCompleteRequired bool, checkRequiredCheck func(ast.Field) bool, otherClears ...func(int, bool, bool, bool)) (clearFuncs, recoverFuncs []func()) {
	clearedFieldIndexes := make(map[int]bool)
	for index, item := range doc.Fields {
		fieldName := doc.FieldNameString(index)
		isQueryRawField := strings.HasSuffix(fieldName, queryRawFieldSuffix)
		isExecuteRawField := strings.HasSuffix(fieldName, executeRawFieldSuffix)
		clearRequired := checkRequiredCheck(item)
		if clearRequired && !(isQueryRawField || isExecuteRawField) || autoCompleteRequired && isExecuteRawField {
			clearedFieldIndexes[index] = true
		}
		for _, otherClear := range otherClears {
			otherClear(index, isQueryRawField, isExecuteRawField, clearRequired)
		}
	}

	if len(clearedFieldIndexes) > 0 {
		h.recallDefinedField(clearedFieldIndexes, doc)
		clearedSelectionIndexes := make(map[int]bool)
		for index, item := range doc.Selections {
			if item.Kind == ast.SelectionKindField {
				if _, cleared := clearedFieldIndexes[item.Ref]; cleared {
					clearedSelectionIndexes[index] = true
				}
			}
		}
		for index, selectionSet := range doc.SelectionSets {
			originSelectionRefs, originSelectionRefsLength := selectionSet.SelectionRefs, len(selectionSet.SelectionRefs)
			if originSelectionRefsLength == 0 {
				continue
			}
			savedSelectionRefs := make([]int, 0, originSelectionRefsLength)
			for _, selectionRef := range originSelectionRefs {
				if _, cleared := clearedSelectionIndexes[selectionRef]; cleared {
					continue
				}
				savedSelectionRefs = append(savedSelectionRefs, selectionRef)
			}
			if !slices.Equal(originSelectionRefs, savedSelectionRefs) {
				clearSetIndex := index
				clearFuncs = append(clearFuncs, func() {
					doc.SelectionSets[clearSetIndex].SelectionRefs = savedSelectionRefs
				})
				if autoCompleteRequired {
					recoverFuncs = append(recoverFuncs, func() {
						doc.SelectionSets[clearSetIndex].SelectionRefs = originSelectionRefs
					})
				}
				if len(savedSelectionRefs) == 0 {
					for i := range doc.Fields {
						if !doc.Fields[i].HasSelections || doc.Fields[i].SelectionSet != clearSetIndex {
							continue
						}

						clearFieldIndex := i
						clearFuncs = append(clearFuncs, func() {
							doc.Fields[clearFieldIndex].HasSelections = false
						})
						if autoCompleteRequired {
							recoverFuncs = append(recoverFuncs, func() {
								doc.Fields[clearFieldIndex].HasSelections = true
							})
						}
						break
					}
				}
			}
		}
	}
	return
}

func (h *GraphQLHandler) clearDocumentForPreparePlan(doc *ast.Document) {
	h.clearDocumentForVariables(doc, false, func(variableDef ast.VariableDefinition) bool {
		variableValue := variableDef.VariableValue
		return variableValue.Kind == ast.ValueKindVariable && doc.VariableValues[variableValue.Ref].Generated
	})
	for index, item := range doc.Arguments {
		if item.HasOriginValue {
			doc.Arguments[index].Value = item.OriginValue
		}
	}
	for index, item := range doc.ObjectFields {
		if item.HasOriginValue {
			doc.ObjectFields[index].Value = item.OriginValue
		}
	}
}

func (h *GraphQLHandler) recallDefinedField(clearedFieldIndexes map[int]bool, doc *ast.Document) {
	walker := astvisitor.NewWalker(48)
	_recallVisitor := &recallDefinedField{
		Walker:              &walker,
		clearedFieldIndexes: clearedFieldIndexes,
	}
	walker.RegisterEnterDocumentVisitor(_recallVisitor)
	walker.RegisterEnterFieldVisitor(_recallVisitor)
	_recallVisitor.Walk(doc, h.definition, nil)
}

type recallDefinedField struct {
	*astvisitor.Walker
	clearedFieldIndexes map[int]bool
	operation           *ast.Document
	definition          *ast.Document
}

func (f *recallDefinedField) EnterDocument(operation, definition *ast.Document) {
	f.operation = operation
	f.definition = definition
}

func (f *recallDefinedField) EnterField(ref int) {
	switch f.EnclosingTypeDefinition.Kind {
	case ast.NodeKindInterfaceTypeDefinition, ast.NodeKindObjectTypeDefinition:
		f.tryRecall(ref, f.EnclosingTypeDefinition)
	}
}

func (f *recallDefinedField) tryRecall(ref int, enclosingTypeDefinition ast.Node) {
	if _, ok := f.clearedFieldIndexes[ref]; !ok {
		return
	}
	fieldName := f.operation.FieldNameBytes(ref)
	if bytes.Equal(fieldName, literal.TYPENAME) {
		return
	}
	definitions := f.definition.NodeFieldDefinitions(enclosingTypeDefinition)
	for _, i := range definitions {
		definitionName := f.definition.FieldDefinitionNameBytes(i)
		if bytes.Equal(fieldName, definitionName) {
			delete(f.clearedFieldIndexes, ref)
			return
		}
	}
}
