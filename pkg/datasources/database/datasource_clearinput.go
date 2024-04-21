package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/astparser"
	"github.com/wundergraph/graphql-go-tools/pkg/astprinter"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"golang.org/x/exp/slices"
	"strconv"
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

func getEndIndexOffset(data []byte, index int) (offset int) {
	if data[index] == charCOMMA {
		offset++
	}
	if data[index+1] == charSPACE {
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
			input = rendererBuf.Bytes()
			pool.PutBytesBuffer(rendererBuf)
			continue
		}

		startIndex := getKeywordStartIndex(input[:variableIndex], jointIndexes)
		endIndex := variableIndex + nullBytesLength
		endIndex += getEndIndexOffset(input, endIndex)
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

		var nextClearStart int
		nextClearEnd := clearedLength + 1
		if parenCleared {
			nextClearStart = clearedLength - 1
		} else {
			nextClearStart = getKeywordStartIndex(clearedBytes[:clearedLength-1], nextJointIndexes)
			if braceCleared && input[start+1] == charRBRACK {
				start++
				nextClearEnd++
			}
			nextClearEnd += getEndIndexOffset(input, start+1)
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

func clearSkipFetchFieldPaths(skipFieldJsonPaths map[string]bool, request []byte) ([]byte, error) {
	originQueryBytes, _, _, _ := jsonparser.Get(request, "query")
	doc, report := astparser.ParseGraphqlDocumentBytes(originQueryBytes)
	if report.HasErrors() {
		return nil, errors.New(report.Error())
	}

	for _, operation := range doc.OperationDefinitions {
		if operation.HasSelections {
			clearDocumentWithSkipFieldJsonPaths(&doc, skipFieldJsonPaths, operation.SelectionSet)
		}
	}
	clearedQueryString, err := astprinter.PrintString(&doc, nil)
	if err != nil {
		return nil, err
	}
	return jsonparser.Set(request, []byte(strconv.Quote(clearedQueryString)), "query")
}

func clearDocumentWithSkipFieldJsonPaths(doc *ast.Document, skipFieldJsonPaths map[string]bool, selectionSet int, parent ...string) (clearOvered, noneSaved bool) {
	var (
		itemNoneSaved bool
		itemWalkIndex int
	)
	parentLength := len(parent)
	originSelectionRefs := doc.SelectionSets[selectionSet].SelectionRefs
	savedSelectionRefs := make([]int, 0, len(originSelectionRefs))
	for i, selectionRef := range originSelectionRefs {
		if clearOvered {
			break
		}
		itemWalkIndex = i
		selection := doc.Selections[selectionRef]
		if selection.Kind != ast.SelectionKindField {
			savedSelectionRefs = append(savedSelectionRefs, selectionRef)
			continue
		}

		fieldJsonPath := make([]string, parentLength+1)
		copy(fieldJsonPath, parent)
		fieldJsonPath[parentLength] = doc.FieldNameString(selection.Ref)
		fieldJsonPathStr := strings.Join(fieldJsonPath, ".")
		if _, skip := skipFieldJsonPaths[fieldJsonPathStr]; skip {
			delete(skipFieldJsonPaths, fieldJsonPathStr)
			clearOvered = len(skipFieldJsonPaths) == 0
			continue
		}
		if field := doc.Fields[selection.Ref]; field.HasSelections {
			clearOvered, itemNoneSaved = clearDocumentWithSkipFieldJsonPaths(doc, skipFieldJsonPaths, field.SelectionSet, fieldJsonPath...)
			if itemNoneSaved {
				continue
			}
		}
		savedSelectionRefs = append(savedSelectionRefs, selectionRef)
	}
	savedSelectionRefs = append(savedSelectionRefs, originSelectionRefs[itemWalkIndex+1:]...)
	if !slices.Equal(originSelectionRefs, savedSelectionRefs) {
		doc.SelectionSets[selectionSet].SelectionRefs = savedSelectionRefs
		noneSaved = len(savedSelectionRefs) == 0
	}
	return
}
