package database

import (
	"bytes"
	"context"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"golang.org/x/exp/slices"
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
