package postresolvetransform

import (
	"bytes"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"slices"
	"strings"
	"sync"
)

type Transformer struct {
	graphqlTransformEnabled bool
	transformations         []*wgpb.PostResolveTransformation
	mathTempMapPool         sync.Pool
}

type transformerMathTemp struct {
	path   []string
	values [][]byte
}

func NewTransformer(transformations []*wgpb.PostResolveTransformation) *Transformer {
	return &Transformer{
		transformations: transformations,
		mathTempMapPool: sync.Pool{
			New: func() any { return make(map[string]*transformerMathTemp, 16) },
		},
	}
}

func (t *Transformer) SetGraphqlTransformEnabled(enabled bool) {
	t.graphqlTransformEnabled = enabled
}

func (t *Transformer) getMathTempMap() map[string]*transformerMathTemp {
	return t.mathTempMapPool.Get().(map[string]*transformerMathTemp)
}

func (t *Transformer) freeMathTempMap(mathTempMap map[string]*transformerMathTemp) {
	for k, v := range mathTempMap {
		for i := range v.values {
			v.values[i] = nil
		}
		v.path = nil
		v.values = nil
		delete(mathTempMap, k)
	}
	t.mathTempMapPool.Put(mathTempMap)
}

func (t *Transformer) handleMath(output []byte, mathTempMap map[string]*transformerMathTemp, math wgpb.PostResolveTransformationMath) ([]byte, error) {
	var err error
	for _, v := range mathTempMap {
		mathPath, mathValues := v.path, v.values
		itemsLength := len(mathValues)
		if itemsLength == 0 {
			switch math {
			case wgpb.PostResolveTransformationMath_MIN,
				wgpb.PostResolveTransformationMath_MAX,
				wgpb.PostResolveTransformationMath_FIRST,
				wgpb.PostResolveTransformationMath_LAST:
				output = jsonparser.Delete(output, mathPath...)
				continue
			}
		}

		var mathResult []byte
		switch math {
		case wgpb.PostResolveTransformationMath_MIN:
			mathResult = slices.MinFunc(mathValues, bytes.Compare)
		case wgpb.PostResolveTransformationMath_MAX:
			mathResult = slices.MaxFunc(mathValues, bytes.Compare)
		case wgpb.PostResolveTransformationMath_SUM, wgpb.PostResolveTransformationMath_AVG:
			var numberValue float64
			for j := range mathValues {
				numberValue += cast.ToFloat64(string(mathValues[j]))
			}
			if numberValue > 0 {
				if math == wgpb.PostResolveTransformationMath_AVG {
					numberValue = numberValue / float64(itemsLength)
				}
				mathResult = []byte(fmt.Sprintf(`%f`, numberValue))
			} else {
				mathResult = literal.ZeroNumberValue
			}
		case wgpb.PostResolveTransformationMath_COUNT:
			if itemsLength > 0 {
				mathResult = []byte(fmt.Sprintf(`%d`, itemsLength))
			} else {
				mathResult = literal.ZeroNumberValue
			}
		case wgpb.PostResolveTransformationMath_FIRST:
			mathResult = mathValues[0]
		case wgpb.PostResolveTransformationMath_LAST:
			mathResult = mathValues[len(mathValues)-1]
		default:
			continue
		}
		if output, err = jsonparser.Set(output, mathResult, mathPath...); err != nil {
			return nil, err
		}
	}
	return output, err
}

func (t *Transformer) applyGet(input []byte, get *wgpb.PostResolveGetTransformation, math *wgpb.PostResolveTransformationMath) (output []byte, err error) {
	output = input
	froms := t.resolvePaths(output, [][]string{get.From})
	var tos [][]string
	if slices.Equal(get.From, get.To) {
		tos = froms
	} else {
		tos = t.resolvePaths(output, [][]string{get.To})
	}
	if len(froms) != len(tos) {
		if len(froms) == 0 {
			output, err = jsonparser.Set(output, []byte("null"), tos[0][:len(tos[0])-1]...)
			return
		}
		err = fmt.Errorf("applyGet: from and to must have the same length")
		return
	}

	var (
		value       []byte
		valueType   jsonparser.ValueType
		tosLength   = len(tos)
		mathTempMap map[string]*transformerMathTemp
	)
	if math != nil {
		mathTempMap = t.getMathTempMap()
		defer t.freeMathTempMap(mathTempMap)
	}
	for i := range froms {
		value, valueType, _, err = jsonparser.Get(output, froms[i]...)
		if err != nil {
			if _, _, _, err = jsonparser.Get(output, froms[i][:len(froms[i])-1]...); err != nil {
				err = nil
				continue
			}
			value = []byte("null")
		} else if valueType == jsonparser.String {
			value = []byte(`"` + string(value) + `"`)
		}
		if math != nil {
			mathPath := tos[i][:len(tos[i])-1]
			mathPathStr := strings.Join(mathPath, ",")
			mathTemp, ok := mathTempMap[mathPathStr]
			if !ok {
				mathTemp = &transformerMathTemp{values: make([][]byte, 0, tosLength), path: mathPath}
				mathTempMap[mathPathStr] = mathTemp
			}
			mathTemp.values = append(mathTemp.values, value)
			continue
		}
		if output, err = jsonparser.Set(output, value, tos[i]...); err != nil {
			return
		}
	}

	if math != nil {
		if len(tos) == 0 {
			for _, item := range t.resolvePaths(output, [][]string{get.To[:len(get.To)-1]}) {
				itemPath := item
				mathTempMap[strings.Join(itemPath, ",")] = &transformerMathTemp{path: itemPath}
			}
		}
		output, err = t.handleMath(output, mathTempMap, *math)
	}
	return
}

func (t *Transformer) resolvePaths(input []byte, paths [][]string) [][]string {
	if !t.pathsContainArray(paths) {
		return paths
	}
	out := make([][]string, 0, len(paths)*3)
	for i := range paths {
		containsArray, j := t.pathContainsArray(paths[i])
		if !containsArray {
			out = append(out, paths[i])
			continue
		}
		preArrayPath := paths[i][:j]
		postArrayPath := paths[i][j+1:]
		index := 0
		_, _ = jsonparser.ArrayEach(input, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			pre := make([]string, len(preArrayPath))
			copy(pre, preArrayPath)
			post := make([]string, len(postArrayPath))
			copy(post, postArrayPath)
			itemPath := append(pre, append([]string{fmt.Sprintf("[%d]", index)}, post...)...)
			out = append(out, itemPath)
			index++
		}, preArrayPath...)
	}
	if t.pathsContainArray(out) {
		return t.resolvePaths(input, out)
	}
	return out
}

func (t *Transformer) pathContainsArray(path []string) (bool, int) {
	for i := range path {
		if path[i] == "[]" {
			return true, i
		}
	}
	return false, 0
}

func (t *Transformer) pathsContainArray(paths [][]string) bool {
	for i := range paths {
		if contains, _ := t.pathContainsArray(paths[i]); contains {
			return true
		}
	}
	return false
}

func (t *Transformer) Transform(input []byte) (output []byte, err error) {

	output = input

	if t.graphqlTransformEnabled || len(t.transformations) == 0 || gjson.Null == gjson.GetBytes(output, "data").Type {
		return
	}

	for _, transformation := range t.transformations {
		switch transformation.Kind {
		case wgpb.PostResolveTransformationKind_GET_POST_RESOLVE_TRANSFORMATION:
			output, err = t.applyGet(output, transformation.Get, transformation.Math)
			if err != nil {
				return
			}
		}
	}

	return
}
