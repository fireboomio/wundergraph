package postresolvetransform

import (
	"bytes"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"slices"
	"strings"
)

type Transformer struct {
	transformations []*wgpb.PostResolveTransformation
}

func NewTransformer(transformations []*wgpb.PostResolveTransformation) *Transformer {
	return &Transformer{
		transformations: transformations,
	}
}

func (t *Transformer) handleMath(output []byte, tos [][]string, math *wgpb.PostResolveTransformationMath) ([]byte, error) {
	var (
		err     error
		touched []string
	)
	for i := range tos {
		_to := tos[i][:len(tos[i])-1]
		_toPath := strings.Join(_to, ".")
		if slices.Contains(touched, _toPath) {
			continue
		}
		touched = append(touched, _toPath)
		value, valueType, _, _ := jsonparser.Get(output, _to...)
		if valueType != jsonparser.Array {
			return nil, fmt.Errorf("data on path [%s] expect array for Math [%s]", _toPath, math.String())
		}
		var arrayItems [][]byte
		_, _ = jsonparser.ArrayEach(value, func(itemVal []byte, itemType jsonparser.ValueType, _ int, _ error) {
			if valueType == jsonparser.String {
				itemVal = []byte(`"` + string(itemVal) + `"`)
			}
			arrayItems = append(arrayItems, itemVal)
		})
		if len(arrayItems) == 0 {
			continue
		}

		var mathResult []byte
		switch *math {
		case wgpb.PostResolveTransformationMath_MIN:
			mathResult = slices.MinFunc(arrayItems, bytes.Compare)
		case wgpb.PostResolveTransformationMath_MAX:
			mathResult = slices.MaxFunc(arrayItems, bytes.Compare)
		case wgpb.PostResolveTransformationMath_SUM, wgpb.PostResolveTransformationMath_AVG:
			var numberValue float64
			for j := range arrayItems {
				numberValue += cast.ToFloat64(string(arrayItems[j]))
			}
			if *math == wgpb.PostResolveTransformationMath_AVG {
				numberValue = numberValue / float64(len(arrayItems))
			}
			mathResult = []byte(fmt.Sprintf(`%f`, numberValue))
		case wgpb.PostResolveTransformationMath_COUNT:
			mathResult = []byte(fmt.Sprintf(`%d`, len(arrayItems)))
		case wgpb.PostResolveTransformationMath_FIRST:
			mathResult = arrayItems[0]
		case wgpb.PostResolveTransformationMath_LAST:
			mathResult = arrayItems[len(arrayItems)-1]
		default:
			continue
		}
		if output, err = jsonparser.Set(output, mathResult, _to...); err != nil {
			return nil, err
		}
	}
	return output, nil
}

func (t *Transformer) applyGet(input []byte, get *wgpb.PostResolveGetTransformation, math *wgpb.PostResolveTransformationMath) (output []byte, err error) {
	output = input
	froms := t.resolvePaths(output, [][]string{get.From})
	tos := t.resolvePaths(output, [][]string{get.To})
	if len(froms) != len(tos) {
		if len(froms) == 0 {
			output, err = jsonparser.Set(output, []byte("null"), tos[0][:len(tos[0])-1]...)
			return
		}
		err = fmt.Errorf("applyGet: from and to must have the same length")
		return
	}
	var (
		value     []byte
		valueType jsonparser.ValueType
	)
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
		if output, err = jsonparser.Set(output, value, tos[i]...); err != nil {
			return
		}
	}
	if math != nil && len(tos) > 0 {
		output, err = t.handleMath(output, tos, math)
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

	if len(t.transformations) == 0 || gjson.Null == gjson.GetBytes(output, "data").Type {
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
