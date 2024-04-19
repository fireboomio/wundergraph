package postresolvetransform

import (
	"fmt"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"

	"github.com/wundergraph/wundergraph/pkg/wgpb"
)

type Transformer struct {
	transformations []*wgpb.PostResolveTransformation
}

func NewTransformer(transformations []*wgpb.PostResolveTransformation) *Transformer {
	return &Transformer{
		transformations: transformations,
	}
}

func formatDateTimeValue(value []byte, get *wgpb.PostResolveGetTransformation) []byte {
	if get.DateTimeFormat == "" {
		return value
	}

	return []byte(strconv.Quote(cast.ToTime(strings.Trim(string(value), `""`)).Format(get.DateTimeFormat)))
}

func (t *Transformer) applyGet(input []byte, get *wgpb.PostResolveGetTransformation) (out []byte, err error) {
	froms := t.resolvePaths(input, [][]string{get.From})
	tos := t.resolvePaths(input, [][]string{get.To})
	if len(froms) != len(tos) {
		if len(froms) == 0 {
			input, err = jsonparser.Set(input, []byte("null"), tos[0][:len(tos[0])-1]...)
			return input, err
		}
		return nil, fmt.Errorf("applyGet: from and to must have the same length")
	}
	for i := range froms {
		value, valueType, offset, err := jsonparser.Get(input, froms[i]...)
		if err != nil {
			input, err = jsonparser.Set(input, []byte("null"), tos[i]...)
			if err != nil {
				return nil, err
			}
			continue
		}
		if valueType == jsonparser.String {
			value = input[offset-len(value)-2 : offset]
		}
		value = formatDateTimeValue(value, get)
		if input, err = jsonparser.Set(input, value, tos[i]...); err != nil {
			return nil, fmt.Errorf("applyGet: %s", err)
		}
	}
	return input, nil
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
			output, err = t.applyGet(output, transformation.Get)
			if err != nil {
				return
			}
		}
	}

	return
}
