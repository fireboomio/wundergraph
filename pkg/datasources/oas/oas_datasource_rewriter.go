package oas_datasource

import (
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/datasource/httpclient"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"golang.org/x/exp/slices"
	"strings"
)

const (
	FieldNameEqualFlag = "name="
	fieldName          = "name"
	valueField         = "value"

	arrayIndexFormat = "[%d]"
	arrayFlag        = "[]"
	pointFlag        = "*"
)

type rewriteFunc func([]string, []byte, jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType)

var (
	specialFlags       = []string{arrayFlag, pointFlag}
	inputAllowedPrefix = []string{httpclient.BODY, httpclient.QUERYPARAMS}
	ignoredValueTypes  = []jsonparser.ValueType{jsonparser.Unknown, jsonparser.NotExist, jsonparser.Null}
)

func (s *Source) rewriteOutput(output []byte) []byte {
	if len(s.responseRewriters) == 0 {
		return output
	}

	var (
		itemPath        []string
		itemRewriteFunc rewriteFunc
	)
	for _, rewriter := range s.responseRewriters {
		itemPath, itemRewriteFunc = rewriter.PathComponents, nil
		itemPath = rewriter.PathComponents
		switch rewriter.Type {
		case wgpb.DataSourceRESTRewriterType_fieldRewrite:
			itemRewriteFunc = func(path []string, _ []byte, valueType jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
				if rewriterValueType := jsonparser.ValueType(rewriter.ValueType); rewriterValueType != jsonparser.NotExist {
					valueType = rewriterValueType
				}
				return append(path[:len(path)-1], rewriter.FieldRewriteTo), nil, valueType
			}
		case wgpb.DataSourceRESTRewriterType_applyAllSubObject:
			itemRewriteFunc = func(_ []string, value []byte, _ jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
				applyValue := []byte("{}")
				_ = jsonparser.ObjectEach(value, func(key []byte, data []byte, dataType jsonparser.ValueType, _ int) error {
					for _, object := range rewriter.ApplySubObjects {
						for _, field := range object.Fields {
							if field.Name != string(key) || jsonparser.ValueType(field.Type) != dataType {
								continue
							}
							applyValue, _ = jsonparser.Set(applyValue, quoteIfStringType(data, dataType), object.Name, field.Name)
						}
					}
					return nil
				})
				return nil, applyValue, jsonparser.Object
			}
		case wgpb.DataSourceRESTRewriterType_applyBySubCommonFieldValue:
			itemRewriteFunc = func(_ []string, value []byte, valueType jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
				commonFieldValue, _, _, _ := jsonparser.Get(value, rewriter.ApplySubCommonField)
				applyField := rewriter.ApplySubCommonFieldValues[string(commonFieldValue)]
				value, _ = jsonparser.Set([]byte("{}"), quoteIfStringType(value, valueType), applyField)
				value, _ = jsonparser.Set(value, quoteIfStringType([]byte(applyField), jsonparser.String), rewriter.CustomEnumField)
				return nil, value, jsonparser.Object
			}
		case wgpb.DataSourceRESTRewriterType_applyBySubfieldType:
			itemRewriteFunc = func(_ []string, value []byte, valueType jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
				for _, field := range rewriter.ApplySubFieldTypes {
					if jsonparser.ValueType(field.Type) == valueType {
						value, _ = jsonparser.Set([]byte("{}"), quoteIfStringType(value, valueType), field.Name)
						value, _ = jsonparser.Set(value, quoteIfStringType([]byte(field.Name), jsonparser.String), rewriter.CustomEnumField)
						break
					}
				}
				return nil, value, jsonparser.Object
			}
		}
		if itemRewriteFunc != nil {
			output, _, _, _ = transform(output, itemPath, itemRewriteFunc)
		}
	}
	return output
}

func (s *Source) rewriteInput(input []byte) []byte {
	if len(s.requestRewriters) == 0 {
		return input
	}

	var (
		itemPath        []string
		itemRewriteFunc rewriteFunc
	)
	for _, rewriter := range s.requestRewriters {
		itemPath, itemRewriteFunc = rewriter.PathComponents, nil
		if len(itemPath) < 2 || !slices.Contains(inputAllowedPrefix, itemPath[0]) {
			continue
		}

		switch rewriter.Type {
		case wgpb.DataSourceRESTRewriterType_valueRewrite:
			itemRewriteFunc = func(_ []string, value []byte, valueType jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
				return nil, []byte(rewriter.ValueRewrites[string(value)]), valueType
			}
		case wgpb.DataSourceRESTRewriterType_fieldRewrite:
			itemRewriteFunc = func(path []string, _ []byte, valueType jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
				return append(path[:len(path)-1], rewriter.FieldRewriteTo), nil, valueType
			}
		case wgpb.DataSourceRESTRewriterType_extractAllSubfield:
			itemRewriteFunc = func(_ []string, value []byte, _ jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
				var extractValue []byte
				_ = jsonparser.ObjectEach(value, func(_ []byte, data []byte, dataType jsonparser.ValueType, _ int) error {
					if dataType != jsonparser.Object {
						return nil
					}
					if extractValue == nil {
						extractValue = data
						return nil
					}
					_ = jsonparser.ObjectEach(data, func(key []byte, value []byte, valueType jsonparser.ValueType, _ int) error {
						extractValue, _ = jsonparser.Set(extractValue, quoteIfStringType(value, valueType), string(key))
						return nil
					})
					return nil
				})
				return nil, extractValue, jsonparser.Object
			}
		case wgpb.DataSourceRESTRewriterType_extractCustomEnumFieldValue:
			itemRewriteFunc = func(_ []string, value []byte, valueType jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
				customEnumValue, _, _, _ := jsonparser.Get(value, rewriter.CustomEnumField)
				value, valueType, _, _ = jsonparser.Get(value, string(customEnumValue))
				return nil, value, valueType
			}
		}
		if itemRewriteFunc != nil {
			input, _, _, _ = transform(input, itemPath, itemRewriteFunc)
		}
	}
	return input
}

func transform(input []byte, path []string, rewrite rewriteFunc) (output []byte, outputType jsonparser.ValueType, modified, overed bool) {
	output = input
	indexOfFlag := slices.IndexFunc(path, func(s string) bool {
		return slices.Contains(specialFlags, s) || strings.HasPrefix(s, FieldNameEqualFlag)
	})
	if indexOfFlag != -1 {
		var (
			valueModified bool
			valueType     jsonparser.ValueType
		)
		jsonpathBeforeFlag, jsonpathAfterFlag := path[:indexOfFlag], path[indexOfFlag+1:]
		switch flag := path[indexOfFlag]; flag {
		case arrayFlag:
			var valueIndex int
			_, _ = jsonparser.ArrayEach(input, func(value []byte, _ jsonparser.ValueType, _ int, _ error) {
				if overed {
					return
				}
				if value, valueType, valueModified, overed = transform(value, jsonpathAfterFlag, rewrite); valueModified {
					outputType, modified = jsonparser.Array, true
					valuePath := makeNextValuePath(jsonpathBeforeFlag, fmt.Sprintf(arrayIndexFormat, valueIndex))
					output, _ = jsonparser.Set(output, quoteIfStringType(value, valueType), valuePath...)
				}
				valueIndex++
			}, jsonpathBeforeFlag...)
		case pointFlag:
			_ = jsonparser.ObjectEach(input, func(key []byte, value []byte, _ jsonparser.ValueType, _ int) error {
				if overed {
					return nil
				}
				if value, valueType, valueModified, overed = transform(value, jsonpathAfterFlag, rewrite); valueModified {
					outputType, modified = jsonparser.Object, true
					valuePath := makeNextValuePath(jsonpathBeforeFlag, string(key))
					output, _ = jsonparser.Set(output, quoteIfStringType(value, valueType), valuePath...)
				}
				return nil
			}, jsonpathBeforeFlag...)
		default:
			fieldNameBytes, _, _, _ := jsonparser.Get(input, makeNextValuePath(jsonpathBeforeFlag, fieldName)...)
			if string(fieldNameBytes) == strings.TrimPrefix(flag, FieldNameEqualFlag) {
				var fieldValueBytes []byte
				valuePath := makeNextValuePath(jsonpathBeforeFlag, valueField)
				fieldValueBytes, valueType, _, _ = jsonparser.Get(input, valuePath...)
				fieldValueBytes = quoteIfStringType(fieldValueBytes, valueType)
				if fieldValueBytes, valueType, valueModified, _ = transform(fieldValueBytes, jsonpathAfterFlag, rewrite); valueModified {
					outputType, modified, overed = jsonparser.Object, true, true
					output, _ = jsonparser.Set(output, quoteIfStringType(fieldValueBytes, valueType), valuePath...)
				}
				return
			}
		}
		return
	}

	value, valueType, _, _ := jsonparser.Get(input, path...)
	if slices.Contains(ignoredValueTypes, valueType) {
		return
	}

	pathNotEmpty := len(path) > 0
	pathModified, valueModified, valueModifiedType := rewrite(path, value, valueType)
	if len(valueModified) > 0 {
		modified = true
		value = quoteIfStringType(valueModified, valueModifiedType)
		if pathNotEmpty {
			outputType = valueType
			output, _ = jsonparser.Set(output, value, path...)
		} else {
			output, outputType = value, valueModifiedType
		}
	}
	if len(pathModified) > 0 {
		if !modified {
			outputType, modified = valueType, true
			value = quoteIfStringType(value, valueType)
		}
		if pathNotEmpty {
			output = jsonparser.Delete(output, path...)
		}
		output, _ = jsonparser.Set(output, value, pathModified...)
	}
	return
}

func makeNextValuePath(path []string, flag string) []string {
	if len(path) == 0 {
		return []string{flag}
	}

	pathLength := len(path)
	nextPath := make([]string, pathLength+1)
	copy(nextPath, path)
	nextPath[pathLength] = flag
	return nextPath
}

func quoteIfStringType(value []byte, valueType jsonparser.ValueType) []byte {
	if valueType == jsonparser.String {
		value = []byte(`"` + string(value) + `"`)
	}
	return value
}
