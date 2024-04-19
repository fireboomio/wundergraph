package oas_datasource

import (
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"testing"
)

func Test_transform(t *testing.T) {
	var (
		input   []byte
		path    []string
		rewrite rewriteFunc
	)
	input = []byte(`{"data": {"content": {"name": "haha", "age": 123}}}}}`)
	path = []string{"data", "content"}
	rewrite = func(path []string, value []byte, valueType jsonparser.ValueType) ([]string, []byte, jsonparser.ValueType) {
		applySubObjects := []*wgpb.DataSourceRESTSubObject{
			{Name: "Person", Fields: []*wgpb.DataSourceRESTSubfield{
				{Name: "name", Type: int32(jsonparser.String)},
				{Name: "age", Type: int32(jsonparser.Number)},
			}},
			{Name: "PP", Fields: []*wgpb.DataSourceRESTSubfield{
				{Name: "age", Type: int32(jsonparser.Number)},
			}},
		}
		applyValue := []byte("{}")
		_ = jsonparser.ObjectEach(value, func(key []byte, data []byte, dataType jsonparser.ValueType, _ int) error {
			for _, object := range applySubObjects {
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
	output, outputModified, _, _ := transform(input, path, rewrite)
	fmt.Println(output, outputModified)
}
