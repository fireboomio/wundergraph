package interpolate

import (
	"fmt"
	"github.com/getkin/kin-openapi/openapi3"
	json "github.com/json-iterator/go"
	"golang.org/x/exp/slices"
	"strings"

	"github.com/buger/jsonparser"
)

const (
	Openapi3SchemaRefPrefix = "#/components/schemas/"
	arrayPath               = "[]"
)

func init() {
	openapi3.SchemaErrorDetailsDisabled = true
}

type Openapi3StringInterpolator struct {
	schema            *openapi3.SchemaRef
	hasInterpolations bool
	jsonOnlyMode      bool
	defs              openapi3.Schemas
	configuredDefs    map[string]struct{}
	PropertiesTypeMap map[string]string
}

func NewOpenapi3StringInterpolator(schema *openapi3.SchemaRef, defs openapi3.Schemas) (*Openapi3StringInterpolator, error) {
	interpolator := &Openapi3StringInterpolator{
		schema:            schema,
		defs:              defs,
		configuredDefs:    map[string]struct{}{},
		PropertiesTypeMap: map[string]string{},
	}
	interpolator.configure(schema)
	return interpolator, nil
}

func NewOpenapi3StringInterpolatorJSONOnly(schema *openapi3.SchemaRef, defs openapi3.Schemas) (*Openapi3StringInterpolator, error) {
	interpolator := &Openapi3StringInterpolator{
		schema:         schema,
		defs:           defs,
		jsonOnlyMode:   true,
		configuredDefs: map[string]struct{}{},
	}
	interpolator.configure(schema)
	return interpolator, nil
}

func (s *Openapi3StringInterpolator) configure(schema *openapi3.SchemaRef, path ...string) {
	if schema.Ref != "" {
		if _, ok := s.configuredDefs[schema.Ref]; ok {
			return
		}
		s.configuredDefs[schema.Ref] = struct{}{}
		definitionName := strings.TrimPrefix(schema.Ref, Openapi3SchemaRefPrefix)
		definition, ok := s.defs[definitionName]
		if !ok {
			return
		}
		s.configure(definition, path...)
		return
	}

	schemaValue := schema.Value
	if len(path) > 0 && s.PropertiesTypeMap != nil {
		s.PropertiesTypeMap[strings.Join(path, ".")] = schemaValue.Type
	}

	if s.hasInterpolations {
		return
	}

	switch schemaValue.Type {
	case openapi3.TypeString:
		if s.jsonOnlyMode {
			return
		}
		s.hasInterpolations = true
	case openapi3.TypeObject:
		if schemaValue.Properties == nil {
			return
		}
		for prop, propSchema := range schemaValue.Properties {
			s.configure(propSchema, append(path, prop)...)
		}
	case openapi3.TypeArray:
		if schemaValue.Items == nil {
			return
		}
		s.configure(schemaValue.Items, append(path, arrayPath)...)
	case "":
		s.hasInterpolations = true
	}
}

func (s *Openapi3StringInterpolator) traverse(schema *openapi3.SchemaRef, data *[]byte, path []string) {
	if schema.Ref != "" {
		definitionName := strings.TrimPrefix(schema.Ref, Openapi3SchemaRefPrefix)
		if s.defs == nil || s.defs[definitionName] == nil {
			return
		}

		s.traverse(s.defs[definitionName], data, path)
		return
	}

	schemaValue := schema.Value
	switch schemaValue.Type {
	case openapi3.TypeString:
		if s.jsonOnlyMode {
			return
		}
		value, dataType, _, err := jsonparser.Get(*data, path...)
		if err != nil {
			return
		}
		switch dataType {
		case jsonparser.Number, jsonparser.Boolean:
			if dataType != jsonparser.String {
				*data, _ = jsonparser.Set(*data, append([]byte("\""), append(value, []byte("\"")...)...), path...)
			}
		}
	case openapi3.TypeObject:
		if schemaValue.Properties == nil {
			return
		}
		for prop, propSchema := range schemaValue.Properties {
			isRequired := slices.Contains(schemaValue.Required, prop)
			path := append(path, prop)
			_, _, _, err := jsonparser.Get(*data, path...)
			if err != nil && !isRequired {
				continue
			}
			s.traverse(propSchema, data, path)
		}
	case openapi3.TypeArray:
		if schemaValue.Items == nil {
			return
		}
		var i int
		_, _ = jsonparser.ArrayEach(*data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			s.traverse(schemaValue.Items, data, append(path, fmt.Sprintf("[%d]", i)))
			i++
		}, path...)
	case "":
		if !s.jsonOnlyMode {
			return
		}
		value, valueType, _, err := jsonparser.Get(*data, path...)
		if err != nil {
			return
		}
		switch valueType {
		// 数组类型数据不做stringify处理
		case jsonparser.Array:
		case jsonparser.String:
			stringified := make([]byte, len(value)+2)
			stringified[0] = '"'
			stringified[len(stringified)-1] = '"'
			copy(stringified[1:], value)
			*data, _ = jsonparser.Set(*data, stringified, path...)
		default:
			stringified, err := json.Marshal(string(value))
			if err != nil {
				return
			}
			*data, _ = jsonparser.Set(*data, stringified, path...)
		}
	}
}

func (s *Openapi3StringInterpolator) Interpolate(data []byte) []byte {
	if !s.hasInterpolations {
		return data
	}
	s.traverse(s.schema, &data, nil)
	return data
}
