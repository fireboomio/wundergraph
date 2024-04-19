package inputvariables

import (
	"context"
	"github.com/qri-io/jsonpointer"
	"github.com/qri-io/jsonschema"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	nullableKeyword = "nullable"
	typeKeyword     = "type"
	typeNull        = "null"
)

func init() {
	jsonschema.LoadDraft2019_09()
	jsonschema.RegisterKeyword(nullableKeyword, NewNullable)
	jsonschema.SetKeywordOrder(nullableKeyword, 0)
}

type nullable bool

// NewNullable allocates a new Nullable keyword
func NewNullable() jsonschema.Keyword {
	return new(nullable)
}

// ValidateKeyword implements the Keyword interface for Nullable
func (s nullable) ValidateKeyword(_ context.Context, state *jsonschema.ValidationState, data interface{}) {
	if !s || data != nil {
		return
	}

	schemaBytes, _ := state.Local.MarshalJSON()
	schemaTypeResult := gjson.GetBytes(schemaBytes, typeKeyword)
	if !schemaTypeResult.Exists() || schemaTypeResult.IsArray() {
		return
	}

	schemaBytes, _ = sjson.SetBytes(schemaBytes, typeKeyword, []string{schemaTypeResult.String(), typeNull})
	_ = state.Local.UnmarshalJSON(schemaBytes)
}

// Register implements the Keyword interface for Nullable
func (s nullable) Register(string, *jsonschema.SchemaRegistry) {}

// Resolve implements the Keyword interface for Nullable
func (s nullable) Resolve(jsonpointer.Pointer, string) *jsonschema.Schema {
	return nil
}
