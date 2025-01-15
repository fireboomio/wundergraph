package database

import (
	"bytes"
	"encoding/json"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"go.uber.org/zap"
)

const createDatabase schemaMethod = "createDatabase"

func NewCreateDatabaseCmd(logger *zap.Logger) *CreateDatabaseCmdExecute {
	return &CreateDatabaseCmdExecute{
		Method: createDatabase,
		logger: logger,
	}
}

type (
	CreateDatabaseCmdExecute = SchemaCommandExecute[CreateDatabaseInput, CreateDatabaseOutput]
	CreateDatabaseInput      struct {
		Datasource DatasourceParam `json:"datasource"`
	}
	DatasourceParamAlias DatasourceParam
	DatasourceParam      struct {
		SchemaPath       *PathContainer   `json:"SchemaPath,omitempty"`
		ConnectionString *UrlContainer    `json:"ConnectionString,omitempty"`
		SchemaString     *SchemaContainer `json:"SchemaString,omitempty"`
	}
	CreateDatabaseOutput struct {
		DatabaseName string `json:"databaseName"`
	}
)

func (p *DatasourceParam) MarshalJSON() ([]byte, error) {
	return makeRustEnumJsonBytes(DatasourceParamAlias(*p))
}

const rustEnumTagName = "tag"

func makeRustEnumJsonBytes(data any) ([]byte, error) {
	var rustJsonBytes []byte
	jsonBytes, err := json.Marshal(data)
	_ = jsonparser.ObjectEach(jsonBytes, func(key []byte, value []byte, _ jsonparser.ValueType, _ int) error {
		if !(bytes.HasPrefix(value, literal.LBRACE) && bytes.HasSuffix(value, literal.RBRACE)) {
			value = []byte(`{}`)
		}
		rustJsonBytes, _ = jsonparser.Set(value, []byte(`"`+string(key)+`"`), rustEnumTagName)
		return nil
	})
	return rustJsonBytes, err
}
