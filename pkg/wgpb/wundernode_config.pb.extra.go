package wgpb

import (
	"bytes"
	json "github.com/json-iterator/go"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"strings"
)

type ConfigurationVariableAlias ConfigurationVariable

func (c *ConfigurationVariable) UnmarshalJSON(data []byte) (err error) {
	if len(data) == 0 || bytes.Equal(data, literal.NULL) {
		return
	}

	var alias ConfigurationVariableAlias
	if bytes.HasPrefix(data, literal.LBRACE) && bytes.HasSuffix(data, literal.RBRACE) {
		err = json.Unmarshal(data, &alias)
	} else {
		alias = ConfigurationVariableAlias{StaticVariableContent: strings.Trim(string(data), `""`)}
	}
	*c = ConfigurationVariable(alias)
	return
}
