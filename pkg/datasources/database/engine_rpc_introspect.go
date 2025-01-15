package database

import "go.uber.org/zap"

const introspect schemaMethod = "introspect"

func NewIntrospectCmd(logger *zap.Logger) *IntrospectCmdExecute {
	return &IntrospectCmdExecute{
		Method:         introspect,
		VerifyRespFunc: introspectVerifyResp,
		logger:         logger,
	}
}

func introspectVerifyResp(output *IntrospectOutput) (warning string, err error) {
	warning = output.Warnings
	return
}

type (
	IntrospectCmdExecute = SchemaCommandExecute[IntrospectInput, IntrospectOutput]
	IntrospectInput      struct {
		CompositeTypeDepth int8     `json:"compositeTypeDepth"`
		Force              bool     `json:"force"`
		Schema             string   `json:"schema"`
		Schemas            []string `json:"schemas,omitempty"`
	}
	IntrospectOutput struct {
		Datamodel string              `json:"datamodel"`
		Views     []IntrospectionView `json:"views,omitempty"`
		Warnings  string              `json:"warnings,omitempty"`
	}
	IntrospectionView struct {
		Definition string `json:"definition"`
		Name       string `json:"name"`
		Schema     string `json:"schema"`
	}
)
