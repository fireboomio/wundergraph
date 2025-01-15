package database

import (
	"fmt"
	"go.uber.org/zap"
	"strings"
)

const schemaPush schemaMethod = "schemaPush"

func NewSchemaPushCmd(logger *zap.Logger) *SchemaPushCmdExecute {
	return &SchemaPushCmdExecute{
		Method:         schemaPush,
		VerifyRespFunc: schemaPushVerifyResp,
		logger:         logger,
	}
}

func schemaPushVerifyResp(output *SchemaPushOutput) (warning string, err error) {
	if len(output.Warnings) > 0 {
		warning = strings.Join(output.Warnings, ". ")
	}
	if unexecutable := output.Unexecutable; len(unexecutable) > 0 {
		err = fmt.Errorf("error while %s database unexecutables: %s", schemaPush, strings.Join(unexecutable, "\n\r"))
		return
	}
	if steps := output.ExecutedSteps; steps != nil && *steps == 0 {
		err = fmt.Errorf("error while %s database: none modified", schemaPush)
		return
	}
	return
}

type (
	SchemaPushCmdExecute = SchemaCommandExecute[SchemaPushInput, SchemaPushOutput]
	SchemaPushInput      struct {
		Force  bool   `json:"force"`
		Schema string `json:"schema"`
	}
	SchemaPushOutput struct {
		ExecutedSteps *uint32  `json:"executedSteps,omitempty"`
		Unexecutable  []string `json:"unexecutable,omitempty"`
		Warnings      []string `json:"warnings,omitempty"`
	}
)
