package database

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/wundergraph/pkg/eventbus"
	"math/rand"
	"os/exec"
	"strings"

	"go.uber.org/zap"
)

type JsonRPCExtension struct {
	CmdArgs []string
	CmdEnvs []string
}

type JsonRPCPayloadParams map[string]interface{}

type JsonRPCPayload struct {
	Id      int    `json:"id"`
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

type JsonRPCResponse struct {
	Id      int    `json:"id"`
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		ExecutedSteps *int     `json:"executedSteps"`
		DataModel     string   `json:"datamodel"`
		Warnings      any      `json:"warnings"`
		Unexecutable  []string `json:"unexecutable"`
	} `json:"result"`
}

func GetRPCPayload(method string, params map[string]interface{}, singled bool) JsonRPCPayload {
	var rpcParams any
	if singled {
		rpcParams = params
	} else {
		rpcParams = []JsonRPCPayloadParams{params}
	}

	var payload = JsonRPCPayload{
		Id:      rand.Int(),
		Jsonrpc: "2.0",
		Method:  method,
		Params:  rpcParams,
	}
	return payload
}

func (e *Engine) RunEngineCommand(ctx context.Context, method string, params map[string]interface{}, extension ...JsonRPCExtension) (string, error) {
	err := e.ensurePrisma()
	if err != nil {
		return "", err
	}

	payload := GetRPCPayload(method, params, true)
	requestStr, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	cmdInput := append(requestStr, []byte("\n")...)
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	enginePath := e.schemaEnginePath

	if len(extension) > 0 {
		//构建command命令行
		e.cmd = exec.CommandContext(ctx, enginePath, extension[0].CmdArgs...)
		e.cmd.Env = append(e.cmd.Env, extension[0].CmdEnvs...)
	} else {
		e.cmd = exec.CommandContext(ctx, enginePath)
	}
	setCmd(e.cmd)
	stdout, err := e.cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	defer stdout.Close()

	stdin, err := e.cmd.StdinPipe()
	if err != nil {
		return "", err
	}
	defer stdin.Close()

	//启动进程
	err = e.cmd.Start()
	if err != nil {
		return "", err
	}
	defer e.StopPrismaEngine()

	_, err = stdin.Write(cmdInput)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(stdout)
	buf := bytes.Buffer{}

Loop:
	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timeout waiting response for %s", enginePath)
		default:
			b, err := reader.ReadByte()
			if err != nil {
				return "", err
			}
			if b == '\n' {
				break Loop
			}
			err = buf.WriteByte(b)
			if err != nil {
				return "", err
			}
		}
	}

	errorBytes, _, _, _ := jsonparser.Get(buf.Bytes(), "error")
	if len(errorBytes) > 0 {
		var errorMsg string
		dataBytes, _, _, _ := jsonparser.Get(errorBytes, "data")
		if dataBytes = translateError(dataBytes); len(dataBytes) > 0 {
			errorMsg = string(dataBytes)
		} else {
			messageBytes, _, _, _ := jsonparser.Get(errorBytes, "message")
			errorMsg = string(messageBytes)
		}
		return "", fmt.Errorf("error while %s database: %s", method, errorMsg)
	}

	var response JsonRPCResponse
	if err = json.NewDecoder(&buf).Decode(&response); err != nil {
		return "", err
	}
	if resultWarnings := response.Result.Warnings; resultWarnings != nil {
		var warnings []string
		switch ws := resultWarnings.(type) {
		case string:
			if ws != "" {
				warnings = append(warnings, ws)
			}
		case []any:
			for _, w := range ws {
				if s, ok := w.(string); ok && s != "" {
					warnings = append(warnings, s)
				} else {
					e.log.Warn("skipping non-string warning", zap.Any("value", w))
				}
			}
		default:
			e.log.Warn("can not process warnings", zap.Any("value", ws))
		}

		if len(warnings) > 0 {
			e.log.Warn("engine appear warnings",
				zap.Any("datasource", ctx.Value(eventbus.ChannelDatasource)),
				zap.String("method", method),
				zap.Any("params", params),
				zap.Error(errors.New(strings.Join(warnings, ". "))))
		}
	}
	if unexecutables := response.Result.Unexecutable; len(unexecutables) > 0 {
		return "", fmt.Errorf("error while %s database unexecutables: %s", method, strings.Join(unexecutables, "\n\r"))
	}
	if steps := response.Result.ExecutedSteps; steps != nil && *steps == 0 {
		return "", fmt.Errorf("error while %s database: none modified", method)
	}
	return response.Result.DataModel, nil
}

// IntrospectPrismaDatabaseSchema 启动内省引擎，发起内省请求
func (e *Engine) IntrospectPrismaDatabaseSchema(ctx context.Context, introspectionSchema string, extension ...JsonRPCExtension) (string, error) {
	return e.RunEngineCommand(ctx,
		"introspect",
		JsonRPCPayloadParams{
			"schema":             introspectionSchema,
			"compositeTypeDepth": -1,
			"force":              false,
		}, extension...)
}

type MigrationAction string

const (
	MigrationCreate MigrationAction = "createMigration"
	MigrationApply  MigrationAction = "applyMigrations"
)

// Migrate 迁移
func (e *Engine) Migrate(ctx context.Context, action MigrationAction, params JsonRPCPayloadParams, extension ...JsonRPCExtension) (string, error) {
	return e.RunEngineCommand(ctx, string(action), params, extension...)
}
