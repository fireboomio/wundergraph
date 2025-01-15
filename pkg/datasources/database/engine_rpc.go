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
	"go.uber.org/zap"
	"math/rand"
	"os"
	"os/exec"
)

type schemaMethod string

type (
	SchemaCommandExecute[I, O any] struct {
		Method         schemaMethod
		VerifyRespFunc func(*O) (string, error)

		logger *zap.Logger
		cancel func()
	}
	SchemaCommandRPCBase struct {
		Id      int    `json:"id"`
		Jsonrpc string `json:"jsonrpc"`
	}
	SchemaCommandRPCPayload[I any] struct {
		SchemaCommandRPCBase
		Method schemaMethod `json:"method"`
		Params *I           `json:"params"`
	}
	SchemaCommandRPCResponse[O any] struct {
		SchemaCommandRPCBase
		Result *O                     `json:"result,omitempty"`
		Params *SchemaCommandRPCPrint `json:"params,omitempty"`
	}
	SchemaCommandRPCPrint struct {
		Content string `json:"content"`
	}
	SchemaCommandRPCExtension struct {
		CmdArgs []string
		CmdEnvs []string
	}
)

func (e *SchemaCommandExecute[I, O]) Run(ctx context.Context, input *I, extension ...SchemaCommandRPCExtension) (o *O, printContent string, err error) {
	if err = EnsurePrismaEngineDownloaded(); err != nil {
		return
	}

	var cmdArgs, cmdEnvs []string
	if len(extension) > 0 {
		cmdArgs = extension[0].CmdArgs
		cmdEnvs = extension[0].CmdEnvs
	}
	cmdEnvs = append(cmdEnvs, "RUST_LOG=trace")
	//构建command命令行
	ctx, e.cancel = context.WithCancel(ctx)
	enginePath := os.Getenv(prismaSchemaEngineEnvKey)
	cmd := exec.CommandContext(ctx, enginePath, cmdArgs...)
	cmd.Env = append(cmd.Env, cmdEnvs...)
	setCmd(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return
	}
	defer func() { _ = stdout.Close() }()

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return
	}
	defer func() { _ = stdin.Close() }()
	cmd.Stderr = &stderrWriter{func(errMsg stderrMsg) { e.logger.Debug(errMsg.Error()) }}

	//启动进程
	if err = cmd.Start(); err != nil {
		return
	}
	defer stopProcess(e.logger, cmd, e.cancel)

	payloadBytes, err := json.Marshal(SchemaCommandRPCPayload[I]{
		SchemaCommandRPCBase: SchemaCommandRPCBase{
			Id:      rand.Int(),
			Jsonrpc: "2.0",
		},
		Method: e.Method,
		Params: input,
	})
	if err != nil {
		return
	}
	cmdInput := append(payloadBytes, []byte("\n")...)
	if _, err = stdin.Write(cmdInput); err != nil {
		return
	}

	reader := bufio.NewReader(stdout)
	buf := bytes.Buffer{}
	var b byte

Loop:
	for {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("timeout waiting response for %s", enginePath)
			return
		default:
			if b, err = reader.ReadByte(); err != nil {
				return
			}
			if b == '\n' {
				break Loop
			}
			if err = buf.WriteByte(b); err != nil {
				return
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
		err = fmt.Errorf("error while %s database: %s", e.Method, errorMsg)
		return
	}

	var response SchemaCommandRPCResponse[O]
	if err = json.NewDecoder(&buf).Decode(&response); err != nil {
		return
	}

	if e.VerifyRespFunc != nil {
		warning, _err := e.VerifyRespFunc(response.Result)
		if len(warning) > 0 {
			e.logger.Warn("engine appear warnings",
				zap.Any("datasource", ctx.Value(eventbus.ChannelDatasource)),
				zap.String("method", string(e.Method)),
				zap.Any("params", input),
				zap.Error(errors.New(warning)))
		}
		if _err != nil {
			err = _err
			return
		}
	}
	o = response.Result
	if response.Params != nil {
		printContent = response.Params.Content
	}
	return
}
