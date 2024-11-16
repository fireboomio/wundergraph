package database

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/opentracing/opentracing-go"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/wundergraph/wundergraph/pkg/eventbus"

	"github.com/gofrs/flock"
	"github.com/phayes/freeport"
	"github.com/prisma/prisma-client-go/binaries"
	"go.uber.org/zap"

	"github.com/wundergraph/graphql-go-tools/pkg/repair"
)

func init() {
	_ = EnsurePrismaEngineDownloaded()
}

const (
	prismaVersion            = "c0b3c9e2ba30efb866d7f418d269710c4209d7b2"
	prismaQueryEngineEnvKey  = "PRISMA_QUERY_ENGINE_BINARY"
	prismaSchemaEngineEnvKey = "PRISMA_INTROSPECTION_ENGINE_BINARY"
	prismaEngineExitTimeout  = 5 * time.Second
)

// EnsurePrismaEngineDownloaded 安装检测prisma引擎环境
func EnsurePrismaEngineDownloaded() (err error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return
	}
	prismaPath := filepath.Join(cacheDir, "fireboom", "prisma")
	if err = os.MkdirAll(prismaPath, os.ModePerm); err != nil {
		return
	}

	// Acquire a file lock before trying to download
	lockPath := filepath.Join(prismaPath, ".lock")
	lock := flock.New(lockPath)
	if err = lock.Lock(); err != nil {
		return fmt.Errorf("creating prisma lockfile: %w", err)
	}
	defer func() { _ = lock.Unlock() }()

	fetchResp, err := binaries.FetchNativeWithVersion(prismaPath, prismaVersion)
	if err != nil {
		return
	}

	_ = os.Setenv(prismaQueryEngineEnvKey, fetchResp.QueryEnginePath)
	_ = os.Setenv(prismaSchemaEngineEnvKey, fetchResp.SchemaEnginePath)
	return
}

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

type Engine struct {
	wundergraphDir          string
	queryEnginePath         string
	introspectionEnginePath string
	url                     string
	cmd                     *exec.Cmd
	cancel                  func()
	stderr                  error
	client                  *http.Client
	log                     *zap.Logger
}

func NewEngine(client *http.Client, log *zap.Logger, wundergraphDir string) *Engine {
	return &Engine{
		wundergraphDir: wundergraphDir,
		client:         client,
		log:            log,
	}
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

func (e *Engine) RunEngineCommand(ctx context.Context, getEnginePath func(e *Engine) string, method string, params map[string]interface{}, extension ...JsonRPCExtension) (string, error) {
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

	enginePath := getEnginePath(e)

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
	return e.RunEngineCommand(ctx, func(e *Engine) string {
		return e.introspectionEnginePath
	},
		"introspect",
		JsonRPCPayloadParams{
			"schema":             introspectionSchema,
			"compositeTypeDepth": -1,
			"force":              false,
		}, extension...)
}

// 内省GraphQLSchema
func (e *Engine) IntrospectGraphQLSchema(ctx context.Context) (schema string, err error) {
	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context cancelled")
		default:
			res, err := e.client.Get(e.url + "/sdl")
			if err != nil {
				continue
			}
			if res.StatusCode != http.StatusOK {
				continue
			}
			data, err := io.ReadAll(res.Body)
			if err != nil {
				return "", err
			}
			sdl := string(data)
			sdl = "schema { query: Query mutation: Mutation }\n" + sdl
			schema, err = repair.SDL(sdl, repair.OptionsSDL{
				Indent:                       "  ",
				SetAllMutationFieldsNullable: true,
			})
			return schema, err
		}
	}
}

func (e *Engine) IntrospectDMMF(ctx context.Context) (dmmf string, err error) {
	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context cancelled")
		default:
			res, err := e.client.Get(e.url + "/dmmf")
			if err != nil {
				continue
			}
			if res.StatusCode != http.StatusOK {
				continue
			}
			data, err := io.ReadAll(res.Body)
			if err != nil {
				return "", err
			}
			return string(data), err
		}
	}
}

type MigrationAction string

const (
	MigrationCreate MigrationAction = "createMigration"
	MigrationApply  MigrationAction = "applyMigrations"
)

// Migrate 迁移
func (e *Engine) Migrate(ctx context.Context, action MigrationAction, params JsonRPCPayloadParams, extension ...JsonRPCExtension) (string, error) {
	return e.RunEngineCommand(ctx, func(e *Engine) string {
		return e.introspectionEnginePath
	}, string(action), params, extension...)
}

func (e *Engine) Request(ctx context.Context, request []byte, rw io.Writer) (err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/", bytes.NewReader(request))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	modifyRequestForTransaction(ctx, req)
	addTraceParentHeader(ctx, req)
	res, err := e.client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		if res.Body != nil {
			defer res.Body.Close()
			if body, err := io.ReadAll(res.Body); err == nil {
				return errors.New(string(body))
			}
		}
		return errors.New("http status not ok")
	}
	_, err = io.Copy(rw, res.Body)
	return
}

func (e *Engine) StartQueryEngine(schema string, env ...string) error {
	err := e.ensurePrisma()
	if err != nil {
		return err
	}

	freePort, err := freeport.GetFreePort()
	if err != nil {
		return err
	}
	port := strconv.Itoa(freePort)
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.cmd = exec.CommandContext(ctx, e.queryEnginePath, "-p", port, "--enable-raw-queries")
	if opentracing.IsGlobalTracerRegistered() {
		e.cmd.Args = append(e.cmd.Args, "--enable-open-telemetry")
		e.cmd.Stdout = &stdoutWriter{}
	} else {
		e.cmd.Stdout = os.Stdout
	}
	e.cmd.Stderr = &stderrWriter{e}
	// ensure that prisma starts with the dir set to the .wundergraph directory
	// this is important for sqlite support as it's expected that the path of the sqlite file is the same
	// (relative to the .wundergraph directory) during introspection and at runtime
	setCmd(e.cmd)
	e.cmd.Dir = e.wundergraphDir
	if filepath.IsAbs(schema) {
		e.cmd.Env = append(e.cmd.Env, "PRISMA_DML_PATH="+schema)
	} else {
		e.cmd.Env = append(e.cmd.Env, "PRISMA_DML="+schema)
	}
	e.cmd.Env = append(e.cmd.Env, "RUST_MIN_STACK=4194304") // 设置栈空间，4M, 避免栈溢出
	e.cmd.Env = append(e.cmd.Env, env...)
	e.url = "http://localhost:" + port
	if err := e.cmd.Start(); err != nil {
		e.StopPrismaEngine()
		return err
	}
	return nil
}

// 安装检测prisma引擎环境
func (e *Engine) ensurePrisma() (err error) {
	if err = EnsurePrismaEngineDownloaded(); err != nil {
		return
	}

	e.queryEnginePath = os.Getenv(prismaQueryEngineEnvKey)
	e.introspectionEnginePath = os.Getenv(prismaSchemaEngineEnvKey)
	return
}

func (e *Engine) StopPrismaEngine() {
	if e == nil || e.cancel == nil {
		return
	}
	e.cancel()
	exitCh := make(chan error)
	go func() {
		exitCh <- e.cmd.Wait()
		close(exitCh)
	}()
	select {
	case <-exitCh:
		// Ignore errors here, since killing the process with a signal
		// will cause Wait() to return an error and there's no cross-platform
		// way to tell it apart from an interesting failure
	case <-time.After(prismaEngineExitTimeout):
		e.log.Warn(fmt.Sprintf("prisma didn't exit after %s, killing", prismaEngineExitTimeout))
		if err := e.forceKillProcess(); err != nil {
			e.log.Error("killing prisma", zap.Error(err))
		}
	}
	e.cmd, e.cancel, e.stderr = nil, nil, nil
}

func (e *Engine) forceKillProcess() error {
	switch runtime.GOOS {
	case "windows":
		return e.cmd.Process.Kill()
	default:
		return e.cmd.Process.Signal(os.Interrupt)
	}
}

func (e *Engine) WaitUntilReady(ctx context.Context) error {
	done := ctx.Done()
	for {
		select {
		case <-done:
			return fmt.Errorf("WaitUntilReady: context cancelled")
		default:
			if e.stderr != nil {
				return e.stderr
			}
			_, err := http.Get(e.url)
			if err != nil {
				time.Sleep(time.Millisecond * 10)
				continue
			}
			return nil
		}
	}
}

func (e *Engine) HealthCheck() error {
	_, err := http.Get(e.url)
	return err
}
