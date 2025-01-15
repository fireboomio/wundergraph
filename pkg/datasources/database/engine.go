package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

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

type Engine struct {
	wundergraphDir   string
	queryEnginePath  string
	schemaEnginePath string
	url              string
	cmd              *exec.Cmd
	cancel           func()
	stderr           error
	client           *http.Client
	log              *zap.Logger
}

func NewEngine(client *http.Client, log *zap.Logger, wundergraphDir string) *Engine {
	return &Engine{
		wundergraphDir: wundergraphDir,
		client:         client,
		log:            log,
	}
}

// IntrospectGraphQLSchema 内省GraphQLSchema
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
			defer func() { _ = res.Body.Close() }()
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
	e.cmd.Stderr = &stderrWriter{func(errMsg stderrMsg) { e.stderr = errMsg }}
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
	e.schemaEnginePath = os.Getenv(prismaSchemaEngineEnvKey)
	return
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

func (e *Engine) StopPrismaEngine() {
	if e == nil || e.cancel == nil {
		return
	}
	stopProcess(e.log, e.cmd, e.cancel)
	e.cmd, e.cancel, e.stderr = nil, nil, nil
}

func stopProcess(logger *zap.Logger, cmd *exec.Cmd, cancel func()) {
	cancel()
	exitCh := make(chan error)
	go func() {
		exitCh <- cmd.Wait()
		close(exitCh)
	}()
	select {
	case <-exitCh:
		// Ignore errors here, since killing the process with a signal
		// will cause Wait() to return an error and there's no cross-platform
		// way to tell it apart from an interesting failure
	case <-time.After(prismaEngineExitTimeout):
		logger.Warn(fmt.Sprintf("prisma didn't exit after %s, killing", prismaEngineExitTimeout))
		if err := forceKillProcess(cmd); err != nil {
			logger.Error("killing prisma", zap.Error(err))
		}
	}
}

func forceKillProcess(cmd *exec.Cmd) error {
	switch runtime.GOOS {
	case "windows":
		return cmd.Process.Kill()
	default:
		return cmd.Process.Signal(os.Interrupt)
	}
}
