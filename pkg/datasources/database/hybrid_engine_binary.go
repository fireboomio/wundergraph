//go:build !prisma_cgo
// +build !prisma_cgo

package database

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
)

func NewHybridEngine(client *http.Client, prismaSchema, wundergraphDir string, log *zap.Logger, environmentVariable string) (HybridEngine, error) {
	var envs []string
	if environmentVariable != "" {
		envs = append(envs, fmt.Sprintf("%s=%s", environmentVariable, os.Getenv(environmentVariable)))
	}
	engine := NewEngine(client, log, wundergraphDir)
	err := engine.StartQueryEngine(prismaSchema, envs...)
	if err != nil {
		return nil, err
	}
	return &BinaryEngine{
		engine: engine,
	}, nil
}

type BinaryEngine struct {
	engine *Engine
}

func (e *BinaryEngine) HealthCheck() error {
	return e.engine.HealthCheck()
}

func (e *BinaryEngine) WaitUntilReady(ctx context.Context) error {
	return e.engine.WaitUntilReady(ctx)
}

func (e *BinaryEngine) Close() {
	e.engine.StopPrismaEngine()
}

func (e *BinaryEngine) Execute(ctx context.Context, request []byte, w io.Writer) error {
	return e.engine.Request(ctx, request, w)
}
