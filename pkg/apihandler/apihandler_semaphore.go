package apihandler

import (
	"context"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"golang.org/x/sync/semaphore"
	"net/http"
	"time"
)

func ensureRequiresSemaphore(operation *wgpb.Operation, handler http.Handler) http.Handler {
	operationSemaphore := operation.Semaphore
	if operationSemaphore == nil || !operationSemaphore.Enabled {
		return handler
	}

	weighted := semaphore.NewWeighted(operationSemaphore.Tickets)
	timeout := time.Duration(operationSemaphore.TimeoutSeconds) * time.Second
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := weighted.Acquire(ctx, 1); err != nil {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		handler.ServeHTTP(w, r)
		weighted.Release(1)
	})
}
