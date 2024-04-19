package node

import (
	"bytes"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"go.uber.org/zap"
	"net/http"
	"time"
)

func (n *Node) requestLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		debug := n.log.Core().Enabled(zap.DebugLevel)
		var startedAt time.Time
		rl := &requestLogger{writer: w}
		if debug {
			startedAt = time.Now()
			rl.bodyBuf = (*bodyBuffer)(new(bytes.Buffer))
		}

		var spanFunc []func(opentracing.Span)
		r, callback := logging.StartTraceRequest(r)
		defer func() { callback(append(spanFunc, logging.SpanWithLogResponse(rl.buildResponse()))...) }()

		next.ServeHTTP(rl, r)
		if debug {
			var errMsg string
			if rl.statusCode >= http.StatusBadRequest {
				errMsg = fmt.Sprintf(" %s", rl.bodyBuf.String())
			}
			n.log.Debug(fmt.Sprintf("%s %s %d %v%s", r.Method, r.RequestURI, rl.statusCode, time.Since(startedAt), errMsg))
			if rl.statusCode == http.StatusOK && !logging.NoneStreamContentType(rl.Header()) {
				spanFunc = append(spanFunc, func(span opentracing.Span) {
					span.SetTag("stream-output", rl.bodyBuf.String())
				})
			}
		}
	})
}

type (
	requestLogger struct {
		statusCode int
		writer     http.ResponseWriter
		bodyBuf    *bodyBuffer
	}
	bodyBuffer bytes.Buffer
)

func (b *bodyBuffer) Read(p []byte) (n int, err error) {
	return (*bytes.Buffer)(b).Read(p)
}

func (b *bodyBuffer) Write(p []byte) (n int, err error) {
	return (*bytes.Buffer)(b).Write(p)
}

func (b *bodyBuffer) String() string {
	return (*bytes.Buffer)(b).String()
}

func (b *bodyBuffer) Close() error {
	(*bytes.Buffer)(b).Reset()
	return nil
}

func (rl *requestLogger) buildResponse() *http.Response {
	return &http.Response{
		StatusCode: rl.statusCode,
		Header:     rl.Header(),
		Body:       rl.bodyBuf,
	}
}

func (rl *requestLogger) Header() http.Header {
	return rl.writer.Header()
}

func (rl *requestLogger) WriteHeader(statusCode int) {
	if rl.statusCode == 0 {
		rl.writer.WriteHeader(statusCode)
	}
	rl.statusCode = statusCode
}

func (rl *requestLogger) Write(b []byte) (int, error) {
	if rl.statusCode == 0 {
		rl.statusCode = http.StatusOK
	}
	if rl.bodyBuf != nil {
		defer func() { _, _ = rl.bodyBuf.Write(b) }()
	}
	return rl.writer.Write(b)
}

func (rl *requestLogger) Flush() {
	if fw, ok := rl.writer.(http.Flusher); ok {
		fw.Flush()
	}
}

func (n *Node) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if e := recover(); e != nil {
				var err error
				if e == http.ErrAbortHandler {
					panic(e)
				}
				switch t := e.(type) {
				case error:
					err = t
				default:
					err = fmt.Errorf("%v", r)
				}
				n.log.Error("panic appeared", zap.Error(err))
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
