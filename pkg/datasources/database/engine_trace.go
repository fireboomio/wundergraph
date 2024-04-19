package database

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"net/http"
	"strings"
	"time"
)

type span struct {
	TraceId      string                 `json:"trace_id"`
	SpanId       string                 `json:"span_id"`
	ParentSpanId string                 `json:"parent_span_id"`
	Name         string                 `json:"name"`
	StartTime    spanTime               `json:"start_time"`
	EndTime      spanTime               `json:"end_time"`
	Attributes   map[string]interface{} `json:"attributes"`
}

func (s *span) newSpanContext() jaeger.SpanContext {
	traceId, _ := jaeger.TraceIDFromString(s.TraceId)
	spanId, _ := jaeger.SpanIDFromString(s.SpanId)
	parentSpanId, _ := jaeger.SpanIDFromString(s.ParentSpanId)
	return jaeger.NewSpanContext(traceId, spanId, parentSpanId, true, nil)
}

type spanTime []int64

func (s spanTime) toDateTime() time.Time {
	if len(s) != 2 {
		return time.Time{}
	}
	return time.Unix(s[0], s[1])
}

const (
	traceParentKey    = "traceparent"
	traceParentFormat = "00-%s-%s-01"
)

func addTraceParentHeader(ctx context.Context, req *http.Request) {
	if stringer, ok := opentracing.SpanFromContext(ctx).(fmt.Stringer); ok {
		if splitArr := strings.Split(stringer.String(), ":"); len(splitArr) == 4 {
			req.Header[traceParentKey] = []string{fmt.Sprintf(traceParentFormat, splitArr[0], splitArr[1])}
		}
	}
}
