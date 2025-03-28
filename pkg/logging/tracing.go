package logging

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap/zapcore"
	"net/http"
	"net/http/httputil"
	"strings"
)

const (
	SpanLogFieldHttpRequest           = "http.request"
	SpanLogFieldHttpResponse          = "http.response"
	spanLogFieldDatasourceInput       = "datasource.input"
	spanLogFieldDatasourceOriginInput = "datasource.input.origin"
	spanLogFieldDatasourceOutput      = "datasource.output"
)

const (
	startTraceRequestKey   = "StartTraceRequest"
	spanWithLogResponseKey = "SpanWithLogResponse"
)

var (
	LogLevel          zapcore.Level
	spanWithEmptySpan = func(...func(opentracing.Span)) {}
)

func SetLogLevel(level zapcore.Level) {
	LogLevel = level
}

func StartTraceContext(ctx, followCtx context.Context, operationName string, startSpanFunc ...func(span opentracing.Span)) (context.Context, func(...func(opentracing.Span))) {
	if !opentracing.IsGlobalTracerRegistered() {
		return ctx, spanWithEmptySpan
	}

	var startOpts []opentracing.StartSpanOption
	if followCtx != nil {
		if followSpan := opentracing.SpanFromContext(followCtx); followSpan != nil {
			startOpts = append(startOpts, opentracing.FollowsFrom(followSpan.Context()))
		}
	}
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, startOpts...)
	for _, item := range startSpanFunc {
		item(span)
	}
	return ctx, func(finishSpanFunc ...func(span opentracing.Span)) {
		finishTrace(span, finishSpanFunc...)
	}
}

func StartTraceRequest(r *http.Request, startSpanFunc ...func(span opentracing.Span)) (*http.Request, func(...func(opentracing.Span))) {
	requestID := RequestIDFromContext(r.Context())
	if !opentracing.IsGlobalTracerRegistered() || requestID == "" {
		return r, spanWithEmptySpan
	}

	var (
		startOpts             []opentracing.StartSpanOption
		injectSpanCtxRequired bool
	)
	if parentSpanCtx, _ := extractSpanContextFromHttpHeaders(r); parentSpanCtx != nil {
		startOpts = append(startOpts, opentracing.ChildOf(parentSpanCtx))
	}
	if parentSpan := opentracing.SpanFromContext(r.Context()); parentSpan != nil {
		startOpts = append(startOpts, opentracing.ChildOf(parentSpan.Context()))
		injectSpanCtxRequired = true
	}
	span := opentracing.StartSpan(r.URL.Path, startOpts...)
	httpUrl, _ := strings.CutPrefix(r.URL.RequestURI(), "?")
	ext.HTTPUrl.Set(span, httpUrl)
	ext.HTTPMethod.Set(span, r.Method)
	requestBodyBytes, _ := httputil.DumpRequest(r, NoneMultipartContentType(r))
	span.LogFields(log.String(SpanLogFieldHttpRequest, string(requestBodyBytes)))
	for _, item := range startSpanFunc {
		item(span)
	}
	if injectSpanCtxRequired {
		injectSpanContextToHttpHeaders(span.Context(), r)
	}
	ctx := opentracing.ContextWithSpan(r.Context(), span)
	if ctx.Value(startTraceRequestKey) == nil {
		ctx = context.WithValue(ctx, startTraceRequestKey, StartTraceRequest)
	}
	if ctx.Value(spanWithLogResponseKey) == nil {
		ctx = context.WithValue(ctx, spanWithLogResponseKey, SpanWithLogResponse)
	}
	r = r.WithContext(ctx)
	return r, func(finishSpanFunc ...func(span opentracing.Span)) {
		if r.Header.Get(RequestIDHeader) == requestID {
			finishTrace(span, finishSpanFunc...)
		}
	}
}

func finishTrace(itemSpan opentracing.Span, spanFunc ...func(span opentracing.Span)) {
	for _, item := range spanFunc {
		item(itemSpan)
	}
	go itemSpan.Finish()
}

func injectSpanContextToHttpHeaders(spanContext opentracing.SpanContext, r *http.Request) {
	_ = opentracing.GlobalTracer().Inject(spanContext, opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
}

func extractSpanContextFromHttpHeaders(r *http.Request) (opentracing.SpanContext, error) {
	return opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
}

func SpanWithLogResponse(resp *http.Response) func(opentracing.Span) {
	var responseBodyBytes []byte
	if opentracing.IsGlobalTracerRegistered() && resp != nil {
		responseBodyBytes, _ = httputil.DumpResponse(resp, NoneStreamContentType(resp.Header))
	}
	return func(span opentracing.Span) {
		if resp == nil {
			return
		}
		statusCode := uint16(resp.StatusCode)
		ext.HTTPStatusCode.Set(span, statusCode)
		if statusCode >= 400 {
			ext.Error.Set(span, true)
		}
		if len(responseBodyBytes) > 0 {
			span.LogFields(log.String(SpanLogFieldHttpResponse, string(responseBodyBytes)))
		}
	}
}

func SpanWithLogError(err error) func(opentracing.Span) {
	return func(span opentracing.Span) {
		if err != nil {
			ext.LogError(span, err)
		}
	}
}

func SpanWithLogInput(input []byte) func(opentracing.Span) {
	return func(span opentracing.Span) {
		if opentracing.IsGlobalTracerRegistered() && len(input) > 0 {
			span.LogFields(log.String(spanLogFieldDatasourceInput, string(input)))
		}
	}
}

func SpanWithLogOriginInput(originInput []byte) func(opentracing.Span) {
	return func(span opentracing.Span) {
		if opentracing.IsGlobalTracerRegistered() && len(originInput) > 0 {
			span.LogFields(log.String(spanLogFieldDatasourceOriginInput, string(originInput)))
		}
	}
}

func SpanWithLogOutput(output []byte) func(opentracing.Span) {
	return func(span opentracing.Span) {
		if opentracing.IsGlobalTracerRegistered() && len(output) > 0 {
			span.LogFields(log.String(spanLogFieldDatasourceOutput, string(output)))
		}
	}
}
