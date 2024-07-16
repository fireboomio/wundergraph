package apihandler

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/buger/jsonparser"
	json "github.com/json-iterator/go"
	"github.com/wundergraph/wundergraph/pkg/authentication"
	"github.com/wundergraph/wundergraph/pkg/hooks"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"golang.org/x/exp/maps"
	"io"
	"net/http"
	"net/http/httputil"
)

type responseInterceptor struct {
	writer     http.ResponseWriter
	bodyBuffer *bytes.Buffer
	statusCode int
	flushed    bool
}

func (ri *responseInterceptor) Header() http.Header {
	return ri.writer.Header()
}

func (ri *responseInterceptor) WriteHeader(statusCode int) {
	ri.statusCode = statusCode
}

func (ri *responseInterceptor) Write(b []byte) (int, error) {
	if ri.flushed {
		defer func() { _, _ = ri.writer.Write(b) }()
	}
	if ri.statusCode == 0 {
		ri.statusCode = http.StatusOK
	}
	return ri.bodyBuffer.Write(b)
}

func (ri *responseInterceptor) Flush() {
	if fw, ok := ri.writer.(http.Flusher); ok {
		ri.flushed = true
		fw.Flush()
	}
}

func (ri *responseInterceptor) rewriteBodyBytes(b []byte) {
	ri.bodyBuffer.Reset()
	_, _ = ri.bodyBuffer.Write(b)
}

func (ri *responseInterceptor) writeToOrigin() {
	if ri.flushed {
		return
	}

	ri.writer.WriteHeader(ri.statusCode)
	_, _ = ri.bodyBuffer.WriteTo(ri.writer)
}

func intercept(handler http.Handler, hooksClient *hooks.Client, operation *wgpb.Operation, argsAllowList []string) http.Handler {
	hooksConfig := operation.HooksConfiguration
	metaData := &OperationMetaData{OperationName: operation.Name, OperationType: operation.OperationType, ArgsAllowList: argsAllowList}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		if hooksConfig.HttpTransportBeforeRequest {
			buf := pool.GetBytesBuffer()
			defer pool.PutBytesBuffer(buf)

			if r, err = handleBeforeRequestHook(r, metaData, buf, hooksClient); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		var respInterceptor *responseInterceptor
		if hooksConfig.HttpTransportAfterResponse {
			respInterceptor = &responseInterceptor{writer: w, bodyBuffer: pool.GetBytesBuffer()}
			defer pool.PutBytesBuffer(respInterceptor.bodyBuffer)
			w = respInterceptor
		}
		handler.ServeHTTP(w, r)
		if hooksConfig.HttpTransportAfterResponse {
			buf := pool.GetBytesBuffer()
			defer pool.PutBytesBuffer(buf)

			if err = handleAfterResponseHook(respInterceptor, r, metaData, buf, hooksClient); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			respInterceptor.writeToOrigin()
		}
	})
}

func handleBeforeRequestHook(r *http.Request, metaData *OperationMetaData, buf *bytes.Buffer, hooksClient *hooks.Client) (*http.Request, error) {
	var bodyBytes []byte
	if r.Body != nil && logging.NoneMultipartContentType(r) {
		dumpRequestBytes, err := httputil.DumpRequest(r, true)
		if err != nil {
			return nil, err
		}
		copyRequest, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(dumpRequestBytes)))
		if err != nil {
			return nil, err
		}
		if bodyBytes, err = io.ReadAll(copyRequest.Body); err != nil {
			return nil, err
		}
	}
	payload := hooks.OnRequestHookPayload{
		Request: hooks.WunderGraphRequest{
			Method:     r.Method,
			RequestURI: r.URL.String(),
			Headers:    hooks.HeaderSliceToCSV(r.Header),
			OriginBody: bodyBytes,
		},
		OperationName: metaData.OperationName,
		OperationType: metaData.GetOperationTypeString(),
		ArgsAllowList: metaData.ArgsAllowList,
	}
	payload.Request.Headers["X-Real-Ip"] = r.RemoteAddr
	hookData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if userBytes := authentication.UserBytesFromContext(r.Context()); userBytes != nil {
		hookData, _ = jsonparser.Set(hookData, userBytes, "__wg", "user")
	}

	out, err := hooksClient.DoGlobalRequest(r.Context(), hooks.HttpTransportBeforeRequest, hookData, buf)
	if err != nil {
		return nil, err
	}

	var response hooks.OnRequestHookResponse
	err = json.Unmarshal(out.Response, &response)
	if err != nil {
		return nil, err
	}
	if response.Skip {
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		return r, nil
	}
	if response.Cancel {
		return nil, errors.New("canceled")
	}
	if modifiedRequest := response.Request; modifiedRequest != nil {
		originBodyReader, originIsMultipart := r.Body, !logging.NoneMultipartContentType(r)
		if r, err = http.NewRequestWithContext(r.Context(), modifiedRequest.Method, modifiedRequest.RequestURI,
			bytes.NewReader(modifiedRequest.OriginBody)); err != nil {
			return nil, err
		}
		if originIsMultipart && originBodyReader != nil {
			r.Body = originBodyReader
		}
		r.Header = hooks.HeaderCSVToSlice(modifiedRequest.Headers)
	}
	return r, nil
}

func handleAfterResponseHook(w *responseInterceptor, r *http.Request, metaData *OperationMetaData, buf *bytes.Buffer, hooksClient *hooks.Client) error {
	var bodyBytes []byte
	if logging.NoneStreamContentType(w.Header()) {
		bodyBytes = w.bodyBuffer.Bytes()
	}
	payload := hooks.OnResponseHookPayload{
		Response: hooks.WunderGraphResponse{
			Method:     r.Method,
			RequestURI: r.URL.String(),
			Headers:    hooks.HeaderSliceToCSV(w.Header()),
			OriginBody: bodyBytes,
			StatusCode: w.statusCode,
			Status:     http.StatusText(w.statusCode),
		},
		OperationName: metaData.OperationName,
		OperationType: metaData.GetOperationTypeString(),
	}
	hookData, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if userBytes := authentication.UserBytesFromContext(r.Context()); userBytes != nil {
		hookData, _ = jsonparser.Set(hookData, userBytes, "__wg", "user")
	}

	out, err := hooksClient.DoGlobalRequest(r.Context(), hooks.HttpTransportAfterResponse, hookData, buf)
	if err != nil || w.flushed {
		return err
	}

	var response hooks.OnResponseHookResponse
	err = json.Unmarshal(out.Response, &response)
	if err != nil {
		return err
	}
	if response.Skip {
		return nil
	}
	if response.Cancel {
		return errors.New("canceled")
	}
	if modifiedResponse := response.Response; modifiedResponse != nil {
		w.WriteHeader(modifiedResponse.StatusCode)
		w.rewriteBodyBytes(modifiedResponse.OriginBody)
		headers := w.Header()
		maps.Clear(headers)
		for k, v := range hooks.HeaderCSVToSlice(modifiedResponse.Headers) {
			headers[k] = v
		}
	}
	return nil
}
