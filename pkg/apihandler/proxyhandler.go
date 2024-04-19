package apihandler

import (
	"bytes"
	"encoding/json"
	"github.com/buger/jsonparser"
	"github.com/gorilla/mux"
	"github.com/wundergraph/wundergraph/pkg/authentication"
	"github.com/wundergraph/wundergraph/pkg/hooks"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strings"
)

func (r *Builder) registerProxyOperation(operation *wgpb.Operation, apiPath string) error {
	return registerProxyOperation(r.router, r.log, r.middlewareClient, operation, apiPath, false)
}

func (i *InternalBuilder) registerProxyOperation(operation *wgpb.Operation, apiPath string) error {
	return registerProxyOperation(i.router, i.log, i.middlewareClient, operation, apiPath, true)
}

func registerProxyOperation(router *mux.Router, log *zap.Logger, middlewareClient *hooks.Client, operation *wgpb.Operation, apiPath string, internal bool) error {
	apiName := apiPath
	if internal {
		apiName = internalPrefix + apiName
	}
	route := router.Path(apiPath).Name(apiName)
	handler := &ProxyHandler{
		log:          log,
		operation:    operation,
		rbacEnforcer: authentication.NewRBACEnforcer(operation),
		hooksClient:  middlewareClient,
	}

	var routeHandler http.Handler
	if !internal {
		routeHandler = ensureRequiresRateLimiter(operation, handler)
		routeHandler = ensureRequiresSemaphore(operation, routeHandler)
		routeHandler = authentication.EnsureRequiresAuthentication(operation, routeHandler)
		routeHandler = intercept(routeHandler, middlewareClient, operation, nil)
	} else {
		routeHandler = handler
	}
	route.Handler(routeHandler)

	log.Debug("registered ProxyHandler",
		zap.String("operation", operation.Path),
		zap.String("Endpoint", apiPath),
		zap.String("method", operation.OperationType.String()),
	)
	return nil
}

type ProxyHandler struct {
	log          *zap.Logger
	operation    *wgpb.Operation
	rbacEnforcer *authentication.RBACEnforcer
	hooksClient  *hooks.Client
}

func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if proceed := h.rbacEnforcer.Enforce(r); !proceed {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	variablesBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(variablesBuf)
	_, err := io.Copy(variablesBuf, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	payload := hooks.OnRequestHookPayload{
		Request: hooks.WunderGraphRequest{
			Method:     r.Method,
			RequestURI: r.URL.String(),
			Headers:    hooks.HeaderSliceToCSV(r.Header),
			OriginBody: variablesBuf.Bytes(),
		},
		OperationName: h.operation.Name,
		OperationType: h.operation.OperationType.String(),
	}
	hookData, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if userBytes := authentication.UserBytesFromContext(r.Context()); userBytes != nil {
		hookData, _ = jsonparser.Set(hookData, userBytes, "__wg", "user")
	}
	buf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(buf)
	proxyPath := strings.TrimPrefix(h.operation.Path, "proxy/")
	out, err := h.hooksClient.DoProxyRequest(r.Context(), hooks.MiddlewareHook(proxyPath), hookData, buf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var response hooks.OnResponseHookResponse
	if err = json.Unmarshal(out.Response, &response); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if response.Cancel {
		http.Error(w, "canceled", http.StatusBadRequest)
		return
	}
	if resp := response.Response; resp != nil {
		w.WriteHeader(resp.StatusCode)
		for k, v := range response.Response.Headers {
			w.Header().Add(k, v)
		}
		_, err = io.Copy(w, io.NopCloser(bytes.NewBuffer(resp.OriginBody)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}
