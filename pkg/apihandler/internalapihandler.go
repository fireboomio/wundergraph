package apihandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/wundergraph/wundergraph/pkg/apicache"
	"github.com/wundergraph/wundergraph/pkg/datasources/database"
	"github.com/wundergraph/wundergraph/pkg/eventbus"
	"golang.org/x/exp/maps"
	"io"
	"net/http"

	"github.com/buger/jsonparser"
	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/astvalidation"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"

	"github.com/wundergraph/wundergraph/pkg/authentication"
	"github.com/wundergraph/wundergraph/pkg/engineconfigloader"
	"github.com/wundergraph/wundergraph/pkg/hooks"
	"github.com/wundergraph/wundergraph/pkg/interpolate"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"github.com/wundergraph/wundergraph/pkg/postresolvetransform"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
)

const internalPrefix = "/internal"

type InternalBuilder struct {
	cache            apicache.Cache
	ctx              context.Context
	pool             *pool.Pool
	log              *zap.Logger
	loader           *engineconfigloader.EngineConfigLoader
	api              *Api
	planConfig       plan.Configuration
	resolver         *resolve.Resolver
	definition       *ast.Document
	router           *mux.Router
	renameTypeNames  []resolve.RenameTypeName
	middlewareClient *hooks.Client
}

func NewInternalBuilder(pool *pool.Pool, log *zap.Logger, hooksClient *hooks.Client) *InternalBuilder {
	return &InternalBuilder{
		pool:             pool,
		log:              log,
		middlewareClient: hooksClient,
	}
}

func (i *InternalBuilder) BuildAndMountInternalApiHandler(ctx context.Context, router *mux.Router, api *Api) (streamClosers []chan struct{}, err error) {
	i.api = api
	i.resolver = resolve.New(ctx, resolve.NewFetcher(true), true)
	i.router = router.PathPrefix("/internal").Subrouter()

	i.log.Debug("configuring API",
		zap.Int("numOfOperations", len(api.Operations)),
	)

	for _, operation := range api.Operations {
		err = i.registerOperation(operation, ctx)
		if err != nil {
			i.log.Error("registerInternalOperation", zap.String("operation", operation.Path), zap.Error(err))
		}
	}

	i.router.Methods(http.MethodPost).Path("/notifyTransactionFinish").Handler(http.HandlerFunc(i.notifyTransactionFinish))

	i.ctx = ctx
	eventbus.EnsureEventSubscribe(i)
	return streamClosers, err
}

func (i *InternalBuilder) notifyTransactionFinish(w http.ResponseWriter, r *http.Request) {
	bodyBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(bodyBuf)
	if _, err := io.Copy(bodyBuf, r.Body); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	modifiedCtx, ok := database.AddTransactionManagerContext(r, i.cache)
	if !ok {
		http.Error(w, "transactionManager not found", http.StatusBadRequest)
		return
	}

	var err error
	if errorBody := gjson.GetBytes(bodyBuf.Bytes(), "error"); errorBody.Exists() {
		err = errors.New(errorBody.String())
	}
	if !database.NotifyTransactionFinish(r.WithContext(modifiedCtx), i.cache, err) {
		http.Error(w, "transactionManager not found any started", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (i *InternalBuilder) registerOperation(operation *wgpb.Operation, ctx context.Context) error {

	operationSchema, ok := i.api.OperationSchemas[operation.Path]
	if !ok {
		return fmt.Errorf("not found schema for %s", operation.Path)
	}

	apiPath := OperationApiPath(operation.Path)
	apiName := internalPrefix + apiPath

	switch operation.Engine {
	case wgpb.OperationExecutionEngine_ENGINE_FUNCTION:
		return i.registerFunctionOperation(operation, apiPath, operationSchema)
	case wgpb.OperationExecutionEngine_ENGINE_PROXY:
		return i.registerProxyOperation(operation, apiPath)
	}

	shared := i.pool.GetShared(ctx, i.planConfig, pool.Config{
		RenameTypeNames: i.renameTypeNames,
	})

	shared.Doc.Input.ResetInputString(operation.Content)
	shared.Parser.Parse(shared.Doc, shared.Report)

	if shared.Report.HasErrors() {
		return fmt.Errorf(ErrMsgOperationParseFailed, shared.Report)
	}

	shared.Normalizer.NormalizeNamedOperation(shared.Doc, i.definition, []byte(operation.Name), shared.Report)
	if shared.Report.HasErrors() {
		return fmt.Errorf(ErrMsgOperationNormalizationFailed, shared.Report)
	}

	state := shared.Validation.Validate(shared.Doc, i.definition, shared.Report)
	if state != astvalidation.Valid {
		return fmt.Errorf(ErrMsgOperationValidationFailed, shared.Report)
	}

	preparedPlan := shared.Planner.Plan(shared.Doc, i.definition, operation.Name, shared.Report)
	shared.Postprocess.Process(preparedPlan)

	postResolveTransformer := postresolvetransform.NewTransformer(operation.PostResolveTransformations)
	postResolveTransformer.SetGraphqlTransformEnabled(operation.GraphqlTransformEnabled)
	hooksPipelineCommonConfig := hooks.PipelineConfig{
		Client:             i.middlewareClient,
		Operation:          operation,
		Transformer:        postResolveTransformer,
		Logger:             i.log,
		GeneratedVariables: getGeneratedVariables(shared.Doc),
	}

	queryParamsAllowList := maps.Keys(operationSchema.InternalVariables.Value.Properties)
	switch operation.OperationType {
	case wgpb.OperationType_QUERY,
		wgpb.OperationType_MUTATION:
		p, ok := preparedPlan.(*plan.SynchronousResponsePlan)
		if !ok {
			return nil
		}

		extractedVariables := make([]byte, len(shared.Doc.Input.Variables))
		copy(extractedVariables, shared.Doc.Input.Variables)

		hooksPipelineConfig := hooks.SynchronousOperationPipelineConfig{
			PipelineConfig: hooksPipelineCommonConfig,
			Resolver:       i.resolver,
			Plan:           p,
		}
		hooksPipeline := hooks.NewSynchonousOperationPipeline(hooksPipelineConfig)

		handler := &InternalApiHandler{
			cache:                i.cache,
			preparedPlan:         p,
			operation:            operation,
			extractedVariables:   extractedVariables,
			log:                  i.log,
			resolver:             i.resolver,
			renameTypeNames:      i.renameTypeNames,
			hooksPipeline:        hooksPipeline,
			queryParamsAllowList: queryParamsAllowList,
		}

		i.router.Methods(http.MethodPost).Path(apiPath).Handler(handler).Name(apiName)
	case wgpb.OperationType_SUBSCRIPTION:
		p, ok := preparedPlan.(*plan.SubscriptionResponsePlan)
		if !ok {
			return nil
		}

		extractedVariables := make([]byte, len(shared.Doc.Input.Variables))
		copy(extractedVariables, shared.Doc.Input.Variables)

		hooksPipelineConfig := hooks.SubscriptionOperationPipelineConfig{
			PipelineConfig: hooksPipelineCommonConfig,
			Resolver:       i.resolver,
			Plan:           p,
		}
		hooksPipeline := hooks.NewSubscriptionOperationPipeline(hooksPipelineConfig)

		handler := &InternalSubscriptionApiHandler{
			cache:                i.cache,
			preparedPlan:         p,
			operation:            operation,
			extractedVariables:   extractedVariables,
			log:                  i.log,
			resolver:             i.resolver,
			renameTypeNames:      i.renameTypeNames,
			hooksPipeline:        hooksPipeline,
			queryParamsAllowList: queryParamsAllowList,
		}

		i.router.Methods(http.MethodPost).Path(apiPath).Handler(handler).Name(apiName)
	}

	return nil
}

func (i *InternalBuilder) registerFunctionOperation(operation *wgpb.Operation, apiPath string, operationSchema *OperationSchema) error {
	stringInterpolator, err := interpolate.NewOpenapi3StringInterpolator(operationSchema.Variables, operationSchema.Definitions)
	if err != nil {
		return err
	}

	route := i.router.Methods(http.MethodPost).Path(apiPath).Name(internalPrefix + apiPath)

	handler := &FunctionHandler{
		operation:                  operation,
		log:                        i.log,
		rbacEnforcer:               authentication.NewRBACEnforcer(operation),
		hooksClient:                i.middlewareClient,
		queryParamsAllowList:       maps.Keys(stringInterpolator.PropertiesTypeMap),
		openapi3StringInterpolator: stringInterpolator,
		operationSchema:            operationSchema,
		postResolveTransformer:     postresolvetransform.NewTransformer(operation.PostResolveTransformations),
	}

	if operation.LiveQueryConfig != nil && operation.LiveQueryConfig.Enabled {
		handler.liveQuery = liveQueryConfig{
			enabled:                true,
			pollingIntervalSeconds: operation.LiveQueryConfig.PollingIntervalSeconds,
		}
	}

	route.Handler(handler)
	return nil
}

func (i *InternalBuilder) Close() error {
	i.router = nil
	i.api = nil
	i.loader = nil
	i.planConfig.Fields = nil
	i.planConfig.Types = nil
	i.planConfig.DataSources = nil
	i.resolver = nil
	i.definition = nil
	i.renameTypeNames = nil
	return nil
}

type InternalApiHandler struct {
	cache              apicache.Cache
	preparedPlan       *plan.SynchronousResponsePlan
	operation          *wgpb.Operation
	extractedVariables []byte
	log                *zap.Logger
	resolver           *resolve.Resolver
	renameTypeNames    []resolve.RenameTypeName
	hooksPipeline      *hooks.SynchronousOperationPipeline

	queryParamsAllowList []string
}

func withUserContext(r *http.Request, body []byte) *http.Request {
	userBytes, _, _, _ := jsonparser.Get(body, "__wg", "user")
	if userBytes != nil {
		var user authentication.User
		_ = json.Unmarshal(userBytes, &user)
		r = r.WithContext(context.WithValue(r.Context(), "user", &user))
		r = r.WithContext(context.WithValue(r.Context(), "userBytes", userBytes))
	}
	return r
}

func (h *InternalApiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := r.Header.Get(logging.RequestIDHeader)
	requestLogger := h.log.With(logging.WithRequestID(reqID))
	r = r.WithContext(context.WithValue(r.Context(), logging.RequestIDKey{}, reqID))

	r = setOperationMetaData(r, h.operation, h.queryParamsAllowList, h.cache)

	bodyBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(bodyBuf)
	variables, applyContext := ensureMultipartFormData(r, h.operation.MultipartForms)
	if applyContext != nil {
		_, _ = bodyBuf.Write(variables)
	} else {
		_, err := io.Copy(bodyBuf, r.Body)
		if err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
	}

	body := bodyBuf.Bytes()

	// internal requests transmit the client request as a JSON object
	// this makes it possible to expose the original client request to hooks triggered by internal requests
	clientRequest, err := NewRequestFromWunderGraphClientRequest(r.Context(), body)
	if err != nil {
		requestLogger.Error("InternalApiHandler.ServeHTTP: Could not create request from __wg.clientRequest",
			zap.Error(err),
			zap.String("url", r.RequestURI),
		)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	r = withUserContext(r, body)
	ctx := pool.GetCtx(r, clientRequest, pool.Config{
		RenameTypeNames: h.renameTypeNames,
	})
	if h.operation.GraphqlTransformEnabled {
		ctx.Context = context.WithValue(ctx.Context, resolve.TransformEnabled, true)
	}
	defer pool.PutCtx(ctx)
	if applyContext != nil {
		applyContext(ctx)
	}

	variablesBuf, _, _, _ := jsonparser.Get(body, "input")
	if len(variablesBuf) == 0 {
		ctx.Variables = []byte("{}")
	} else {
		ctx.Variables, _ = handleSpecial(variablesBuf)
	}

	compactBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(compactBuf)
	err = json.Compact(compactBuf, ctx.Variables)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx.Variables = compactBuf.Bytes()

	if len(h.extractedVariables) != 0 {
		ctx.Variables = MergeJsonRightIntoLeft(h.extractedVariables, ctx.Variables)
	}

	ctx.Variables, err = injectVariables(h.operation, r, ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx.Variables = injectWhereInputVariables(h.operation, ctx.Variables)

	buf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(buf)

	resp, err := h.hooksPipeline.Run(ctx, w, r, buf)
	defer database.NotifyTransactionFinish(r, h.cache, err)
	if done := handleOperationErr(requestLogger, err, w, "hooks pipeline failed", h.operation); done {
		return
	}

	if resp.Done {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(resp.Data); err != nil {
		requestLogger.Error("writing response", zap.Error(err))
	}
}

type InternalSubscriptionApiHandler struct {
	cache              apicache.Cache
	preparedPlan       *plan.SubscriptionResponsePlan
	operation          *wgpb.Operation
	extractedVariables []byte
	log                *zap.Logger
	resolver           *resolve.Resolver
	renameTypeNames    []resolve.RenameTypeName
	hooksPipeline      *hooks.SubscriptionOperationPipeline

	queryParamsAllowList []string
}

func (h *InternalSubscriptionApiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := r.Header.Get(logging.RequestIDHeader)
	requestLogger := h.log.With(logging.WithRequestID(reqID))
	r = r.WithContext(context.WithValue(r.Context(), logging.RequestIDKey{}, reqID))

	r = setOperationMetaData(r, h.operation, h.queryParamsAllowList, h.cache)

	bodyBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(bodyBuf)
	_, err := io.Copy(bodyBuf, r.Body)
	if err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	body := bodyBuf.Bytes()

	// internal requests transmit the client request as a JSON object
	// this makes it possible to expose the original client request to hooks triggered by internal requests
	clientRequest, err := NewRequestFromWunderGraphClientRequest(r.Context(), body)
	if err != nil {
		requestLogger.Error("InternalApiHandler.ServeHTTP: Could not create request from __wg.clientRequest",
			zap.Error(err),
			zap.String("url", r.RequestURI),
		)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	ctx := pool.GetCtx(r, clientRequest, pool.Config{
		RenameTypeNames: h.renameTypeNames,
	})
	defer pool.PutCtx(ctx)

	variablesBuf, _, _, _ := jsonparser.Get(body, "input")
	if len(variablesBuf) == 0 {
		ctx.Variables = []byte("{}")
	} else {
		ctx.Variables = variablesBuf
	}

	compactBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(compactBuf)
	err = json.Compact(compactBuf, ctx.Variables)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx.Variables = compactBuf.Bytes()

	if len(h.extractedVariables) != 0 {
		ctx.Variables = MergeJsonRightIntoLeft(ctx.Variables, h.extractedVariables)
	}

	ctx.Variables, err = injectVariables(h.operation, r, ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx.Variables = injectWhereInputVariables(h.operation, ctx.Variables)

	buf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(buf)

	flushWriter, ok := getHooksFlushWriter(ctx, r, w, h.hooksPipeline, h.log)
	if !ok {
		http.Error(w, "Connection not flushable", http.StatusBadRequest)
		return
	}

	flushWriter.postResolveTransformer = postresolvetransform.NewTransformer(h.operation.PostResolveTransformations)
	_, err = h.hooksPipeline.RunSubscription(ctx, flushWriter, r)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// e.g. client closed connection
			return
		}
		// if the deadline is exceeded (e.g. timeout), we don't have to return an HTTP error
		// we've already flushed a response to the client
		requestLogger.Error("ResolveGraphQLSubscription", zap.Error(err))
		return
	}
	flushWriter.Canceled()
}

func NewRequestFromWunderGraphClientRequest(ctx context.Context, body []byte) (*http.Request, error) {
	clientRequest, _, _, _ := jsonparser.Get(body, "__wg", "clientRequest")
	if clientRequest != nil {
		method, err := jsonparser.GetString(body, "__wg", "clientRequest", "method")
		if err != nil {
			return nil, err
		}
		requestURI, err := jsonparser.GetString(body, "__wg", "clientRequest", "requestURI")
		if err != nil {
			return nil, err
		}

		// create a new request from the client request
		// excluding the body because the body is the graphql operation query
		request, err := http.NewRequestWithContext(ctx, method, requestURI, nil)
		if err != nil {
			return nil, err
		}

		err = jsonparser.ObjectEach(body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			request.Header.Set(string(key), string(value))
			return nil
		}, "__wg", "clientRequest", "headers")
		if err != nil {
			return nil, err
		}

		return request, nil
	}

	return nil, nil
}
