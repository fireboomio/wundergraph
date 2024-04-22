package apihandler

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/PaesslerAG/gval"
	"github.com/getkin/kin-openapi/openapi3"
	jsonIterator "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/wundergraph/wundergraph/pkg/customhttpclient"
	"github.com/wundergraph/wundergraph/pkg/datasources/database"
	"github.com/wundergraph/wundergraph/pkg/eventbus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/cespare/xxhash"
	"github.com/dgraph-io/ristretto"
	"github.com/gorilla/mux"
	"github.com/gorilla/securecookie"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-uuid"
	"github.com/mattbaird/jsonpatch"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/astvalidation"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/datasource/introspection_datasource"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/pkg/graphql"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"github.com/wundergraph/graphql-go-tools/pkg/operationreport"

	"github.com/wundergraph/wundergraph/internal/unsafebytes"
	"github.com/wundergraph/wundergraph/pkg/apicache"
	"github.com/wundergraph/wundergraph/pkg/authentication"
	"github.com/wundergraph/wundergraph/pkg/engineconfigloader"
	"github.com/wundergraph/wundergraph/pkg/graphiql"
	"github.com/wundergraph/wundergraph/pkg/hooks"
	"github.com/wundergraph/wundergraph/pkg/inputvariables"
	"github.com/wundergraph/wundergraph/pkg/interpolate"
	"github.com/wundergraph/wundergraph/pkg/loadvariable"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"github.com/wundergraph/wundergraph/pkg/postresolvetransform"
	"github.com/wundergraph/wundergraph/pkg/s3uploadclient"
	"github.com/wundergraph/wundergraph/pkg/webhookhandler"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
)

const (
	WgCacheHeader           = "X-Wg-Cache"
	WgInternalApiCallHeader = "X-WG-Internal-GraphQL-API"

	WgPrefix             = "wg_"
	WgVariables          = WgPrefix + "variables"
	WgLiveParam          = WgPrefix + "live"
	WgJsonPatchParam     = WgPrefix + "json_patch"
	WgSseParam           = WgPrefix + "sse"
	WgSubscribeOnceParam = WgPrefix + "subscribe_once"
)

type WgRequestParams struct {
	UseJsonPatch  bool
	UseSse        bool
	SubscribeOnce bool
}

func NewWgRequestParams(url *url.URL) WgRequestParams {
	q := url.Query()
	return WgRequestParams{
		UseJsonPatch:  q.Has(WgJsonPatchParam),
		UseSse:        q.Has(WgSseParam),
		SubscribeOnce: q.Has(WgSubscribeOnceParam),
	}
}

type Builder struct {
	ctx      context.Context
	router   *mux.Router
	loader   *engineconfigloader.EngineConfigLoader
	api      *Api
	resolver *resolve.Resolver
	pool     *pool.Pool

	middlewareClient *hooks.Client

	definition *ast.Document

	log *zap.Logger

	planConfig plan.Configuration

	cache             *apicache.CacheStruct
	authCache         *ristretto.Cache
	authRequiredPaths map[string]bool

	insecureCookies     bool
	forceHttpsRedirects bool
	enableDebugMode     bool
	enableIntrospection bool
	devMode             bool
	enableCSRFProtect   bool // 是否开启csrf保护

	renameTypeNames []resolve.RenameTypeName

	githubAuthDemoClientID     string
	githubAuthDemoClientSecret string
}

type BuilderConfig struct {
	InsecureCookies            bool
	ForceHttpsRedirects        bool
	EnableDebugMode            bool
	EnableIntrospection        bool
	GitHubAuthDemoClientID     string
	GitHubAuthDemoClientSecret string
	DevMode                    bool
	EnableCSRFProtect          bool // 是否开启csrf保护
}

func NewBuilder(pool *pool.Pool, log *zap.Logger, hooksClient *hooks.Client, config BuilderConfig) *Builder {
	return &Builder{
		log:                        log,
		pool:                       pool,
		insecureCookies:            config.InsecureCookies,
		middlewareClient:           hooksClient,
		forceHttpsRedirects:        config.ForceHttpsRedirects,
		enableDebugMode:            config.EnableDebugMode,
		enableIntrospection:        config.EnableIntrospection,
		githubAuthDemoClientID:     config.GitHubAuthDemoClientID,
		githubAuthDemoClientSecret: config.GitHubAuthDemoClientSecret,
		devMode:                    config.DevMode,
		enableCSRFProtect:          config.EnableCSRFProtect,
	}
}

func (r *Builder) BuildAndMountApiHandler(ctx context.Context, router *mux.Router, api *Api) (streamClosers []chan struct{}, err error) {
	r.api = api
	r.router = r.createSubRouter(router)

	for _, webhook := range api.Webhooks {
		err = r.registerWebhook(webhook)
		if err != nil {
			r.log.Error("register webhook", zap.Error(err))
		}
	}

	r.resolver = resolve.New(ctx, resolve.NewFetcher(true), true)

	if r.enableIntrospection {
		introspectionFactory, err := introspection_datasource.NewIntrospectionConfigFactory(r.definition)
		if err != nil {
			return streamClosers, err
		}
		fieldConfigs := introspectionFactory.BuildFieldConfigurations()
		r.planConfig.Fields = append(r.planConfig.Fields, fieldConfigs...)
		dataSource := introspectionFactory.BuildDataSourceConfiguration()
		r.planConfig.DataSources = append(r.planConfig.DataSources, dataSource)
	}

	// limiter := rate.NewLimiter(rate.Every(time.Second), 10)

	r.log.Debug("configuring API",
		zap.Int("numOfOperations", len(api.Operations)),
	)

	// Redirect from old-style URL, for temporary backwards compatibility
	r.router.MatcherFunc(func(r *http.Request, rm *mux.RouteMatch) bool {
		components := strings.Split(r.URL.Path, "/")
		return len(components) > 2 && components[2] == "main"
	}).HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		components := strings.Split(req.URL.Path, "/")
		//prefix := strings.Join(components[:3], "/")
		//const format = "URLs with the %q prefix are deprecated and will be removed in a future release, " +
		//	"see https://github.com/wundergraph/wundergraph/blob/main/docs/migrations/sdk-0.122.0-0.123.0.md"
		//r.log.Warn(fmt.Sprintf(format, prefix), zap.String("URL", req.URL.Path))
		req.URL.Path = "/" + strings.Join(components[3:], "/")
		r.router.ServeHTTP(w, req)
	})

	if len(api.Hosts) > 0 {
		r.router.Use(func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for i := range api.Hosts {
					if r.Host == api.Hosts[i] || api.Hosts[i] == "*" {
						handler.ServeHTTP(w, r)
						return
					}
				}
				http.Error(w, fmt.Sprintf("Host not found: %s", r.Host), http.StatusNotFound)
			})
		})
	}

	if r.enableDebugMode {
		r.router.Use(logRequestMiddleware(os.Stderr))
	}

	if err := r.registerAuth(r.insecureCookies); err != nil {
		if !r.devMode {
			// If authentication fails in production, consider this a fatal error
			return nil, err
		}
		r.log.Error("configuring auth", zap.Error(err))
	}

	for _, s3Provider := range api.S3UploadConfiguration {
		r.registerS3UploadClient(s3Provider)
	}

	for _, operation := range api.Operations {
		err = r.registerOperation(operation, ctx)
		if err != nil {
			r.log.Error("registerOperation", zap.String("operation", operation.Path), zap.Error(err))
		}
	}

	for _, operationName := range api.InvalidOperationNames {
		r.registerInvalidOperation(operationName)
	}

	if api.EnableGraphqlEndpoint {
		graphqlHandler := &GraphQLHandler{
			ctx:             ctx,
			planConfig:      r.planConfig,
			definition:      r.definition,
			resolver:        r.resolver,
			log:             r.log,
			pool:            r.pool,
			sf:              &singleflight.Group{},
			prepared:        map[uint64]planWithExtractedVariables{},
			preparedMux:     &sync.RWMutex{},
			renameTypeNames: r.renameTypeNames,
		}
		apiPath := "/graphql"
		r.router.Methods(http.MethodPost, http.MethodOptions).Path(apiPath).Handler(graphqlHandler)
		r.log.Debug("registered GraphQLHandler",
			zap.String("method", http.MethodPost),
			zap.String("path", apiPath),
		)

		graphqlPlaygroundHandler := &GraphQLPlaygroundHandler{
			ctx:     ctx,
			log:     r.log,
			html:    graphiql.GetGraphiqlPlaygroundHTML(),
			nodeUrl: api.Options.PublicNodeUrl,
		}
		r.router.Methods(http.MethodGet, http.MethodOptions).Path(apiPath).Handler(graphqlPlaygroundHandler)
		r.log.Debug("registered GraphQLPlaygroundHandler",
			zap.String("method", http.MethodGet),
			zap.String("path", apiPath),
		)

	}

	r.ctx = ctx
	eventbus.EnsureEventSubscribe(r)
	return streamClosers, err
}

// returns a middleware that logs all requests to the given io.Writer
func logRequestMiddleware(logger io.Writer) mux.MiddlewareFunc {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			logBody := logging.NoneMultipartContentType(request)
			suffix := ""
			if !logBody {
				suffix = "<body omitted>"
			}
			requestDump, err := httputil.DumpRequest(request, logBody)
			if err == nil {
				_, _ = fmt.Fprintf(logger,
					"\n\n--- ClientRequest start ---\n\n%s%s\n\n\n\n--- ClientRequest end ---\n\n",
					string(requestDump), suffix)
			}
			handler.ServeHTTP(w, request)
		})
	}
}

func (r *Builder) createSubRouter(router *mux.Router) *mux.Router {
	route := router.NewRoute()

	r.log.Debug("create sub router")
	return route.Subrouter()
}

func (r *Builder) registerS3UploadClient(s3Provider *wgpb.S3UploadConfiguration) {
	profiles := make(map[string]*s3uploadclient.UploadProfile, len(s3Provider.UploadProfiles))
	for name, profile := range s3Provider.UploadProfiles {
		profiles[name] = &s3uploadclient.UploadProfile{
			RequireAuthentication: profile.RequireAuthentication,
			MaxFileSizeBytes:      int(profile.MaxAllowedUploadSizeBytes),
			MaxAllowedFiles:       int(profile.MaxAllowedFiles),
			AllowedMimeTypes:      append([]string(nil), profile.AllowedMimeTypes...),
			AllowedFileExtensions: append([]string(nil), profile.AllowedFileExtensions...),
			MetadataJSONSchema:    profile.MetadataJSONSchema,
			UsePreUploadHook:      profile.Hooks.PreUpload,
			UsePostUploadHook:     profile.Hooks.PostUpload,
		}
	}
	s3, err := s3uploadclient.NewS3UploadClient(loadvariable.String(s3Provider.Endpoint),
		s3uploadclient.Options{
			Logger:          r.log,
			BucketName:      loadvariable.String(s3Provider.BucketName),
			BucketLocation:  loadvariable.String(s3Provider.BucketLocation),
			AccessKeyID:     loadvariable.String(s3Provider.AccessKeyID),
			SecretAccessKey: loadvariable.String(s3Provider.SecretAccessKey),
			UseSSL:          s3Provider.UseSSL,
			Profiles:        profiles,
			HooksClient:     r.middlewareClient,
			Name:            s3Provider.Name,
		},
	)
	if err != nil {
		r.log.Error("registerS3UploadClient", zap.String("storage", s3Provider.Name), zap.Error(err))
	} else {
		s3Path := fmt.Sprintf("/s3/%s/upload", s3Provider.Name)
		r.router.Handle(s3Path, http.HandlerFunc(s3.UploadFile)).Name(s3Path)
		r.log.Debug("registerS3UploadClient", zap.String("storage", s3Provider.Name), zap.String("endpoint", s3Path))
	}
}

func (r *Builder) registerWebhook(config *wgpb.WebhookConfiguration) error {
	handler, err := webhookhandler.New(config, r.api.Options.ServerUrl, r.log)
	if err != nil {
		return err
	}
	webhookPath := fmt.Sprintf("/webhooks/%s", config.Name)
	r.router.
		Methods(http.MethodPost, http.MethodGet).
		Path(webhookPath).
		Handler(handler)
	return nil
}

func OperationApiPath(name string) string {
	return fmt.Sprintf("/operations/%s", name)
}

func (r *Builder) registerInvalidOperation(name string) {
	apiPath := OperationApiPath(name)
	route := r.router.Methods(http.MethodGet, http.MethodPost, http.MethodOptions).Path(apiPath).Name(apiPath)
	route.Handler(&EndpointUnavailableHandler{
		OperationName: name,
		Logger:        r.log,
	})
	r.log.Warn("EndpointUnavailableHandler",
		zap.String("Operation", name),
		zap.String("Endpoint", apiPath),
		zap.String("Help", "This operation is invalid. Please, check the logs"),
	)
}

func (r *Builder) registerOperation(operation *wgpb.Operation, ctx context.Context) error {
	if operation.Internal {
		return nil
	}

	operationSchema, ok := r.api.OperationSchemas[operation.Path]
	if !ok {
		return fmt.Errorf("not found schema for %s", operation.Path)
	}

	apiPath := OperationApiPath(operation.Path)

	switch operation.Engine {
	case wgpb.OperationExecutionEngine_ENGINE_FUNCTION:
		return r.registerFunctionOperation(operation, apiPath, operationSchema)
	case wgpb.OperationExecutionEngine_ENGINE_PROXY:
		return r.registerProxyOperation(operation, apiPath)
	}

	var (
		operationIsConfigured bool
	)

	defer func() {
		if !operationIsConfigured {
			r.registerInvalidOperation(operation.Path)
		}
	}()

	shared := r.pool.GetShared(ctx, r.planConfig, pool.Config{})

	shared.Doc.Input.ResetInputString(operation.Content)
	shared.Parser.Parse(shared.Doc, shared.Report)

	if shared.Report.HasErrors() {
		return fmt.Errorf(ErrMsgOperationParseFailed, shared.Report)
	}

	shared.Normalizer.NormalizeNamedOperation(shared.Doc, r.definition, []byte(operation.Name), shared.Report)
	if shared.Report.HasErrors() {
		return fmt.Errorf(ErrMsgOperationNormalizationFailed, shared.Report)
	}

	state := shared.Validation.Validate(shared.Doc, r.definition, shared.Report)
	if state != astvalidation.Valid {
		return fmt.Errorf(ErrMsgOperationValidationFailed, shared.Report)
	}

	preparedPlan := shared.Planner.Plan(shared.Doc, r.definition, operation.Name, shared.Report)
	if shared.Report.HasErrors() {
		return fmt.Errorf(ErrMsgOperationPlanningFailed, shared.Report)
	}
	shared.Postprocess.Process(preparedPlan)

	stringInterpolator, err := interpolate.NewOpenapi3StringInterpolator(operationSchema.Variables, operationSchema.Definitions)
	if err != nil {
		return err
	}

	jsonStringInterpolator, err := interpolate.NewOpenapi3StringInterpolatorJSONOnly(operationSchema.InternalVariables, operationSchema.Definitions)
	if err != nil {
		return err
	}

	queryParamsAllowList := maps.Keys(stringInterpolator.PropertiesTypeMap)

	postResolveTransformer := postresolvetransform.NewTransformer(operation.PostResolveTransformations)

	hooksPipelineCommonConfig := hooks.PipelineConfig{
		Client:             r.middlewareClient,
		Operation:          operation,
		Transformer:        postResolveTransformer,
		Logger:             r.log,
		GeneratedVariables: getGeneratedVariables(shared.Doc),
	}

	switch operation.OperationType {
	case wgpb.OperationType_QUERY:
		synchronousPlan, ok := preparedPlan.(*plan.SynchronousResponsePlan)
		if !ok {
			break
		}
		hooksPipelineConfig := hooks.SynchronousOperationPipelineConfig{
			PipelineConfig: hooksPipelineCommonConfig,
			Resolver:       r.resolver,
			Plan:           synchronousPlan,
		}
		hooksPipeline := hooks.NewSynchonousOperationPipeline(hooksPipelineConfig)
		handler := &QueryHandler{
			resolver:                       r.resolver,
			log:                            r.log,
			preparedPlan:                   synchronousPlan,
			pool:                           r.pool,
			extractedVariables:             make([]byte, len(shared.Doc.Input.Variables)),
			cache:                          r.cache,
			configHash:                     []byte(r.api.ApiConfigHash),
			operation:                      operation,
			rbacEnforcer:                   authentication.NewRBACEnforcer(operation),
			operationSchema:                operationSchema,
			openapi3StringInterpolator:     stringInterpolator,
			openapi3JsonStringInterpolator: jsonStringInterpolator,
			postResolveTransformer:         postResolveTransformer,
			renameTypeNames:                r.renameTypeNames,
			queryParamsAllowList:           queryParamsAllowList,
			hooksPipeline:                  hooksPipeline,
		}

		if operation.LiveQueryConfig != nil && operation.LiveQueryConfig.Enabled {
			handler.liveQuery = liveQueryConfig{
				enabled:                true,
				pollingIntervalSeconds: operation.LiveQueryConfig.PollingIntervalSeconds,
			}
		}

		if operation.CacheConfig != nil && operation.CacheConfig.Enabled {
			handler.cacheConfig = cacheConfig{
				enabled:              operation.CacheConfig.Enabled,
				maxAge:               operation.CacheConfig.MaxAge,
				public:               operation.CacheConfig.Public,
				staleWhileRevalidate: operation.CacheConfig.StaleWhileRevalidate,
			}
		}

		copy(handler.extractedVariables, shared.Doc.Input.Variables)

		route := r.router.Methods(http.MethodGet, http.MethodOptions).Path(apiPath).Name(apiPath)
		routeHandler := ensureRequiresRateLimiter(operation, handler)
		routeHandler = ensureRequiresSemaphore(operation, routeHandler)
		routeHandler, authRequired := authentication.EnsureRequiresAuthentication(operation, routeHandler)
		if authRequired {
			r.authRequiredPaths[apiPath] = true
		}
		routeHandler = intercept(routeHandler, r.middlewareClient, operation, queryParamsAllowList)
		route.Handler(routeHandler)

		operationIsConfigured = true

		r.log.Debug("registered QueryHandler",
			zap.String("method", http.MethodGet),
			zap.String("path", apiPath),
			zap.Bool("mock", operation.HooksConfiguration.MockResolve.Enabled),
			zap.Bool("cacheEnabled", handler.cacheConfig.enabled),
			zap.Int("cacheMaxAge", int(handler.cacheConfig.maxAge)),
			zap.Int("cacheStaleWhileRevalidate", int(handler.cacheConfig.staleWhileRevalidate)),
			zap.Bool("cachePublic", handler.cacheConfig.public),
			zap.Bool("authRequired", operation.AuthenticationConfig != nil && operation.AuthenticationConfig.AuthRequired),
		)
	case wgpb.OperationType_MUTATION:
		synchronousPlan, ok := preparedPlan.(*plan.SynchronousResponsePlan)
		if !ok {
			break
		}
		hooksPipelineConfig := hooks.SynchronousOperationPipelineConfig{
			PipelineConfig: hooksPipelineCommonConfig,
			Resolver:       r.resolver,
			Plan:           synchronousPlan,
		}
		hooksPipeline := hooks.NewSynchonousOperationPipeline(hooksPipelineConfig)
		handler := &MutationHandler{
			cache:                          r.cache,
			resolver:                       r.resolver,
			log:                            r.log,
			preparedPlan:                   synchronousPlan,
			pool:                           r.pool,
			extractedVariables:             make([]byte, len(shared.Doc.Input.Variables)),
			operation:                      operation,
			rbacEnforcer:                   authentication.NewRBACEnforcer(operation),
			operationSchema:                operationSchema,
			openapi3StringInterpolator:     stringInterpolator,
			openapi3JsonStringInterpolator: jsonStringInterpolator,
			queryParamsAllowList:           queryParamsAllowList,
			postResolveTransformer:         postResolveTransformer,
			renameTypeNames:                r.renameTypeNames,
			hooksPipeline:                  hooksPipeline,
		}
		copy(handler.extractedVariables, shared.Doc.Input.Variables)

		route := r.router.Methods(http.MethodPost, http.MethodOptions).Path(apiPath).Name(apiPath)
		routeHandler := ensureRequiresRateLimiter(operation, handler)
		routeHandler = ensureRequiresSemaphore(operation, routeHandler)
		routeHandler, authRequired := authentication.EnsureRequiresAuthentication(operation, routeHandler)
		if authRequired {
			r.authRequiredPaths[apiPath] = true
		}
		routeHandler = intercept(routeHandler, r.middlewareClient, operation, queryParamsAllowList)
		route.Handler(routeHandler)

		operationIsConfigured = true

		r.log.Debug("registered MutationHandler",
			zap.String("method", http.MethodPost),
			zap.String("path", apiPath),
			zap.Bool("mock", operation.HooksConfiguration.MockResolve.Enabled),
			zap.Bool("authRequired", operation.AuthenticationConfig != nil && operation.AuthenticationConfig.AuthRequired),
		)
	case wgpb.OperationType_SUBSCRIPTION:
		subscriptionPlan, ok := preparedPlan.(*plan.SubscriptionResponsePlan)
		if !ok {
			break
		}
		hooksPipelineConfig := hooks.SubscriptionOperationPipelineConfig{
			PipelineConfig: hooksPipelineCommonConfig,
			Resolver:       r.resolver,
			Plan:           subscriptionPlan,
		}
		hooksPipeline := hooks.NewSubscriptionOperationPipeline(hooksPipelineConfig)
		handler := &SubscriptionHandler{
			cache:                          r.cache,
			resolver:                       r.resolver,
			log:                            r.log,
			preparedPlan:                   subscriptionPlan,
			pool:                           r.pool,
			extractedVariables:             make([]byte, len(shared.Doc.Input.Variables)),
			operation:                      operation,
			rbacEnforcer:                   authentication.NewRBACEnforcer(operation),
			operationSchema:                operationSchema,
			openapi3StringInterpolator:     stringInterpolator,
			openapi3JsonStringInterpolator: jsonStringInterpolator,
			postResolveTransformer:         postResolveTransformer,
			renameTypeNames:                r.renameTypeNames,
			queryParamsAllowList:           queryParamsAllowList,
			hooksPipeline:                  hooksPipeline,
		}
		copy(handler.extractedVariables, shared.Doc.Input.Variables)

		route := r.router.Methods(http.MethodGet, http.MethodOptions).Path(apiPath).Name(apiPath)
		routeHandler := ensureRequiresRateLimiter(operation, handler)
		routeHandler = ensureRequiresSemaphore(operation, routeHandler)
		routeHandler, authRequired := authentication.EnsureRequiresAuthentication(operation, routeHandler)
		if authRequired {
			r.authRequiredPaths[apiPath] = true
		}
		routeHandler = intercept(routeHandler, r.middlewareClient, operation, queryParamsAllowList)
		route.Handler(routeHandler)

		operationIsConfigured = true

		r.log.Debug("registered SubscriptionHandler",
			zap.String("method", http.MethodGet),
			zap.String("path", apiPath),
			zap.Bool("mock", operation.HooksConfiguration.MockResolve.Enabled),
			zap.Bool("authRequired", operation.AuthenticationConfig != nil && operation.AuthenticationConfig.AuthRequired),
		)
	default:
		r.log.Debug("operation type unknown",
			zap.String("name", operation.Name),
			zap.String("content", operation.Content),
		)
	}

	return nil
}

func (r *Builder) Close() error {
	if closer, ok := r.cache.Cache.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if r.authCache != nil {
		r.authCache.Close()
	}
	if r.definition != nil {
		r.definition.Reset()
	}
	r.router = nil
	r.api = nil
	r.loader = nil
	r.resolver = nil
	r.definition = nil
	r.planConfig.Fields = nil
	r.planConfig.Types = nil
	r.planConfig.DataSources = nil
	r.renameTypeNames = nil
	return nil
}

type GraphQLPlaygroundHandler struct {
	ctx     context.Context
	log     *zap.Logger
	html    string
	nodeUrl string
}

func (h *GraphQLPlaygroundHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if invokeRemoteIpLimit(w, r, h.ctx) {
		return
	}

	tpl := strings.Replace(h.html, "{{apiURL}}", h.nodeUrl, 1)
	resp := []byte(tpl)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(resp)))
	_, _ = w.Write(resp)
}

type GraphQLHandler struct {
	planConfig plan.Configuration
	definition *ast.Document
	resolver   *resolve.Resolver
	log        *zap.Logger
	pool       *pool.Pool
	sf         *singleflight.Group
	ctx        context.Context

	prepared    map[uint64]planWithExtractedVariables
	preparedMux *sync.RWMutex

	renameTypeNames []resolve.RenameTypeName
}

type planWithExtractedVariables struct {
	preparedPlan plan.Plan
	variables    []byte
}

var (
	errInvalid = errors.New("invalid")
)

const requestHeaderTag = "X-FB-Tag"

func invokeRemoteIpLimit(w http.ResponseWriter, r *http.Request, ctx context.Context) (invoked bool) {
	if invoked = r.Header.Get(requestHeaderTag) != ctx.Value(requestHeaderTag); invoked {
		http.Error(w, "not allow remote request", http.StatusMethodNotAllowed)
	}
	return
}

func (h *GraphQLHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if invokeRemoteIpLimit(w, r, h.ctx) {
		return
	}

	requestLogger := h.log.With(logging.WithRequestIDFromContext(r.Context()))

	buf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(buf)
	_, err := io.Copy(buf, r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	body := buf.Bytes()

	requestQuery, _ := jsonparser.GetString(body, "query")
	requestOperationName, parsedOperationNameDataType, _, _ := jsonparser.Get(body, "operationName")
	requestVariables, _, _, _ := jsonparser.Get(body, "variables")

	// An operationName set to { "operationName": null } will be parsed by 'jsonparser' to "null" string
	// and this will make the planner unable to find the operation to execute in selectOperation step.
	// to ensure that the operationName match what planner expect we set it to null.
	if parsedOperationNameDataType == jsonparser.Null {
		requestOperationName = nil
	}

	shared := h.pool.GetSharedFromRequest(h.ctx, r, h.planConfig, pool.Config{
		RenameTypeNames: h.renameTypeNames,
	})
	defer h.pool.PutShared(shared)

	shared.Ctx.Variables = requestVariables
	shared.Ctx.Context = r.Context()
	shared.Ctx.Request.Header = r.Header
	shared.Doc.Input.ResetInputString(requestQuery)
	shared.Parser.Parse(shared.Doc, shared.Report)

	if shared.Report.HasErrors() {
		h.logInternalErrors(shared.Report, requestLogger)
		h.writeRequestErrors(shared.Report, w, requestLogger)

		w.WriteHeader(http.StatusBadRequest)
		return
	}

	autoCompleteRequired := r.URL.Query().Has("autoComplete")
	autoComplete, err := clearDocumentForClearRequired(shared.Ctx, shared.Doc, autoCompleteRequired)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_, _ = shared.Hash.Write(requestOperationName)

	err = shared.Printer.Print(shared.Doc, h.definition, shared.Hash)
	if err != nil {
		requestLogger.Error("shared printer print failed", zap.Error(err))
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if shared.Report.HasErrors() {
		h.logInternalErrors(shared.Report, requestLogger)
		h.writeRequestErrors(shared.Report, w, requestLogger)

		w.WriteHeader(http.StatusBadRequest)
		return
	}

	operationHash := shared.Hash.Sum64()

	h.preparedMux.RLock()
	prepared, exists := h.prepared[operationHash]
	h.preparedMux.RUnlock()
	if !exists {
		prepared, err = h.preparePlan(operationHash, requestOperationName, shared)
		if err != nil {
			if shared.Report.HasErrors() {
				h.logInternalErrors(shared.Report, requestLogger)
				h.writeRequestErrors(shared.Report, w, requestLogger)
			} else {
				requestLogger.Error("prepare plan failed", zap.Error(err))
				http.Error(w, err.Error(), http.StatusBadRequest)
			}

			return
		}
		if autoCompleteRequired {
			autoComplete.prependRecoverFunc(func() { clearDocumentForPreparePlan(shared.Doc) })
		}
	}
	shared.Ctx.Variables, _ = handleSpecial(shared.Ctx.Variables)
	if len(prepared.variables) != 0 {
		// we have to merge query variables into extracted variables to been able to override default values
		shared.Ctx.Variables = MergeJsonRightIntoLeft(prepared.variables, shared.Ctx.Variables)
	}

	switch p := prepared.preparedPlan.(type) {
	case *plan.SynchronousResponsePlan:
		w.Header().Set("Content-Type", "application/json")

		executionBuf := pool.GetBytesBuffer()
		defer pool.PutBytesBuffer(executionBuf)

		if err = h.resolver.ResolveGraphQLResponse(shared.Ctx, p.Response, nil, executionBuf); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if autoCompleteRequired {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			requestErrors := graphql.RequestErrors{
				{Message: err.Error()},
				{Message: "could not resolve response"},
			}

			if _, err := requestErrors.WriteResponse(w); err != nil {
				requestLogger.Error("could not write response", zap.Error(err))
			}

			requestLogger.Error("ResolveGraphQLResponse", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if autoCompleteRequired {
			if errorMsg := gjson.GetBytes(executionBuf.Bytes(), "errors.0.message"); errorMsg.Exists() {
				http.Error(w, errorMsg.String(), http.StatusBadRequest)
				return
			}
			executionBuf.Reset()
			if err = autoComplete.autoComplete(executionBuf); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		if _, err = executionBuf.WriteTo(w); err != nil {
			requestLogger.Error("respond to client", zap.Error(err))
			return
		}
	case *plan.SubscriptionResponsePlan:
		var (
			flushWriter *httpFlushWriter
			ok          bool
		)
		shared.Ctx.Context, flushWriter, ok = getFlushWriter(shared.Ctx.Context, shared.Ctx.Variables, r, w)
		if !ok {
			requestLogger.Error("connection not flushable")
			http.Error(w, "Connection not flushable", http.StatusBadRequest)
			return
		}

		err := h.resolver.ResolveGraphQLSubscription(shared.Ctx, p.Response, flushWriter)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			requestErrors := graphql.RequestErrors{
				{Message: err.Error()},
				{Message: "could not resolve response"},
			}

			if _, err := requestErrors.WriteResponse(w); err != nil {
				requestLogger.Error("could not write response", zap.Error(err))
			}

			requestLogger.Error("ResolveGraphQLSubscription", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	case *plan.StreamingResponsePlan:
		http.Error(w, "not implemented", http.StatusNotFound)
	}
}

func (h *GraphQLHandler) logInternalErrors(report *operationreport.Report, requestLogger *zap.Logger) {
	var internalErr error
	for _, err := range report.InternalErrors {
		internalErr = multierror.Append(internalErr, err)
	}

	if internalErr != nil {
		requestLogger.Error("internal error", zap.Error(internalErr))
	}
}

func (h *GraphQLHandler) writeRequestErrors(report *operationreport.Report, w http.ResponseWriter, requestLogger *zap.Logger) {
	requestErrors := graphql.RequestErrorsFromOperationReport(*report)
	if requestErrors != nil {
		if _, err := requestErrors.WriteResponse(w); err != nil {
			requestLogger.Error("error writing response", zap.Error(err))
		}
	}
}

func (h *GraphQLHandler) preparePlan(operationHash uint64, requestOperationName []byte, shared *pool.Shared) (planWithExtractedVariables, error) {
	preparedPlan, err, _ := h.sf.Do(strconv.Itoa(int(operationHash)), func() (interface{}, error) {
		if len(requestOperationName) == 0 {
			shared.Normalizer.NormalizeOperation(shared.Doc, h.definition, shared.Report)
		} else {
			shared.Normalizer.NormalizeNamedOperation(shared.Doc, h.definition, requestOperationName, shared.Report)
		}
		if shared.Report.HasErrors() {
			return nil, fmt.Errorf(ErrMsgOperationNormalizationFailed, shared.Report)
		}

		state := shared.Validation.Validate(shared.Doc, h.definition, shared.Report)
		if state != astvalidation.Valid {
			return nil, errInvalid
		}

		preparedPlan := shared.Planner.Plan(shared.Doc, h.definition, unsafebytes.BytesToString(requestOperationName), shared.Report)
		shared.Postprocess.Process(preparedPlan)

		prepared := planWithExtractedVariables{
			preparedPlan: preparedPlan,
			variables:    make([]byte, len(shared.Doc.Input.Variables)),
		}

		copy(prepared.variables, shared.Doc.Input.Variables)

		h.preparedMux.Lock()
		h.prepared[operationHash] = prepared
		h.preparedMux.Unlock()

		return prepared, nil
	})
	if err != nil {
		return planWithExtractedVariables{}, err
	}
	return preparedPlan.(planWithExtractedVariables), nil
}

func postProcessVariables(operation *wgpb.Operation, r *http.Request, ctx *resolve.Context) ([]byte, error) {
	variables, err := injectVariables(operation, r, ctx)
	if err != nil {
		return nil, err
	}
	variables, err = injectClaims(operation, r, variables)
	if err != nil {
		return nil, err
	}

	return injectWhereInputVariables(operation, variables), nil
}

func fetchWellKnownClaim(claimType wgpb.ClaimType, user *authentication.User) ([]byte, error) {
	wrapQuoteRequired := true
	var replacement string
	switch claimType {
	case wgpb.ClaimType_ISSUER:
		replacement = user.ProviderID
	case wgpb.ClaimType_SUBJECT: // handles  wgpb.ClaimType_USERID too
		replacement = user.UserID
	case wgpb.ClaimType_NAME:
		replacement = user.Name
	case wgpb.ClaimType_GIVEN_NAME:
		replacement = user.FirstName
	case wgpb.ClaimType_FAMILY_NAME:
		replacement = user.LastName
	case wgpb.ClaimType_MIDDLE_NAME:
		replacement = user.MiddleName
	case wgpb.ClaimType_NICKNAME:
		replacement = user.NickName
	case wgpb.ClaimType_PREFERRED_USERNAME:
		replacement = user.PreferredUsername
	case wgpb.ClaimType_PROFILE:
		replacement = user.Profile
	case wgpb.ClaimType_PICTURE:
		replacement = user.Picture
	case wgpb.ClaimType_WEBSITE:
		replacement = user.Website
	case wgpb.ClaimType_EMAIL:
		replacement = user.Email
	case wgpb.ClaimType_EMAIL_VERIFIED:
		replacement = cast.ToString(user.EmailVerified)
		wrapQuoteRequired = false
	case wgpb.ClaimType_GENDER:
		replacement = user.Gender
	case wgpb.ClaimType_BIRTH_DATE:
		replacement = user.BirthDate
	case wgpb.ClaimType_ZONE_INFO:
		replacement = user.ZoneInfo
	case wgpb.ClaimType_LOCALE:
		replacement = user.Locale
	case wgpb.ClaimType_LOCATION:
		replacement = user.Location
	case wgpb.ClaimType_ROLES:
		replacement = makeArrayRawString(user.Roles, true)
		wrapQuoteRequired = false
	default:
		return nil, fmt.Errorf("unhandled well known claim %s", claimType)
	}

	if wrapQuoteRequired {
		replacement = `"` + replacement + `"`
	}
	return []byte(replacement), nil
}

var customTypeMap = map[wgpb.ValueType]jsonparser.ValueType{
	wgpb.ValueType_BOOLEAN: jsonparser.Boolean,
	wgpb.ValueType_INT:     jsonparser.Number,
	wgpb.ValueType_FLOAT:   jsonparser.Number,
	wgpb.ValueType_STRING:  jsonparser.String,
	wgpb.ValueType_ARRAY:   jsonparser.Array,
}

func makeArrayRawString(strArr []string, wrapQuote bool) string {
	var array []string
	for _, str := range strArr {
		if wrapQuote {
			str = `"` + str + `"`
		}
		array = append(array, str)
	}
	return fmt.Sprintf("[%s]", strings.Join(array, ","))
}

func fetchCustomClaim(customClaim *wgpb.CustomClaim, user *authentication.User, compareValueType bool) (result []byte, err error) {
	customClaimsBytes, _ := json.Marshal(user.CustomClaims)
	setValue, setValueType, _, _ := jsonparser.Get(customClaimsBytes, customClaim.JsonPathComponents...)
	if setValueType == jsonparser.Null || setValueType == jsonparser.NotExist {
		if customClaim.Required {
			err = inputvariables.NewValidationError(fmt.Sprintf("required customClaim %s not found", customClaim.Name), nil, nil)
		}
		return
	}

	match, ok := customTypeMap[customClaim.Type]
	if !ok {
		err = inputvariables.NewValidationError(fmt.Sprintf("customClaim %s not support type %s", customClaim.Name, customClaim.Type.String()), nil, nil)
		return
	}

	if compareValueType && match != setValueType {
		err = inputvariables.NewValidationError(fmt.Sprintf("customClaim %s expected to be of type %s, found %s instead", customClaim.Name, customClaim.Type.String(), setValueType.String()), nil, nil)
		return
	}

	if setValueType == jsonparser.String {
		setValue = []byte(`"` + string(setValue) + `"`)
	}

	result = setValue
	return
}

func injectClaims(operation *wgpb.Operation, r *http.Request, variables []byte) ([]byte, error) {
	claims := operation.GetAuthorizationConfig().GetClaims()
	if len(claims) == 0 {
		return variables, nil
	}
	user := authentication.UserFromContext(r.Context())
	if user == nil {
		return variables, nil
	}
	var err error
	var claimVariables []byte
	for _, claim := range claims {
		removeIfNoneExisted := claim.RemoveIfNoneMatch != nil
		if claim.GetClaimType() == wgpb.ClaimType_CUSTOM {
			claimVariables, err = fetchCustomClaim(claim.Custom, user, !removeIfNoneExisted)
		} else {
			claimVariables, err = fetchWellKnownClaim(claim.ClaimType, user)
		}
		if err != nil {
			return nil, err
		}
		if claimVariables == nil {
			continue
		}

		if removeIfNoneExisted {
			claimVariablesResult := gjson.ParseBytes(claimVariables)
			var compareValues []string
			if claimVariablesResult.IsArray() {
				claimVariablesResult.ForEach(func(_, value gjson.Result) bool {
					compareValues = append(compareValues, value.String())
					return true
				})
			} else {
				compareValues = append(compareValues, string(claimVariables))
			}
			var compareFunc func(s string) bool
			switch claim.RemoveIfNoneMatch.Type {
			case wgpb.ClaimRemoveIfNoneMatchType_Environment:
				compareFunc = func(s string) bool { return os.Getenv(claim.RemoveIfNoneMatch.Name) == s }
			case wgpb.ClaimRemoveIfNoneMatchType_Header:
				compareFunc = func(s string) bool { return slices.Contains(r.Header[claim.RemoveIfNoneMatch.Name], s) }
			default:
				return nil, fmt.Errorf("unhandled RemoveIfNoneMatchType %s", claim.RemoveIfNoneMatch.Type.String())
			}
			if !slices.ContainsFunc(compareValues, compareFunc) {
				variables = jsonparser.Delete(variables, claim.VariablePathComponents...)
			}
			continue
		}

		if variables, err = jsonparser.Set(variables, claimVariables, claim.VariablePathComponents...); err != nil {
			return nil, fmt.Errorf("error replacing variable for claim %s: %w", claim.ClaimType.String(), err)
		}
	}
	return variables, nil
}

func injectWhereInputVariables(operation *wgpb.Operation, variables []byte) []byte {
	variablesConfiguration := operation.VariablesConfiguration
	if variablesConfiguration == nil {
		return variables
	}

	for _, whereItem := range variablesConfiguration.WhereInputs {
		var injectSucceed bool
		value, valueType, _, _ := jsonparser.Get(variables, whereItem.VariablePathComponents...)
		switch valueType {
		case jsonparser.Array:
			var multipleWhere []string
			_, _ = jsonparser.ArrayEach(value, func(item []byte, itemValueType jsonparser.ValueType, _ int, _ error) {
				if itemValueType != jsonparser.Object {
					multipleWhere = append(multipleWhere, string(buildWhereInput(item, itemValueType, whereItem.WhereInput)))
				}
			})
			value = []byte(makeArrayRawString(multipleWhere, false))
			injectSucceed = len(multipleWhere) > 0
		case jsonparser.String, jsonparser.Number, jsonparser.Boolean:
			value = buildWhereInput(value, valueType, whereItem.WhereInput)
			injectSucceed = true
		}
		if injectSucceed {
			variables, _ = jsonparser.Set(variables, value, whereItem.VariablePathComponents...)
		}
	}
	return variables
}

var arrayValueFilterTypes = []wgpb.VariableWhereInputScalarFilterType{
	wgpb.VariableWhereInputScalarFilterType_in,
	wgpb.VariableWhereInputScalarFilterType_notIn,
}

func buildWhereInput(value []byte, valueType jsonparser.ValueType, whereInput *wgpb.VariableWhereInput) []byte {
	if whereInput.Not != nil {
		value, _ = jsonparser.Set([]byte("{}"), buildWhereInput(value, valueType, whereInput.Not), "NOT")
	} else if filter := whereInput.Filter; filter != nil {
		if filter.Relation != nil {
			value = buildWhereInputRelationFilter(value, valueType, filter.Relation)
		} else if filter.Scalar != nil {
			value = buildWhereInputScalarFilter(value, valueType, filter.Scalar)
		}
		value, _ = jsonparser.Set([]byte("{}"), value, filter.Field)
	}
	return value
}

func buildWhereInputRelationFilter(value []byte, valueType jsonparser.ValueType, relationFilter *wgpb.VariableWhereInputRelationFilter) []byte {
	value, _ = jsonparser.Set([]byte("{}"), buildWhereInput(value, valueType, relationFilter.Where), relationFilter.Type.String())
	return value
}

func buildWhereInputScalarFilter(value []byte, valueType jsonparser.ValueType, scalarFilter *wgpb.VariableWhereInputScalarFilter) []byte {
	isStringValue := jsonparser.String == valueType
	if slices.Contains(arrayValueFilterTypes, scalarFilter.Type) {
		value = []byte(makeArrayRawString(strings.Split(string(value), ","), isStringValue))
	} else if isStringValue {
		value = []byte(`"` + string(value) + `"`)
	}
	value, _ = jsonparser.Set([]byte("{}"), value, scalarFilter.Type.String())
	if isStringValue && scalarFilter.Insensitive {
		value, _ = jsonparser.Set(value, []byte(`"insensitive"`), "mode")
	}
	return value
}

var environments map[string]string

func makeRuleParameters(operation *wgpb.Operation, r *http.Request, ctx *resolve.Context) (ruleParameters map[string]interface{}) {
	if operation.RuleExpressionExisted {
		var arguments map[string]interface{}
		_ = json.Unmarshal(ctx.Variables, &arguments)
		if environments == nil {
			environments = make(map[string]string)
			for _, item := range os.Environ() {
				k, v, found := strings.Cut(item, "=")
				if found {
					environments[k] = v
				}
			}
		}
		ruleParameters = map[string]interface{}{
			"arguments":    arguments,
			"headers":      hooks.HeaderSliceToCSV(r.Header),
			"environments": environments,
		}
		if userBytes := authentication.UserBytesFromContext(r.Context()); userBytes != nil {
			var user map[string]interface{}
			_ = json.Unmarshal(userBytes, &user)
			ruleParameters["user"] = user
		}
		ctxVariables := ctx.Variables
		ctx.RuleEvaluate = func(variablesBytes []byte, expression string) bool {
			if !bytes.Equal(variablesBytes, ctxVariables) {
				_ = json.Unmarshal(variablesBytes, &arguments)
				ruleParameters["arguments"] = arguments
				ctxVariables = variablesBytes
			}
			expression = strings.ReplaceAll(expression, "'", "`")
			ruleValue, err := GvalFullLanguage.Evaluate(expression, ruleParameters)
			if err != nil {
				return false
			}
			return !GvalIsEmptyValue(ruleValue)
		}
	}
	return
}

var (
	GvalFullLanguage gval.Language
	GvalIsEmptyValue = func(any) bool { return false }
)

func injectVariables(operation *wgpb.Operation, r *http.Request, ctx *resolve.Context) ([]byte, error) {
	ruleParameters := makeRuleParameters(operation, r, ctx)
	variablesConfiguration := operation.VariablesConfiguration
	if variablesConfiguration == nil {
		return ctx.Variables, nil
	}

	variables := ctx.Variables
	for _, currentVariable := range variablesConfiguration.InjectVariables {
		var (
			replacement             string
			replaceWrapQuoteIgnored bool
		)
		kind := currentVariable.VariableKind
		switch kind {
		case wgpb.InjectVariableKind_UUID:
			replacement, _ = uuid.GenerateUUID()
		case wgpb.InjectVariableKind_FROM_HEADER:
			replacement = r.Header.Get(currentVariable.FromHeaderName)
		case wgpb.InjectVariableKind_DATE_TIME:
			format := currentVariable.DateFormat
			now := addDateOffset(time.Now(), currentVariable.DateOffset)
			replacement = now.Format(format)
		case wgpb.InjectVariableKind_ENVIRONMENT_VARIABLE:
			replacement = os.Getenv(currentVariable.EnvironmentVariableName)
		case wgpb.InjectVariableKind_RULE_EXPRESSION:
			ruleValue, err := GvalFullLanguage.Evaluate(currentVariable.RuleExpression, ruleParameters)
			if err != nil {
				return nil, err
			}
			switch typeName := currentVariable.RuleValueTypeName; typeName {
			case openapi3.TypeArray, openapi3.TypeObject:
				var buf bytes.Buffer
				if err = gob.NewEncoder(&buf).Encode(ruleValue); err != nil {
					return nil, err
				}
				replacement = buf.String()
				replaceWrapQuoteIgnored = true
			default:
				replacement = fmt.Sprintf("%v", ruleValue)
				replaceWrapQuoteIgnored = typeName != openapi3.TypeString
			}
		}
		if replacement == "" {
			continue
		}
		if !replaceWrapQuoteIgnored {
			replacement = strconv.Quote(replacement)
		}

		variables, _ = jsonparser.Set(variables, []byte(replacement), currentVariable.VariablePathComponents...)
	}
	return variables, nil
}

func addDateOffset(datetime time.Time, offset *wgpb.DateOffset) time.Time {
	if offset == nil {
		return datetime
	}

	value := int(offset.Value)
	if offset.Previous {
		value *= -1
	}
	year, month, day := datetime.Date()
	hour, minute, second := datetime.Clock()
	switch offset.Unit {
	case wgpb.DateOffsetUnit_YEAR:
		year += value
	case wgpb.DateOffsetUnit_MONTH:
		month += time.Month(value)
	case wgpb.DateOffsetUnit_DAY:
		day += value
	case wgpb.DateOffsetUnit_HOUR:
		hour += value
	case wgpb.DateOffsetUnit_MINUTE:
		minute += value
	case wgpb.DateOffsetUnit_SECOND:
		second += value
	}
	return time.Date(year, month, day, hour, minute, second, datetime.Nanosecond(), datetime.Location())
}

type cacheConfig struct {
	enabled              bool
	maxAge               int64
	public               bool
	staleWhileRevalidate int64
}

type QueryResolver interface {
	ResolveGraphQLResponse(ctx *resolve.Context, response *resolve.GraphQLResponse, data []byte, writer io.Writer) (err error)
}

type SubscriptionResolver interface {
	ResolveGraphQLSubscription(ctx *resolve.Context, subscription *resolve.GraphQLSubscription, writer resolve.FlushWriter) (err error)
}

type liveQueryConfig struct {
	enabled                bool
	pollingIntervalSeconds int64
}

func setRawVariablesByType(rawVariables, name string, val []string, typeMap map[string]string) (string, bool) {
	typeName, ok := typeMap[name]
	if !ok {
		return rawVariables, false
	}

	switch typeName {
	case openapi3.TypeBoolean, openapi3.TypeInteger, openapi3.TypeNumber, openapi3.TypeObject:
		rawVariables, _ = sjson.SetRaw(rawVariables, name, val[0])
	case openapi3.TypeArray:
		rawVariables, _ = sjson.Set(rawVariables, name, val)
	default:
		rawVariables, _ = sjson.Set(rawVariables, name, val[0])
	}
	return rawVariables, true
}

func parseQueryVariables(r *http.Request, allowList []string, typeMap map[string]string) []byte {
	urlQuery := r.URL.Query()
	rawVariables := urlQuery.Get(WgVariables)
	if rawVariables == "" {
		rawVariables = "{}"
		for name, val := range urlQuery {
			if len(val) > 0 && !strings.HasPrefix(name, WgPrefix) {
				if !stringSliceContainsValue(allowList, name) {
					continue
				}

				// 通过variables的schema倒推入参类型
				if rawOk, ok := setRawVariablesByType(rawVariables, name, val, typeMap); ok {
					rawVariables = rawOk
					continue
				}

				// check if the user work with JSON values
				if gjson.Valid(val[0]) {
					rawVariables, _ = sjson.SetRaw(rawVariables, name, val[0])
				} else {
					rawVariables, _ = sjson.Set(rawVariables, name, val[0])
				}
			}
		}
	}
	return unsafebytes.StringToBytes(rawVariables)
}

func stringSliceContainsValue(list []string, value string) bool {
	for i := range list {
		if list[i] == value {
			return true
		}
	}
	return false
}

type QueryHandler struct {
	resolver               QueryResolver
	log                    *zap.Logger
	preparedPlan           *plan.SynchronousResponsePlan
	extractedVariables     []byte
	pool                   *pool.Pool
	cacheConfig            cacheConfig
	cache                  apicache.Cache
	configHash             []byte
	liveQuery              liveQueryConfig
	operation              *wgpb.Operation
	rbacEnforcer           *authentication.RBACEnforcer
	postResolveTransformer *postresolvetransform.Transformer
	renameTypeNames        []resolve.RenameTypeName
	queryParamsAllowList   []string
	hooksPipeline          *hooks.SynchronousOperationPipeline

	openapi3StringInterpolator     *interpolate.Openapi3StringInterpolator
	openapi3JsonStringInterpolator *interpolate.Openapi3StringInterpolator
	operationSchema                *OperationSchema
}

func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestLogger := h.log.With(logging.WithRequestIDFromContext(r.Context()))
	r = setOperationMetaData(r, h.operation, h.queryParamsAllowList, h.cache)

	if proceed := h.rbacEnforcer.Enforce(r); !proceed {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var (
		cacheKey     string
		cacheIsStale = false
	)

	isLive := h.liveQuery.enabled && r.URL.Query().Has(WgLiveParam)

	buf := pool.GetBytesBuffer()
	ctx := pool.GetCtx(r, r, pool.Config{
		RenameTypeNames: h.renameTypeNames,
	})

	defer func() {
		if cacheIsStale {
			buf.Reset()
			ctx.Context = context.WithValue(context.Background(), "user", authentication.UserFromContext(r.Context()))
			ctx.Context = context.WithValue(ctx.Context, "userBytes", authentication.UserBytesFromContext(r.Context()))
			err := h.resolver.ResolveGraphQLResponse(ctx, h.preparedPlan.Response, nil, buf)
			if err == nil {
				bufferedData := buf.Bytes()
				cacheData := make([]byte, len(bufferedData))
				copy(cacheData, bufferedData)
				h.cache.SetWithTTL(cacheKey, cacheData, time.Second*time.Duration(h.cacheConfig.maxAge+h.cacheConfig.staleWhileRevalidate))
			}
		}

		pool.PutCtx(ctx)
		pool.PutBytesBuffer(buf)
	}()

	ctx.Variables = parseQueryVariables(r, h.queryParamsAllowList, h.openapi3StringInterpolator.PropertiesTypeMap)
	ctx.Variables = h.openapi3StringInterpolator.Interpolate(ctx.Variables)

	if !openapi3ValidateInputVariables(requestLogger, ctx.Variables, h.operationSchema, w) {
		return
	}

	compactBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(compactBuf)
	err := json.Compact(compactBuf, ctx.Variables)
	if err != nil {
		requestLogger.Error("Could not compact variables in query handler", zap.Bool("isLive", isLive), zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx.Variables = compactBuf.Bytes()

	ctx.Variables = h.openapi3JsonStringInterpolator.Interpolate(ctx.Variables)

	if len(h.extractedVariables) != 0 {
		ctx.Variables = MergeJsonRightIntoLeft(h.extractedVariables, ctx.Variables)
	}

	ctx.Variables, err = postProcessVariables(h.operation, r, ctx)
	if err != nil {
		if done := handleOperationErr(requestLogger, err, w, "postProcessVariables failed", h.operation); done {
			return
		}
	}

	flusher, flusherOk := w.(http.Flusher)
	if isLive {
		if !flusherOk {
			requestLogger.Error("Could not flush in query handler", zap.Bool("isLive", isLive))
			http.Error(w, "requires flushing", http.StatusBadRequest)
			return
		}
		setSubscriptionHeaders(w)
		flusher.Flush()
	} else {
		w.Header().Set("Content-Type", "application/json")
	}

	if isLive {
		h.handleLiveQuery(r, w, ctx, buf, flusher, requestLogger)
		return
	}

	if h.cacheConfig.enabled {
		cacheKey = string(h.configHash) + r.RequestURI
		item, hit := h.cache.Get(ctx.Context, cacheKey)
		if hit {

			w.Header().Set(WgCacheHeader, "HIT")

			_, _ = buf.Write(item.Data)

			hash := xxhash.New()
			_, _ = hash.Write(h.configHash)
			_, _ = hash.Write(buf.Bytes())
			ETag := fmt.Sprintf("W/\"%d\"", hash.Sum64())

			w.Header()["ETag"] = []string{ETag}

			if h.cacheConfig.public {
				w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d, stale-while-revalidate=%d", h.cacheConfig.maxAge, h.cacheConfig.staleWhileRevalidate))
			} else {
				w.Header().Set("Cache-Control", fmt.Sprintf("private, max-age=%d, stale-while-revalidate=%d", h.cacheConfig.maxAge, h.cacheConfig.staleWhileRevalidate))
			}

			age := item.Age()
			w.Header().Set("Age", fmt.Sprintf("%d", age))

			if age > h.cacheConfig.maxAge {
				cacheIsStale = true
			}

			ifNoneMatch := r.Header.Get("If-None-Match")
			if ifNoneMatch == ETag {
				w.WriteHeader(http.StatusNotModified)
				return
			}

			w.WriteHeader(http.StatusOK)
			_, _ = buf.WriteTo(w)
			return
		}
		w.Header().Set(WgCacheHeader, "MISS")
	}

	resp, err := h.hooksPipeline.Run(ctx, w, r, buf)
	defer database.NotifyTransactionFinish(r, h.cache, err)
	if done := handleOperationErr(requestLogger, err, w, "hooks pipeline failed", h.operation); done {
		return
	}

	if resp.Done {
		return
	}

	transformed := resp.Data

	hash := xxhash.New()
	_, _ = hash.Write(h.configHash)
	_, _ = hash.Write(transformed)
	ETag := fmt.Sprintf("W/\"%d\"", hash.Sum64())

	w.Header()["ETag"] = []string{ETag}

	if h.cacheConfig.enabled {
		if h.cacheConfig.public {
			w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d, stale-while-revalidate=%d", h.cacheConfig.maxAge, h.cacheConfig.staleWhileRevalidate))
		} else {
			w.Header().Set("Cache-Control", fmt.Sprintf("private, max-age=%d, stale-while-revalidate=%d", h.cacheConfig.maxAge, h.cacheConfig.staleWhileRevalidate))
		}

		w.Header().Set("Age", "0")

		cacheData := make([]byte, len(transformed))
		copy(cacheData, transformed)

		h.cache.SetWithTTL(cacheKey, cacheData, time.Second*time.Duration(h.cacheConfig.maxAge+h.cacheConfig.staleWhileRevalidate))
	}

	ifNoneMatch := r.Header.Get("If-None-Match")
	if ifNoneMatch == ETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	reader := bytes.NewReader(transformed)
	_, err = reader.WriteTo(w)
	if done := handleOperationErr(requestLogger, err, w, "writing response failed", h.operation); done {
		return
	}
}

func (h *QueryHandler) handleLiveQueryEvent(ctx *resolve.Context, w http.ResponseWriter, r *http.Request, requestBuf *bytes.Buffer) ([]byte, error) {

	requestBuf.Reset()
	hooksResponse, err := h.hooksPipeline.Run(ctx, w, r, requestBuf)
	defer database.NotifyTransactionFinish(r, h.cache, err)
	if err != nil {
		return nil, fmt.Errorf("handleLiveQueryEvent: %w", err)
	}

	if hooksResponse.Done {
		// Response is already written by hooks pipeline, tell
		// caller to stop
		return nil, context.Canceled
	}

	return hooksResponse.Data, nil
}

func (h *QueryHandler) handleLiveQuery(r *http.Request, w http.ResponseWriter, ctx *resolve.Context, requestBuf *bytes.Buffer, flusher http.Flusher, requestLogger *zap.Logger) {
	wgParams := NewWgRequestParams(r.URL)

	done := ctx.Context.Done()

	hookBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(hookBuf)

	lastData := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(lastData)

	currentData := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(currentData)

	if wgParams.UseSse {
		defer func() {
			_, _ = fmt.Fprintf(w, "event: close\n\n")
			flusher.Flush()
		}()
	}

	for {
		var hookError bool
		response, err := h.handleLiveQueryEvent(ctx, w, r, requestBuf)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// context was canceled (e.g. client disconnected)
				// we've already flushed a header, so we simply return
				return
			}
			hookError = true
			requestLogger.Error("HandleLiveQueryEvent failed",
				zap.Error(err),
				zap.String("operation", h.operation.Path),
			)

			graphqlError := graphql.Response{
				Errors: graphql.RequestErrors{
					graphql.RequestError{
						Message: err.Error(),
					},
				},
			}
			graphqlErrorPayload, marshalErr := graphqlError.Marshal()
			if marshalErr != nil {
				requestLogger.Error("HandleLiveQueryEvent could not marshal graphql error", zap.Error(marshalErr))
			} else {
				response = graphqlErrorPayload
			}
		}

		// only send the response if the content has changed
		if !bytes.Equal(response, lastData.Bytes()) {
			currentData.Reset()
			_, _ = currentData.Write(response)
			if wgParams.UseSse {
				_, _ = w.Write([]byte("data: "))
			}
			if wgParams.SubscribeOnce {
				flusher.Flush()
				return
			}
			if wgParams.UseJsonPatch && lastData.Len() != 0 {
				last := lastData.Bytes()
				current := currentData.Bytes()
				patch, err := jsonpatch.CreatePatch(last, current)
				if err != nil {
					requestLogger.Error("HandleLiveQueryEvent could not create json patch", zap.Error(err))
					continue
				}
				patchBytes, err := json.Marshal(patch)
				if err != nil {
					requestLogger.Error("HandleLiveQueryEvent could not marshal json patch", zap.Error(err))
					continue
				}
				// we only send the patch if it's smaller than the full response
				if len(patchBytes) < len(current) {
					_, err = w.Write(patchBytes)
					if err != nil {
						requestLogger.Error("HandleLiveQueryEvent could not write json patch", zap.Error(err))
						return
					}
				} else {
					_, err = w.Write(current)
					if err != nil {
						requestLogger.Error("HandleLiveQueryEvent could not write response", zap.Error(err))
						return
					}
				}
			} else {
				_, err = w.Write(currentData.Bytes())
			}
			if err != nil {
				requestLogger.Error("HandleLiveQueryEvent could not write response", zap.Error(err))
				return
			}
			_, _ = w.Write(literal.LINETERMINATOR)
			_, err = w.Write(literal.LINETERMINATOR)
			if err != nil {
				return
			}
			flusher.Flush()
			lastData.Reset()
			_, _ = lastData.Write(currentData.Bytes())
		}

		// After hook error we return the graphql compatible error to the client
		// and abort the stream
		if hookError {
			requestLogger.Error("HandleLiveQueryEvent cancel due to hook error", zap.Error(err))
			return
		}

		select {
		case <-done:
			return
		case <-time.After(time.Second * time.Duration(h.liveQuery.pollingIntervalSeconds)):
			continue
		}
	}
}

type MutationHandler struct {
	cache                  apicache.Cache
	resolver               *resolve.Resolver
	log                    *zap.Logger
	preparedPlan           *plan.SynchronousResponsePlan
	extractedVariables     []byte
	pool                   *pool.Pool
	operation              *wgpb.Operation
	rbacEnforcer           *authentication.RBACEnforcer
	postResolveTransformer *postresolvetransform.Transformer
	renameTypeNames        []resolve.RenameTypeName
	hooksPipeline          *hooks.SynchronousOperationPipeline

	queryParamsAllowList           []string
	openapi3StringInterpolator     *interpolate.Openapi3StringInterpolator
	openapi3JsonStringInterpolator *interpolate.Openapi3StringInterpolator
	operationSchema                *OperationSchema
}

func (h *MutationHandler) parseFormVariables(r *http.Request) []byte {
	if err := r.ParseForm(); err == nil {
		return makeRawVariables(r.Form)
	}
	return []byte("{}")
}

func makeRawVariables(data map[string][]string) []byte {
	rawVariables := "{}"
	for name, val := range data {
		if len(val) == 0 || strings.HasSuffix(val[0], WgPrefix) {
			continue
		}
		// check if the user works with JSON values
		if gjson.Valid(val[0]) {
			rawVariables, _ = sjson.SetRaw(rawVariables, name, val[0])
		} else {
			rawVariables, _ = sjson.Set(rawVariables, name, val[0])
		}
	}
	return []byte(rawVariables)
}

func handleSpecial(input []byte) ([]byte, bool) {
	var modified bool
	// 处理富文本场景
	inputJson := gjson.ParseBytes(input)
	inputJson.ForEach(func(key, value gjson.Result) bool {
		var valueModified bool
		valueBytes := []byte(value.Raw)
		switch value.Type {
		case gjson.JSON:
			valueBytes, valueModified = handleSpecial(valueBytes)
		case gjson.String:
			valueBytes, valueModified = handleEscapeChar(valueBytes)
		}
		if valueModified {
			modified = true
			input, _ = sjson.SetRawBytes(input, key.String(), valueBytes)
		}
		return true
	})
	return input, modified
}

var (
	backSlashByte byte = '\\'
	quoteByte     byte = '"'
	dotByte       byte = '.'
	escapeChars        = maps.Values(escapeCharMap)
	escapeCharMap      = map[byte]byte{
		'\n': 'n',
		'\r': 'r',
		'\t': 't',
		'\b': 'b',
		'\f': 'f',
		'\v': 'v',
	}
)

func handleEscapeChar(input []byte) (output []byte, modified bool) {
	var escapeCharCount int
	for _, c := range input {
		if c == backSlashByte {
			escapeCharCount++
			continue
		}

		if r, ok := escapeCharMap[c]; ok {
			c = r
			escapeCharCount = 2
		} else if escapeCharCount > 0 {
			if slices.Contains(escapeChars, c) {
				escapeCharCount = 2
			} else if c != quoteByte {
				escapeCharCount = int(math.Floor(float64(escapeCharCount)/2)) * 2
				if c == dotByte && escapeCharCount == 2 {
					escapeCharCount *= 2
				}
			}
		}
		for {
			if escapeCharCount == 0 {
				break
			}
			output = append(output, backSlashByte)
			escapeCharCount--
		}
		output = append(output, c)
	}
	modified = len(input) != len(output)
	return
}

func (h *MutationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestLogger := h.log.With(logging.WithRequestIDFromContext(r.Context()))
	r = setOperationMetaData(r, h.operation, h.queryParamsAllowList, h.cache)

	if proceed := h.rbacEnforcer.Enforce(r); !proceed {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	variablesBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(variablesBuf)

	ctx := pool.GetCtx(r, r, pool.Config{
		RenameTypeNames: h.renameTypeNames,
	})
	defer pool.PutCtx(ctx)

	ct := r.Header.Get("Content-Type")

	if ct == "application/x-www-form-urlencoded" {
		ctx.Variables = h.parseFormVariables(r)
	} else if variables, applyContext := ensureMultipartFormData(r, h.operation.MultipartForms); applyContext != nil {
		ctx.Variables = variables
		applyContext(ctx)
	} else {
		_, err := io.Copy(variablesBuf, r.Body)
		if err != nil {
			requestLogger.Error("failed to copy variables buf", zap.Error(err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// 处理反斜杠，将单反斜杠替换为双反斜杠，直接拷贝body没有对转义字符进行处理
		ctx.Variables, _ = handleSpecial(variablesBuf.Bytes())
	}

	if len(ctx.Variables) == 0 {
		ctx.Variables = []byte("{}")
	}
	ctx.Variables = h.openapi3StringInterpolator.Interpolate(ctx.Variables)

	if !openapi3ValidateInputVariables(requestLogger, ctx.Variables, h.operationSchema, w) {
		return
	}

	compactBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(compactBuf)
	err := json.Compact(compactBuf, ctx.Variables)
	if err != nil {
		requestLogger.Error("failed to compact variables", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx.Variables = compactBuf.Bytes()

	ctx.Variables = h.openapi3JsonStringInterpolator.Interpolate(ctx.Variables)

	if len(h.extractedVariables) != 0 {
		ctx.Variables = MergeJsonRightIntoLeft(h.extractedVariables, ctx.Variables)
	}

	ctx.Variables, err = postProcessVariables(h.operation, r, ctx)
	if err != nil {
		if done := handleOperationErr(requestLogger, err, w, "postProcessVariables failed", h.operation); done {
			return
		}
	}

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

	reader := bytes.NewReader(resp.Data)
	_, err = reader.WriteTo(w)
	if done := handleOperationErr(requestLogger, err, w, "writing response failed", h.operation); done {
		return
	}
}

func ensureMultipartFormData(r *http.Request, multipartForms []*wgpb.OperationMultipartForm) (variables []byte, applyContext func(*resolve.Context)) {
	if len(multipartForms) == 0 || logging.NoneMultipartContentType(r) {
		return
	}

	if err := r.ParseMultipartForm(math.MaxInt64); err != nil {
		return
	}

	variables = makeRawVariables(r.MultipartForm.Value)
	multipartFiles := make(map[string][]*multipart.FileHeader)
	for _, form := range multipartForms {
		formFiles, ok := r.MultipartForm.File[form.FieldName]
		if !ok {
			return
		}

		multipartFiles[form.FieldName] = formFiles
		var filePlaceHolderValue any
		filePlaceHolderStr := fmt.Sprintf("$$%s$$", form.FieldName)
		if form.IsArray {
			var filePlaceHolderSlice []string
			for i := 0; i < len(formFiles); i++ {
				filePlaceHolderSlice = append(filePlaceHolderSlice, filePlaceHolderStr)
			}
			filePlaceHolderValue = filePlaceHolderSlice
		} else {
			filePlaceHolderValue = filePlaceHolderStr
		}
		variables, _ = sjson.SetBytes(variables, form.FieldName, filePlaceHolderValue)
	}
	applyContext = func(ctx *resolve.Context) {
		ctx.Context = context.WithValue(ctx.Context, customhttpclient.MultipartFiles, multipartFiles)
		ctx.Context = context.WithValue(ctx.Context, customhttpclient.MultipartHeader, r.Header.Get("Content-Type"))
	}
	return
}

type SubscriptionHandler struct {
	cache                  apicache.Cache
	resolver               SubscriptionResolver
	log                    *zap.Logger
	preparedPlan           *plan.SubscriptionResponsePlan
	extractedVariables     []byte
	pool                   *pool.Pool
	operation              *wgpb.Operation
	rbacEnforcer           *authentication.RBACEnforcer
	postResolveTransformer *postresolvetransform.Transformer
	renameTypeNames        []resolve.RenameTypeName
	queryParamsAllowList   []string
	hooksPipeline          *hooks.SubscriptionOperationPipeline

	openapi3StringInterpolator     *interpolate.Openapi3StringInterpolator
	openapi3JsonStringInterpolator *interpolate.Openapi3StringInterpolator
	operationSchema                *OperationSchema
}

func (h *SubscriptionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestLogger := h.log.With(logging.WithRequestIDFromContext(r.Context()))
	r = setOperationMetaData(r, h.operation, h.queryParamsAllowList, h.cache)

	if proceed := h.rbacEnforcer.Enforce(r); !proceed {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	ctx := pool.GetCtx(r, r, pool.Config{
		RenameTypeNames: h.renameTypeNames,
	})
	defer pool.PutCtx(ctx)

	ctx.Variables = parseQueryVariables(r, h.queryParamsAllowList, h.openapi3StringInterpolator.PropertiesTypeMap)
	ctx.Variables = h.openapi3StringInterpolator.Interpolate(ctx.Variables)

	if !openapi3ValidateInputVariables(requestLogger, ctx.Variables, h.operationSchema, w) {
		return
	}

	compactBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(compactBuf)
	err := json.Compact(compactBuf, ctx.Variables)
	if err != nil {
		requestLogger.Error("Could not compact variables", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx.Variables = compactBuf.Bytes()

	ctx.Variables = h.openapi3JsonStringInterpolator.Interpolate(ctx.Variables)

	if len(h.extractedVariables) != 0 {
		ctx.Variables = MergeJsonRightIntoLeft(h.extractedVariables, ctx.Variables)
	}

	ctx.Variables, err = postProcessVariables(h.operation, r, ctx)
	if err != nil {
		if done := handleOperationErr(requestLogger, err, w, "postProcessVariables failed", h.operation); done {
			return
		}
	}

	flushWriter, ok := getHooksFlushWriter(ctx, r, w, h.hooksPipeline, h.log)
	if !ok {
		http.Error(w, "Connection not flushable", http.StatusBadRequest)
		return
	}

	flushWriter.postResolveTransformer = h.postResolveTransformer
	_, err = h.hooksPipeline.RunSubscription(ctx, flushWriter, r)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// e.g. client closed connection
			return
		}
		// if the deadline is exceeded (e.g. timeout), we don't have to return an HTTP error
		// we've already flushed a response to the client
		requestLogger.Error("ResolveGraphQLSubscription", zap.Error(err))
	}
	flushWriter.Canceled()
}

type httpFlushWriter struct {
	ctx                    context.Context
	writer                 http.ResponseWriter
	flusher                http.Flusher
	postResolveTransformer *postresolvetransform.Transformer
	subscribeOnce          bool
	sse                    bool
	useJsonPatch           bool
	close                  func()
	buf                    *bytes.Buffer
	lastMessage            *bytes.Buffer
	variables              []byte

	// Used for hooks
	resolveContext *resolve.Context
	request        *http.Request
	hooksPipeline  *hooks.SubscriptionOperationPipeline
	logger         *zap.Logger
}

func (f *httpFlushWriter) Header() http.Header {
	return f.writer.Header()
}

func (f *httpFlushWriter) WriteHeader(statusCode int) {
	f.writer.WriteHeader(statusCode)
}

func (f *httpFlushWriter) Write(p []byte) (n int, err error) {
	return f.buf.Write(p)
}

func (f *httpFlushWriter) Close() {
	if f.sse {
		_, _ = f.writer.Write([]byte("event: done\n\n"))
		f.flusher.Flush()
	}
}

func (f *httpFlushWriter) Canceled() {
	if f.ctx.Err() == nil {
		return
	}

	f.resolveContext.Context = context.WithoutCancel(f.resolveContext.Context)
	_, _ = f.hooksPipeline.PostResolve(f.resolveContext, nil, f.request, nil)
}

func (f *httpFlushWriter) Flush() {
	resp := f.buf.Bytes()
	f.buf.Reset()
	if f.postResolveTransformer != nil {
		resp, _ = f.postResolveTransformer.Transform(resp)
	}

	if f.hooksPipeline != nil {
		postResolveResponse, err := f.hooksPipeline.PostResolve(f.resolveContext, nil, f.request, resp)
		if err != nil {
			if f.logger != nil {
				f.logger.Error("subscription postResolve hooks", zap.Error(err))
			}
		} else {
			resp = postResolveResponse.Data
		}
	}

	if f.useJsonPatch && f.lastMessage.Len() != 0 {
		last := f.lastMessage.Bytes()
		patch, err := jsonpatch.CreatePatch(last, resp)
		if err != nil {
			if f.logger != nil {
				f.logger.Error("subscription json patch", zap.Error(err))
			}
			return
		}
		if len(patch) == 0 {
			// no changes
			return
		}
		patchData, err := json.Marshal(patch)
		if err != nil {
			if f.logger != nil {
				f.logger.Error("subscription json patch", zap.Error(err))
			}
			return
		}
		if f.sse {
			_, _ = f.writer.Write([]byte("data: "))
		}
		if len(patchData) < len(resp) {
			_, _ = f.writer.Write(patchData)
		} else {
			_, _ = f.writer.Write(resp)
		}
	}

	if f.lastMessage.Len() == 0 || !f.useJsonPatch {
		if f.sse {
			_, _ = f.writer.Write([]byte("data: "))
		}
		_, _ = f.writer.Write(resp)
	}

	f.lastMessage.Reset()
	_, _ = f.lastMessage.Write(resp)

	if f.subscribeOnce {
		f.flusher.Flush()
		f.close()
		return
	}
	_, _ = f.writer.Write([]byte("\n\n"))
	f.flusher.Flush()
}

// MergeJsonRightIntoLeft merges the right JSON into the left JSON while overriding the left side
func MergeJsonRightIntoLeft(left, right []byte) []byte {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	result := gjson.ParseBytes(right)
	result.ForEach(func(key, value gjson.Result) bool {
		if value.Raw != "{}" {
			left, _ = sjson.SetRawBytes(left, key.Str, unsafebytes.StringToBytes(value.Raw))
		}
		return true
	})
	return left
}

func (r *Builder) authenticationHooks() authentication.Hooks {
	return hooks.NewAuthenticationHooks(hooks.AuthenticationConfig{
		Client:                     r.middlewareClient,
		Log:                        r.log,
		PostAuthentication:         r.api.AuthenticationConfig.Hooks.PostAuthentication,
		MutatingPostAuthentication: r.api.AuthenticationConfig.Hooks.MutatingPostAuthentication,
		PostLogout:                 r.api.AuthenticationConfig.Hooks.PostLogout,
		RevalidateAuthentication:   r.api.AuthenticationConfig.Hooks.RevalidateAuthentication,
	})
}

func (r *Builder) registerAuth(insecureCookies bool) error {

	var (
		hashKey, blockKey, csrfSecret []byte
		jwksProviders                 []*wgpb.JwksAuthProvider
	)

	if h := loadvariable.String(r.api.AuthenticationConfig.CookieBased.HashKey); h != "" {
		hashKey = []byte(h)
	} else if fallback := r.api.CookieBasedSecrets.HashKey; fallback != nil {
		hashKey = fallback
	}

	if b := loadvariable.String(r.api.AuthenticationConfig.CookieBased.BlockKey); b != "" {
		blockKey = []byte(b)
	} else if fallback := r.api.CookieBasedSecrets.BlockKey; fallback != nil {
		blockKey = fallback
	}

	if c := loadvariable.String(r.api.AuthenticationConfig.CookieBased.CsrfSecret); c != "" {
		csrfSecret = []byte(c)
	} else if fallback := r.api.CookieBasedSecrets.CsrfSecret; fallback != nil {
		csrfSecret = fallback
	}

	if r.api == nil || r.api.HasCookieAuthEnabled() && (hashKey == nil || blockKey == nil || csrfSecret == nil) {
		panic("API is nil or hashkey, blockkey, csrfsecret invalid: This should never have happened. Either validation didn't detect broken configuration, or someone broke the validation code")
	}

	cookie := securecookie.New(hashKey, blockKey)

	if r.api.AuthenticationConfig.JwksBased != nil {
		jwksProviders = r.api.AuthenticationConfig.JwksBased.Providers
	}

	authHooks := r.authenticationHooks()

	loadUserConfig := authentication.LoadUserConfig{
		Log:           r.log,
		Cookie:        cookie,
		JwksProviders: jwksProviders,
		Hooks:         authHooks,
	}

	authRequiredPaths, authCache, authHandler := authentication.NewLoadUserMw(loadUserConfig)
	r.authCache = authCache
	r.authRequiredPaths = authRequiredPaths
	r.router.Use(authHandler)
	if r.enableCSRFProtect {
		r.router.Use(authentication.NewCSRFMw(authentication.CSRFConfig{
			InsecureCookies: insecureCookies,
			Secret:          csrfSecret,
		}))
	}

	userHandler := &authentication.UserHandler{
		Hooks:           authHooks,
		Log:             r.log,
		InsecureCookies: insecureCookies,
		Cookie:          cookie,
		PublicClaims:    r.api.AuthenticationConfig.PublicClaims,
	}

	r.router.Path("/auth/user").Methods(http.MethodGet, http.MethodOptions).Handler(userHandler)

	// fallback for old token user path
	// @deprecated use /auth/user instead
	r.router.Path("/auth/token/user").Methods(http.MethodGet, http.MethodOptions).Handler(userHandler)

	cookieBasedAuth := r.router.PathPrefix("/auth/cookie").Subrouter()

	// fallback for old cookie user path
	// @deprecated use /auth/user instead
	cookieBasedAuth.Path("/user").Methods(http.MethodGet, http.MethodOptions).Handler(userHandler)

	cookieBasedAuth.Path("/csrf").Methods(http.MethodGet, http.MethodOptions).Handler(&authentication.CSRFTokenHandler{})

	return r.registerCookieAuthHandlers(cookieBasedAuth, cookie, authHooks)
}

func (r *Builder) registerCookieAuthHandlers(router *mux.Router, cookie *securecookie.SecureCookie, authHooks authentication.Hooks) error {

	oidcProviders, err := r.configureOpenIDConnectProviders()
	if err != nil {
		return fmt.Errorf("error configuring OIDC providers: %w", err)
	}

	router.Path("/user/logout").Methods(http.MethodGet, http.MethodOptions).Handler(&authentication.UserLogoutHandler{
		InsecureCookies: r.insecureCookies,
		OpenIDProviders: oidcProviders,
		Hooks:           authHooks,
		Log:             r.log,
	})

	if r.api.AuthenticationConfig == nil || r.api.AuthenticationConfig.CookieBased == nil {
		return nil
	}

	for _, provider := range r.api.AuthenticationConfig.CookieBased.Providers {
		r.configureCookieProvider(router, provider, cookie)
	}

	return nil
}

func (r *Builder) configureOpenIDConnectProviders() (*authentication.OpenIDConnectProviderSet, error) {
	var providers authentication.OpenIDConnectProviderSet

	httpClient := &http.Client{
		Timeout: r.api.Options.DefaultTimeout,
	}

	for _, provider := range r.api.AuthenticationConfig.CookieBased.Providers {
		var flavor authentication.OpenIDConnectFlavor
		switch provider.Kind {
		case wgpb.AuthProviderKind_AuthProviderOIDC:
			flavor = authentication.OpenIDConnectFlavorDefault
		case wgpb.AuthProviderKind_AuthProviderAuth0:
			flavor = authentication.OpenIDConnectFlavorAuth0
		default:
			continue
		}
		if provider.OidcConfig == nil {
			continue
		}
		issuer := loadvariable.String(provider.OidcConfig.Issuer)
		clientID := loadvariable.String(provider.OidcConfig.ClientId)
		clientSecret := loadvariable.String(provider.OidcConfig.ClientId)

		oidc, err := authentication.NewOpenIDConnectProvider(issuer, clientID, clientSecret, &authentication.OpenIDConnectProviderOptions{
			Flavor:     flavor,
			HTTPClient: httpClient,
			Logger:     r.log,
		})
		if err != nil {
			r.log.Warn("error in OIDC provider", zap.String("authentication", provider.Id), zap.Error(err))
			return nil, fmt.Errorf("error in %s OIDC provider: %w", provider.Id, err)
		}
		if err := providers.Add(provider.Id, oidc); err != nil {
			r.log.Warn("could not register OIDC provider", zap.String("authentication", provider.Id), zap.Error(err))
			return nil, fmt.Errorf("could not register OIDC provider %s: %w", provider.Id, err)
		}
	}

	return &providers, nil
}

func (r *Builder) configureCookieProvider(router *mux.Router, provider *wgpb.AuthProvider, cookie *securecookie.SecureCookie) {

	router.Use(authentication.RedirectAlreadyAuthenticatedUsers(
		loadvariable.Strings(r.api.AuthenticationConfig.CookieBased.AuthorizedRedirectUris),
		loadvariable.Strings(r.api.AuthenticationConfig.CookieBased.AuthorizedRedirectUriRegexes),
	))

	authorizeRouter := router.PathPrefix("/authorize").Subrouter()
	authorizeRouter.Use(authentication.ValidateRedirectURIQueryParameter(
		loadvariable.Strings(r.api.AuthenticationConfig.CookieBased.AuthorizedRedirectUris),
		loadvariable.Strings(r.api.AuthenticationConfig.CookieBased.AuthorizedRedirectUriRegexes),
	))

	callbackRouter := router.PathPrefix("/callback").Subrouter()

	switch provider.Kind {
	case wgpb.AuthProviderKind_AuthProviderGithub:
		if provider.GithubConfig == nil {
			return
		}
		if loadvariable.String(provider.GithubConfig.ClientId) == "demo" {
			provider.GithubConfig.ClientId = &wgpb.ConfigurationVariable{
				Kind:                  wgpb.ConfigurationVariableKind_STATIC_CONFIGURATION_VARIABLE,
				StaticVariableContent: r.githubAuthDemoClientID,
			}
			provider.GithubConfig.ClientSecret = &wgpb.ConfigurationVariable{
				Kind:                  wgpb.ConfigurationVariableKind_STATIC_CONFIGURATION_VARIABLE,
				StaticVariableContent: r.githubAuthDemoClientSecret,
			}
		}
		github := authentication.NewGithubCookieHandler(r.api.Options.PublicNodeUrl, r.log)
		github.Register(authorizeRouter, callbackRouter, authentication.GithubConfig{
			ClientID:           loadvariable.String(provider.GithubConfig.ClientId),
			ClientSecret:       loadvariable.String(provider.GithubConfig.ClientSecret),
			ProviderID:         provider.Id,
			InsecureCookies:    r.insecureCookies,
			ForceRedirectHttps: r.forceHttpsRedirects,
			Cookie:             cookie,
		}, r.authenticationHooks())
		r.log.Debug("api.configureCookieProvider",
			zap.String("provider", "github"),
			zap.String("providerId", provider.Id),
			zap.String("clientID", loadvariable.String(provider.GithubConfig.ClientId)),
		)
	case wgpb.AuthProviderKind_AuthProviderOIDC:
		fallthrough
	case wgpb.AuthProviderKind_AuthProviderAuth0:
		if provider.OidcConfig == nil {
			return
		}

		queryParameters := make([]authentication.QueryParameter, 0, len(provider.OidcConfig.QueryParameters))
		for _, p := range provider.OidcConfig.QueryParameters {
			queryParameters = append(queryParameters, authentication.QueryParameter{
				Name:  loadvariable.String(p.Name),
				Value: loadvariable.String(p.Value),
			})
		}

		openID := authentication.NewOpenIDConnectCookieHandler(r.api.Options.PublicNodeUrl, r.log)
		openID.Register(authorizeRouter, callbackRouter, authentication.OpenIDConnectConfig{
			Issuer:             loadvariable.String(provider.OidcConfig.Issuer),
			ClientID:           loadvariable.String(provider.OidcConfig.ClientId),
			ClientSecret:       loadvariable.String(provider.OidcConfig.ClientSecret),
			QueryParameters:    queryParameters,
			ProviderID:         provider.Id,
			InsecureCookies:    r.insecureCookies,
			ForceRedirectHttps: r.forceHttpsRedirects,
			Cookie:             cookie,
		}, r.authenticationHooks())
		r.log.Debug("api.configureCookieProvider",
			zap.String("provider", "oidc"),
			zap.String("providerId", provider.Id),
			zap.String("issuer", loadvariable.String(provider.OidcConfig.Issuer)),
			zap.String("clientID", loadvariable.String(provider.OidcConfig.ClientId)),
		)
	default:
		panic("unreachable")
	}
}

func (r *Builder) registerFunctionOperation(operation *wgpb.Operation, apiPath string, operationSchema *OperationSchema) error {
	var (
		route *mux.Route
	)

	if operation.OperationType == wgpb.OperationType_MUTATION {
		route = r.router.Methods(http.MethodPost, http.MethodOptions).Path(apiPath).Name(apiPath)
	} else {
		// query and subscription
		route = r.router.Methods(http.MethodGet, http.MethodOptions).Path(apiPath).Name(apiPath)
	}

	stringInterpolator, err := interpolate.NewOpenapi3StringInterpolator(operationSchema.Variables, operationSchema.Definitions)
	if err != nil {
		return err
	}

	queryParamsAllowList := maps.Keys(stringInterpolator.PropertiesTypeMap)
	handler := &FunctionHandler{
		cache:                      r.cache,
		operation:                  operation,
		log:                        r.log,
		rbacEnforcer:               authentication.NewRBACEnforcer(operation),
		hooksClient:                r.middlewareClient,
		queryParamsAllowList:       queryParamsAllowList,
		postResolveTransformer:     postresolvetransform.NewTransformer(operation.PostResolveTransformations),
		openapi3StringInterpolator: stringInterpolator,
		operationSchema:            operationSchema,
	}

	if operation.LiveQueryConfig != nil && operation.LiveQueryConfig.Enabled {
		handler.liveQuery = liveQueryConfig{
			enabled:                true,
			pollingIntervalSeconds: operation.LiveQueryConfig.PollingIntervalSeconds,
		}
	}

	routeHandler := ensureRequiresRateLimiter(operation, handler)
	routeHandler = ensureRequiresSemaphore(operation, routeHandler)
	routeHandler, authRequired := authentication.EnsureRequiresAuthentication(operation, routeHandler)
	if authRequired {
		r.authRequiredPaths[apiPath] = true
	}
	routeHandler = intercept(routeHandler, r.middlewareClient, operation, queryParamsAllowList)
	route.Handler(routeHandler)

	r.log.Debug("registered FunctionHandler",
		zap.String("operation", operation.Path),
		zap.String("Endpoint", apiPath),
		zap.String("method", operation.OperationType.String()),
	)

	return nil
}

type FunctionHandler struct {
	cache                apicache.Cache
	operation            *wgpb.Operation
	log                  *zap.Logger
	variablesValidator   *inputvariables.Validator
	rbacEnforcer         *authentication.RBACEnforcer
	hooksClient          *hooks.Client
	queryParamsAllowList []string
	liveQuery            liveQueryConfig

	postResolveTransformer     *postresolvetransform.Transformer
	openapi3StringInterpolator *interpolate.Openapi3StringInterpolator
	operationSchema            *OperationSchema
}

func (h *FunctionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	reqID := r.Header.Get(logging.RequestIDHeader)
	requestLogger := h.log.With(logging.WithRequestID(reqID))
	r = r.WithContext(context.WithValue(r.Context(), logging.RequestIDKey{}, reqID))

	r = setOperationMetaData(r, h.operation, h.queryParamsAllowList, h.cache)

	isInternal := strings.HasPrefix(r.URL.Path, "/internal/")

	ctx := pool.GetCtx(r, r, pool.Config{})
	defer pool.PutCtx(ctx)

	if proceed := h.rbacEnforcer.Enforce(r); !proceed {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	variablesBuf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(variablesBuf)

	ct := r.Header.Get("Content-Type")
	if r.Method == http.MethodGet {
		ctx.Variables = parseQueryVariables(r, h.queryParamsAllowList, h.openapi3StringInterpolator.PropertiesTypeMap)
	} else if ct == "application/x-www-form-urlencoded" {
		ctx.Variables = h.parseFormVariables(r)
	} else {
		_, err := io.Copy(variablesBuf, r.Body)
		if err != nil {
			requestLogger.Error("failed to copy variables buf", zap.Error(err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if isInternal {
			ctx.Variables, _, _, _ = jsonparser.Get(variablesBuf.Bytes(), "input")
		} else {
			ctx.Variables = variablesBuf.Bytes()
		}
	}

	if len(ctx.Variables) == 0 {
		ctx.Variables = []byte("{}")
	} else {
		ctx.Variables = h.openapi3StringInterpolator.Interpolate(ctx.Variables)
	}

	variablesBuf.Reset()
	err := json.Compact(variablesBuf, ctx.Variables)
	if err != nil {
		requestLogger.Error("failed to compact variables", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx.Variables, _ = handleSpecial(variablesBuf.Bytes())

	if !openapi3ValidateInputVariables(requestLogger, ctx.Variables, h.operationSchema, w) {
		return
	}

	isLive := h.liveQuery.enabled && r.URL.Query().Has(WgLiveParam)

	buf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(buf)

	input, err := hooks.EncodeData(r, buf, ctx.Variables, nil)
	if done := handleOperationErr(requestLogger, err, w, "encoding hook data failed", h.operation); done {
		return
	}

	switch {
	case isLive:
		h.handleLiveQuery(ctx, w, r, input, requestLogger)
	case h.operation.OperationType == wgpb.OperationType_SUBSCRIPTION:
		h.handleSubscriptionRequest(ctx, w, r, input, requestLogger)
	default:
		h.handleRequest(ctx, w, input, requestLogger)
	}
}

func (h *FunctionHandler) handleLiveQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, input []byte, requestLogger *zap.Logger) {

	var (
		err error
		fw  *httpFlushWriter
		ok  bool
		out *hooks.MiddlewareHookResponse
	)

	ctx, fw, ok = getFlushWriter(ctx, input, r, w)
	if !ok {
		errMsg := "request doesn't support flushing"
		requestLogger.Error(errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	fw.postResolveTransformer = h.postResolveTransformer
	buf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(buf)

	var (
		lastResponse bytes.Buffer
	)

	defer fw.Close()

	for {
		select {
		case <-ctx.Done():
			fw.Canceled()
			return
		default:
			out, err = h.hooksClient.DoFunctionRequest(ctx, h.operation.Path, input, buf)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				requestLogger.Error("failed to execute function", zap.Error(err))
				return
			}
			if bytes.Equal(out.Response, lastResponse.Bytes()) {
				continue
			}
			_, err = fw.Write(out.Response)
			if err != nil {
				requestLogger.Error("failed to write response", zap.Error(err))
				return
			}
			fw.Flush()
			lastResponse.Reset()
			lastResponse.Write(out.Response)
			time.Sleep(time.Duration(h.liveQuery.pollingIntervalSeconds) * time.Second)
		}
	}
}

func (h *FunctionHandler) handleRequest(ctx context.Context, w http.ResponseWriter, input []byte, requestLogger *zap.Logger) {

	buf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(buf)

	out, err := h.hooksClient.DoFunctionRequest(ctx, h.operation.Path, input, buf)
	if err != nil {
		if ctx.Err() != nil {
			requestLogger.Debug("request cancelled")
			return
		}
		requestLogger.Error("failed to call function", zap.Error(err))
		// w.WriteHeader(http.StatusInternalServerError)
		// _, _ = w.Write([]byte(fmt.Sprintf(`{"error": "%s"}`, err.Error())))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(out.ClientResponseStatusCode)
	if len(out.Response) > 0 {
		_, _ = w.Write(out.Response)
	}
}

func (h *FunctionHandler) handleSubscriptionRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, input []byte, requestLogger *zap.Logger) {
	wgParams := NewWgRequestParams(r.URL)

	setSubscriptionHeaders(w)
	buf := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(buf)
	err := h.hooksClient.DoFunctionSubscriptionRequest(ctx, h.operation.Path, input, wgParams.SubscribeOnce, wgParams.UseSse, wgParams.UseJsonPatch, w, buf, h.postResolveTransformer)
	if err != nil {
		if ctx.Err() != nil {
			requestLogger.Debug("request cancelled")
			return
		}
		requestLogger.Error("failed to call function", zap.Error(err))
		// w.WriteHeader(http.StatusInternalServerError)
		// _, _ = w.Write([]byte(fmt.Sprintf(`{"error": "%s"}`, err.Error())))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *FunctionHandler) parseFormVariables(r *http.Request) []byte {
	if err := r.ParseForm(); err == nil {
		return makeRawVariables(r.Form)
	}
	return []byte("{}")
}

type EndpointUnavailableHandler struct {
	OperationName string
	Logger        *zap.Logger
}

func (m *EndpointUnavailableHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.Logger.Error("operation not available", zap.String("operationName", m.OperationName), zap.String("URL", r.URL.Path))
	http.Error(w, fmt.Sprintf("Endpoint not available for Operation: %s, please check the logs.", m.OperationName), http.StatusServiceUnavailable)
}

type OperationMetaData struct {
	OperationName string
	OperationType wgpb.OperationType
	ArgsAllowList []string
}

func (o *OperationMetaData) GetOperationTypeString() string {
	switch o.OperationType {
	case wgpb.OperationType_MUTATION:
		return "mutation"
	case wgpb.OperationType_QUERY:
		return "query"
	case wgpb.OperationType_SUBSCRIPTION:
		return "subscription"
	default:
		return "unknown"
	}
}

func setOperationMetaData(r *http.Request, operation *wgpb.Operation, argsAllowList []string, cache apicache.Cache) *http.Request {
	metaData := &OperationMetaData{
		OperationName: operation.Name,
		OperationType: operation.OperationType,
		ArgsAllowList: argsAllowList,
	}
	ctx := database.AddTransactionContext(r, operation.Transaction, cache)
	ctx = context.WithValue(ctx, "operationMetaData", metaData)
	return r.WithContext(ctx)
}

func getOperationMetaData(r *http.Request) *OperationMetaData {
	maybeMetaData := r.Context().Value("operationMetaData")
	if maybeMetaData == nil {
		return nil
	}
	return maybeMetaData.(*OperationMetaData)
}

func setSubscriptionHeaders(w http.ResponseWriter) {
	w.Header().Set(customhttpclient.ContentTypeHeader, customhttpclient.TextEventStreamMine)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// allow unbuffered responses, it's used when it's necessary just to pass response through
	// setting this to “yes” will allow the response to be cached
	w.Header().Set("X-Accel-Buffering", "no")
}

func getHooksFlushWriter(ctx *resolve.Context, r *http.Request, w http.ResponseWriter, pipeline *hooks.SubscriptionOperationPipeline, logger *zap.Logger) (*httpFlushWriter, bool) {
	var flushWriter *httpFlushWriter
	var ok bool
	ctx.Context, flushWriter, ok = getFlushWriter(ctx.Context, ctx.Variables, r, w)
	if !ok {
		return nil, false
	}

	flushWriter.resolveContext = ctx
	flushWriter.request = r
	flushWriter.hooksPipeline = pipeline
	flushWriter.logger = logger
	return flushWriter, true
}

func getFlushWriter(ctx context.Context, variables []byte, r *http.Request, w http.ResponseWriter) (context.Context, *httpFlushWriter, bool) {
	wgParams := NewWgRequestParams(r.URL)

	flusher, ok := w.(http.Flusher)
	if !ok {
		return ctx, nil, false
	}

	if !wgParams.SubscribeOnce {
		setSubscriptionHeaders(w)
	}

	flusher.Flush()

	flushWriter := &httpFlushWriter{
		writer:       w,
		flusher:      flusher,
		sse:          wgParams.UseSse,
		useJsonPatch: wgParams.UseJsonPatch,
		buf:          &bytes.Buffer{},
		lastMessage:  &bytes.Buffer{},
		ctx:          ctx,
		variables:    variables,
	}

	if wgParams.SubscribeOnce {
		flushWriter.subscribeOnce = true
		ctx, flushWriter.close = context.WithCancel(ctx)
	}

	return ctx, flushWriter, true
}

func handleOperationErr(log *zap.Logger, err error, w http.ResponseWriter, errorMessage string, operation *wgpb.Operation) (done bool) {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		// client closed connection
		http.Error(w, err.Error(), 499)
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		// request timeout exceeded
		log.Error("request timeout exceeded",
			zap.String("operationName", operation.Name),
			zap.String("operationType", operation.OperationType.String()),
		)
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return true
	}
	var validationError *inputvariables.ValidationError
	if errors.As(err, &validationError) {
		w.WriteHeader(http.StatusBadRequest)
		enc := json.NewEncoder(w)
		if err := enc.Encode(&validationError); err != nil {
			log.Error("error encoding validation error", zap.Error(err))
		}
		return true
	}
	log.Error(errorMessage,
		zap.String("operationName", operation.Name),
		zap.String("operationType", operation.OperationType.String()),
		zap.String("error", err.Error()),
	)
	http.Error(w, fmt.Sprintf("%s: %s", errorMessage, errors.Cause(err).Error()), http.StatusInternalServerError)
	return true
}

func openapi3ValidateInputVariables(log *zap.Logger, variables []byte, operationSchema *OperationSchema, w http.ResponseWriter) (ok bool) {
	var err error
	defer func() {
		if err != nil {
			errString := err.Error()
			log.Error("failed to validate input variables", zap.String("error", errString))
			http.Error(w, errString, http.StatusBadRequest)
		} else {
			ok = true
		}
	}()

	var data map[string]interface{}
	if err = jsonIterator.Unmarshal(variables, &data); err != nil {
		return
	}

	customizeSchemaResolve := func(ref string) *openapi3.Schema {
		schemaRefName := strings.TrimPrefix(ref, interpolate.Openapi3SchemaRefPrefix)
		if schema, existed := operationSchema.Definitions[schemaRefName]; existed {
			return schema.Value
		}
		return nil
	}
	err = operationSchema.Variables.Value.VisitJSON(data,
		openapi3.EnableUnknownPropertyValidation(),
		openapi3.SetCustomizeSchemaResolve(customizeSchemaResolve))
	return
}
