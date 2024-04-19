package database

import (
	"context"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/hashicorp/go-uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/spf13/cast"
	"github.com/wundergraph/wundergraph/pkg/apicache"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	HeaderTransactionManually  = "X-Transaction-Manually"
	HeaderTransactionId        = "X-Transaction-Id"
	transactionStartInput      = `{"tx_id": "%s", "max_wait": %d, "timeout": %d, "isolation_level": "%s"}`
	pathTransactionRequest     = "transactionRequest"
	transactionRequestStart    = "/transaction/start"
	transactionRequestCommit   = "/transaction/%s/commit"
	transactionRequestRollback = "/transaction/%s/rollback"
	transactionManagerKey      = "transactionManager"
)

var DefaultTransaction = &wgpb.OperationTransaction{
	MaxWaitSeconds: 30,
	TimeoutSeconds: 30,
	IsolationLevel: wgpb.OperationTransactionIsolationLevel_read_committed,
}

func AddTransactionContext(r *http.Request, transaction *wgpb.OperationTransaction, cache apicache.Cache) context.Context {
	isManually := isTransactionManually(r)
	if transaction == nil && isManually {
		transaction = DefaultTransaction
	}
	if transaction == nil {
		return r.Context()
	}

	transactionData := &Transaction{setting: transaction}
	transactionData.id = r.Header.Get(HeaderTransactionId)
	if len(transactionData.id) == 0 {
		transactionData.id, _ = uuid.GenerateUUID()
		r.Header.Set(HeaderTransactionId, transactionData.id)
	}
	ctx := context.WithValue(r.Context(), Transaction{}, transactionData)
	var txManager *transactionManager
	if !cache.GetValue(ctx, transactionData.id, &txManager) {
		txManager = &transactionManager{metas: make(map[string]*transactionManagerMeta)}
		cache.SetValue(transactionData.id, txManager)
		if !isManually {
			ctx = context.WithValue(ctx, transactionManagerKey, txManager)
		}
	}
	transactionData.manager = txManager
	return ctx
}

func AddTransactionManagerContext(r *http.Request, cache apicache.Cache) (context.Context, bool) {
	transactionId := r.Header.Get(HeaderTransactionId)
	if len(transactionId) == 0 {
		return nil, false
	}

	var txManager *transactionManager
	if !cache.GetValue(r.Context(), transactionId, &txManager) {
		return nil, false
	}

	return context.WithValue(r.Context(), transactionManagerKey, txManager), true
}

func NotifyTransactionFinish(r *http.Request, cache apicache.Cache, err error) bool {
	transactionId := r.Header.Get(HeaderTransactionId)
	if len(transactionId) == 0 {
		return false
	}

	txManager, ok := r.Context().Value(transactionManagerKey).(*transactionManager)
	if !ok {
		return false
	}

	txManager.Lock()
	defer txManager.Unlock()
	defer cache.Delete(context.Background(), transactionId)
	for _, meta := range txManager.metas {
		meta.finishFunc(r.Context(), err)
	}
	return true
}

type (
	Transaction struct {
		id      string
		setting *wgpb.OperationTransaction
		manager *transactionManager
	}
	transactionManager struct {
		sync.Mutex
		metas map[string]*transactionManagerMeta
	}
	transactionManagerMeta struct {
		errors     []error
		startedCtx context.Context
		finishFunc func(context.Context, error)
		finishedAt time.Time
	}
)

func (t *Transaction) finishTransaction(ctx, startedCtx context.Context, s *Source, err error) {
	var transactionRequest string
	if err != nil || slices.ContainsFunc(maps.Values(t.manager.metas), func(item *transactionManagerMeta) bool {
		return len(item.errors) > 0
	}) {
		transactionRequest = transactionRequestRollback
	} else {
		transactionRequest = transactionRequestCommit
	}
	transactionRequest = fmt.Sprintf(transactionRequest, t.id)
	ctx = context.WithValue(ctx, pathTransactionRequest, transactionRequest)

	var (
		callback  func(...func(opentracing.Span))
		spanFuncs []func(opentracing.Span)
	)
	ctx, callback = logging.StartTraceContext(ctx, startedCtx, transactionRequest, s.spanWithDatasource())
	defer func() { callback(append(spanFuncs, logging.SpanWithLogError(err))...) }()

	buf, err := s.execute(ctx, nil, time.Second*5)
	defer pool.PutBytesBuffer(buf)
	spanFuncs = append(spanFuncs, logging.SpanWithLogOutput(buf.Bytes()))
	return
}

func (t *Transaction) startTransaction(ctx context.Context, s *Source) (startedMeta *transactionManagerMeta, err error) {
	t.manager.Lock()
	defer t.manager.Unlock()
	datasourceName := s.realDatasourceName()
	startedMeta, ok := t.manager.metas[datasourceName]
	if ok {
		return
	}

	maxWait, timeout := t.setting.MaxWaitSeconds*int64(time.Second), t.setting.TimeoutSeconds*int64(time.Second)
	isolationLevel := strings.ReplaceAll(t.setting.IsolationLevel.String(), "_", " ")
	request := []byte(fmt.Sprintf(transactionStartInput, t.id, maxWait, timeout, isolationLevel))
	ctx = context.WithValue(ctx, pathTransactionRequest, transactionRequestStart)

	var spanFuncs []func(opentracing.Span)
	ctx, callback := logging.StartTraceContext(ctx, nil, transactionRequestStart, s.spanWithDatasource(), logging.SpanWithLogInput(request))
	defer func() { callback(append(spanFuncs, logging.SpanWithLogError(err))...) }()

	buf, err := s.execute(ctx, request, time.Second*5)
	defer pool.PutBytesBuffer(buf)
	if err != nil {
		return
	}

	dataBytes := buf.Bytes()
	idBytes, _, _, _ := jsonparser.Get(dataBytes, "id")
	if string(idBytes) != t.id {
		err = fmt.Errorf("failed to start transaction [%s]", string(buf.Bytes()))
		return
	}

	// 需要先复制一下上下文中的链路，之前的可能已经结束
	startedMeta = &transactionManagerMeta{
		startedCtx: context.WithoutCancel(ctx),
		finishFunc: func(ctx context.Context, err error) {
			if startedMeta.finishedAt.IsZero() {
				t.finishTransaction(ctx, startedMeta.startedCtx, s, err)
				startedMeta.finishedAt = time.Now()
			}
		},
	}
	t.manager.metas[datasourceName] = startedMeta
	spanFuncs = append(spanFuncs, logging.SpanWithLogOutput(dataBytes))
	return
}

func (s *Source) fetchTransactionCtx(ctx context.Context) (transactionCtx context.Context, followCtx context.Context, reportError func(error), err error) {
	transactionCtx, reportError = ctx, func(error) {}
	action, ok := ctx.Value(Transaction{}).(*Transaction)
	if !ok {
		return
	}

	transactionMeta, err := action.startTransaction(ctx, s)
	if err != nil {
		return
	}

	transactionCtx = context.WithValue(transactionCtx, HeaderTransactionId, action.id)
	followCtx = transactionMeta.startedCtx
	reportError = func(err error) {
		if err != nil {
			transactionMeta.errors = append(transactionMeta.errors, err)
		}
	}
	return
}

func (s *Source) realDatasourceName() string {
	return strings.TrimPrefix(s.datasourceName, joinMutationPrefix)
}

func (s *Source) spanWithDatasource() func(opentracing.Span) {
	return func(span opentracing.Span) {
		ext.Component.Set(span, wgpb.DataSourceKind_PRISMA.String())
		ext.DBInstance.Set(span, s.realDatasourceName())
	}
}

func (s *Source) spanOperationName() string {
	return strings.Join([]string{s.rootTypeName, s.rootFieldName}, ".")
}

func isTransactionManually(r *http.Request) bool {
	return cast.ToBool(r.Header.Get(HeaderTransactionManually))
}

func modifyRequestForTransaction(ctx context.Context, req *http.Request) {
	if transactionId, ok := ctx.Value(HeaderTransactionId).(string); ok {
		req.Header[HeaderTransactionId] = []string{transactionId}
	}
	if transactionRequest, ok := ctx.Value(pathTransactionRequest).(string); ok {
		req.URL.Path = transactionRequest
	}
}
