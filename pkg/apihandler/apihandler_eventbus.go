package apihandler

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/wundergraph/wundergraph/pkg/eventbus"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"go.uber.org/zap"
	"net/http"
)

func (r *Builder) Subscribe() {
	r.eventbusSubscribeOperation()
	r.eventbusSubscribeS3UploadClient()
}

func (r *Builder) eventbusSubscribeOperation() {
	eventbus.Subscribe(eventbus.ChannelOperation, eventbus.EventInsert, func(data any) any {
		operation := data.(*wgpb.Operation)
		if err := r.registerOperation(operation, r.ctx); err != nil {
			r.log.Error("registerOperation", zap.String("operation", operation.Path), zap.Error(err))
		}
		return data
	})
	eventbus.Subscribe(eventbus.ChannelOperation, eventbus.EventBatchInsert, func(data any) any {
		for _, operation := range data.([]*wgpb.Operation) {
			eventbus.DirectCall(eventbus.ChannelOperation, eventbus.EventInsert, operation)
		}
		return data
	})
	eventbus.Subscribe(eventbus.ChannelOperation, eventbus.EventDelete, func(data any) any {
		operationPath := data.(string)
		if apiPath := OperationApiPath(operationPath); addRouteSkipMatcher(r.router, apiPath) {
			delete(r.authRequiredFuncs, apiPath)
			r.log.Debug("unregisterOperation", zap.String("operation", operationPath))
		}
		return data
	})
	eventbus.Subscribe(eventbus.ChannelOperation, eventbus.EventBatchDelete, func(data any) any {
		for _, path := range data.([]string) {
			eventbus.DirectCall(eventbus.ChannelOperation, eventbus.EventDelete, path)
		}
		return data
	})
	eventbus.Subscribe(eventbus.ChannelOperation, eventbus.EventUpdate, func(data any) any {
		operation := data.(*wgpb.Operation)
		eventbus.DirectCall(eventbus.ChannelOperation, eventbus.EventDelete, operation.Path)
		if operation.Name != "" {
			eventbus.DirectCall(eventbus.ChannelOperation, eventbus.EventInsert, operation)
		}
		return data
	})
	eventbus.Subscribe(eventbus.ChannelOperation, eventbus.EventBatchUpdate, func(data any) any {
		for _, operation := range data.([]*wgpb.Operation) {
			eventbus.DirectCall(eventbus.ChannelOperation, eventbus.EventUpdate, operation)
		}
		return data
	})
	eventbus.Subscribe(eventbus.ChannelOperation, eventbus.EventInvalid, func(data any) any {
		r.registerInvalidOperation(data.(string))
		return data
	})
}

func (r *Builder) eventbusSubscribeS3UploadClient() {
	eventbus.Subscribe(eventbus.ChannelStorage, eventbus.EventInsert, func(data any) any {
		r.registerS3UploadClient(data.(*wgpb.S3UploadConfiguration))
		return data
	})
	eventbus.Subscribe(eventbus.ChannelStorage, eventbus.EventDelete, func(data any) any {
		provider := data.(string)
		if apiPath := fmt.Sprintf("/s3/%s/upload", provider); addRouteSkipMatcher(r.router, apiPath) {
			delete(r.authRequiredFuncs, apiPath)
			r.log.Debug("unregisterS3UploadClient", zap.String("storage", provider))
		}
		return data
	})
	eventbus.Subscribe(eventbus.ChannelStorage, eventbus.EventUpdate, func(data any) any {
		storageConfig := data.(*wgpb.S3UploadConfiguration)
		eventbus.DirectCall(eventbus.ChannelStorage, eventbus.EventDelete, storageConfig.Name)
		eventbus.DirectCall(eventbus.ChannelStorage, eventbus.EventInsert, storageConfig)
		return data
	})
}

func addRouteSkipMatcher(router *mux.Router, apiPath string) bool {
	if route := router.GetRoute(apiPath); route != nil {
		route.MatcherFunc(func(request *http.Request, match *mux.RouteMatch) bool { return false })
		return true
	}
	return false
}
