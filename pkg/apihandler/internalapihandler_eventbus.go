package apihandler

import (
	"github.com/wundergraph/wundergraph/pkg/eventbus"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"go.uber.org/zap"
)

func (i *InternalBuilder) Subscribe() {
	i.eventbusSubscribeOperation()
}

func (i *InternalBuilder) eventbusSubscribeOperation() {
	eventbus.Subscribe(eventbus.ChannelOperation, eventbus.EventInsert, func(data any) any {
		operation := data.(*wgpb.Operation)
		if err := i.registerOperation(operation, i.ctx); err != nil {
			i.log.Error("registerInternalOperation", zap.String("operation", operation.Path), zap.Error(err))
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
		addRouteSkipMatcher(i.router, internalPrefix+OperationApiPath(data.(string)))
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
}
