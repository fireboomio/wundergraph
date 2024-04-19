package eventbus

import (
	"golang.org/x/exp/slices"
	"runtime"
	"sync"
	"time"
)

type Channel string

const (
	ChannelAuthentication Channel = "authentication"
	ChannelGlobalSetting  Channel = "globalSetting"
	ChannelDatasource     Channel = "datasource"
	ChannelOperation      Channel = "operation"
	ChannelRole           Channel = "role"
	ChannelSdk            Channel = "sdk"
	ChannelStorage        Channel = "storage"
)

type Event string

const (
	EventInsert      Event = "insert"
	EventBatchInsert Event = "batchInsert"
	EventUpdate      Event = "update"
	EventBatchUpdate Event = "batchUpdate"
	EventDelete      Event = "delete"
	EventBatchDelete Event = "batchDelete"
	EventInvalid     Event = "invalid"
	EventRuntime     Event = "runtime"
	EventBreak       Event = "break"
)

type (
	EventSubscribe interface {
		Subscribe()
	}
	EventBreakData interface {
		BreakData()
	}
	eventbus struct {
		subscribers map[Channel][]*subscriber
		notices     []*noticer
		lock        sync.RWMutex
	}
	noticer struct {
		events  []Event
		handler func(Channel, Event, any)
	}
	subscriber struct {
		caller  string
		event   Event
		handler func(any) any
	}
	BreakData struct {
		Event Event
		Cost  time.Duration
	}
)

func searchSubscriber(channel Channel, event Event) (result *subscriber, caller string) {
	_, caller, _, ok := runtime.Caller(2)
	if !ok {
		return
	}

	subscribers := eb.subscribers[channel]
	index := slices.IndexFunc(subscribers, func(item *subscriber) bool {
		return item.caller == caller && item.event == event
	})
	if index == -1 {
		return
	}

	result = subscribers[index]
	return
}

func EnsureEventBreakData(data any) {
	if bus, ok := data.(EventBreakData); ok {
		bus.BreakData()
	}
}

func EnsureEventSubscribe(data any) {
	if bus, ok := data.(EventSubscribe); ok {
		bus.Subscribe()
	}
}

func DirectCall(channel Channel, event Event, data any) any {
	if result, _ := searchSubscriber(channel, event); result != nil {
		return result.handler(data)
	}

	return nil
}

func Notice(notice func(Channel, Event, any), event ...Event) {
	eb.lock.Lock()
	defer eb.lock.Unlock()

	eb.notices = append(eb.notices, &noticer{event, notice})
}

func Subscribe(channel Channel, event Event, handler func(any) any) {
	eb.lock.Lock()
	defer eb.lock.Unlock()

	result, caller := searchSubscriber(channel, event)
	if result != nil {
		result.handler = handler
	} else {
		eb.subscribers[channel] = append(eb.subscribers[channel], &subscriber{caller, event, handler})
	}
}

func Publish(channel Channel, event Event, data any) bool {
	eb.lock.RLock()

	subscribers := eb.subscribers[channel]
	var handlers []func(any) any
	var breakHandler func(any) any
	for _, item := range subscribers {
		switch item.event {
		case event:
			handlers = append(handlers, item.handler)
		case EventBreak:
			breakHandler = item.handler
		}
	}
	if len(handlers) == 0 || breakHandler == nil {
		eb.lock.RUnlock()
		return false
	}

	go func() {
		latestData := data
		start := time.Now()
		for _, handler := range handlers {
			if latestData = handler(latestData); latestData == nil {
				break
			}
		}
		if latestData != nil {
			breakHandler(&BreakData{event, time.Since(start)})
		}
		for _, notice := range eb.notices {
			if slices.Contains(notice.events, event) {
				notice.handler(channel, event, data)
			}
		}
		eb.lock.RUnlock()
	}()
	return true
}

var eb *eventbus

func init() {
	eb = &eventbus{subscribers: make(map[Channel][]*subscriber)}
}
