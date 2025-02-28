package oas_datasource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/r3labs/sse/v2"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"github.com/wundergraph/wundergraph/pkg/customhttpclient"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"io"
	"math"
	"net/http"
	"strings"
)

const (
	errorResponseFormat = `{"errors":[{"message":"%s"}]}`
	dataResponseFormat  = `{"data":%s}`
)

var (
	headerData       = []byte("data:")
	headerDataLength = len(headerData)
)

type subscriptionSource struct {
	*Source
	config          SubscriptionConfiguration
	streamingClient *http.Client
}

func (p *Planner) ConfigureSubscription() plan.SubscriptionConfiguration {
	input := p.configureInput()
	return plan.SubscriptionConfiguration{
		Input: string(input),
		DataSource: &subscriptionSource{
			Source:          p.newSource(),
			config:          p.config.Subscription,
			streamingClient: p.streamingClient,
		},
	}
}

func (s *subscriptionSource) Start(ctx context.Context, input []byte, next chan<- []byte) error {
	if !s.config.Enabled {
		return errors.New("subscription is not enabled")
	}

	go s.subscribe(context.WithoutCancel(ctx), s.rewriteInput(input), next)
	return nil
}

func (s *subscriptionSource) subscribe(ctx context.Context, input []byte, next chan<- []byte) {
	var (
		err       error
		spanFuncs []func(opentracing.Span)
	)
	defer func() {
		if err != nil {
			next <- []byte(fmt.Sprintf(errorResponseFormat, err.Error()))
		}
		close(next)
	}()
	ctx, callback := logging.StartTraceContext(ctx, nil, s.spanOperationName(), s.spanWithDatasource(), logging.SpanWithLogInput(input))
	defer func() { callback(append(spanFuncs, logging.SpanWithLogError(err))...) }()

	_, resp, err := customhttpclient.Do(s.streamingClient, ctx, input)
	if err != nil {
		err = errors.New(strings.ReplaceAll(err.Error(), `"`, `'`))
		return
	}
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		bodyBytes = bytes.TrimSuffix(bodyBytes, literal.LINETERMINATOR)
		if bytes.HasPrefix(bodyBytes, literal.LBRACE) && bytes.HasSuffix(bodyBytes, literal.RBRACE) {
			newBodyBytes := make([]byte, 0, len(bodyBytes)*2)
			var quoted bool
			for i := range bodyBytes {
				switch bodyBytes[i] {
				case '"':
					quoted = !quoted
					newBodyBytes = append(newBodyBytes, literal.BACKSLASH...)
					newBodyBytes = append(newBodyBytes, literal.QUOTE...)
				case ' ', '\n':
					if quoted {
						newBodyBytes = append(newBodyBytes, bodyBytes[i])
					}
				default:
					newBodyBytes = append(newBodyBytes, bodyBytes[i])
				}
			}
			bodyBytes = newBodyBytes
		}
		err = errors.New(string(bodyBytes))
		return
	}
	if !strings.HasPrefix(resp.Header.Get(customhttpclient.ContentTypeHeader), customhttpclient.TextEventStreamMine) {
		err = errors.New("response is not a text event stream")
		return
	}
	if resp.Body == nil {
		err = errors.New("response body is nil")
		return
	}
	defer func() { _ = resp.Body.Close() }()

	var msg []byte
	reader := sse.NewEventStreamReader(resp.Body, math.MaxInt)
	for {
		msg, err = reader.ReadEvent()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		if len(msg) == 0 {
			continue
		}

		lines := bytes.FieldsFunc(msg, func(r rune) bool { return r == '\n' || r == '\r' })
		for _, line := range lines {
			switch {
			case bytes.HasPrefix(line, headerData):
				data := trim(line[headerDataLength:])
				if len(data) == 0 {
					continue
				}
				if string(data) == s.config.DoneData {
					return
				}

				data = []byte(fmt.Sprintf(dataResponseFormat, string(s.rewriteOutput(data))))
				next <- data
			default:
				continue
			}
		}
	}
}

func trim(data []byte) []byte {
	// remove the leading space
	data = bytes.TrimLeft(data, " \t")
	// remove the trailing new line
	data = bytes.TrimRight(data, "\n")
	return data
}
