package hooks

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/wundergraph/wundergraph/pkg/postresolvetransform"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/mattbaird/jsonpatch"
	"go.uber.org/zap"

	"github.com/wundergraph/wundergraph/pkg/authentication"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"github.com/wundergraph/wundergraph/pkg/pool"
)

type WunderGraphRequest struct {
	Method     string          `json:"method"`
	RequestURI string          `json:"requestURI"`
	Headers    RequestHeaders  `json:"headers"`
	Body       json.RawMessage `json:"body,omitempty"`
	OriginBody []byte          `json:"originBody,omitempty"`
}

type WunderGraphResponse struct {
	StatusCode int             `json:"statusCode"`
	Status     string          `json:"status"`
	Method     string          `json:"method"`
	RequestURI string          `json:"requestURI"`
	Headers    RequestHeaders  `json:"headers"`
	Body       json.RawMessage `json:"body,omitempty"`
	OriginBody []byte          `json:"originBody,omitempty"`
}

func HttpRequestToWunderGraphRequestJSON(r *http.Request, withBody bool) ([]byte, error) {
	var body []byte
	if withBody {
		body, _ = ioutil.ReadAll(r.Body)
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
	}
	return json.Marshal(WunderGraphRequest{
		Method:     r.Method,
		RequestURI: r.URL.String(),
		Headers:    HeaderSliceToCSV(r.Header),
		Body:       body,
	})
}

func HeaderSliceToCSV(headers map[string][]string) map[string]string {
	result := make(map[string]string, len(headers))
	for k, v := range headers {
		result[k] = strings.Join(v, ",")
	}
	return result
}

func HeaderCSVToSlice(headers map[string]string) map[string][]string {
	result := make(map[string][]string, len(headers))
	for k, v := range headers {
		result[k] = strings.Split(v, ",")
	}
	return result
}

type HookResponse interface {
	ResponseError() string
}

type OnWsConnectionInitHookPayload struct {
	DataSourceID string             `json:"dataSourceId"`
	Request      WunderGraphRequest `json:"request"`
}

type OnRequestHookPayload struct {
	Request       WunderGraphRequest `json:"request"`
	OperationName string             `json:"operationName"`
	OperationType string             `json:"operationType"`
	ArgsAllowList []string           `json:"argsAllowList"`
}

type OnRequestHookResponse struct {
	Skip    bool                `json:"skip"`
	Cancel  bool                `json:"cancel"`
	Request *WunderGraphRequest `json:"request"`
}

type OnResponseHookPayload struct {
	Response      WunderGraphResponse `json:"response"`
	OperationName string              `json:"operationName"`
	OperationType string              `json:"operationType"`
}

type OnResponseHookResponse struct {
	Skip     bool                 `json:"skip"`
	Cancel   bool                 `json:"cancel"`
	Response *WunderGraphResponse `json:"response"`
}

type MiddlewareHookResponse struct {
	Error                   string          `json:"error,omitempty"`
	Op                      string          `json:"op"`
	Hook                    string          `json:"hook"`
	Response                json.RawMessage `json:"response"`
	Input                   json.RawMessage `json:"input"`
	SetClientRequestHeaders RequestHeaders  `json:"setClientRequestHeaders"`
	// ClientResponseStatusCode is the status code that should be returned to the client
	ClientResponseStatusCode int `json:"-"`
}

type RequestHeaders map[string]string

func (r *MiddlewareHookResponse) ResponseError() string { return r.Error }

type UploadHookResponse struct {
	Error   string `json:"error"`
	FileKey string `json:"fileKey"`
}

func (r *UploadHookResponse) ResponseError() string { return r.Error }

type MiddlewareHook string

const (
	MockResolve                MiddlewareHook = "mockResolve"
	PreResolve                 MiddlewareHook = "preResolve"
	PostResolve                MiddlewareHook = "postResolve"
	CustomResolve              MiddlewareHook = "customResolve"
	MutatingPreResolve         MiddlewareHook = "mutatingPreResolve"
	MutatingPostResolve        MiddlewareHook = "mutatingPostResolve"
	PostAuthentication         MiddlewareHook = "postAuthentication"
	PostLogout                 MiddlewareHook = "postLogout"
	MutatingPostAuthentication MiddlewareHook = "mutatingPostAuthentication"
	RevalidateAuthentication   MiddlewareHook = "revalidateAuthentication"

	// HttpTransportBeforeRequest to the origin
	HttpTransportBeforeRequest MiddlewareHook = "beforeOriginRequest"
	HttpTransportAfterResponse MiddlewareHook = "afterOriginResponse"
	// HttpTransportOnRequest to the origin
	HttpTransportOnRequest MiddlewareHook = "onOriginRequest"
	// HttpTransportOnResponse from the origin
	HttpTransportOnResponse MiddlewareHook = "onOriginResponse"

	WsTransportOnConnectionInit MiddlewareHook = "onConnectionInit"
)

type UploadHook string

const (
	PreUpload  UploadHook = "preUpload"
	PostUpload UploadHook = "postUpload"
)

type Client struct {
	serverUrl           string
	httpClient          *retryablehttp.Client
	subscriptionsClient *retryablehttp.Client
	log                 *zap.Logger
	healthQuery         map[string]interface{}
}

func NewClient(serverUrl string, logger *zap.Logger) *Client {
	return &Client{
		serverUrl:           serverUrl,
		httpClient:          buildClient(60 * time.Second),
		subscriptionsClient: buildClient(0),
		log:                 logger,
	}
}

func NewHealthClient(logger *zap.Logger) *Client {
	client := buildClient(5 * time.Second)
	client.RetryMax = 3
	return &Client{
		httpClient: client,
		log:        logger,
	}
}

func buildClient(requestTimeout time.Duration) *retryablehttp.Client {
	httpClient := retryablehttp.NewClient()
	// we will try 10 times with a constant delay of 100ms after max 1s we will give up
	httpClient.RetryMax = 10
	// keep it low and linear to increase the chance
	// that we can continue as soon as the server is back from a cold start
	httpClient.Backoff = retryablehttp.LinearJitterBackoff
	httpClient.RetryWaitMax = 100 * time.Millisecond
	httpClient.RetryWaitMin = 100 * time.Millisecond
	httpClient.HTTPClient.Timeout = requestTimeout
	httpClient.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	httpClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		if resp != nil && resp.StatusCode == http.StatusInternalServerError {
			return false, nil
		}
		return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
	}
	return httpClient
}

func (c *Client) ResetServerUrl(serverUrl string) {
	c.serverUrl = serverUrl
}

func (c *Client) ResetHealthQuery(query map[string]interface{}) {
	c.healthQuery = query
}

func (c *Client) DoProxyRequest(ctx context.Context, hook MiddlewareHook, jsonData []byte, buf *bytes.Buffer) (*MiddlewareHookResponse, error) {
	return c.doMiddlewareRequest(ctx, "proxy", hook, jsonData, buf)
}

func (c *Client) DoGlobalRequest(ctx context.Context, hook MiddlewareHook, jsonData []byte, buf *bytes.Buffer) (*MiddlewareHookResponse, error) {
	return c.doMiddlewareRequest(ctx, "global/httpTransport", hook, jsonData, buf)
}

func (c *Client) DoWsTransportRequest(ctx context.Context, hook MiddlewareHook, jsonData []byte, buf *bytes.Buffer) (*MiddlewareHookResponse, error) {
	return c.doMiddlewareRequest(ctx, "global/wsTransport", hook, jsonData, buf)
}

func (c *Client) DoOperationRequest(ctx context.Context, operationName string, hook MiddlewareHook, jsonData []byte, buf *bytes.Buffer) (*MiddlewareHookResponse, error) {
	return c.doMiddlewareRequest(ctx, "operation/"+operationName, hook, jsonData, buf)
}

func (c *Client) DoFunctionRequest(ctx context.Context, operationName string, jsonData []byte, buf *bytes.Buffer) (*MiddlewareHookResponse, error) {
	jsonData = c.setInternalHookData(ctx, jsonData, buf)
	operationName = strings.TrimPrefix(operationName, "function/")
	r, err := http.NewRequestWithContext(ctx, "POST", c.serverUrl+"/function/"+operationName, bytes.NewReader(jsonData))
	if err != nil {
		return nil, err
	}

	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(logging.RequestIDHeader, logging.RequestIDFromContext(ctx))

	var spanFunc []func(opentracing.Span)
	r, callback := logging.StartTraceRequest(r)
	defer func() { callback(append(spanFunc, logging.SpanWithLogError(err))...) }()

	req, err := retryablehttp.FromRequest(r)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.WithMessagef(err, "error calling function %s", operationName)
	}
	spanFunc = append(spanFunc, logging.SpanWithLogResponse(resp))

	if resp == nil {
		err = fmt.Errorf("error calling function %s: no response", operationName)
		return nil, err
	}

	dec := json.NewDecoder(resp.Body)

	var hookRes MiddlewareHookResponse
	if err = dec.Decode(&hookRes); err != nil {
		return nil, errors.WithMessage(err, "error calling function response")
	}

	if hookRes.Error != "" {
		err = fmt.Errorf("error calling function %s: %s", operationName, hookRes.Error)
		return nil, err
	}

	hookRes.ClientResponseStatusCode = resp.StatusCode

	return &hookRes, nil
}

func (c *Client) DoFunctionSubscriptionRequest(ctx context.Context, operationName string, jsonData []byte, subscribeOnce, useSSE, useJsonPatch bool, out io.Writer, buf *bytes.Buffer, transformer *postresolvetransform.Transformer) error {
	jsonData = c.setInternalHookData(ctx, jsonData, buf)
	operationName = strings.TrimPrefix(operationName, "function/")
	r, err := http.NewRequestWithContext(ctx, "POST", c.serverUrl+"/function/"+operationName, bytes.NewReader(jsonData))
	if err != nil {
		return err
	}

	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(logging.RequestIDHeader, logging.RequestIDFromContext(ctx))

	var spanFunc []func(opentracing.Span)
	r, callback := logging.StartTraceRequest(r)
	defer func() { callback(append(spanFunc, logging.SpanWithLogError(err))...) }()

	if subscribeOnce {
		r.Header.Set("X-WG-Subscribe-Once", "true")
	}

	req, err := retryablehttp.FromRequest(r)
	if err != nil {
		return err
	}

	resp, err := c.subscriptionsClient.Do(req)
	if err != nil {
		return errors.WithMessagef(err, "error calling function %s", operationName)
	}
	spanFunc = append(spanFunc, logging.SpanWithLogResponse(resp))

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("server function call did not return 200: %s", resp.Status)
		return err
	}

	if resp.Body == nil {
		err = fmt.Errorf("server function call did not return a body")
		return err
	}

	defer func() { _ = resp.Body.Close() }()

	flusher, ok := out.(http.Flusher)
	if !ok {
		err = fmt.Errorf("client connection is not flushable")
		return err
	}

	if useSSE {
		defer func() {
			_, _ = out.Write([]byte("data: done\n\n"))
			flusher.Flush()
		}()
	}

	reader := bufio.NewReader(resp.Body)
	lastLine := pool.GetBytesBuffer()
	defer pool.PutBytesBuffer(lastLine)

	var (
		line []byte
	)

	for {
		line, err = reader.ReadBytes('\n')
		if err == nil {
			_, err = reader.ReadByte()
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if transformer != nil {
			line, _ = transformer.Transform(line)
		}
		if useSSE {
			_, err = out.Write([]byte("data: "))
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return errors.WithMessage(err, "error writing to client")
			}
		}
		if useJsonPatch && lastLine.Len() != 0 {
			patchOperation, err := jsonpatch.CreatePatch(lastLine.Bytes(), line[:len(line)-1]) // remove newline
			if err != nil {
				return errors.WithMessage(err, "error creating json patch")
			}
			patch, err := json.Marshal(patchOperation)
			if err != nil {
				return errors.WithMessage(err, "error marshalling json patch")
			}
			if len(patch) < len(line) {
				_, err = out.Write(patch)
				if err != nil {
					return err
				}
				_, err = out.Write([]byte("\n")) // add newline again
				if err != nil {
					return err
				}
			} else {
				_, err = out.Write(line)
				if err != nil {
					return err
				}
			}
		} else {
			_, err = out.Write(line)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.WithMessage(err, "error writing to client")
		}
		// we only need to write one newline, the second one is already in the line above
		_, err = out.Write([]byte("\n"))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.WithMessage(err, "error writing to client")
		}
		flusher.Flush()
		lastLine.Reset()
		_, _ = lastLine.Write(line)
	}
}

func (c *Client) DoAuthenticationRequest(ctx context.Context, hook MiddlewareHook, jsonData []byte, buf *bytes.Buffer) (*MiddlewareHookResponse, error) {
	return c.doMiddlewareRequest(ctx, "authentication", hook, jsonData, buf)
}

func (c *Client) DoUploadRequest(ctx context.Context, providerName string, profileName string, hook UploadHook, jsonData []byte, buf *bytes.Buffer) (*UploadHookResponse, error) {
	var hookResponse UploadHookResponse
	if err := c.doRequest(ctx, &hookResponse, path.Join("upload", providerName, profileName), MiddlewareHook(hook), jsonData, buf); err != nil {
		return nil, err
	}
	return &hookResponse, nil
}

func (c *Client) setInternalHookData(ctx context.Context, jsonData []byte, buf *bytes.Buffer) []byte {
	if len(jsonData) == 0 {
		jsonData = []byte(`{}`)
	}
	// Make sure we account for both pool.ClientRequestKey being nil and being non present
	if clientRequest, ok := ctx.Value(pool.ClientRequestKey).(*http.Request); ok && clientRequest != nil {
		_, wgClientRequestType, _, _ := jsonparser.Get(jsonData, "__wg", "clientRequest")
		if clientRequestData, err := HttpRequestToWunderGraphRequestJSON(clientRequest, false); err == nil && wgClientRequestType == jsonparser.NotExist {
			buf.Reset()
			_, _ = buf.Write(jsonData)
			// because we modify the original json data, we need to make sure that the original data is not modified
			// so we copy it first with enough space to append the clientRequestData
			jsonData, _ = jsonparser.Set(buf.Bytes(), clientRequestData, "__wg", "clientRequest")
		}
	}
	return jsonData
}

func (c *Client) doRequest(ctx context.Context, hookResponse HookResponse, action string, hook MiddlewareHook, jsonData []byte, buf *bytes.Buffer) error {
	jsonData = c.setInternalHookData(ctx, jsonData, buf)
	r, err := http.NewRequestWithContext(ctx, "POST", c.serverUrl+"/"+action+"/"+string(hook), bytes.NewReader(jsonData))
	if err != nil {
		return err
	}

	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(logging.RequestIDHeader, logging.RequestIDFromContext(ctx))

	var spanFunc []func(opentracing.Span)
	r, callback := logging.StartTraceRequest(r)
	defer func() { callback(append(spanFunc, logging.SpanWithLogError(err))...) }()

	req, err := retryablehttp.FromRequest(r)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.WithMessagef(err, "hook %s failed with error", string(hook))
	}
	spanFunc = append(spanFunc, logging.SpanWithLogResponse(resp))
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.WithMessagef(err, "hook %s failed with reading body", string(hook))
	}

	if resp.StatusCode != http.StatusOK {
		err = errors.WithMessagef(errors.New(string(data)), "hook %s failed with invalid status code: %d", string(hook), resp.StatusCode)
		return err
	}

	err = json.Unmarshal(data, hookResponse)
	if err != nil {
		return errors.WithMessagef(err, "hook %s response could not be decoded", string(hook))
	}

	if respErr := hookResponse.ResponseError(); respErr != "" {
		err = errors.WithMessagef(errors.New(respErr), "hook %s failed", string(hook))
		return err
	}

	return nil
}

func (c *Client) doMiddlewareRequest(ctx context.Context, action string, hook MiddlewareHook, jsonData []byte, buf *bytes.Buffer) (*MiddlewareHookResponse, error) {
	var hookResponse MiddlewareHookResponse
	if err := c.doRequest(ctx, &hookResponse, action, hook, jsonData, buf); err != nil {
		return nil, err
	}
	return &hookResponse, nil
}

func (c *Client) DoHealthCheckRequest(ctx context.Context, writer ...io.Writer) (status bool) {
	url := c.serverUrl + "/health"
	if c.healthQuery != nil {
		queries := make([]string, 0, len(c.healthQuery))
		for k, v := range c.healthQuery {
			queries = append(queries, fmt.Sprintf("%s=%v", k, v))
		}
		url += "?" + strings.Join(queries, "&")
	}
	req, err := retryablehttp.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false
	}
	resp, err := c.httpClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		return
	}

	if len(writer) > 0 {
		_, _ = io.Copy(writer[0], resp.Body)
	}
	return true
}

func encodeData(r *http.Request, w *bytes.Buffer, variables []byte, response []byte) ([]byte, error) {
	const (
		wgKey = "__wg"
		root  = `{"` + wgKey + `":{}}`
	)
	var err error
	buf := w.Bytes()[0:]
	buf = append(buf, []byte(root)...)

	if userBytes := authentication.UserBytesFromContext(r.Context()); userBytes != nil {
		if buf, err = jsonparser.Set(buf, userBytes, wgKey, "user"); err != nil {
			return nil, err
		}
	}
	if len(variables) > 2 {
		if buf, err = jsonparser.Set(buf, variables, "input"); err != nil {
			return nil, err
		}
	}
	if len(response) != 0 {
		if buf, err = jsonparser.Set(buf, response, "response"); err != nil {
			return nil, err
		}
	}
	if r.Context().Err() != nil {
		if buf, err = jsonparser.Set(buf, []byte("true"), "canceled"); err != nil {
			return nil, err
		}
	}
	if r != nil {
		counterHeader := r.Header.Get("Wg-Cycle-Counter")
		counter, _ := strconv.ParseInt(counterHeader, 10, 64)
		counterValue := []byte(strconv.FormatInt(counter+1, 10))
		if buf, err = jsonparser.Set(buf, counterValue, "cycleCounter"); err != nil {
			return nil, err
		}
	}
	// If buf required more bytes than what w provided, copy buf into w so next time w's backing slice has enough
	// room to fit the whole payload
	if cap(buf) > w.Cap() {
		_, _ = w.Write(buf)
	}
	return buf, nil
}

// EncodeData encodes the given input data for a hook as a JSON payload to be sent to the hooks server
func EncodeData(r *http.Request, buf *bytes.Buffer, variables []byte, response []byte) ([]byte, error) {
	data, err := encodeData(r, buf, variables, response)
	if err != nil {
		return nil, errors.WithMessage(err, "encoding hook data")
	}
	return data, nil
}
