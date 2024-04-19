package customhttpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/buger/jsonparser"
	"github.com/hetiansu5/urlquery"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/tidwall/sjson"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"

	"github.com/wundergraph/wundergraph/internal/unsafebytes"
	"github.com/wundergraph/wundergraph/pkg/pool"
)

const (
	ContentEncodingHeader = "Content-Encoding"
	AcceptEncodingHeader  = "Accept-Encoding"
	MultipartHeader       = "multipartHeader"
	MultipartFiles        = "multipartBody"

	ContentTypeHeader   = "Content-Type"
	TextEventStreamMine = "text/event-stream"
)

var (
	queryParamsKeys = [][]string{
		{"name"},
		{"value"},
	}
)

// unescapeJSON tries to unescape the given JSON data over itself (to avoid)
// any allocations. This means that in case of an error the data will be
// malformed.
func unescapeJSON(in []byte) ([]byte, error) {
	// All variables that can be fed into Do()/DoWithStatus() output must be
	// first formatted as valid JSON. This means the only escaping that will
	// be added by the second encoding run is prepending a second "layer" of
	// escaping and this should all be "deescapable" onto the same source
	// slice because we're always at least writing one byte behind where we
	// read from.
	return jsonparser.Unescape(in, in)
}

func Do(client *http.Client, ctx context.Context, requestInput []byte) (req *http.Request, resp *http.Response, err error) {
	var bodyBuffer io.Reader
	url, method, body, urlEncoded, headers, queryParams := requestInputParams(requestInput)
	hasURLEncodeBody := bytes.Equal(urlEncoded, literal.TRUE)
	var contentType string
	if files, ok := ctx.Value(MultipartFiles).(map[string][]*multipart.FileHeader); ok {
		contentType = ctx.Value(MultipartHeader).(string)
		bodyBuffer = new(bytes.Buffer)
		multipartWriter := multipart.NewWriter(bodyBuffer.(*bytes.Buffer))
		_, boundary, _ := strings.Cut(contentType, "boundary=")
		_ = multipartWriter.SetBoundary(boundary)
		for k, v := range files {
			for _, vv := range v {
				fileWriter, fileErr := multipartWriter.CreateFormFile(k, vv.Filename)
				if fileErr != nil {
					continue
				}

				fileOpen, fileErr := vv.Open()
				if fileErr != nil {
					continue
				}

				_, _ = io.Copy(fileWriter, fileOpen)
				_ = fileOpen.Close()
			}
			body, _ = sjson.DeleteBytes(body, k)
		}
		_ = jsonparser.ObjectEach(body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			if dataType == jsonparser.Unknown || dataType == jsonparser.Null {
				return nil
			}
			return multipartWriter.WriteField(string(key), string(value))
		})
		_ = multipartWriter.Close()
	} else if !hasURLEncodeBody && len(body) != 0 {
		bodyBuffer = bytes.NewBuffer(body)
		contentType = "application/json"
	}

	if req, err = http.NewRequestWithContext(ctx, string(method), string(url), bodyBuffer); err != nil {
		return
	}

	if headers != nil {
		if err = jsonparser.ObjectEach(headers, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			_, err := jsonparser.ArrayEach(value, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
				if err != nil {
					return
				}
				req.Header.Add(string(key), string(value))
			})
			return err
		}); err != nil {
			return
		}
	}

	req.Header.Add("accept", "application/json")

	if hasURLEncodeBody {
		req.Body = nil
		req.Header.Set(ContentTypeHeader, "application/x-www-form-urlencoded")
		if req.URL.RawQuery, err = urlEncodeBody(body); err != nil {
			return
		}
	} else {
		req.Header.Add(ContentTypeHeader, contentType)
	}

	if queryParams != nil {
		// Copy queryParams to a buffer, so we can modify it in place
		queryParamsBuf := pool.GetBytesBuffer()
		defer pool.PutBytesBuffer(queryParamsBuf)
		if _, err = io.Copy(queryParamsBuf, bytes.NewReader(queryParams)); err != nil {
			return
		}
		if req.URL.RawQuery, err = encodeQueryParams(queryParamsBuf.Bytes()); err != nil {
			return
		}
	}

	resp, err = client.Do(req)
	return
}

func HandleNormal(req *http.Request, resp *http.Response, out io.Writer) (err error) {
	defer func() { _ = resp.Body.Close() }()
	var respReader io.Reader
	if respReader, err = respBodyReader(req, resp); err != nil {
		return
	}
	_, err = io.Copy(out, respReader)
	return
}

func respBodyReader(req *http.Request, resp *http.Response) (io.ReadCloser, error) {
	if req.Header.Get(AcceptEncodingHeader) == "" {
		return resp.Body, nil
	}

	switch resp.Header.Get(ContentEncodingHeader) {
	case "gzip":
		return gzip.NewReader(resp.Body)
	case "deflate":
		return flate.NewReader(resp.Body), nil
	}

	return resp.Body, nil
}

var rewriteArrayBytesMap = map[string][]byte{
	`:"[`: []byte(`:[`),
	`]"}`: []byte(`]}`),
}

func rewriteArrayValue(value []byte) []byte {
	for src, dst := range rewriteArrayBytesMap {
		value = bytes.ReplaceAll(value, []byte(src), dst)
	}
	return value
}

// encodeQueryParams encodes the query parameters received as a JSON
// array into a valid URL query string. NOTICE: If queryParams contains escape
// sequences, they will be unescaped in place, overwriting the data.
func encodeQueryParams(queryParams []byte) (string, error) {
	var jsonErr error
	query := make(url.Values)
	_, err := jsonparser.ArrayEach(queryParams, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if jsonErr != nil {
			return
		}
		jsonErr = err
		var (
			parameterName, parameterValue []byte
		)
		value = rewriteArrayValue(value)
		jsonparser.EachKey(value, func(i int, bytes []byte, valueType jsonparser.ValueType, err error) {
			if jsonErr != nil {
				return
			}
			if err != nil {
				jsonErr = err
				return
			}
			switch i {
			case 0:
				parameterName, jsonErr = unescapeJSON(bytes)
			case 1:
				parameterValue, jsonErr = unescapeJSON(bytes)
			}
		}, queryParamsKeys...)
		if len(parameterName) == 0 || len(parameterValue) == 0 {
			return
		}
		if bytes.Equal(parameterValue[:1], literal.LBRACK) {
			_, _ = jsonparser.ArrayEach(parameterValue, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
				query.Add(unsafebytes.BytesToString(parameterName), unsafebytes.BytesToString(value))
			})
		} else {
			query.Add(unsafebytes.BytesToString(parameterName), unsafebytes.BytesToString(parameterValue))
		}
	})
	if err != nil {
		return "", err
	}
	if jsonErr != nil {
		return "", err
	}
	return query.Encode(), nil
}

const (
	URL           = "url"
	METHOD        = "method"
	BODY          = "body"
	HEADER        = "header"
	QUERYPARAMS   = "query_params"
	URLENCODEBODY = "url_encode_body"
)

var (
	inputPaths = [][]string{
		{URL},
		{METHOD},
		{BODY},
		{HEADER},
		{QUERYPARAMS},
		{URLENCODEBODY},
	}
)

func requestInputParams(input []byte) (url, method, body, urlencodebody, headers, queryParams []byte) {
	jsonparser.EachKey(input, func(i int, bytes []byte, valueType jsonparser.ValueType, err error) {
		switch i {
		case 0:
			url = bytes
		case 1:
			method = bytes
		case 2:
			body = bytes
		case 3:
			headers = bytes
		case 4:
			queryParams = bytes
		case 5:
			urlencodebody = bytes
		}
	}, inputPaths...)
	return
}

func urlEncodeBody(body []byte) (string, error) {
	var input interface{}
	err := json.Unmarshal(body, &input)
	if err != nil {
		return "", err
	}
	out, err := urlquery.Marshal(input)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
