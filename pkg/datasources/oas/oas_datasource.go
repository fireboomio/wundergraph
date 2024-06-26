package oas_datasource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"golang.org/x/exp/slices"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/buger/jsonparser"

	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/datasource/httpclient"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"github.com/wundergraph/graphql-go-tools/pkg/pool"

	"github.com/wundergraph/wundergraph/pkg/customhttpclient"
)

type Planner struct {
	client              *http.Client
	streamingClient     *http.Client
	visitor             *plan.Visitor
	config              Configuration
	selectionFieldNames []string
	rootTypeName        string // rootTypeName - holds name of top level type
	rootFieldName       string // rootFieldName - holds name of root type field
	rootFieldRef        int    // rootFieldRef - holds ref of root type field
	operationDefinition int
}

func (p *Planner) DownstreamResponseFieldAlias(_ int) (alias string, exists bool) {
	// the REST DataSourcePlanner doesn't rewrite upstream fields: skip
	return
}

func (p *Planner) DataSourcePlanningBehavior() plan.DataSourcePlanningBehavior {
	return plan.DataSourcePlanningBehavior{
		MergeAliasedRootNodes:      false,
		OverrideFieldPathFromAlias: false,
	}
}

func (p *Planner) EnterOperationDefinition(ref int) {
	p.operationDefinition = ref
}

type Factory struct {
	Client          *http.Client
	StreamingClient *http.Client
}

func (f *Factory) WithHTTPClient(client *http.Client) *Factory {
	return &Factory{
		Client:          client,
		StreamingClient: f.StreamingClient,
	}
}

func (f *Factory) Planner(ctx context.Context) plan.DataSourcePlanner {
	return &Planner{
		client:          f.Client,
		streamingClient: f.StreamingClient,
	}
}

type Configuration struct {
	DatasourceName         string
	Fetch                  FetchConfiguration
	Subscription           SubscriptionConfiguration
	StatusCodeTypeMappings []StatusCodeTypeMapping
	DefaultTypeName        string
	RequestRewriters       []*wgpb.DataSourceRESTRewriter
	ResponseRewriters      []*wgpb.DataSourceRESTRewriter
	ResponseExtractor      *wgpb.DataSourceRESTResponseExtractor
}

type StatusCodeTypeMapping struct {
	StatusCode                   int
	StatusCodeByteString         []byte
	InjectStatusCodeIntoResponse bool
	TypeNameStringBytes          []byte
}

var plannerConfigCache map[string]Configuration

func ResetPlannerConfigCache() {
	plannerConfigCache = make(map[string]Configuration)
}

func AddPlannerConfigCache(rootNodes []*wgpb.TypeField, config Configuration) {
	if len(rootNodes) == 0 {
		return
	}

	rootNode := rootNodes[0]
	rootField := []string{rootNode.TypeName}
	rootField = append(rootField, rootNode.FieldNames...)
	plannerConfigCache[strings.Join(rootField, ".")] = config
}

func getPlannerConfig(rootNodes []plan.TypeField) (config Configuration) {
	if len(rootNodes) == 0 {
		return
	}

	rootNode := rootNodes[0]
	rootField := []string{rootNode.TypeName}
	rootField = append(rootField, rootNode.FieldNames...)
	return plannerConfigCache[strings.Join(rootField, ".")]
}

func ConfigJSON(config Configuration) json.RawMessage {
	out, _ := json.Marshal(config)
	return out
}

type SubscriptionConfiguration struct {
	Enabled                 bool
	PollingIntervalMillis   int64
	SkipPublishSameResponse bool
	DoneData                string
}

type FetchConfiguration struct {
	URL                 string
	Method              string
	Header              http.Header
	Query               []QueryConfiguration
	Body                string
	URLEncodeBody       bool
	RequestContentType  string
	ResponseContentType string
}

type QueryConfiguration struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (p *Planner) Register(visitor *plan.Visitor, configuration plan.DataSourceConfiguration, isNested bool) error {
	p.visitor = visitor
	visitor.Walker.RegisterEnterFieldVisitor(p)
	visitor.Walker.RegisterEnterOperationVisitor(p)
	p.config = getPlannerConfig(configuration.RootNodes)
	return nil
}

func (p *Planner) EnterField(ref int) {
	p.rootFieldRef = ref
	if p.visitor.Operation.Fields[ref].HasSelections {
		selectionSet := p.visitor.Operation.Fields[ref].SelectionSet
		for _, selectionRef := range p.visitor.Operation.SelectionSets[selectionSet].SelectionRefs {
			selectionName := p.visitor.Operation.FieldNameUnsafeString(p.visitor.Operation.Selections[selectionRef].Ref)
			p.selectionFieldNames = append(p.selectionFieldNames, selectionName)
		}
	}

	// store root field name and ref
	if p.rootFieldName == "" {
		p.rootFieldName = p.visitor.Operation.FieldNameString(ref)
	}
	// store root type name
	if p.rootTypeName == "" {
		p.rootTypeName = p.visitor.Walker.EnclosingTypeDefinition.NameString(p.visitor.Definition)
	}
}

func (p *Planner) configureInput() []byte {
	fetchUrl := p.config.Fetch.URL
	if strings.HasPrefix(fetchUrl, "{{") && strings.HasSuffix(fetchUrl, "}}") {
		fetchUrl = strconv.Quote(fetchUrl)
	}
	input := httpclient.SetInputURL(nil, []byte(fetchUrl))
	input = httpclient.SetInputMethod(input, []byte(p.config.Fetch.Method))
	input = httpclient.SetInputBody(input, []byte(p.config.Fetch.Body))
	input = httpclient.SetInputURLEncodeBody(input, p.config.Fetch.URLEncodeBody)

	header, err := json.Marshal(p.config.Fetch.Header)
	if err == nil && len(header) != 0 && !bytes.Equal(header, literal.NULL) {
		input = httpclient.SetInputHeader(input, header)
	}

	preparedQuery := p.prepareQueryParams(p.rootFieldRef, p.config.Fetch.Query)
	query, err := json.Marshal(preparedQuery)
	if err == nil && len(preparedQuery) != 0 {
		input = httpclient.SetInputQueryParams(input, query)
	}
	return input
}

func (p *Planner) ConfigureFetch() plan.FetchConfiguration {
	return plan.FetchConfiguration{
		Input:                string(p.configureInput()),
		DataSource:           p.newSource(),
		DisallowSingleFlight: p.config.Fetch.Method != http.MethodGet,
		DisableDataLoader:    true,
	}
}

func (p *Planner) newSource() *Source {
	source := &Source{
		client:              p.client,
		statusCodeMappings:  p.config.StatusCodeTypeMappings,
		requestRewriters:    p.config.RequestRewriters,
		responseRewriters:   p.config.ResponseRewriters,
		responseExtractor:   p.config.ResponseExtractor,
		selectionFieldNames: p.selectionFieldNames,
		datasourceName:      p.config.DatasourceName,
		rootTypeName:        p.rootTypeName,
		rootFieldName:       p.rootFieldName,
	}
	if p.config.DefaultTypeName != "" {
		source.defaultTypeName = []byte("\"" + p.config.DefaultTypeName + "\"")
	}
	return source
}

var selectorRegex = regexp.MustCompile(`{{\s(.*?)\s}}`)

func (p *Planner) prepareQueryParams(field int, query []QueryConfiguration) []QueryConfiguration {
	out := make([]QueryConfiguration, 0, len(query))
Next:
	for i := range query {
		matches := selectorRegex.FindAllStringSubmatch(query[i].Value, -1)
		for j := range matches {
			if len(matches[j]) == 2 {
				path := matches[j][1]
				path = strings.TrimPrefix(path, ".")
				elements := strings.Split(path, ".")
				if len(elements) < 2 {
					continue
				}
				if elements[0] != "arguments" {
					continue
				}
				argumentName := elements[1]
				arg, ok := p.visitor.Operation.FieldArgument(field, []byte(argumentName))
				if !ok {
					continue Next
				}
				value := p.visitor.Operation.Arguments[arg].Value
				if value.Kind != ast.ValueKindVariable {
					continue Next
				}
				variableName := p.visitor.Operation.VariableValueNameString(value.Ref)
				if !p.visitor.Operation.OperationDefinitionHasVariableDefinition(p.operationDefinition, variableName) {
					continue Next
				}
			}
		}
		out = append(out, query[i])
	}
	return out
}

type Source struct {
	client              *http.Client
	statusCodeMappings  []StatusCodeTypeMapping
	defaultTypeName     []byte
	requestRewriters    []*wgpb.DataSourceRESTRewriter
	responseRewriters   []*wgpb.DataSourceRESTRewriter
	responseExtractor   *wgpb.DataSourceRESTResponseExtractor
	selectionFieldNames []string
	datasourceName      string
	rootTypeName        string
	rootFieldName       string
}

var jsonFlags = []byte{'{', '['}

func (s *Source) Load(ctx context.Context, input []byte, w io.Writer) (err error) {
	input = s.rewriteInput(input)
	buf := pool.FastBuffer.Get()
	defer pool.FastBuffer.Put(buf)

	var spanFuncs []func(opentracing.Span)
	ctx, callback := logging.StartTraceContext(ctx, nil, s.spanOperationName(), s.spanWithDatasource(), logging.SpanWithLogInput(input))
	defer func() { callback(append(spanFuncs, logging.SpanWithLogError(err))...) }()

	req, resp, err := customhttpclient.Do(s.client, ctx, input)
	if err != nil {
		return err
	}
	if err = customhttpclient.HandleNormal(req, resp, buf); err != nil {
		return
	}

	data, statusCode := buf.Bytes(), resp.StatusCode
	if statusCode >= 400 {
		spanFuncs = append(spanFuncs, func(span opentracing.Span) { ext.Error.Set(span, true) })
		if len(data) == 0 {
			err = fmt.Errorf("status code not ok: %d", statusCode)
			return
		}
		if !slices.Contains(jsonFlags, data[0]) {
			err = errors.New(string(data))
			return
		}
	}

	if len(data) == 0 {
		data = []byte("{}")
	} else {
		data = s.rewriteOutput(data)
		if err = s.applyResponseExtractor(data); err != nil {
			return
		}
	}
	for i := range s.statusCodeMappings {
		if statusCode == s.statusCodeMappings[i].StatusCode {
			data, err = jsonparser.Set(data, s.statusCodeMappings[i].TypeNameStringBytes, "__typename")
			if err != nil {
				return err
			}
			if s.statusCodeMappings[i].InjectStatusCodeIntoResponse {
				data, err = jsonparser.Set(data, s.statusCodeMappings[i].StatusCodeByteString, "statusCode")
				if err != nil {
					return err
				}
			}
			_, err = w.Write(data)
			return err
		}
	}
	if len(s.defaultTypeName) != 0 {
		data, err = jsonparser.Set(data, s.defaultTypeName, "__typename")
		if err != nil {
			return err
		}
		statusStr := strconv.Itoa(statusCode)
		data, err = jsonparser.Set(data, []byte(statusStr), "statusCode")
		if err != nil {
			return err
		}
	}
	spanFuncs = append(spanFuncs, logging.SpanWithLogOutput(data))
	_, err = w.Write(data)
	return err
}

func (s *Source) spanWithDatasource() func(opentracing.Span) {
	return func(span opentracing.Span) {
		ext.Component.Set(span, wgpb.DataSourceKind_REST.String())
		ext.DBInstance.Set(span, s.datasourceName)
	}
}

func (s *Source) spanOperationName() string {
	return strings.Join([]string{s.rootTypeName, s.rootFieldName}, ".")
}

func (s *Source) applyResponseExtractor(data []byte) error {
	if s.responseExtractor == nil {
		return nil
	}

	statusCodeJsonpath := s.responseExtractor.StatusCodeJsonpath
	if !slices.ContainsFunc(s.selectionFieldNames, func(name string) bool {
		return strings.HasSuffix(statusCodeJsonpath, name)
	}) {
		return nil
	}

	extractStatusCode, err := jsonparser.GetInt(data, statusCodeJsonpath)
	if errors.Is(err, jsonparser.KeyPathNotFoundError) {
		return errors.New(string(data))
	}
	if len(s.responseExtractor.StatusCodeScopes) == 0 && extractStatusCode == http.StatusOK {
		return nil
	}

	extractStatusCode32 := int32(extractStatusCode)
	if slices.ContainsFunc(s.responseExtractor.StatusCodeScopes, func(scope *wgpb.DataSourceRESTResponseStatusCodeScope) bool {
		return extractStatusCode32 >= scope.Min && extractStatusCode32 <= scope.Max
	}) {
		return nil
	}

	extractErrorMsg, _ := jsonparser.GetString(data, s.responseExtractor.ErrorMessageJsonpath)
	return errors.New(extractErrorMsg)
}
