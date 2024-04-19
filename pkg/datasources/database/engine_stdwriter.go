package database

import (
	"bytes"
	json "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	"github.com/tidwall/gjson"
	"github.com/uber/jaeger-client-go"
)

type stdoutWriter struct{}

func (t *stdoutWriter) Write(p []byte) (n int, err error) {
	for _, line := range bytes.Split(p, []byte("\n")) {
		if len(line) == 0 {
			continue
		}

		spansResult := gjson.GetBytes(line, "spans")
		if !spansResult.IsArray() {
			continue
		}

		var spans []span
		_ = json.Unmarshal([]byte(spansResult.String()), &spans)
		if len(spans) == 0 {
			continue
		}

		for _, item := range spans {
			itemSpan := opentracing.StartSpan(item.Name, opentracing.StartTime(item.StartTime.toDateTime()),
				jaeger.SelfRef(item.newSpanContext()))
			for k, v := range item.Attributes {
				itemSpan.SetTag(k, v)
			}
			itemSpan.FinishWithOptions(opentracing.FinishOptions{FinishTime: item.EndTime.toDateTime()})
		}
	}
	return len(p), nil
}

type (
	stderrWriter struct{ engine *Engine }
	stderrMsg    string
)

func (p stderrMsg) Error() string {
	return string(p)
}

func (t *stderrWriter) Write(p []byte) (n int, err error) {
	if msg := gjson.GetBytes(p, "message"); msg.Exists() {
		t.engine.stderr = stderrMsg(msg.String())
	}
	return len(p), nil
}
