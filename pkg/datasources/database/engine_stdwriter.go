package database

import (
	"bytes"
	"fmt"
	json "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/tidwall/gjson"
	"github.com/uber/jaeger-client-go"
	"strings"
)

type stdoutWriter struct{}

func (t *stdoutWriter) Write(p []byte) (n int, err error) {
	for _, line := range bytes.Split(p, []byte("\n")) {
		if len(line) == 0 {
			continue
		}

		spansResult := gjson.GetBytes(line, "spans")
		if !spansResult.IsArray() {
			fmt.Println(string(line))
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
				itemSpan.LogFields(log.Object(k, v))
				switch vv := strings.ToUpper(fmt.Sprintf("%v", v)); vv {
				case "BEGIN", "COMMIT", "ROLLBACK":
					itemSpan.SetTag("db.transaction", vv)
				}
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
	if v := translateError(p); len(v) > 0 {
		t.engine.stderr = stderrMsg(v)
	}
	return len(p), nil
}
