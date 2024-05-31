package database

import (
	"fmt"
	"github.com/buger/jsonparser"
	"strings"
)

var TranslateErrorFunc func(string) string

func extractBytesByPath(origin, target []byte, from, to []string, convert ...func([]byte) []byte) ([]byte, bool) {
	dataBytes, dataTypes, _, err := jsonparser.Get(origin, from...)
	if err != nil {
		return target, false
	}
	for _, item := range convert {
		dataBytes = item(dataBytes)
	}
	if len(dataBytes) == 0 {
		return target, false
	}
	if dataTypes == jsonparser.String {
		dataBytes = []byte(`"` + string(dataBytes) + `"`)
	}
	target, _ = jsonparser.Set(target, dataBytes, to...)
	return target, true
}

func translateError(codeBytes, metaBytes []byte) []byte {
	if TranslateErrorFunc == nil || len(codeBytes) == 0 {
		return nil
	}
	codeStr := string(codeBytes)
	result := TranslateErrorFunc(codeStr)
	if result == "" {
		return nil
	}
	if len(metaBytes) > 0 {
		_ = jsonparser.ObjectEach(metaBytes, func(key []byte, value []byte, valueType jsonparser.ValueType, _ int) error {
			valueStr := strings.ReplaceAll(string(value), `"`, "")
			result = strings.ReplaceAll(result, "{"+string(key)+"}", valueStr)
			return nil
		})
	}
	return []byte(fmt.Sprintf("[%s]%s", codeStr, result))
}
