package database

import (
	"fmt"
	"github.com/buger/jsonparser"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"regexp"
	"strings"
)

var (
	TranslateErrorFunc func(string) string
	colorRegexp        = regexp.MustCompile(`(\\)u001b\[\d+(;\d+)?m`)
	placeHolderRegexp  = regexp.MustCompile(`\{[a-zA-Z0-9_]+\}`)

	prismaErrorCodePath = []string{"error_code"}
	prismaErrorMetaPath = []string{"meta"}
)

func translateError(prismaErrorBytes []byte) []byte {
	if TranslateErrorFunc == nil || len(prismaErrorBytes) == 0 {
		return nil
	}

	codeBytes, _, _, _ := jsonparser.Get(prismaErrorBytes, prismaErrorCodePath...)
	metaBytes, _, _, _ := jsonparser.Get(prismaErrorBytes, prismaErrorMetaPath...)
	if len(codeBytes) == 0 || len(metaBytes) == 0 {
		return nil
	}

	codeStr := string(codeBytes)
	codeTranslate := TranslateErrorFunc(codeStr)
	if codeTranslate == "" {
		return nil
	}
	if translateSplit := strings.Split(codeTranslate, "||"); len(translateSplit) > 1 {
		metaData := make(map[string]string)
		_ = jsonparser.ObjectEach(metaBytes, func(key []byte, value []byte, _ jsonparser.ValueType, _ int) error {
			metaData["{"+string(key)+"}"] = strings.ReplaceAll(string(value), `"`, "")
			return nil
		})
		metaKeys := maps.Keys(metaData)
		slices.Sort(metaKeys)
		matchIndex := slices.IndexFunc(translateSplit, func(item string) bool {
			params := placeHolderRegexp.FindAllString(item, -1)
			slices.Sort(params)
			return slices.Equal(params, metaKeys)
		})
		if matchIndex == -1 {
			matchIndex = 0
		}
		codeTranslate = translateSplit[matchIndex]
		for k, v := range metaData {
			codeTranslate = strings.ReplaceAll(codeTranslate, k, v)
		}
	} else {
		_ = jsonparser.ObjectEach(metaBytes, func(key []byte, value []byte, valueType jsonparser.ValueType, _ int) error {
			valueStr := strings.ReplaceAll(string(value), `"`, "")
			codeTranslate = strings.ReplaceAll(codeTranslate, "{"+string(key)+"}", valueStr)
			return nil
		})
	}

	codeTranslate = colorRegexp.ReplaceAllString(codeTranslate, "")
	return []byte(fmt.Sprintf("[%s]%s", codeStr, codeTranslate))
}

func extractAndSetErrorBytes(origin, target []byte, from, to []string, convert ...func([]byte) []byte) ([]byte, bool) {
	dataBytes, _, _, err := jsonparser.Get(origin, from...)
	if err != nil {
		return target, false
	}
	if len(dataBytes) == 0 {
		return target, false
	}
	for _, item := range convert {
		dataBytes = item(dataBytes)
	}
	if len(dataBytes) == 0 {
		return target, false
	}
	target, _ = jsonparser.Set(target, []byte(`"`+string(dataBytes)+`"`), to...)
	return target, true
}
