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
	placeHolderRegexp  = regexp.MustCompile(`\{[a-zA-Z0-9_]+}`)
)

var (
	prismaErrorCodePath    = []string{"error_code"}
	prismaErrorMetaPath    = []string{"meta"}
	prismaErrorMessagePath = []string{"message"}

	graphqlErrorsPath              = []string{"errors"}
	graphqlItemErrorPath           = []string{"error"}
	graphqlItemErrorMessagePath    = []string{"message"}
	graphqlItemUserFacingErrorPath = []string{"user_facing_error"}
)

func RewriteErrors(dataBytes []byte) (resultBytes []byte, rewritten bool) {
	errorsBytes, errorsType, _, _ := jsonparser.Get(dataBytes, graphqlErrorsPath...)
	if rewritten = errorsType == jsonparser.Array; rewritten {
		var index int
		_, _ = jsonparser.ArrayEach(errorsBytes, func(value []byte, _ jsonparser.ValueType, _ int, _ error) {
			newValue, ok := extractAndSetErrorBytes(value, []byte(`{}`), graphqlItemUserFacingErrorPath, graphqlItemErrorMessagePath, translateError)
			if !ok {
				newValue, _ = extractAndSetErrorBytes(value, newValue, graphqlItemErrorPath, graphqlItemErrorMessagePath)
			}
			errorsBytes, _ = jsonparser.Set(errorsBytes, newValue, fmt.Sprintf(`[%d]`, index))
			index++
		})
		resultBytes, _ = jsonparser.Set(dataBytes, errorsBytes, graphqlErrorsPath...)
	}
	return
}

func translateError(prismaErrorBytes []byte) []byte {
	if TranslateErrorFunc == nil || len(prismaErrorBytes) == 0 {
		return nil
	}

	codeBytes, _, _, _ := jsonparser.Get(prismaErrorBytes, prismaErrorCodePath...)
	metaBytes, _, _, _ := jsonparser.Get(prismaErrorBytes, prismaErrorMetaPath...)
	if len(codeBytes) == 0 || len(metaBytes) == 0 {
		messageBytes, _, _, _ := jsonparser.Get(prismaErrorBytes, prismaErrorMessagePath...)
		return messageBytes
	}

	codeStr := string(codeBytes)
	codeTranslate := TranslateErrorFunc(codeStr)
	if codeTranslate == "" {
		return nil
	}
	if codeTranslate = replaceMetaData(metaBytes, codeTranslate, true); codeTranslate == "" {
		return nil
	}
	codeTranslate = colorRegexp.ReplaceAllString(codeTranslate, "")
	return []byte(fmt.Sprintf("[%s]%s", codeStr, codeTranslate))
}

func replaceMetaData(metaBytes []byte, codeTranslate string, top bool) (result string) {
	result = codeTranslate
	metaData := parseObjectData(metaBytes)
	if translateSplit := strings.Split(result, "||"); len(translateSplit) > 1 {
		metaKeys := maps.Keys(metaData)
		slices.Sort(metaKeys)
		matchIndex := slices.IndexFunc(translateSplit, func(item string) bool {
			if cond, _, ok := strings.Cut(item, "<?>"); ok {
				if k, v, ok := strings.Cut(cond, "="); ok {
					return strings.EqualFold(v, metaData[k])
				}
			}
			params := placeHolderRegexp.FindAllString(item, -1)
			slices.Sort(params)
			return slices.EqualFunc(params, metaKeys, func(p, m string) bool {
				return strings.Trim(p, `{}`) == m
			})
		})
		if matchIndex == -1 {
			matchIndex = 0
		}
		if result = translateSplit[matchIndex]; result == "" {
			return
		}
		if _, after, ok := strings.Cut(result, "<?>"); ok {
			result = after
		}
		if before, eachKey, ok := strings.Cut(result, "<each>"); ok {
			eachResult := make([]string, 0)
			_, _ = jsonparser.ArrayEach(metaBytes, func(value []byte, _ jsonparser.ValueType, _ int, _ error) {
				itemResult := strings.TrimPrefix(replaceMetaData(value, codeTranslate, false), result)
				if !slices.Contains(eachResult, itemResult) {
					eachResult = append(eachResult, itemResult)
				}
			}, eachKey)
			if result = strings.Join(eachResult, `\n`); top {
				result = before + result
			}
		}
	}
	for k, v := range metaData {
		result = strings.ReplaceAll(result, "{"+k+"}", v)
	}
	return
}

func parseObjectData(objectBytes []byte) map[string]string {
	metaData := make(map[string]string)
	_ = jsonparser.ObjectEach(objectBytes, func(key []byte, value []byte, valueType jsonparser.ValueType, _ int) error {
		keyStr := string(key)
		valueStr := strings.ReplaceAll(string(value), `\"`, "'")
		metaData[keyStr] = strings.ReplaceAll(valueStr, `"`, "")
		if valueType == jsonparser.Object {
			for k, v := range parseObjectData(value) {
				metaData[keyStr+"."+k] = v
			}
		}
		return nil
	})
	return metaData
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
