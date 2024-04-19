package loadvariable

import (
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/wundergraph/wundergraph/pkg/wgpb"
)

var placeholderRegexp = regexp.MustCompile(`\${([^}]+)}`)

func replacePlaceholderFromEnv(str string) string {
	return placeholderRegexp.ReplaceAllStringFunc(str, func(s string) string {
		before, after, ok := strings.Cut(s[2:len(s)-1], ":")
		if s = os.Getenv(before); s == "" && ok {
			s = after
		}
		return s
	})
}

func String(variable *wgpb.ConfigurationVariable) string {
	if variable == nil {
		return ""
	}
	switch variable.GetKind() {
	case wgpb.ConfigurationVariableKind_ENV_CONFIGURATION_VARIABLE:
		value := os.Getenv(variable.GetEnvironmentVariableName())
		if value != "" {
			return value
		}
		return variable.GetEnvironmentVariableDefaultValue()
	case wgpb.ConfigurationVariableKind_STATIC_CONFIGURATION_VARIABLE:
		return replacePlaceholderFromEnv(variable.GetStaticVariableContent())
	case wgpb.ConfigurationVariableKind_PLACEHOLDER_CONFIGURATION_VARIABLE:
		return variable.GetPlaceholderVariableName()
	default:
		return ""
	}
}

func Strings(variables []*wgpb.ConfigurationVariable) []string {
	out := make([]string, 0, len(variables))
	for _, variable := range variables {
		str := String(variable)
		if str != "" {
			out = append(out, strings.Split(str, ",")...)
		}
	}
	return out
}

func Bool(variable *wgpb.ConfigurationVariable) bool {
	if variable == nil {
		return false
	}
	switch variable.GetKind() {
	case wgpb.ConfigurationVariableKind_ENV_CONFIGURATION_VARIABLE:
		value := os.Getenv(variable.GetEnvironmentVariableName())
		if value != "" {
			return value == "true"
		}
		return variable.GetEnvironmentVariableDefaultValue() == "true"
	case wgpb.ConfigurationVariableKind_STATIC_CONFIGURATION_VARIABLE:
		return variable.GetStaticVariableContent() == "true"
	default:
		return false
	}
}

func Int64(variable *wgpb.ConfigurationVariable) int64 {
	if variable == nil {
		return 0
	}
	switch variable.GetKind() {
	case wgpb.ConfigurationVariableKind_ENV_CONFIGURATION_VARIABLE:
		value := os.Getenv(variable.GetEnvironmentVariableName())
		if value != "" {
			i, _ := strconv.Atoi(value)
			return int64(i)
		}
		i, _ := strconv.Atoi(variable.GetEnvironmentVariableDefaultValue())
		return int64(i)
	case wgpb.ConfigurationVariableKind_STATIC_CONFIGURATION_VARIABLE:
		i, _ := strconv.Atoi(variable.GetStaticVariableContent())
		return int64(i)
	default:
		return 0
	}
}

func Int(variable *wgpb.ConfigurationVariable) int {
	if variable == nil {
		return 0
	}
	switch variable.GetKind() {
	case wgpb.ConfigurationVariableKind_ENV_CONFIGURATION_VARIABLE:
		value := os.Getenv(variable.GetEnvironmentVariableName())
		if value != "" {
			i, _ := strconv.Atoi(value)
			return i
		}
		i, _ := strconv.Atoi(variable.GetEnvironmentVariableDefaultValue())
		return i
	case wgpb.ConfigurationVariableKind_STATIC_CONFIGURATION_VARIABLE:
		i, _ := strconv.Atoi(variable.GetStaticVariableContent())
		return i
	default:
		return 0
	}
}
