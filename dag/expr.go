package dag

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// EvalCondition evaluates a CEL expression against context values.
// String values that are valid JSON arrays, objects, numbers, or booleans
// are passed to CEL with their native types so expressions like
// fetch._runs.size() or fetch._runs[0].status work naturally.
func EvalCondition(expression string, ctx *Context) (bool, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return false, fmt.Errorf("empty expression")
	}

	values := ctx.ValuesForEval()

	// Parse string values into native types where possible.
	activation := make(map[string]any, len(values))
	for k, v := range values {
		activation[k] = inferTypedValue(v)
	}

	varDecls := make([]cel.EnvOption, 0, len(values)+8)
	for k, v := range activation {
		varDecls = append(varDecls, cel.Variable(k, inferCELType(v)))
	}

	// Declare identifiers in the expression that aren't in values.
	// Skip identifiers that are sub-paths of an existing map or list variable
	// (e.g. don't declare "judge.details.outcome" when "judge.details" is a map).
	for _, ident := range extractIdentifiers(expression, values) {
		if isSubpathOfStructuredVar(ident, activation) {
			continue
		}
		varDecls = append(varDecls, cel.Variable(ident, cel.StringType))
		activation[ident] = ""
	}

	env, err := cel.NewEnv(varDecls...)
	if err != nil {
		return false, fmt.Errorf("cel env: %w", err)
	}

	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("cel compile: %w", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf("cel program: %w", err)
	}

	out, _, err := prog.Eval(activation)
	if err != nil {
		return false, fmt.Errorf("cel eval: %w", err)
	}

	return celToBool(out), nil
}

// inferTypedValue attempts to parse a string as a JSON array or object.
// Scalars (numbers, booleans) are left as strings because executor outputs
// are map[string]string and values like "0" or "true" are intended as
// string comparands in edge conditions.
func inferTypedValue(s string) any {
	if s == "" {
		return s
	}
	first := s[0]
	if first != '[' && first != '{' {
		return s
	}
	var parsed any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return s
	}
	switch parsed.(type) {
	case []any, map[string]any:
		return parsed
	default:
		return s
	}
}

// inferCELType returns the CEL type declaration for a Go value.
func inferCELType(v any) *cel.Type {
	switch v.(type) {
	case []any:
		return cel.ListType(cel.DynType)
	case map[string]any:
		return cel.MapType(cel.StringType, cel.DynType)
	case float64:
		return cel.DoubleType
	case bool:
		return cel.BoolType
	default:
		return cel.StringType
	}
}

func celToBool(val ref.Val) bool {
	switch v := val.Value().(type) {
	case bool:
		return v
	case string:
		v = strings.TrimSpace(v)
		return v != "" && !strings.EqualFold(v, "false")
	default:
		if val.Type() == types.BoolType {
			return val.Value().(bool)
		}
		return val != types.NullValue
	}
}

func extractIdentifiers(expression string, known map[string]string) []string {
	var result []string
	seen := make(map[string]bool)

	inQuote := byte(0)
	start := -1
	for i := 0; i <= len(expression); i++ {
		var ch byte
		if i < len(expression) {
			ch = expression[i]
		}

		if inQuote != 0 {
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			inQuote = ch
			continue
		}

		isIdent := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') || ch == '_' || ch == '.'
		if isIdent {
			if start < 0 {
				start = i
			}
			continue
		}

		if start >= 0 {
			token := expression[start:i]
			start = -1

			if isReserved(token) || (token[0] >= '0' && token[0] <= '9') || strings.HasPrefix(token, ".") {
				continue
			}
			if _, ok := known[token]; !ok && !seen[token] {
				seen[token] = true
				result = append(result, token)
			}
		}
	}

	return result
}

// isSubpathOfStructuredVar returns true if ident is a dotted extension of
// an existing activation key whose value is a map or list. For example,
// "judge.details.outcome" is a sub-path of "judge.details" if that key
// holds a map. In that case CEL should resolve .outcome as field access
// rather than treating the whole thing as a variable name.
func isSubpathOfStructuredVar(ident string, activation map[string]any) bool {
	for i := len(ident) - 1; i > 0; i-- {
		if ident[i] != '.' {
			continue
		}
		prefix := ident[:i]
		if v, ok := activation[prefix]; ok {
			switch v.(type) {
			case map[string]any, []any:
				return true
			}
		}
	}
	return false
}

func isReserved(token string) bool {
	switch token {
	case "true", "false", "null", "in", "int", "uint", "double", "bool", "string",
		"bytes", "list", "map", "type", "has", "size", "contains", "startsWith",
		"endsWith", "matches", "exists", "all", "filter", "timestamp", "duration":
		return true
	}
	return false
}
