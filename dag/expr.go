package dag

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// EvalCondition evaluates a CEL expression against context values.
func EvalCondition(expression string, ctx *Context) (bool, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return false, fmt.Errorf("empty expression")
	}

	values := ctx.ValuesForEval()

	varDecls := make([]cel.EnvOption, 0, len(values)+8)
	for k := range values {
		varDecls = append(varDecls, cel.Variable(k, cel.StringType))
	}

	// Declare identifiers in the expression that aren't in values.
	for _, ident := range extractIdentifiers(expression, values) {
		varDecls = append(varDecls, cel.Variable(ident, cel.StringType))
		values[ident] = ""
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

	activation := make(map[string]any, len(values))
	for k, v := range values {
		activation[k] = v
	}

	out, _, err := prog.Eval(activation)
	if err != nil {
		return false, fmt.Errorf("cel eval: %w", err)
	}

	return celToBool(out), nil
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

func isReserved(token string) bool {
	switch token {
	case "true", "false", "null", "in", "int", "uint", "double", "bool", "string",
		"bytes", "list", "map", "type", "has", "size", "contains", "startsWith",
		"endsWith", "matches", "exists", "all", "filter", "timestamp", "duration":
		return true
	}
	return false
}
