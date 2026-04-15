package dag

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
)

// celCache stores compiled CEL programs keyed on (expression + variable type
// signature). Entries are immutable and safe for concurrent use. The cache is
// unbounded but naturally plateaus — expressions come from blueprint edge
// conditions and cel_eval configs, which are finite per blueprint.
var celCache sync.Map // string → cel.Program

// EvalCondition evaluates a CEL expression against context values and returns
// the result as a boolean.
func EvalCondition(expression string, ctx *Context) (bool, error) {
	val, err := evalCEL(expression, ctx)
	if err != nil {
		return false, err
	}
	return celToBool(val), nil
}

// EvalExpression evaluates a CEL expression against context values and returns
// the result as a string.
func EvalExpression(expression string, ctx *Context) (string, error) {
	val, err := evalCEL(expression, ctx)
	if err != nil {
		return "", err
	}
	return celToString(val), nil
}

// evalCEL is the shared CEL evaluation core. It parses context values into
// native types, resolves referenced variables, and evaluates the expression.
// Compiled programs are cached keyed on (expression, variable type signature).
func evalCEL(expression string, ctx *Context) (ref.Val, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil, fmt.Errorf("empty expression")
	}

	values := ctx.ValuesForEval()

	// Parse string values into native types where possible.
	fullActivation := make(map[string]any, len(values))
	for k, v := range values {
		fullActivation[k] = inferTypedValue(v)
	}

	// Only declare variables the expression actually references.
	allIdents := extractIdentifiers(expression, nil)
	varDecls, activation, varSig := resolveExpressionVars(allIdents, fullActivation)

	cacheKey := expression + "\x00" + varSig
	if cached, ok := celCache.Load(cacheKey); ok {
		out, _, err := cached.(cel.Program).Eval(activation)
		if err != nil {
			return nil, fmt.Errorf("cel eval: %w", err)
		}
		return out, nil
	}

	env, err := cel.NewEnv(append(varDecls, ext.Strings())...)
	if err != nil {
		return nil, fmt.Errorf("cel env: %w", err)
	}

	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("cel compile: %w", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("cel program: %w", err)
	}

	celCache.Store(cacheKey, prog)

	out, _, err := prog.Eval(activation)
	if err != nil {
		return nil, fmt.Errorf("cel eval: %w", err)
	}

	return out, nil
}

// resolveExpressionVars builds CEL variable declarations and a minimal
// activation map for only the variables an expression references. It returns
// a deterministic type signature string for cache keying.
func resolveExpressionVars(exprIdents []string, fullActivation map[string]any) ([]cel.EnvOption, map[string]any, string) {
	sort.Strings(exprIdents)

	decls := make([]cel.EnvOption, 0, len(exprIdents))
	activation := make(map[string]any, len(exprIdents))
	declared := make(map[string]struct{})
	var sig strings.Builder

	for _, ident := range exprIdents {
		// Direct match in context.
		if v, ok := fullActivation[ident]; ok {
			if _, done := declared[ident]; !done {
				celType := inferCELType(v)
				decls = append(decls, cel.Variable(ident, celType))
				activation[ident] = v
				declared[ident] = struct{}{}
				sig.WriteString(ident)
				sig.WriteByte(':')
				sig.WriteString(celType.String())
				sig.WriteByte(',')
			}
			continue
		}

		// Check if a prefix is a structured var (map or list) — CEL
		// resolves the remainder as field access / method call.
		// E.g. ident="fetch._runs.size" → declare "fetch._runs" as list.
		prefixFound := false
		for i := len(ident) - 1; i > 0; i-- {
			if ident[i] != '.' {
				continue
			}
			prefix := ident[:i]
			if v, ok := fullActivation[prefix]; ok {
				switch v.(type) {
				case map[string]any, []any:
					if _, done := declared[prefix]; !done {
						celType := inferCELType(v)
						decls = append(decls, cel.Variable(prefix, celType))
						activation[prefix] = v
						declared[prefix] = struct{}{}
						sig.WriteString(prefix)
						sig.WriteByte(':')
						sig.WriteString(celType.String())
						sig.WriteByte(',')
					}
					prefixFound = true
				}
				break
			}
		}
		if prefixFound {
			continue
		}

		// Unknown variable — declare as empty string.
		if _, done := declared[ident]; !done {
			decls = append(decls, cel.Variable(ident, cel.StringType))
			activation[ident] = ""
			declared[ident] = struct{}{}
			sig.WriteString(ident)
			sig.WriteString(":string,")
		}
	}

	return decls, activation, sig.String()
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

func celToString(val ref.Val) string {
	if val == nil || val == types.NullValue {
		return ""
	}
	switch v := val.Value().(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int64:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	default:
		// Lists, maps, and other complex types → JSON.
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(data)
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
			// Strip trailing CEL method name from dotted identifiers.
			// e.g. "fetch.value.split" → "fetch.value"
			if dot := strings.LastIndexByte(token, '.'); dot > 0 {
				if isCELMethod(token[dot+1:]) {
					token = token[:dot]
				}
			}
			if token == "" {
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

// isCELMethod returns true for CEL built-in and ext.Strings() method names.
// Used to strip trailing method calls from dotted identifier tokens so that
// "fetch.value.split" is treated as variable "fetch.value" + method "split".
func isCELMethod(name string) bool {
	switch name {
	// built-in receiver methods
	case "contains", "startsWith", "endsWith", "matches", "exists", "all",
		"filter", "size", "has",
		// ext.Strings() methods
		"charAt", "format", "indexOf", "lastIndexOf", "lowerAscii", "replace",
		"split", "substring", "trim", "upperAscii", "reverse", "join":
		return true
	}
	return false
}
