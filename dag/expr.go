package dag

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
)

// celCache stores compiled CEL programs keyed on (expression + variable type
// signature). Entries are immutable and safe for concurrent use. The cache is
// unbounded but naturally plateaus — expressions come from blueprint edge
// conditions and cel_eval configs, which are finite per blueprint.
//
// Staleness safety: the type signature encodes the actual observed types of
// each referenced variable at eval time. If a context value changes type
// between calls (e.g. a key switches from string to JSON list), varSig
// changes and a new program is compiled — the old one is never reused with
// the wrong shape.
var celCache sync.Map // string → cel.Program

// planCache memoizes per-expression parsing work that is independent of
// context values: the sorted list of referenced identifiers and the set of
// candidate keys (idents + all dotted prefixes) that activation-building
// might need to look up. Both are pure functions of the expression text.
var planCache sync.Map // string → *exprPlan

// exprPlan holds the immutable, context-independent parse of an expression.
type exprPlan struct {
	identifiers []string // sorted, deduped
	candidates  []string // identifiers + every dotted prefix, for a single GetMulti call
}

// EvalCondition evaluates a CEL expression against the invocation and returns
// the result as a boolean.
func EvalCondition(expression string, inv *Invocation) (bool, error) {
	val, err := evalCEL(expression, invocationCELGetter(inv))
	if err != nil {
		return false, err
	}
	return celToBool(val), nil
}

// EvalExpression evaluates a CEL expression against the invocation and returns
// the result as a string.
func EvalExpression(expression string, inv *Invocation) (string, error) {
	val, err := evalCEL(expression, invocationCELGetter(inv))
	if err != nil {
		return "", err
	}
	return celToString(val), nil
}

// celGetter resolves a namespaced CEL variable by exact key. Returns the
// value and whether the key exists (matches the "present but empty vs
// absent" distinction evalCEL needs for type inference).
//
// Replaces the previous eager flattenInvocationForCEL: evalCEL only calls
// the getter for keys its plan actually references, so we never walk the
// full invocation keyspace on hot edge-condition paths.
type celGetter func(key string) (string, bool)

// invocationCELGetter returns a getter that resolves namespaced CEL paths
// directly against an invocation:
//
//	inputs.<key>            — caller input
//	results.<node>.<field>  — recorded node output
//	results.<node>.history  — back-edge history as JSON array
//	run.id | run.blueprint_id | run.started_at
//
// Anything outside these forms returns ("", false) so CEL declares the
// ident as an empty string (the previous flatten-then-filter behavior).
func invocationCELGetter(inv *Invocation) celGetter {
	if inv == nil {
		return func(string) (string, bool) { return "", false }
	}
	return func(key string) (string, bool) {
		switch key {
		case "run.id":
			return inv.Run.ID, true
		case "run.blueprint_id":
			return inv.Run.BlueprintID, true
		case "run.started_at":
			if inv.Run.StartedAt.IsZero() {
				return "", false
			}
			return inv.Run.StartedAt.UTC().Format(time.RFC3339Nano), true
		}
		if strings.HasPrefix(key, "inputs.") {
			v, ok := inv.Run.Inputs[strings.TrimPrefix(key, "inputs.")]
			return v, ok
		}
		if strings.HasPrefix(key, "results.") {
			rest := strings.TrimPrefix(key, "results.")
			dot := strings.Index(rest, ".")
			if dot < 0 {
				return "", false
			}
			nodeID := rest[:dot]
			field := rest[dot+1:]
			if field == "history" {
				history := inv.Results.History(nodeID)
				if len(history) == 0 {
					return "", false
				}
				data, err := json.Marshal(history)
				if err != nil {
					return "", false
				}
				return string(data), true
			}
			return inv.Results.Get(nodeID, field)
		}
		return "", false
	}
}

// mapCELGetter wraps a pre-built flat map for callers and tests that
// already materialized the keyspace (fuzz tests, backward-compat wrappers).
// New code should prefer invocationCELGetter to avoid the eager flatten.
func mapCELGetter(m map[string]string) celGetter {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

// FlattenInvocation materializes the namespaced keyspace of an invocation
// for observability snapshots (NodeTrace.InputSnapshot). Keys match the
// CEL evaluation shape:
//
//	inputs.<key>
//	results.<node>.<field>
//	results.<node>.history          — JSON-encoded back-edge history
//	run.id | run.blueprint_id | run.started_at
//
// This is the single canonical flatten shape for the engine. CEL eval uses
// the lazy invocationCELGetter instead — only trace recording needs the
// full map, and that's a per-node cost, not per-edge-condition.
func FlattenInvocation(inv *Invocation) map[string]string {
	if inv == nil {
		return map[string]string{}
	}
	flat := make(map[string]string, len(inv.Run.Inputs)+8)
	for k, v := range inv.Run.Inputs {
		flat["inputs."+k] = v
	}
	for _, nodeID := range inv.Results.NodeIDs() {
		for field, value := range inv.Results.OfNode(nodeID) {
			flat["results."+nodeID+"."+field] = value
		}
		history := inv.Results.History(nodeID)
		if len(history) > 0 {
			if data, err := json.Marshal(history); err == nil {
				flat["results."+nodeID+".history"] = string(data)
			}
		}
	}
	flat["run.id"] = inv.Run.ID
	flat["run.blueprint_id"] = inv.Run.BlueprintID
	if !inv.Run.StartedAt.IsZero() {
		flat["run.started_at"] = inv.Run.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	return flat
}

// evalCEL is the shared CEL evaluation core. It resolves only the variables
// the expression actually references through the getter, builds a typed
// activation, and caches the compiled program by (expression, observed type
// signature). The getter indirection keeps evaluation lazy — no eager
// flatten of the whole invocation keyspace on every edge condition.
func evalCEL(expression string, get celGetter) (ref.Val, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil, fmt.Errorf("empty expression")
	}

	plan := getOrBuildPlan(expression)

	// Resolve only the candidate keys through the getter. Preserves the
	// previous "present-but-empty vs missing" distinction via the (string, bool)
	// return.
	raw := make(map[string]string, len(plan.candidates))
	if get != nil {
		for _, key := range plan.candidates {
			if v, ok := get(key); ok {
				raw[key] = v
			}
		}
	}

	varDecls, activation, varSig := buildActivation(plan.identifiers, raw)

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

// getOrBuildPlan returns the cached parse plan for an expression, computing
// it on first access. Identifiers and candidates are pure functions of the
// expression text, so caching is safe regardless of context state.
func getOrBuildPlan(expression string) *exprPlan {
	if cached, ok := planCache.Load(expression); ok {
		return cached.(*exprPlan)
	}
	idents := extractIdentifiers(expression, nil)
	sort.Strings(idents)

	// Candidates include each ident plus every dotted prefix, because
	// activation building may fall back to prefix resolution when an ident
	// itself is absent from context but a prefix is (e.g. JSON list/map).
	candCap := len(idents)
	for _, id := range idents {
		candCap += strings.Count(id, ".")
	}
	candidates := make([]string, 0, candCap)
	for _, id := range idents {
		candidates = append(candidates, id)
		for i := len(id) - 1; i > 0; i-- {
			if id[i] == '.' {
				candidates = append(candidates, id[:i])
			}
		}
	}

	plan := &exprPlan{identifiers: idents, candidates: candidates}
	if actual, loaded := planCache.LoadOrStore(expression, plan); loaded {
		return actual.(*exprPlan)
	}
	return plan
}

// buildActivation constructs CEL variable declarations, the activation map,
// and a deterministic type signature for cache keying. It mirrors the
// resolution rules of the previous full-snapshot implementation:
//
//  1. Direct match: ident is present in context → declare with its inferred
//     type.
//  2. Prefix match: ident absent but longest existing dotted prefix is a
//     JSON list/map → declare the prefix (CEL resolves the tail as field
//     access). Stops at the first existing prefix regardless of type.
//  3. Otherwise: declare ident as an empty string.
//
// The returned signature string uniquely identifies the set of declarations
// used for this call, so the compiled-program cache can't hand back a
// program built against a different type shape.
func buildActivation(idents []string, raw map[string]string) ([]cel.EnvOption, map[string]any, string) {
	decls := make([]cel.EnvOption, 0, len(idents))
	activation := make(map[string]any, len(idents))
	declared := make(map[string]struct{}, len(idents))
	var sig strings.Builder

	for _, ident := range idents {
		if s, ok := raw[ident]; ok {
			typed := inferTypedValue(s)
			if _, done := declared[ident]; !done {
				celType := inferCELType(typed)
				decls = append(decls, cel.Variable(ident, celType))
				activation[ident] = typed
				declared[ident] = struct{}{}
				sig.WriteString(ident)
				sig.WriteByte(':')
				sig.WriteString(celType.String())
				sig.WriteByte(',')
			}
			continue
		}

		prefixFound := false
		for i := len(ident) - 1; i > 0; i-- {
			if ident[i] != '.' {
				continue
			}
			prefix := ident[:i]
			s, ok := raw[prefix]
			if !ok {
				continue
			}
			typed := inferTypedValue(s)
			switch typed.(type) {
			case map[string]any, []any:
				if _, done := declared[prefix]; !done {
					celType := inferCELType(typed)
					decls = append(decls, cel.Variable(prefix, celType))
					activation[prefix] = typed
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
		if prefixFound {
			continue
		}

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

// ExtractIdentifiers returns the free identifiers referenced in a CEL-style
// expression, skipping CEL reserved words, numeric prefixes, and trailing
// CEL method calls on dotted paths (e.g. "fetch.raw.size" → "fetch.raw").
// Used by edge-condition diagnostics to flag references to unknown node
// outputs.
func ExtractIdentifiers(expression string) []string {
	return extractIdentifiers(expression, nil)
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
