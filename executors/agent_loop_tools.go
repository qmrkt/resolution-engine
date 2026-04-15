package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/question-market/resolution-engine/dag"
)

type agentTool interface {
	Name() string
	Description() string
	Parameters() json.RawMessage
	Execute(ctx context.Context, args json.RawMessage) (agentToolResult, error)
	ReadOnly() bool
}

type agentParallelChecker interface {
	CanParallelize(args json.RawMessage) bool
}

type agentToolRegistry struct {
	tools map[string]agentTool
	order []string
}

func newAgentToolRegistry() *agentToolRegistry {
	return &agentToolRegistry{tools: map[string]agentTool{}}
}

func (r *agentToolRegistry) register(t agentTool) error {
	if t == nil {
		return nil
	}
	name := strings.TrimSpace(t.Name())
	if name == "" {
		return fmt.Errorf("agent tool name is required")
	}
	if _, exists := r.tools[name]; exists {
		return fmt.Errorf("duplicate agent tool %q", name)
	}
	r.tools[name] = t
	r.order = append(r.order, name)
	return nil
}

func (r *agentToolRegistry) get(name string) (agentTool, bool) {
	if r == nil {
		return nil, false
	}
	t, ok := r.tools[strings.TrimSpace(name)]
	return t, ok
}

func (r *agentToolRegistry) defs() []agentToolDef {
	if r == nil {
		return nil
	}
	defs := make([]agentToolDef, 0, len(r.order))
	for _, name := range r.order {
		t := r.tools[name]
		defs = append(defs, agentToolDef{
			Name:        t.Name(),
			Description: t.Description(),
			InputSchema: defaultSchemaObject(t.Parameters()),
		})
	}
	return defs
}

func (r *agentToolRegistry) canParallelize(call agentToolCall) bool {
	t, ok := r.get(call.Name)
	if !ok {
		return false
	}
	if t.ReadOnly() {
		return true
	}
	if checker, ok := t.(agentParallelChecker); ok {
		return checker.CanParallelize(call.Input)
	}
	return false
}

type contextAccess struct {
	execCtx   *dag.Context
	allowSet  map[string]struct{}
	allowList []string
}

func newContextAccess(execCtx *dag.Context, allowlist []string) contextAccess {
	access := contextAccess{execCtx: execCtx}
	for _, key := range allowlist {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if access.allowSet == nil {
			access.allowSet = map[string]struct{}{}
		}
		access.allowSet[key] = struct{}{}
		access.allowList = append(access.allowList, key)
	}
	sort.Strings(access.allowList)
	return access
}

func (a contextAccess) readable(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" || strings.HasPrefix(key, "__") {
		return false
	}
	if a.allowSet != nil {
		_, ok := a.allowSet[key]
		return ok
	}
	lower := strings.ToLower(key)
	for _, blocked := range []string{"secret", "password", "token", "api_key", "apikey", "private_key"} {
		if strings.Contains(lower, blocked) {
			return false
		}
	}
	return true
}

func (a contextAccess) readableKeys() []string {
	if a.execCtx == nil {
		return nil
	}
	if a.allowSet != nil {
		return append([]string(nil), a.allowList...)
	}
	snap := a.execCtx.Snapshot()
	keys := make([]string, 0, len(snap))
	for key := range snap {
		if a.readable(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func jsonToolResult(value any) agentToolResult {
	encoded, err := json.Marshal(value)
	if err != nil {
		return agentToolResult{Output: fmt.Sprintf(`{"status":"failed","error":"marshal tool result: %v"}`, err), IsError: true}
	}
	return agentToolResult{Output: string(encoded)}
}

func toolErrorResult(format string, args ...any) agentToolResult {
	msg := fmt.Sprintf(format, args...)
	encoded, _ := json.Marshal(map[string]any{"status": "failed", "error": msg})
	return agentToolResult{Output: string(encoded), IsError: true}
}
