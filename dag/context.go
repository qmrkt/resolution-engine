package dag

import (
	"encoding/json"
	"strings"
	"sync"
)

// Context is a flat string key-value store used during DAG execution.
type Context struct {
	mu     sync.RWMutex
	values map[string]string
}

// NewContext creates a context initialized with the given inputs.
func NewContext(inputs map[string]string) *Context {
	ctx := &Context{
		values: make(map[string]string),
	}
	for k, v := range inputs {
		ctx.values[k] = v
		ctx.values["input."+k] = v
	}
	return ctx
}

// NewContextFromSnapshot creates a context from an already materialized flat
// key-value snapshot without adding input.* aliases.
func NewContextFromSnapshot(values map[string]string) *Context {
	ctx := &Context{
		values: make(map[string]string, len(values)),
	}
	for k, v := range values {
		ctx.values[k] = v
	}
	return ctx
}

// Set stores a key/value pair.
func (c *Context) Set(key, value string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		c.values = make(map[string]string)
	}
	c.values[key] = value
}

// Get returns the value for key, or empty string if missing.
func (c *Context) Get(key string) string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.values[key]
}

// Snapshot returns a shallow copy of all context values.
func (c *Context) Snapshot() map[string]string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := make(map[string]string, len(c.values))
	for k, v := range c.values {
		snap[k] = v
	}
	return snap
}

// ValuesForEval returns a copy for expression evaluation.
func (c *Context) ValuesForEval() map[string]string {
	return c.Snapshot()
}

// SnapshotNode returns all output keys for a node as a flat map.
// Keys are returned without the node ID prefix (e.g. "status", not "fetch.status").
func (c *Context) SnapshotNode(nodeID string) map[string]string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	prefix := nodeID + "."
	snap := make(map[string]string)
	for k, v := range c.values {
		if strings.HasPrefix(k, prefix) {
			field := strings.TrimPrefix(k, prefix)
			if field == "_runs" {
				continue
			}
			snap[field] = v
		}
	}
	return snap
}

// AppendNodeHistory snapshots the current outputs for nodeID and appends
// them to the nodeID._runs context key as a JSON array. Each entry is
// the output map from one execution of the node.
func (c *Context) AppendNodeHistory(nodeID string) {
	if c == nil {
		return
	}
	snap := c.SnapshotNode(nodeID)
	if len(snap) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	runsKey := nodeID + "._runs"
	var history []map[string]string
	if existing := c.values[runsKey]; existing != "" {
		_ = json.Unmarshal([]byte(existing), &history)
	}
	history = append(history, snap)
	encoded, err := json.Marshal(history)
	if err != nil {
		return
	}
	c.values[runsKey] = string(encoded)
}

// Interpolate replaces {{key}} placeholders with context values.
// Replacement is single-pass and non-recursive.
func (c *Context) Interpolate(template string) string {
	if template == "" {
		return ""
	}

	remaining := template
	var out strings.Builder

	for {
		start := strings.Index(remaining, "{{")
		if start == -1 {
			out.WriteString(remaining)
			break
		}

		out.WriteString(remaining[:start])
		remaining = remaining[start+2:]

		end := strings.Index(remaining, "}}")
		if end == -1 {
			out.WriteString("{{")
			out.WriteString(remaining)
			break
		}

		key := strings.TrimSpace(remaining[:end])
		out.WriteString(c.Get(key))
		remaining = remaining[end+2:]
	}

	return out.String()
}
