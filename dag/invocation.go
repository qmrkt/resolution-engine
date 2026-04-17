package dag

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Invocation is the per-run state passed to every executor. It is the sole
// source of truth an executor may read from for run identity, caller inputs,
// and the outputs of previously-completed nodes.
type Invocation struct {
	Run     Run
	Results *Results
}

// Run is the invocation's identity and immutable caller inputs. Fields are
// constant for the life of the run.
type Run struct {
	ID          string            `json:"id"`
	BlueprintID string            `json:"blueprint_id,omitempty"`
	StartedAt   time.Time         `json:"started_at"`
	Inputs      map[string]string `json:"inputs,omitempty"`
}

// Results exposes outputs of previously-completed nodes. Executors read
// through Get/OfNode/History; writes go through engine-internal methods
// (Record / MergeOutputs / SetField / AppendHistory / Reset).
type Results struct {
	mu      sync.RWMutex
	outputs map[string]*NodeResult
}

// NodeResult captures the current outputs and back-edge history for a node.
// History is appended before a back-edge re-executes the node.
type NodeResult struct {
	Outputs map[string]string   `json:"outputs,omitempty"`
	Runs    []map[string]string `json:"runs,omitempty"`
}

// NewInvocation initializes a new invocation with an empty Results map and a
// non-nil Inputs map.
func NewInvocation(run Run) *Invocation {
	if run.Inputs == nil {
		run.Inputs = make(map[string]string)
	}
	return &Invocation{Run: run, Results: NewResults()}
}

// NewInvocationFromInputs is a convenience for tests and call-sites that only
// need to seed caller inputs; everything else is zero-valued.
func NewInvocationFromInputs(inputs map[string]string) *Invocation {
	return NewInvocation(Run{Inputs: inputs})
}

// NewResults creates an empty Results container.
func NewResults() *Results {
	return &Results{outputs: make(map[string]*NodeResult)}
}

// Get returns the value for an output key on a node. The second return value
// is false when the node has no recorded outputs or the key is missing.
func (r *Results) Get(nodeID, key string) (string, bool) {
	if r == nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.outputs[nodeID]
	if !ok || node == nil {
		return "", false
	}
	v, ok := node.Outputs[key]
	return v, ok
}

// OfNode returns a copy of the outputs map for a node. A nil map is returned
// when the node has no recorded outputs.
func (r *Results) OfNode(nodeID string) map[string]string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.outputs[nodeID]
	if !ok || node == nil {
		return nil
	}
	return cloneStringMap(node.Outputs)
}

// History returns a copy of the back-edge history for a node.
func (r *Results) History(nodeID string) []map[string]string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.outputs[nodeID]
	if !ok || node == nil {
		return nil
	}
	out := make([]map[string]string, len(node.Runs))
	for i, run := range node.Runs {
		out[i] = cloneStringMap(run)
	}
	return out
}

// NodeIDs returns the node IDs that have recorded outputs, in arbitrary order.
func (r *Results) NodeIDs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.outputs))
	for id := range r.outputs {
		ids = append(ids, id)
	}
	return ids
}

// Record overwrites the outputs of a node. Existing history is preserved.
// Engine-internal use.
func (r *Results) Record(nodeID string, outputs map[string]string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	node := r.ensureNodeLocked(nodeID)
	node.Outputs = cloneStringMap(outputs)
}

// MergeOutputs merges new outputs onto a node's existing outputs, keeping
// prior entries that are not overwritten.
func (r *Results) MergeOutputs(nodeID string, outputs map[string]string) {
	if r == nil || len(outputs) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	node := r.ensureNodeLocked(nodeID)
	if node.Outputs == nil {
		node.Outputs = make(map[string]string, len(outputs))
	}
	for k, v := range outputs {
		node.Outputs[k] = v
	}
}

// SetField writes a single output key on a node.
func (r *Results) SetField(nodeID, key, value string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	node := r.ensureNodeLocked(nodeID)
	if node.Outputs == nil {
		node.Outputs = map[string]string{}
	}
	node.Outputs[key] = value
}

// Reset clears the current outputs for a node. History is preserved.
func (r *Results) Reset(nodeID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.outputs[nodeID]
	if !ok || node == nil {
		return
	}
	node.Outputs = nil
}

// AppendHistory snapshots the current outputs of a node into its history
// slice. The current outputs remain untouched; callers typically call Reset
// afterward when a back-edge re-activates the node.
func (r *Results) AppendHistory(nodeID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.outputs[nodeID]
	if !ok || node == nil || len(node.Outputs) == 0 {
		return
	}
	node.Runs = append(node.Runs, cloneStringMap(node.Outputs))
}

func (r *Results) ensureNodeLocked(nodeID string) *NodeResult {
	if r.outputs == nil {
		r.outputs = make(map[string]*NodeResult)
	}
	node, ok := r.outputs[nodeID]
	if !ok || node == nil {
		node = &NodeResult{}
		r.outputs[nodeID] = node
	}
	return node
}

// Snapshot returns a deep copy of the results map. Used for observer
// notifications and the durable checkpoint shape.
func (r *Results) Snapshot() map[string]*NodeResult {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.outputs) == 0 {
		return map[string]*NodeResult{}
	}
	out := make(map[string]*NodeResult, len(r.outputs))
	for id, node := range r.outputs {
		if node == nil {
			continue
		}
		cloned := &NodeResult{Outputs: cloneStringMap(node.Outputs)}
		if len(node.Runs) > 0 {
			cloned.Runs = make([]map[string]string, len(node.Runs))
			for i, run := range node.Runs {
				cloned.Runs[i] = cloneStringMap(run)
			}
		}
		out[id] = cloned
	}
	return out
}

// LoadSnapshot replaces the Results content with the provided deep copy.
// Used by the durable manager when resuming a persisted run.
func (r *Results) LoadSnapshot(snapshot map[string]*NodeResult) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outputs = make(map[string]*NodeResult, len(snapshot))
	for id, node := range snapshot {
		if node == nil {
			continue
		}
		cloned := &NodeResult{Outputs: cloneStringMap(node.Outputs)}
		if len(node.Runs) > 0 {
			cloned.Runs = make([]map[string]string, len(node.Runs))
			for i, run := range node.Runs {
				cloned.Runs[i] = cloneStringMap(run)
			}
		}
		r.outputs[id] = cloned
	}
}

// MarshalJSON emits `{"<nodeID>":{"outputs":{...},"runs":[...]}}`.
func (r *Results) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return json.Marshal(r.outputs)
}

// UnmarshalJSON restores results from the standard shape.
func (r *Results) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("Results: nil receiver")
	}
	var snapshot map[string]*NodeResult
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if snapshot == nil {
		r.outputs = make(map[string]*NodeResult)
		return nil
	}
	r.outputs = make(map[string]*NodeResult, len(snapshot))
	for id, node := range snapshot {
		if node == nil {
			continue
		}
		r.outputs[id] = node
	}
	return nil
}

// Interpolate replaces `{{path}}` placeholders with invocation values.
// Supported paths:
//   - run.id, run.blueprint_id, run.started_at
//   - inputs.<name> (literal key lookup, dotted input names supported)
//   - results.<nodeID>.<outputKey>
//   - results.<nodeID>.history (JSON array of prior runs)
//
// Unknown paths expand to empty strings. Replacement is single-pass and
// non-recursive.
func (inv *Invocation) Interpolate(template string) string {
	if template == "" || inv == nil {
		return template
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
		out.WriteString(inv.Lookup(key))
		remaining = remaining[end+2:]
	}
	return out.String()
}

// Lookup resolves a namespaced path against the invocation. Supported forms:
//
//	run.id / run.blueprint_id / run.started_at
//	inputs.<key>            — caller input (key may contain dots)
//	results.<node>.<field>  — recorded node output
//	results.<node>.history  — back-edge history as JSON list
//
// Anything else returns the empty string. Blueprint authors must namespace
// every reference; bare keys are not resolved.
func (inv *Invocation) Lookup(path string) string {
	if inv == nil || path == "" {
		return ""
	}
	switch path {
	case "run.id":
		return inv.Run.ID
	case "run.blueprint_id":
		return inv.Run.BlueprintID
	case "run.started_at":
		if inv.Run.StartedAt.IsZero() {
			return ""
		}
		return inv.Run.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	if strings.HasPrefix(path, "inputs.") {
		if v, ok := inv.Run.Inputs[strings.TrimPrefix(path, "inputs.")]; ok {
			return v
		}
		return ""
	}
	if strings.HasPrefix(path, "results.") {
		rest := strings.TrimPrefix(path, "results.")
		dot := strings.Index(rest, ".")
		if dot < 0 {
			return ""
		}
		nodeID := rest[:dot]
		field := rest[dot+1:]
		if field == "history" {
			history := inv.Results.History(nodeID)
			data, err := json.Marshal(history)
			if err != nil {
				return ""
			}
			return string(data)
		}
		if v, ok := inv.Results.Get(nodeID, field); ok {
			return v
		}
		return ""
	}
	return ""
}

