// Package dag implements a DAG execution engine for resolution logic.
package dag

import (
	"encoding/json"
	"strings"
)

// Blueprint is a resolution logic DAG definition.
type Blueprint struct {
	ID          string     `json:"id"`
	Name        string     `json:"name,omitempty"`
	Description string     `json:"description,omitempty"`
	Version     int        `json:"version"`
	Nodes       []NodeDef  `json:"nodes"`
	Edges       []EdgeDef  `json:"edges"`
	Inputs      []InputDef `json:"inputs,omitempty"`
	Budget      *Budget    `json:"budget,omitempty"`
}

// NodeDef defines a single step in the resolution DAG.
type NodeDef struct {
	ID      string      `json:"id"`
	Type    string      `json:"type"`
	Label   string      `json:"label,omitempty"`
	Config  interface{} `json:"config"`
	OnError string      `json:"on_error,omitempty"` // "fail" (default) or "continue"
}

// DisplayName returns Label if set, otherwise ID. Used by validators when
// emitting user-facing diagnostics.
func (n NodeDef) DisplayName() string {
	if label := strings.TrimSpace(n.Label); label != "" {
		return label
	}
	return n.ID
}

// EdgeDef defines a directed edge between two nodes.
type EdgeDef struct {
	From          string `json:"from"`
	To            string `json:"to"`
	Condition     string `json:"condition,omitempty"`
	MaxTraversals int    `json:"max_traversals,omitempty"`
}

// InputDef defines an input variable for the blueprint.
type InputDef struct {
	Name     string `json:"name"`
	Label    string `json:"label,omitempty"`
	Required bool   `json:"required,omitempty"`
	Default  string `json:"default,omitempty"`
}

// Budget enforces execution limits.
type Budget struct {
	MaxTotalTimeSeconds int                `json:"max_total_time_seconds,omitempty"`
	MaxTotalTokens      int                `json:"max_total_tokens,omitempty"`
	PerNode             map[string]NodeBud `json:"per_node,omitempty"`
}

// NodeBud is per-node budget limits.
type NodeBud struct {
	MaxTokens      int `json:"max_tokens,omitempty"`
	MaxTimeSeconds int `json:"max_time_seconds,omitempty"`
}

// RunState captures the full execution state of a blueprint run.
type RunState struct {
	ID             string               `json:"id"`
	BlueprintID    string               `json:"blueprint_id"`
	Definition     Blueprint            `json:"definition"`
	Status         string               `json:"status"` // running, completed, failed, cancelled
	Error          string               `json:"error,omitempty"`
	Return         json.RawMessage      `json:"return,omitempty"`
	Inputs         map[string]string    `json:"inputs"`
	NodeStates     map[string]NodeState `json:"node_states"`
	Results        *Results             `json:"results,omitempty"`
	EdgeTraversals map[string]int       `json:"edge_traversals"`
	Usage          TokenUsage           `json:"usage"`
	NodeTraces     []NodeTrace          `json:"node_traces,omitempty"`
	StartedAt      string               `json:"started_at"`
	CompletedAt    string               `json:"completed_at,omitempty"`
}

// NodeState tracks the execution state of a single node.
type NodeState struct {
	Status      string     `json:"status"` // pending, running, completed, failed, skipped
	StartedAt   string     `json:"started_at,omitempty"`
	CompletedAt string     `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
	Usage       TokenUsage `json:"usage,omitempty"`
}

// TokenUsage tracks LLM token consumption.
type TokenUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// NodeTrace records a single node execution attempt within a run.
type NodeTrace struct {
	NodeID        string            `json:"node_id"`
	NodeType      string            `json:"node_type"`
	NodeLabel     string            `json:"node_label"`
	Iteration     int               `json:"iteration"`
	Status        string            `json:"status"`
	InputSnapshot map[string]string `json:"input_snapshot"`
	Outputs       map[string]string `json:"outputs,omitempty"`
	Error         string            `json:"error,omitempty"`
	Usage         TokenUsage        `json:"usage"`
	StartedAt     string            `json:"started_at"`
	CompletedAt   string            `json:"completed_at,omitempty"`
}

// ExecutorResult is returned by a node executor after execution. A non-nil
// Suspend parks the node under a suspension-capable engine (the durable run
// manager). The in-memory dag.Engine rejects a non-nil Suspend and fails the
// node; blueprint validation is the primary guard against that reaching
// runtime.
type ExecutorResult struct {
	Outputs     map[string]string
	Usage       TokenUsage
	Suspend     *Suspension
	EarlyReturn *json.RawMessage
}

// SuspensionKind enumerates how a parked node resumes.
const (
	SuspensionKindTimer  = "timer"
	SuspensionKindSignal = "signal"
)

// Suspension is a pure-data description of why and how a node is parked.
// Executors return a Suspension to tell the engine "park me; resume when this
// condition is met." The engine owns the bookkeeping (waiting state, resume
// delivery, timer firing); the executor only declares intent.
//
// The struct is deliberately JSON-serializable: the durable manager persists
// it as part of its waiting-node record so runs survive process restart.
// Never add function-typed fields — they would silently lose meaning across
// serialization boundaries.
type Suspension struct {
	// Kind selects the resume trigger. "timer" fires when wall-clock passes
	// ResumeAtUnix. "signal" fires when a matching Signal is delivered, or
	// ResumeAtUnix is reached first (timeout path).
	Kind string `json:"kind"`

	// Reason is human-readable context for the pause. Optional.
	Reason string `json:"reason,omitempty"`

	// RecoveryOwner identifies a subsystem that owns live in-process state
	// backing this wait — typically a goroutine that would deliver the
	// resolving signal. A non-empty value tells the durable manager that the
	// wait cannot survive a manager restart and should be failed on recovery
	// instead of parked until timeout. Empty means the wait is self-contained
	// in the persisted record (pure timers, external-signal awaits) and
	// survives restart unchanged.
	RecoveryOwner string `json:"recovery_owner,omitempty"`

	// ResumeAtUnix is the Unix timestamp at which a timer fires. For Kind
	// "timer" it is the sole trigger. For Kind "signal" it is the timeout
	// deadline; 0 means no timeout.
	ResumeAtUnix int64 `json:"resume_at_unix,omitempty"`

	// SignalType matches the Signal.Type on delivery. Prefix matching is
	// used: a waiter for "foo" also matches "foo.bar".
	SignalType string `json:"signal_type,omitempty"`

	// CorrelationKey scopes a signal to a specific waiter. The engine
	// supplies an auto-generated key when this is empty or equal to "auto"
	// (format: "<run_id>:<node_id>").
	CorrelationKey string `json:"correlation_key,omitempty"`

	// RequiredPayload lists payload keys that must be present and non-empty
	// on a delivered signal for it to count as a match.
	RequiredPayload []string `json:"required_payload,omitempty"`

	// DefaultOutputs seed the node's output map on signal-driven resume.
	// The signal payload is merged on top.
	DefaultOutputs map[string]string `json:"default_outputs,omitempty"`

	// Outputs are emitted verbatim when a pure-timer suspension fires.
	Outputs map[string]string `json:"outputs,omitempty"`

	// TimeoutOutputs are emitted when a signal suspension's deadline passes
	// before a matching signal arrives. Ignored for pure timers.
	TimeoutOutputs map[string]string `json:"timeout_outputs,omitempty"`
}

// Clone returns a deep copy. Useful when the durable manager needs to snapshot
// a suspension from an executor result into its waiting entry without sharing
// maps with the executor's call frame.
func (s *Suspension) Clone() *Suspension {
	if s == nil {
		return nil
	}
	clone := *s
	if len(s.RequiredPayload) > 0 {
		clone.RequiredPayload = append([]string(nil), s.RequiredPayload...)
	}
	clone.DefaultOutputs = cloneStringMap(s.DefaultOutputs)
	clone.Outputs = cloneStringMap(s.Outputs)
	clone.TimeoutOutputs = cloneStringMap(s.TimeoutOutputs)
	return &clone
}

// ValidateBlueprint checks a blueprint for structural errors.
func ValidateBlueprint(bp Blueprint) []error {
	var errs []error

	ids := make(map[string]struct{}, len(bp.Nodes))
	for _, node := range bp.Nodes {
		if _, dup := ids[node.ID]; dup {
			errs = append(errs, &ValidationError{Msg: "duplicate node ID: " + node.ID})
		}
		ids[node.ID] = struct{}{}
		if node.Type == "" {
			errs = append(errs, &ValidationError{Msg: "node " + node.ID + " has no type"})
		}
	}

	for _, edge := range bp.Edges {
		if _, ok := ids[edge.From]; !ok {
			errs = append(errs, &ValidationError{Msg: "edge from unknown node: " + edge.From})
		}
		if _, ok := ids[edge.To]; !ok {
			errs = append(errs, &ValidationError{Msg: "edge to unknown node: " + edge.To})
		}
	}

	return errs
}

type ValidationError struct {
	Msg string
}

func (e *ValidationError) Error() string { return e.Msg }
