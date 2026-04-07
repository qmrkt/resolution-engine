// Package dag implements a DAG execution engine for resolution logic.
package dag

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
	Inputs         map[string]string    `json:"inputs"`
	NodeStates     map[string]NodeState `json:"node_states"`
	Context        map[string]string    `json:"context"`
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

// ExecutorResult is returned by a node executor after execution.
type ExecutorResult struct {
	Outputs map[string]string
	Usage   TokenUsage
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
