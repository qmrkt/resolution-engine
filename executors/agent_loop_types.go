package executors

import (
	"encoding/json"

	"github.com/qmrkt/resolution-engine/dag"
)

const (
	AgentOutputModeText       = "text"
	AgentOutputModeStructured = "structured"
	AgentOutputModeResolution = "resolution"

	AgentToolKindBuiltin   = "builtin"
	AgentToolKindBlueprint = "blueprint"

	AgentBuiltinContextGet   = "context_get"
	AgentBuiltinContextList  = "context_list"
	AgentBuiltinSourceFetch  = "source_fetch"
	AgentBuiltinJSONExtract  = "json_extract"
	AgentBuiltinRunBlueprint = "run_blueprint"
	AgentBuiltinCompleteTask = "complete_task"
	AgentBuiltinYieldControl = "yield_control"

	defaultAgentMaxToolCalls       = 16
	defaultAgentToolTimeoutSeconds = 20
	defaultAgentMaxToolResultBytes = 12_000
	defaultAgentToolResultHistory  = 2
	defaultAgentMaxHistoryMessages = 24
	defaultAgentBlueprintMaxDepth  = 1
	defaultAgentLoopTimeoutSeconds = 300
	defaultAgentOutputToolName     = "record_output"
	defaultResolutionToolName      = "record_resolution"
)

// AgentLoopConfig is the node config for agent_loop steps.
type AgentLoopConfig struct {
	Provider               string                  `json:"provider,omitempty"`
	Model                  string                  `json:"model,omitempty"`
	SystemPrompt           string                  `json:"system_prompt,omitempty"`
	Prompt                 string                  `json:"prompt"`
	AutoContinue           *bool                   `json:"auto_continue,omitempty"`
	RequireExplicitYield   bool                    `json:"require_explicit_yield,omitempty"`
	MaxContinues           int                     `json:"max_continues,omitempty"`
	TimeoutSeconds         int                     `json:"timeout_seconds,omitempty"`
	ToolTimeoutSeconds     int                     `json:"tool_timeout_seconds,omitempty"`
	MaxSteps               int                     `json:"max_steps,omitempty"`
	MaxToolCalls           int                     `json:"max_tool_calls,omitempty"`
	MaxToolResultBytes     int                     `json:"max_tool_result_bytes,omitempty"`
	ToolResultHistory      int                     `json:"tool_result_history,omitempty"`
	MaxHistoryMessages     int                     `json:"max_history_messages,omitempty"`
	MaxTokens              int                     `json:"max_tokens,omitempty"`
	Reasoning              string                  `json:"reasoning,omitempty"`
	OutputMode             string                  `json:"output_mode,omitempty"`
	OutputTool             *AgentOutputToolConfig  `json:"output_tool,omitempty"`
	Tools                  []AgentToolConfig       `json:"tools,omitempty"`
	ToolsSpecified         bool                    `json:"-"`
	ContextAllowlist       []string                `json:"context_allowlist,omitempty"`
	AllowedOutcomesKey     string                  `json:"allowed_outcomes_key,omitempty"`
	CanCompleteKey         string                  `json:"can_complete_key,omitempty"`
	Completion             *AgentCompletionConfig  `json:"completion,omitempty"`
	EnableDynamicBlueprint bool                    `json:"enable_dynamic_blueprints,omitempty"`
	DynamicBlueprintPolicy *DynamicBlueprintPolicy `json:"dynamic_blueprint_policy,omitempty"`

	// Async opts the agent into detached execution: the executor returns
	// immediately with a dag.Suspension and the chat loop runs on an
	// isolated goroutine. The goroutine resumes the node via a Signal
	// delivered to the configured AgentSignalFn. Only supported under
	// durable engines; the in-memory engine rejects the suspension.
	Async bool `json:"async,omitempty"`
}

type AgentCompletionConfig struct {
	ApprovalKey          string `json:"approval_key,omitempty"`
	FeedbackKey          string `json:"feedback_key,omitempty"`
	RequestKey           string `json:"request_key,omitempty"`
	AttemptKey           string `json:"attempt_key,omitempty"`
	InvalidateOnMutation *bool  `json:"invalidate_on_mutation,omitempty"`
}

func (c *AgentLoopConfig) UnmarshalJSON(data []byte) error {
	type alias AgentLoopConfig
	var raw alias
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	raw.ToolsSpecified = false
	if _, ok := fields["tools"]; ok {
		raw.ToolsSpecified = true
	}
	*c = AgentLoopConfig(raw)
	return nil
}

// AgentDoneSignalType is the signal type the async agent goroutine posts
// back to resume the waiting node.
const AgentDoneSignalType = "agent.done"

// AgentLoopRecoveryOwner tags async agent_loop waits so the durable manager
// can detect them on startup. The live goroutine that would deliver the
// resolving signal does not survive a manager restart, so the manager fails
// the wait immediately rather than parking it until timeout.
const AgentLoopRecoveryOwner = "agent_loop"

// AgentSignalFn delivers a completion signal for an async agent run. Usage is
// carried out-of-band from payload so the durable manager can preserve token
// accounting without leaking transport fields into node outputs.
//
// The durable engine host is expected to install this callback at
// construction time.
type AgentSignalFn func(runID, correlationKey string, payload map[string]string, usage dag.TokenUsage) error

// AgentOutputToolConfig defines the synthetic tool used by structured outputs.
type AgentOutputToolConfig struct {
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// AgentToolConfig defines an agent-visible tool.
type AgentToolConfig struct {
	Name           string            `json:"name"`
	Kind           string            `json:"kind,omitempty"`
	Builtin        string            `json:"builtin,omitempty"`
	Description    string            `json:"description,omitempty"`
	Parameters     json.RawMessage   `json:"parameters,omitempty"`
	Inline         *dag.Blueprint    `json:"inline,omitempty"`
	InputMappings  map[string]string `json:"input_mappings,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	MaxDepth       int               `json:"max_depth,omitempty"`
}

// DynamicBlueprintPolicy restricts blueprint JSON supplied dynamically by a model.
type DynamicBlueprintPolicy struct {
	AllowedNodeTypes    []string `json:"allowed_node_types,omitempty"`
	MaxNodes            int      `json:"max_nodes,omitempty"`
	MaxEdges            int      `json:"max_edges,omitempty"`
	MaxDepth            int      `json:"max_depth,omitempty"`
	MaxTotalTimeSeconds int      `json:"max_total_time_seconds,omitempty"`
	MaxTotalTokens      int      `json:"max_total_tokens,omitempty"`
	AllowAgentLoop      bool     `json:"allow_agent_loop,omitempty"`
}

type agentMessage struct {
	Role    string             `json:"role"`
	Content []agentContentPart `json:"content"`
}

type agentContentPart struct {
	Type         string          `json:"type"`
	Text         string          `json:"text,omitempty"`
	ToolUseID    string          `json:"tool_use_id,omitempty"`
	ToolName     string          `json:"tool_name,omitempty"`
	ToolInput    json.RawMessage `json:"tool_input,omitempty"`
	ToolResultID string          `json:"tool_result_id,omitempty"`
	ToolOutput   string          `json:"tool_output,omitempty"`
	IsError      bool            `json:"is_error,omitempty"`
	ProviderMeta string          `json:"provider_meta,omitempty"`
}

type agentToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type agentToolCall struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type agentProviderRequest struct {
	Provider      string
	Model         string
	System        string
	Messages      []agentMessage
	Tools         []agentToolDef
	RequireTool   bool
	ForceToolName string
	MaxTokens     int
	Reasoning     string
}

type agentProviderResponse struct {
	Message    agentMessage
	Calls      []agentToolCall
	Usage      dag.TokenUsage
	StopReason string
	ResponseID string
	Raw        string
}

type agentToolResult struct {
	Output     string
	IsError    bool
	Complete   bool
	OutputJSON json.RawMessage
}

type agentStepRecord struct {
	Step    int               `json:"step"`
	Calls   []agentToolCall   `json:"calls,omitempty"`
	Results []agentToolResult `json:"results,omitempty"`
}
