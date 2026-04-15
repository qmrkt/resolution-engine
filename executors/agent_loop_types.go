package executors

import (
	"encoding/json"

	"github.com/question-market/resolution-engine/dag"
)

const (
	AgentOutputModeText       = "text"
	AgentOutputModeStructured = "structured"
	AgentOutputModeResolution = "resolution"

	AgentToolKindBuiltin   = "builtin"
	AgentToolKindBlueprint = "blueprint"

	AgentBuiltinContextGet    = "context_get"
	AgentBuiltinContextList   = "context_list"
	AgentBuiltinSourceFetch   = "source_fetch"
	AgentBuiltinJSONExtract   = "json_extract"
	AgentBuiltinRunBlueprint  = "run_blueprint"

	defaultAgentMaxSteps           = 8
	defaultAgentMaxToolCalls       = 16
	defaultAgentToolTimeoutSeconds = 20
	defaultAgentMaxToolResultBytes = 12_000
	defaultAgentOutputToolName     = "record_output"
	defaultResolutionToolName      = "record_resolution"
)

// AgentLoopConfig is the node config for agent_loop steps.
type AgentLoopConfig struct {
	Provider               string                  `json:"provider,omitempty"`
	Model                  string                  `json:"model,omitempty"`
	SystemPrompt           string                  `json:"system_prompt,omitempty"`
	Prompt                 string                  `json:"prompt"`
	TimeoutSeconds         int                     `json:"timeout_seconds,omitempty"`
	ToolTimeoutSeconds     int                     `json:"tool_timeout_seconds,omitempty"`
	MaxSteps               int                     `json:"max_steps,omitempty"`
	MaxToolCalls           int                     `json:"max_tool_calls,omitempty"`
	MaxToolResultBytes     int                     `json:"max_tool_result_bytes,omitempty"`
	MaxTokens              int                     `json:"max_tokens,omitempty"`
	Reasoning              string                  `json:"reasoning,omitempty"`
	OutputMode             string                  `json:"output_mode,omitempty"`
	OutputTool             *AgentOutputToolConfig  `json:"output_tool,omitempty"`
	Tools                  []AgentToolConfig       `json:"tools,omitempty"`
	ContextAllowlist       []string                `json:"context_allowlist,omitempty"`
	AllowedOutcomesKey     string                  `json:"allowed_outcomes_key,omitempty"`
	EnableDynamicBlueprint bool                    `json:"enable_dynamic_blueprints,omitempty"`
	DynamicBlueprintPolicy *DynamicBlueprintPolicy `json:"dynamic_blueprint_policy,omitempty"`
}

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
	OutputKeys     []string          `json:"output_keys,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
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
	AllowTerminalNodes  bool     `json:"allow_terminal_nodes,omitempty"`
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
	Provider  string
	Model     string
	System    string
	Messages  []agentMessage
	Tools     []agentToolDef
	MaxTokens int
	Reasoning string
}

type agentProviderResponse struct {
	Message    agentMessage
	Calls      []agentToolCall
	Usage      dag.TokenUsage
	StopReason string
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
