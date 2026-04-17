package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/qmrkt/resolution-engine/dag"
)

const agentRoleUser = "user"

// AgentLoopExecutor runs a model-driven tool-calling loop inside a blueprint
// node. Async mode (AgentLoopConfig.Async) dispatches the chat loop on a
// goroutine, returns a dag.Suspension, and resumes the waiting node via
// signalFn. See CancelCorrelation for the registry used by back-edge reset
// and run-level cancellation.
type AgentLoopExecutor struct {
	client                *agentProviderClient
	engine                *dag.Engine
	AllowLocalSourceFetch bool

	signalMu sync.RWMutex
	signalFn AgentSignalFn

	asyncBaseMu  sync.RWMutex
	asyncBaseCtx context.Context

	// cancellers maps correlation key → context.CancelFunc for in-flight
	// async goroutines. Populated by dispatchAsync, cleared by the
	// goroutine on natural completion, and invoked externally via
	// CancelCorrelation when the engine needs to stop a superseded run.
	cancellersMu sync.Mutex
	cancellers   map[string]agentAsyncCanceller
}

type agentLoopSettings struct {
	maxSteps           int
	maxToolCalls       int
	maxToolResultBytes int
	toolResultHistory  int
	maxHistoryMessages int
	maxTokens          int
	toolTimeout        time.Duration
	outputMode         string
	outputToolName     string
}

type agentAsyncCanceller struct {
	runID  string
	cancel context.CancelFunc
}

type agentOutputSpec struct {
	enabled     bool
	mode        string
	name        string
	description string
	parameters  json.RawMessage
}

type agentResolutionOutput struct {
	Status       string          `json:"status,omitempty"`
	OutcomeIndex int             `json:"outcome_index"`
	Confidence   json.RawMessage `json:"confidence"`
	Reasoning    string          `json:"reasoning"`
	Citations    []string        `json:"citations,omitempty"`
}

func (o agentResolutionOutput) confidenceString() string {
	return normalizeConfidenceString(o.Confidence)
}

// NewAgentLoopExecutorWithConfig creates an agent loop executor using the same
// provider configuration as llm_call.
func NewAgentLoopExecutorWithConfig(cfg LLMCallExecutorConfig, engine *dag.Engine) *AgentLoopExecutor {
	return &AgentLoopExecutor{
		client: newAgentProviderClient(cfg),
		engine: engine,
	}
}

// SetSignalFn installs the callback used by async agent goroutines to deliver
// their final outputs. Unset signalFn causes async dispatch to fail at request
// time rather than silently drop results.
func (e *AgentLoopExecutor) SetSignalFn(fn AgentSignalFn) {
	if e == nil {
		return
	}
	e.signalMu.Lock()
	e.signalFn = fn
	e.signalMu.Unlock()
}

func (e *AgentLoopExecutor) getSignalFn() AgentSignalFn {
	e.signalMu.RLock()
	defer e.signalMu.RUnlock()
	return e.signalFn
}

// SetAsyncBaseContext installs the long-lived parent context for async agent
// goroutines so manager shutdown cancels in-flight detached requests.
func (e *AgentLoopExecutor) SetAsyncBaseContext(ctx context.Context) {
	if e == nil {
		return
	}
	e.asyncBaseMu.Lock()
	e.asyncBaseCtx = ctx
	e.asyncBaseMu.Unlock()
}

func (e *AgentLoopExecutor) getAsyncBaseContext() context.Context {
	e.asyncBaseMu.RLock()
	defer e.asyncBaseMu.RUnlock()
	if e.asyncBaseCtx == nil {
		return context.Background()
	}
	return e.asyncBaseCtx
}

func (e *AgentLoopExecutor) registerCanceller(runID, correlationKey string, cancel context.CancelFunc) {
	if runID == "" || correlationKey == "" || cancel == nil {
		return
	}
	e.cancellersMu.Lock()
	if e.cancellers == nil {
		e.cancellers = make(map[string]agentAsyncCanceller)
	}
	e.cancellers[correlationKey] = agentAsyncCanceller{runID: runID, cancel: cancel}
	e.cancellersMu.Unlock()
}

// releaseCanceller removes the entry without invoking the cancel func.
// Paired with natural goroutine completion.
func (e *AgentLoopExecutor) releaseCanceller(correlationKey string) {
	if correlationKey == "" {
		return
	}
	e.cancellersMu.Lock()
	delete(e.cancellers, correlationKey)
	e.cancellersMu.Unlock()
}

// InFlightCount returns the number of async agent goroutines whose cancel
// hooks are currently registered. Best-effort snapshot under concurrent
// mutation; intended for tests and introspection.
func (e *AgentLoopExecutor) InFlightCount() int {
	if e == nil {
		return 0
	}
	e.cancellersMu.Lock()
	defer e.cancellersMu.Unlock()
	return len(e.cancellers)
}

// drainCancellers atomically removes every registered canceller that matches
// the predicate and returns their cancel funcs. The caller invokes the cancel
// funcs outside the registry lock so a slow cancel can't block concurrent
// register/release operations.
func (e *AgentLoopExecutor) drainCancellers(match func(agentAsyncCanceller) bool) []context.CancelFunc {
	e.cancellersMu.Lock()
	defer e.cancellersMu.Unlock()
	cancels := make([]context.CancelFunc, 0, len(e.cancellers))
	for correlationKey, entry := range e.cancellers {
		if !match(entry) {
			continue
		}
		delete(e.cancellers, correlationKey)
		cancels = append(cancels, entry.cancel)
	}
	return cancels
}

func invokeAll(cancels []context.CancelFunc) int {
	for _, cancel := range cancels {
		cancel()
	}
	return len(cancels)
}

// CancelCorrelation cancels the in-flight async goroutine keyed by
// correlationKey. Safe to call with unknown keys (no-op). Returns true if a
// cancel was triggered.
func (e *AgentLoopExecutor) CancelCorrelation(correlationKey string) bool {
	if e == nil || correlationKey == "" {
		return false
	}
	e.cancellersMu.Lock()
	entry, ok := e.cancellers[correlationKey]
	if ok {
		delete(e.cancellers, correlationKey)
	}
	e.cancellersMu.Unlock()
	if !ok {
		return false
	}
	entry.cancel()
	return true
}

// CancelRun cancels every in-flight async goroutine associated with runID.
// Closes the pre-persist window where Cancel(runID) can race ahead of the
// waiting entry reaching durable state.
func (e *AgentLoopExecutor) CancelRun(runID string) int {
	if e == nil || runID == "" {
		return 0
	}
	return invokeAll(e.drainCancellers(func(entry agentAsyncCanceller) bool {
		return entry.runID == runID
	}))
}

// CancelAll cancels every registered async goroutine. Intended for durable
// manager shutdown.
func (e *AgentLoopExecutor) CancelAll() int {
	if e == nil {
		return 0
	}
	return invokeAll(e.drainCancellers(func(agentAsyncCanceller) bool { return true }))
}

// CanSuspend reports whether the agent_loop node would dispatch async. Only
// configs with async=true return a Suspension; the default synchronous path
// completes in-place and is safe for inline sub-blueprints.
func (e *AgentLoopExecutor) CanSuspend(node dag.NodeDef) bool {
	cfg, err := ParseConfig[AgentLoopConfig](node.Config)
	if err != nil {
		return false
	}
	return cfg.Async
}

func (*AgentLoopExecutor) ConfigSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "required": ["prompt"],
  "properties": {
    "provider": {"type": "string", "enum": ["anthropic", "openai", "google"]},
    "model": {"type": "string"},
    "system_prompt": {"type": "string"},
    "prompt": {"type": "string", "description": "User prompt. Supports {{key}} interpolation from the shared context."},
    "timeout_seconds": {"type": "integer", "minimum": 0},
    "tool_timeout_seconds": {"type": "integer", "minimum": 0},
    "max_steps": {"type": "integer", "minimum": 0},
    "max_tool_calls": {"type": "integer", "minimum": 0},
    "max_tool_result_bytes": {"type": "integer", "minimum": 0},
    "tool_result_history": {"type": "integer", "minimum": 1},
    "max_history_messages": {"type": "integer", "minimum": 2},
    "max_tokens": {"type": "integer", "minimum": 0},
    "reasoning": {"type": "string", "description": "Provider-specific reasoning level (e.g. \"low\", \"medium\", \"high\")."},
    "output_mode": {"type": "string", "enum": ["text", "structured", "resolution"], "default": "text", "description": "text returns free-form. structured requires output_tool.parameters. resolution uses the canonical resolution schema."},
    "output_tool": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "description": {"type": "string"},
        "parameters": {"type": "object", "description": "JSON Schema for the structured output. Required when output_mode is structured."}
      }
    },
    "tools": {"type": "array", "items": {"type": "object", "description": "AgentToolConfig: name, kind (builtin|blueprint), inline blueprint, builtin id, parameters schema."}},
    "context_allowlist": {"type": "array", "items": {"type": "string"}, "description": "Context keys the agent's tools may read. When empty, defaults to all non-internal keys."},
    "allowed_outcomes_key": {"type": "string"},
    "enable_dynamic_blueprints": {"type": "boolean", "description": "Allow the run_blueprint builtin tool. Off by default."},
    "dynamic_blueprint_policy": {"type": "object"},
    "async": {"type": "boolean", "description": "When true, dispatches the chat loop on a goroutine and returns a signal suspension. Only valid under the durable engine."}
  },
  "additionalProperties": false
}`)
}

func (*AgentLoopExecutor) OutputKeys() []string {
	// Shared across modes: status, error, diagnostics. Mode-specific keys:
	// text: summary, text. structured: output_json, raw, output.<key> (dynamic).
	// resolution: outcome, resolution_status, confidence, reasoning,
	// output_json, raw, citations_json, citations_count.
	return []string{
		"status", "error", "outcome",
		"summary", "text",
		"output_json", "raw",
		"resolution_status", "confidence", "reasoning",
		"citations_json", "citations_count",
		"tool_calls_count", "tool_calls_json", "steps_json", "transcript_tail",
	}
}

func (e *AgentLoopExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	if e == nil || e.client == nil {
		return dag.ExecutorResult{}, fmt.Errorf("agent_loop executor is not configured")
	}
	cfg, err := ParseConfig[AgentLoopConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("agent_loop config: %w", err)
	}
	if strings.TrimSpace(cfg.Prompt) == "" {
		return dag.ExecutorResult{}, fmt.Errorf("agent_loop config: prompt is required")
	}

	provider, model, err := normalizeLLMSelection(cfg.Provider, cfg.Model)
	if err != nil {
		return failureResult(err.Error()), nil
	}
	allowedOutcomes, err := allowedOutcomesFromContext(inv, cfg.AllowedOutcomesKey)
	if err != nil {
		return failureResult(err.Error()), nil
	}

	settings, outputSpec, err := resolveAgentLoopSettings(cfg)
	if err != nil {
		return failureResult(err.Error()), nil
	}
	registry, err := e.buildToolRegistry(cfg, outputSpec, inv)
	if err != nil {
		return failureResult(err.Error()), nil
	}

	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(defaultAgentLoopTimeoutSeconds) * time.Second
	}

	prompt := inv.Interpolate(cfg.Prompt)
	system := buildAgentSystemPrompt(inv.Interpolate(cfg.SystemPrompt), outputSpec, allowedOutcomes)

	req := agentProviderRequest{
		Provider:  provider,
		Model:     model,
		System:    system,
		Tools:     registry.defs(),
		MaxTokens: settings.maxTokens,
		Reasoning: cfg.Reasoning,
	}

	if cfg.Async {
		return e.dispatchAsync(ctx, node, inv, req, prompt, registry, settings, allowedOutcomes, timeout)
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return e.runLoop(runCtx, req, prompt, registry, settings, allowedOutcomes), nil
}

// dispatchAsync spawns the chat loop on a goroutine and returns a signal-based
// suspension. The goroutine posts the final outputs back through signalFn,
// keyed on a per-invocation correlation key so stale results from a prior
// iteration (back-edge re-entry) are rejected by the durable manager's
// signal matcher.
func (e *AgentLoopExecutor) dispatchAsync(
	ctx context.Context,
	node dag.NodeDef,
	inv *dag.Invocation,
	req agentProviderRequest,
	prompt string,
	registry *agentToolRegistry,
	settings agentLoopSettings,
	allowedOutcomes []string,
	timeout time.Duration,
) (dag.ExecutorResult, error) {
	signalFn := e.getSignalFn()
	if signalFn == nil {
		return failureResult("agent_loop async requires a configured signal sink"), nil
	}
	runID := strings.TrimSpace(inv.Run.ID)
	if runID == "" {
		return failureResult("agent_loop async requires a non-empty Invocation.Run.ID"), nil
	}

	correlationKey := AutoCorrelationKey("", node.ID, inv) + ":" + uuid.NewString()

	// The dispatched goroutine owns its own context — detached from the
	// executor's ctx, which will be cancelled when Execute returns. Without
	// detaching, the chat loop would be cancelled the moment we suspend.
	// Cancellation reaches the goroutine via (a) the per-invocation timeout
	// below, (b) CancelCorrelation / CancelRun triggered by the durable
	// manager, and (c) manager shutdown through asyncBaseCtx.
	goroutineCtx, cancelGoroutine := context.WithTimeout(e.getAsyncBaseContext(), timeout)
	e.registerCanceller(runID, correlationKey, cancelGoroutine)

	// Capture goroutine-independent state so the closure doesn't race with
	// callers who might mutate inv after suspension.
	reqCopy := req
	settingsCopy := settings
	allowed := append([]string(nil), allowedOutcomes...)

	go func() {
		defer func() {
			cancelGoroutine()
			e.releaseCanceller(correlationKey)
		}()
		defer func() {
			if r := recover(); r != nil {
				slog.Default().Error(
					"async agent_loop panicked",
					"node", node.ID,
					"run", runID,
					"panic", fmt.Sprintf("%v", r),
				)
				payload := map[string]string{
					"status": "failed",
					"error":  fmt.Sprintf("agent panicked: %v", r),
				}
				_ = signalFn(runID, correlationKey, payload, dag.TokenUsage{})
			}
		}()
		result := e.runLoop(goroutineCtx, reqCopy, prompt, registry, settingsCopy, allowed)
		// If the goroutine context was cancelled externally (back-edge
		// reset or run cancel), drop the result on the floor — the
		// waiting entry it belonged to is already gone and the
		// correlation key will no longer match.
		if goroutineCtx.Err() != nil {
			slog.Default().Info(
				"async agent_loop cancelled in flight",
				"node", node.ID,
				"run", runID,
				"cause", goroutineCtx.Err(),
			)
			return
		}
		payload := make(map[string]string, len(result.Outputs))
		for k, v := range result.Outputs {
			payload[k] = v
		}
		if payload["status"] == "" {
			payload["status"] = "success"
		}
		if err := signalFn(runID, correlationKey, payload, result.Usage); err != nil {
			slog.Default().Warn(
				"async agent_loop signal delivery failed",
				"node", node.ID,
				"run", runID,
				"error", err,
			)
		}
	}()

	return dag.ExecutorResult{
		Suspend: &dag.Suspension{
			Kind:           dag.SuspensionKindSignal,
			Reason:         "agent_loop async",
			RecoveryOwner:  AgentLoopRecoveryOwner,
			SignalType:     AgentDoneSignalType,
			CorrelationKey: correlationKey,
			ResumeAtUnix:   time.Now().Add(timeout).Unix(),
			TimeoutOutputs: map[string]string{
				"status":  "failed",
				"error":   "agent_loop async timeout",
				"outcome": "inconclusive",
			},
		},
	}, nil
}

func resolveAgentLoopSettings(cfg AgentLoopConfig) (agentLoopSettings, agentOutputSpec, error) {
	outputMode := strings.ToLower(strings.TrimSpace(cfg.OutputMode))
	if outputMode == "" {
		outputMode = AgentOutputModeText
	}
	switch outputMode {
	case AgentOutputModeText, AgentOutputModeStructured, AgentOutputModeResolution:
	default:
		return agentLoopSettings{}, agentOutputSpec{}, fmt.Errorf("unsupported agent_loop output_mode %q", cfg.OutputMode)
	}

	settings := agentLoopSettings{
		maxSteps:           cfg.MaxSteps,
		maxToolCalls:       cfg.MaxToolCalls,
		maxToolResultBytes: cfg.MaxToolResultBytes,
		toolResultHistory:  cfg.ToolResultHistory,
		maxHistoryMessages: cfg.MaxHistoryMessages,
		maxTokens:          cfg.MaxTokens,
		toolTimeout:        time.Duration(cfg.ToolTimeoutSeconds) * time.Second,
		outputMode:         outputMode,
	}
	if settings.maxToolCalls <= 0 {
		settings.maxToolCalls = defaultAgentMaxToolCalls
	}
	if settings.maxToolResultBytes <= 0 {
		settings.maxToolResultBytes = defaultAgentMaxToolResultBytes
	}
	if settings.toolResultHistory <= 0 {
		settings.toolResultHistory = defaultAgentToolResultHistory
	}
	if settings.maxHistoryMessages <= 0 {
		settings.maxHistoryMessages = defaultAgentMaxHistoryMessages
	}
	if settings.toolTimeout <= 0 {
		settings.toolTimeout = time.Duration(defaultAgentToolTimeoutSeconds) * time.Second
	}
	if settings.maxSteps < 0 {
		return agentLoopSettings{}, agentOutputSpec{}, fmt.Errorf("max_steps must be non-negative")
	}
	if settings.maxToolCalls < 1 {
		return agentLoopSettings{}, agentOutputSpec{}, fmt.Errorf("max_tool_calls must be positive")
	}
	if settings.maxHistoryMessages < 2 {
		return agentLoopSettings{}, agentOutputSpec{}, fmt.Errorf("max_history_messages must be >= 2")
	}
	if settings.toolResultHistory < 1 {
		return agentLoopSettings{}, agentOutputSpec{}, fmt.Errorf("tool_result_history must be >= 1")
	}

	outputSpec := agentOutputSpec{mode: outputMode}
	switch outputMode {
	case AgentOutputModeStructured:
		if cfg.OutputTool == nil || len(cfg.OutputTool.Parameters) == 0 || !json.Valid(cfg.OutputTool.Parameters) {
			return agentLoopSettings{}, agentOutputSpec{}, fmt.Errorf("structured output_mode requires output_tool.parameters containing a JSON schema")
		}
		outputSpec.enabled = true
		outputSpec.name = strings.TrimSpace(cfg.OutputTool.Name)
		if outputSpec.name == "" {
			outputSpec.name = defaultAgentOutputToolName
		}
		outputSpec.description = cfg.OutputTool.Description
		outputSpec.parameters = cfg.OutputTool.Parameters
	case AgentOutputModeResolution:
		outputSpec.enabled = true
		outputSpec.name = defaultResolutionToolName
		outputSpec.description = "Record the final market-resolution decision and finish the agent loop."
		outputSpec.parameters = buildResolutionOutputSchema()
		if cfg.OutputTool != nil {
			if strings.TrimSpace(cfg.OutputTool.Name) != "" {
				outputSpec.name = strings.TrimSpace(cfg.OutputTool.Name)
			}
			if strings.TrimSpace(cfg.OutputTool.Description) != "" {
				outputSpec.description = cfg.OutputTool.Description
			}
			if len(cfg.OutputTool.Parameters) > 0 {
				if !json.Valid(cfg.OutputTool.Parameters) {
					return agentLoopSettings{}, agentOutputSpec{}, fmt.Errorf("output_tool.parameters must be valid JSON")
				}
				outputSpec.parameters = cfg.OutputTool.Parameters
			}
		}
	}
	settings.outputToolName = outputSpec.name
	return settings, outputSpec, nil
}

func buildResolutionOutputSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"status":{"type":"string","enum":["success","inconclusive"],"description":"Use success when an outcome is determined, otherwise inconclusive."},"outcome_index":{"type":"integer","description":"Zero-based selected outcome index, or -1 when status is inconclusive."},"confidence":{"type":"string","enum":["high","medium","low"]},"reasoning":{"type":"string"},"citations":{"type":"array","items":{"type":"string"}}},"required":["outcome_index","confidence","reasoning"],"additionalProperties":false}`)
}

func (e *AgentLoopExecutor) buildToolRegistry(cfg AgentLoopConfig, outputSpec agentOutputSpec, inv *dag.Invocation) (*agentToolRegistry, error) {
	registry := newAgentToolRegistry()
	access := newContextAccess(inv, cfg.ContextAllowlist)

	if len(cfg.Tools) == 0 {
		for _, builtin := range []string{
			AgentBuiltinContextGet,
			AgentBuiltinContextList,
			AgentBuiltinSourceFetch,
			AgentBuiltinJSONExtract,
		} {
			if err := registry.register(e.newBuiltinTool(builtin, AgentToolConfig{Name: builtin}, access, cfg, inv)); err != nil {
				return nil, err
			}
		}
	} else {
		for _, toolCfg := range cfg.Tools {
			tool, err := e.newConfiguredTool(toolCfg, access, cfg, inv)
			if err != nil {
				return nil, err
			}
			if err := registry.register(tool); err != nil {
				return nil, err
			}
		}
	}

	if cfg.EnableDynamicBlueprint {
		if _, exists := registry.get(AgentBuiltinRunBlueprint); !exists {
			policy := defaultDynamicBlueprintPolicy(cfg.DynamicBlueprintPolicy)
			if err := registry.register(&dynamicBlueprintTool{
				engine:    e.engine,
				parentCtx: inv,
				policy:    policy,
			}); err != nil {
				return nil, err
			}
		}
	}

	if outputSpec.enabled {
		if err := registry.register(&outputCaptureTool{
			name:        outputSpec.name,
			description: outputSpec.description,
			parameters:  outputSpec.parameters,
		}); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func (e *AgentLoopExecutor) newConfiguredTool(
	toolCfg AgentToolConfig,
	access contextAccess,
	agentCfg AgentLoopConfig,
	inv *dag.Invocation,
) (agentTool, error) {
	kind := strings.ToLower(strings.TrimSpace(toolCfg.Kind))
	if kind == "" {
		if toolCfg.Inline != nil {
			kind = AgentToolKindBlueprint
		} else {
			kind = AgentToolKindBuiltin
		}
	}
	switch kind {
	case AgentToolKindBuiltin:
		builtin := strings.TrimSpace(toolCfg.Builtin)
		if builtin == "" {
			builtin = strings.TrimSpace(toolCfg.Name)
		}
		if builtin == AgentBuiltinRunBlueprint && !agentCfg.EnableDynamicBlueprint {
			return nil, fmt.Errorf("builtin tool %q requires enable_dynamic_blueprints", builtin)
		}
		tool := e.newBuiltinTool(builtin, toolCfg, access, agentCfg, inv)
		if missing, ok := tool.(*missingConfiguredTool); ok {
			return nil, fmt.Errorf("unknown builtin tool %q", missing.builtin)
		}
		return tool, nil
	case AgentToolKindBlueprint:
		if strings.TrimSpace(toolCfg.Name) == "" {
			return nil, fmt.Errorf("blueprint tool name is required")
		}
		if e == nil || e.engine == nil {
			return nil, fmt.Errorf("blueprint tool %q requires an engine", toolCfg.Name)
		}
		if err := validateBlueprintTool(e.engine, toolCfg.Inline); err != nil {
			return nil, fmt.Errorf("blueprint tool %q: %w", toolCfg.Name, err)
		}
		return &blueprintTool{
			name:           strings.TrimSpace(toolCfg.Name),
			description:    toolCfg.Description,
			parameters:     toolCfg.Parameters,
			inline:         toolCfg.Inline,
			inputMappings:  toolCfg.InputMappings,
			timeoutSeconds: toolCfg.TimeoutSeconds,
			maxDepth:       toolCfg.MaxDepth,
			engine:         e.engine,
			parentCtx:      inv,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported agent tool kind %q", toolCfg.Kind)
	}
}

func (e *AgentLoopExecutor) newBuiltinTool(
	builtin string,
	toolCfg AgentToolConfig,
	access contextAccess,
	agentCfg AgentLoopConfig,
	inv *dag.Invocation,
) agentTool {
	var tool agentTool
	switch strings.TrimSpace(builtin) {
	case AgentBuiltinContextGet:
		tool = &contextGetTool{access: access}
	case AgentBuiltinContextList:
		tool = &contextListTool{access: access}
	case AgentBuiltinSourceFetch:
		tool = &sourceFetchTool{allowLocal: e != nil && e.AllowLocalSourceFetch}
	case AgentBuiltinJSONExtract:
		tool = &jsonExtractTool{access: access}
	case AgentBuiltinRunBlueprint:
		tool = &dynamicBlueprintTool{
			engine:    e.engine,
			parentCtx: inv,
			policy:    defaultDynamicBlueprintPolicy(agentCfg.DynamicBlueprintPolicy),
		}
	default:
		return &missingConfiguredTool{name: defaultString(strings.TrimSpace(toolCfg.Name), builtin), builtin: builtin}
	}
	return maybeAliasAgentTool(tool, toolCfg)
}

func defaultDynamicBlueprintPolicy(policy *DynamicBlueprintPolicy) DynamicBlueprintPolicy {
	if policy == nil {
		return DynamicBlueprintPolicy{
			AllowedNodeTypes:    []string{"api_fetch", "cel_eval", "wait", "return"},
			MaxNodes:            8,
			MaxEdges:            12,
			MaxDepth:            1,
			MaxTotalTimeSeconds: 30,
			AllowAgentLoop:      false,
		}
	}
	return *policy
}

type missingConfiguredTool struct {
	name    string
	builtin string
}

func (t *missingConfiguredTool) Name() string {
	if strings.TrimSpace(t.name) != "" {
		return strings.TrimSpace(t.name)
	}
	return strings.TrimSpace(t.builtin)
}
func (t *missingConfiguredTool) Description() string { return "Unavailable builtin tool." }
func (t *missingConfiguredTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}
func (t *missingConfiguredTool) ReadOnly() bool { return false }
func (t *missingConfiguredTool) Execute(_ context.Context, _ json.RawMessage) (agentToolResult, error) {
	return toolErrorResult("unknown builtin tool %q", t.builtin), nil
}

type aliasedAgentTool struct {
	base        agentTool
	name        string
	description string
	parameters  json.RawMessage
}

func maybeAliasAgentTool(base agentTool, cfg AgentToolConfig) agentTool {
	name := strings.TrimSpace(cfg.Name)
	description := strings.TrimSpace(cfg.Description)
	parameters := cfg.Parameters
	if name == "" || name == base.Name() {
		if description == "" && len(parameters) == 0 {
			return base
		}
		name = base.Name()
	}
	return &aliasedAgentTool{
		base:        base,
		name:        name,
		description: description,
		parameters:  parameters,
	}
}

func (t *aliasedAgentTool) Name() string { return t.name }
func (t *aliasedAgentTool) Description() string {
	if t.description != "" {
		return t.description
	}
	return t.base.Description()
}
func (t *aliasedAgentTool) Parameters() json.RawMessage {
	if len(t.parameters) > 0 {
		return defaultSchemaObject(t.parameters)
	}
	return t.base.Parameters()
}
func (t *aliasedAgentTool) ReadOnly() bool { return t.base.ReadOnly() }
func (t *aliasedAgentTool) Execute(ctx context.Context, args json.RawMessage) (agentToolResult, error) {
	return t.base.Execute(ctx, args)
}
func (t *aliasedAgentTool) CanParallelize(args json.RawMessage) bool {
	if checker, ok := t.base.(agentParallelChecker); ok {
		return checker.CanParallelize(args)
	}
	return t.base.ReadOnly()
}

func buildAgentSystemPrompt(base string, outputSpec agentOutputSpec, allowedOutcomes []string) string {
	parts := make([]string, 0, 8)
	if strings.TrimSpace(base) != "" {
		parts = append(parts, strings.TrimSpace(base))
	}
	parts = append(parts,
		"You are an autonomous agent running inside a resolution blueprint.",
		"Use tools when they are useful. Keep tool arguments precise and do not invent tool outputs.",
		"Treat readable execution context and tool results as evidence. If evidence is insufficient, say so clearly.",
	)
	if len(allowedOutcomes) > 0 {
		if encoded, err := json.Marshal(allowedOutcomes); err == nil {
			parts = append(parts, "Allowed outcome labels, by zero-based index: "+string(encoded))
		}
	}
	switch outputSpec.mode {
	case AgentOutputModeStructured:
		parts = append(parts, fmt.Sprintf("When finished, call the %q tool exactly once with the final JSON output. Do not answer in prose instead of calling this tool.", outputSpec.name))
	case AgentOutputModeResolution:
		parts = append(parts, fmt.Sprintf("When finished, call the %q tool exactly once. Use outcome_index as a zero-based outcome index. If the market cannot be resolved, set status to inconclusive and outcome_index to -1.", outputSpec.name))
	default:
		parts = append(parts, "When finished, respond directly with the final answer.")
	}
	return strings.Join(parts, "\n\n")
}

func (e *AgentLoopExecutor) runLoop(
	ctx context.Context,
	req agentProviderRequest,
	prompt string,
	registry *agentToolRegistry,
	settings agentLoopSettings,
	allowedOutcomes []string,
) dag.ExecutorResult {
	messages := []agentMessage{{
		Role:    agentRoleUser,
		Content: []agentContentPart{{Type: contentPartTypeText, Text: prompt}},
	}}

	var usage dag.TokenUsage
	var steps []agentStepRecord
	toolCallCount := 0

	for step := 1; ; step++ {
		if settings.maxSteps > 0 && step > settings.maxSteps {
			return agentFailureResult(fmt.Sprintf("agent_loop reached max_steps %d", settings.maxSteps), usage, steps, messages)
		}
		messages = compactAgentMessages(messages, settings.maxHistoryMessages, settings.toolResultHistory)
		req.Messages = append([]agentMessage(nil), messages...)
		resp, err := e.client.chat(ctx, req)
		if err != nil {
			return agentFailureResult(err.Error(), usage, steps, messages)
		}
		usage.InputTokens += resp.Usage.InputTokens
		usage.OutputTokens += resp.Usage.OutputTokens
		messages = append(messages, resp.Message)

		if len(resp.Calls) == 0 {
			return finalizeAgentTextResponse(settings.outputMode, resp.Message, usage, steps, messages, allowedOutcomes)
		}
		if toolCallCount+len(resp.Calls) > settings.maxToolCalls {
			return agentFailureResult(
				fmt.Sprintf("agent_loop exceeded max_tool_calls %d", settings.maxToolCalls),
				usage,
				steps,
				messages,
			)
		}
		toolCallCount += len(resp.Calls)

		results := executeAgentToolCalls(ctx, registry, resp.Calls, settings)
		steps = append(steps, agentStepRecord{Step: step, Calls: scrubAgentToolCalls(resp.Calls), Results: results})

		resultParts := make([]agentContentPart, 0, len(results))
		for idx, result := range results {
			call := resp.Calls[idx]
			if result.Complete {
				return finalizeAgentStructuredResult(settings.outputMode, result.OutputJSON, result.Output, usage, steps, messages, allowedOutcomes)
			}
			resultParts = append(resultParts, agentContentPart{
				Type:         contentPartTypeToolResult,
				ToolResultID: call.ID,
				ToolName:     call.Name,
				ToolOutput:   result.Output,
				IsError:      result.IsError,
			})
		}
		if len(resultParts) == 0 {
			return agentFailureResult("agent_loop produced no tool results to continue with", usage, steps, messages)
		}
		messages = append(messages, agentMessage{Role: agentRoleUser, Content: resultParts})
	}

}

func compactAgentMessages(messages []agentMessage, maxHistoryMessages int, toolResultHistory int) []agentMessage {
	if len(messages) == 0 {
		return messages
	}

	// Count tool-result messages to decide if any work is needed.
	toolResultCount := 0
	for _, msg := range messages {
		for _, part := range msg.Content {
			if part.Type == contentPartTypeToolResult {
				toolResultCount++
				break
			}
		}
	}
	if len(messages) <= maxHistoryMessages && toolResultCount <= toolResultHistory {
		return messages
	}

	// Build index of which messages contain tool results.
	trimmed := make([]agentMessage, 0, len(messages))
	toolResultMessageIndexes := make([]int, 0, toolResultCount)
	for _, msg := range messages {
		if len(msg.Content) == 0 {
			continue
		}
		trimmed = append(trimmed, msg)
		for _, part := range msg.Content {
			if part.Type == contentPartTypeToolResult {
				toolResultMessageIndexes = append(toolResultMessageIndexes, len(trimmed)-1)
				break
			}
		}
	}

	// Strip old tool results beyond the history window.
	droppedResults := len(toolResultMessageIndexes) > toolResultHistory
	if droppedResults {
		dropBefore := len(toolResultMessageIndexes) - toolResultHistory
		cutoff := toolResultMessageIndexes[dropBefore]
		for i := 0; i < cutoff; i++ {
			msg := &trimmed[i]
			parts := make([]agentContentPart, 0, len(msg.Content))
			for _, part := range msg.Content {
				if part.Type != contentPartTypeToolResult {
					parts = append(parts, part)
				}
			}
			msg.Content = parts
		}

		// Remove messages that became empty after stripping.
		compacted := make([]agentMessage, 0, len(trimmed))
		for _, msg := range trimmed {
			if len(msg.Content) > 0 {
				compacted = append(compacted, msg)
			}
		}
		trimmed = compacted
	}

	if len(trimmed) <= maxHistoryMessages {
		return trimmed
	}
	return append([]agentMessage(nil), trimmed[len(trimmed)-maxHistoryMessages:]...)
}

func executeAgentToolCalls(
	ctx context.Context,
	registry *agentToolRegistry,
	calls []agentToolCall,
	settings agentLoopSettings,
) []agentToolResult {
	results := make([]agentToolResult, 0, len(calls))
	for idx := 0; idx < len(calls); {
		if registry.canParallelize(calls[idx]) {
			end := idx + 1
			for end < len(calls) && registry.canParallelize(calls[end]) {
				end++
			}
			groupResults := executeAgentToolGroup(ctx, registry, calls[idx:end], settings)
			results = append(results, groupResults...)
			idx = end
			continue
		}
		result := executeAgentToolCall(ctx, registry, calls[idx], settings)
		results = append(results, result)
		if result.Complete {
			break
		}
		idx++
	}
	return results
}

func executeAgentToolGroup(
	ctx context.Context,
	registry *agentToolRegistry,
	calls []agentToolCall,
	settings agentLoopSettings,
) []agentToolResult {
	results := make([]agentToolResult, len(calls))
	var wg sync.WaitGroup
	for idx, call := range calls {
		wg.Add(1)
		go func(i int, c agentToolCall) {
			defer wg.Done()
			results[i] = executeAgentToolCall(ctx, registry, c, settings)
		}(idx, call)
	}
	wg.Wait()
	return results
}

func executeAgentToolCall(
	ctx context.Context,
	registry *agentToolRegistry,
	call agentToolCall,
	settings agentLoopSettings,
) agentToolResult {
	tool, ok := registry.get(call.Name)
	if !ok {
		return toolErrorResult("unknown tool %q", call.Name)
	}
	toolCtx, cancel := context.WithTimeout(ctx, settings.toolTimeout)
	defer cancel()
	result, err := tool.Execute(toolCtx, defaultRawObject(call.Input))
	if err != nil {
		return toolErrorResult("tool %q failed: %v", call.Name, err)
	}
	return truncateAgentToolResult(result, settings.maxToolResultBytes)
}

func truncateAgentToolResult(result agentToolResult, maxBytes int) agentToolResult {
	if result.Complete || maxBytes <= 0 || len(result.Output) <= maxBytes {
		return result
	}
	contentLimit := maxBytes - 256
	if contentLimit < 256 {
		contentLimit = 256
	}
	if contentLimit > len(result.Output) {
		contentLimit = len(result.Output)
	}
	encoded, err := json.Marshal(map[string]any{
		"status":         "truncated",
		"truncated":      true,
		"original_bytes": len(result.Output),
		"content":        result.Output[:contentLimit],
	})
	if err != nil {
		result.Output = result.Output[:contentLimit]
		return result
	}
	result.Output = string(encoded)
	return result
}

func finalizeAgentTextResponse(
	outputMode string,
	message agentMessage,
	usage dag.TokenUsage,
	steps []agentStepRecord,
	messages []agentMessage,
	allowedOutcomes []string,
) dag.ExecutorResult {
	text := strings.TrimSpace(agentMessageText(message))
	if outputMode == AgentOutputModeText {
		outputs := map[string]string{
			"status":  "success",
			"summary": text,
			"text":    text,
		}
		addAgentDiagnostics(outputs, steps, messages)
		return dag.ExecutorResult{Outputs: outputs, Usage: usage}
	}
	if text == "" {
		return agentFailureResult("agent returned no final text and did not call the required output tool", usage, steps, messages)
	}
	extracted := extractJSON(text)
	if !json.Valid([]byte(extracted)) {
		return agentFailureResult("agent did not call the required output tool or return valid JSON", usage, steps, messages)
	}
	return finalizeAgentStructuredResult(outputMode, json.RawMessage(extracted), text, usage, steps, messages, allowedOutcomes)
}

func finalizeAgentStructuredResult(
	outputMode string,
	rawJSON json.RawMessage,
	raw string,
	usage dag.TokenUsage,
	steps []agentStepRecord,
	messages []agentMessage,
	allowedOutcomes []string,
) dag.ExecutorResult {
	if len(rawJSON) == 0 || !json.Valid(rawJSON) {
		return agentFailureResult("final structured output was not valid JSON", usage, steps, messages)
	}
	switch outputMode {
	case AgentOutputModeResolution:
		return finalizeAgentResolution(rawJSON, raw, usage, steps, messages, allowedOutcomes)
	case AgentOutputModeStructured:
		outputs := map[string]string{
			"status":      "success",
			"output_json": string(rawJSON),
			"raw":         raw,
		}
		flattenStructuredOutput(outputs, rawJSON)
		addAgentDiagnostics(outputs, steps, messages)
		return dag.ExecutorResult{Outputs: outputs, Usage: usage}
	default:
		text := strings.TrimSpace(raw)
		outputs := map[string]string{
			"status":  "success",
			"summary": text,
			"text":    text,
		}
		addAgentDiagnostics(outputs, steps, messages)
		return dag.ExecutorResult{Outputs: outputs, Usage: usage}
	}
}

func finalizeAgentResolution(
	rawJSON json.RawMessage,
	raw string,
	usage dag.TokenUsage,
	steps []agentStepRecord,
	messages []agentMessage,
	allowedOutcomes []string,
) dag.ExecutorResult {
	var output agentResolutionOutput
	if err := json.Unmarshal(rawJSON, &output); err != nil {
		return agentFailureResult("parse resolution output: "+err.Error(), usage, steps, messages)
	}
	status := strings.ToLower(strings.TrimSpace(output.Status))
	if status == "" {
		status = "success"
	}
	outputs := map[string]string{
		"status":            "success",
		"resolution_status": status,
		"confidence":        output.confidenceString(),
		"reasoning":         output.Reasoning,
		"output_json":       string(rawJSON),
		"raw":               raw,
	}
	if status == "inconclusive" || output.OutcomeIndex < 0 {
		outputs["outcome"] = "inconclusive"
		addAgentDiagnostics(outputs, steps, messages)
		return dag.ExecutorResult{Outputs: outputs, Usage: usage}
	}
	if err := validateOutcomeIndex(output.OutcomeIndex, allowedOutcomes); err != nil {
		result := failureResult(err.Error())
		result.Usage = usage
		result.Outputs["output_json"] = string(rawJSON)
		addAgentDiagnostics(result.Outputs, steps, messages)
		return result
	}
	outputs["outcome"] = strconv.Itoa(output.OutcomeIndex)
	if len(output.Citations) > 0 {
		citationsJSON, _ := json.Marshal(output.Citations)
		outputs["citations_json"] = string(citationsJSON)
		outputs["citations_count"] = strconv.Itoa(len(output.Citations))
	}
	addAgentDiagnostics(outputs, steps, messages)
	return dag.ExecutorResult{Outputs: outputs, Usage: usage}
}

func flattenStructuredOutput(outputs map[string]string, rawJSON json.RawMessage) {
	var decoded map[string]any
	if err := json.Unmarshal(rawJSON, &decoded); err != nil {
		return
	}
	for key, value := range decoded {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		switch v := value.(type) {
		case string:
			outputs["output."+key] = v
		case float64:
			outputs["output."+key] = strconv.FormatFloat(v, 'f', -1, 64)
		case bool:
			outputs["output."+key] = strconv.FormatBool(v)
		case nil:
			outputs["output."+key] = ""
		default:
			encoded, err := json.Marshal(v)
			if err == nil {
				outputs["output."+key] = string(encoded)
			}
		}
	}
}

func agentFailureResult(message string, usage dag.TokenUsage, steps []agentStepRecord, messages []agentMessage) dag.ExecutorResult {
	outputs := map[string]string{
		"status":  "failed",
		"error":   message,
		"outcome": "inconclusive",
	}
	addAgentDiagnostics(outputs, steps, messages)
	return dag.ExecutorResult{Outputs: outputs, Usage: usage}
}

func addAgentDiagnostics(outputs map[string]string, steps []agentStepRecord, messages []agentMessage) {
	outputs["tool_calls_count"] = strconv.Itoa(countAgentToolCalls(steps))
	if len(steps) > 0 {
		if encoded, err := json.Marshal(steps); err == nil {
			outputs["steps_json"] = string(encoded)
		}
		if encoded, err := json.Marshal(flattenAgentToolCalls(steps)); err == nil {
			outputs["tool_calls_json"] = string(encoded)
		}
	}
	if tail := transcriptTail(messages); tail != "" {
		outputs["transcript_tail"] = tail
	}
}

func countAgentToolCalls(steps []agentStepRecord) int {
	count := 0
	for _, step := range steps {
		count += len(step.Calls)
	}
	return count
}

func flattenAgentToolCalls(steps []agentStepRecord) []agentToolCall {
	calls := make([]agentToolCall, 0, countAgentToolCalls(steps))
	for _, step := range steps {
		calls = append(calls, step.Calls...)
	}
	return calls
}

func scrubAgentToolCalls(calls []agentToolCall) []agentToolCall {
	out := make([]agentToolCall, len(calls))
	copy(out, calls)
	return out
}

func agentMessageText(message agentMessage) string {
	var parts []string
	for _, part := range message.Content {
		if part.Type == contentPartTypeText && strings.TrimSpace(part.Text) != "" {
			parts = append(parts, part.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func transcriptTail(messages []agentMessage) string {
	for i := len(messages) - 1; i >= 0; i-- {
		text := strings.TrimSpace(agentMessageText(messages[i]))
		if text == "" {
			continue
		}
		if len(text) > 4000 {
			return text[len(text)-4000:]
		}
		return text
	}
	return ""
}
