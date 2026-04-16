package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

const agentRoleUser = "user"

// AgentLoopExecutor runs a model-driven tool-calling loop inside a blueprint node.
type AgentLoopExecutor struct {
	client                *agentProviderClient
	engine                *dag.Engine
	AllowLocalSourceFetch bool
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
	s := strings.Trim(string(o.Confidence), "\"")
	if s == "" || s == "null" {
		return "medium"
	}
	return s
}

// NewAgentLoopExecutorWithConfig creates an agent loop executor using the same
// provider configuration as llm_call.
func NewAgentLoopExecutorWithConfig(cfg LLMCallExecutorConfig, engine *dag.Engine) *AgentLoopExecutor {
	return &AgentLoopExecutor{
		client: newAgentProviderClient(cfg),
		engine: engine,
	}
}

func (e *AgentLoopExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
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
	allowedOutcomes, err := allowedOutcomesFromContext(execCtx, cfg.AllowedOutcomesKey)
	if err != nil {
		return failureResult(err.Error()), nil
	}

	settings, outputSpec, err := resolveAgentLoopSettings(cfg)
	if err != nil {
		return failureResult(err.Error()), nil
	}
	registry, err := e.buildToolRegistry(cfg, outputSpec, execCtx)
	if err != nil {
		return failureResult(err.Error()), nil
	}

	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(defaultAgentLoopTimeoutSeconds) * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	prompt := execCtx.Interpolate(cfg.Prompt)
	system := buildAgentSystemPrompt(execCtx.Interpolate(cfg.SystemPrompt), outputSpec, allowedOutcomes)

	return e.runLoop(runCtx, agentProviderRequest{
		Provider:  provider,
		Model:     model,
		System:    system,
		Tools:     registry.defs(),
		MaxTokens: settings.maxTokens,
		Reasoning: cfg.Reasoning,
	}, prompt, registry, settings, allowedOutcomes), nil
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

func (e *AgentLoopExecutor) buildToolRegistry(cfg AgentLoopConfig, outputSpec agentOutputSpec, execCtx *dag.Context) (*agentToolRegistry, error) {
	registry := newAgentToolRegistry()
	access := newContextAccess(execCtx, cfg.ContextAllowlist)

	if len(cfg.Tools) == 0 {
		for _, builtin := range []string{
			AgentBuiltinContextGet,
			AgentBuiltinContextList,
			AgentBuiltinSourceFetch,
			AgentBuiltinJSONExtract,
		} {
			if err := registry.register(e.newBuiltinTool(builtin, AgentToolConfig{Name: builtin}, access, cfg, execCtx)); err != nil {
				return nil, err
			}
		}
	} else {
		for _, toolCfg := range cfg.Tools {
			tool, err := e.newConfiguredTool(toolCfg, access, cfg, execCtx)
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
				parentCtx: execCtx,
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
	execCtx *dag.Context,
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
		tool := e.newBuiltinTool(builtin, toolCfg, access, agentCfg, execCtx)
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
		if err := validateBlueprintTool(toolCfg.Inline); err != nil {
			return nil, fmt.Errorf("blueprint tool %q: %w", toolCfg.Name, err)
		}
		return &blueprintTool{
			name:           strings.TrimSpace(toolCfg.Name),
			description:    toolCfg.Description,
			parameters:     toolCfg.Parameters,
			inline:         toolCfg.Inline,
			inputMappings:  toolCfg.InputMappings,
			outputKeys:     toolCfg.OutputKeys,
			timeoutSeconds: toolCfg.TimeoutSeconds,
			maxDepth:       toolCfg.MaxDepth,
			engine:         e.engine,
			parentCtx:      execCtx,
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
	execCtx *dag.Context,
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
			parentCtx: execCtx,
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
			AllowedNodeTypes:    []string{"api_fetch", "cel_eval", "wait"},
			MaxNodes:            8,
			MaxEdges:            12,
			MaxDepth:            1,
			MaxTotalTimeSeconds: 30,
			AllowTerminalNodes:  false,
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
