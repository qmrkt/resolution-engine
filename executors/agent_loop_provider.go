package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
)

var agentOpenAIResponsesAPIModels = map[string]bool{
	"gpt-5":              true,
	"gpt-5-mini":         true,
	"gpt-5-nano":         true,
	"gpt-5.1":            true,
	"gpt-5.1-codex":      true,
	"gpt-5.1-codex-max":  true,
	"gpt-5.1-codex-mini": true,
	"gpt-5.2":            true,
	"gpt-5.2-codex":      true,
	"gpt-5.3":            true,
	"gpt-5.3-codex":      true,
	"gpt-5.4":            true,
	"gpt-5.4-mini":       true,
	"gpt-5.4-nano":       true,
	"gpt-5-2025-08-07":   true,
}

type agentProviderClient struct {
	provider providerLayerConfig
}

func newAgentProviderClient(cfg LLMCallExecutorConfig) *agentProviderClient {
	return &agentProviderClient{
		provider: newProviderLayerConfig(cfg),
	}
}

func (c *agentProviderClient) httpClient() *http.Client {
	if c == nil {
		return http.DefaultClient
	}
	return c.provider.httpClient()
}

func (c *agentProviderClient) chat(ctx context.Context, req agentProviderRequest) (agentProviderResponse, error) {
	apiKey, err := c.provider.apiKey(req.Provider)
	if err != nil {
		return agentProviderResponse{}, err
	}
	if apiKey == "" {
		return agentProviderResponse{}, fmt.Errorf("no %s API key configured", req.Provider)
	}

	switch req.Provider {
	case LLMProviderAnthropic:
		return c.chatAnthropic(ctx, req)
	case LLMProviderOpenAI:
		if agentOpenAIUsesResponses(req.Model) {
			return c.chatOpenAIResponses(ctx, req)
		}
		return c.chatOpenAICompletions(ctx, req)
	case LLMProviderGoogle:
		return c.chatGoogle(ctx, req)
	default:
		return agentProviderResponse{}, fmt.Errorf("unsupported llm provider %q", req.Provider)
	}
}

type anthropicAgentContent struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
}

type anthropicAgentMessage struct {
	Role    string                  `json:"role"`
	Content []anthropicAgentContent `json:"content"`
}

func (c *agentProviderClient) chatAnthropic(ctx context.Context, req agentProviderRequest) (agentProviderResponse, error) {
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = DefaultAnthropicMaxTokens
	}
	reasoning := strings.TrimSpace(req.Reasoning)
	normalizedReasoning := strings.ToLower(reasoning)
	normalizedModel := strings.ToLower(req.Model)
	enableThinking := reasoning != "" && normalizedReasoning != "none" &&
		(strings.Contains(normalizedModel, "opus") || strings.Contains(normalizedModel, "sonnet"))
	if enableThinking && maxTokens < 16384 {
		maxTokens = 16384
	}

	body := map[string]any{
		"model":      req.Model,
		"max_tokens": maxTokens,
		"messages":   anthropicMessages(req.Messages),
	}
	if enableThinking {
		body["thinking"] = map[string]string{"type": "adaptive"}
		body["output_config"] = map[string]string{"effort": reasoning}
	}
	if strings.TrimSpace(req.System) != "" {
		body["system"] = req.System
	}
	if len(req.Tools) > 0 {
		body["tools"] = req.Tools
	}

	endpoint, err := c.provider.endpoint(LLMProviderAnthropic, req.Model)
	if err != nil {
		return agentProviderResponse{}, err
	}
	httpReq, err := newJSONRequest(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return agentProviderResponse{}, err
	}
	httpReq.Header.Set("x-api-key", c.provider.AnthropicAPIKey)
	httpReq.Header.Set("anthropic-version", "2023-06-01")

	respBody, err := doRequest(c.httpClient(), httpReq)
	if err != nil {
		return agentProviderResponse{}, err
	}

	var apiResp struct {
		Content []struct {
			Type  string          `json:"type"`
			Text  string          `json:"text"`
			ID    string          `json:"id"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return agentProviderResponse{}, fmt.Errorf("parse anthropic response: %w", err)
	}

	message := agentMessage{Role: "assistant"}
	calls := make([]agentToolCall, 0)
	for _, part := range apiResp.Content {
		switch part.Type {
		case contentPartTypeText:
			message.Content = append(message.Content, agentContentPart{Type: contentPartTypeText, Text: part.Text})
		case contentPartTypeToolUse:
			input := part.Input
			if len(input) == 0 {
				input = json.RawMessage(`{}`)
			}
			message.Content = append(message.Content, agentContentPart{
				Type:      contentPartTypeToolUse,
				ToolUseID: part.ID,
				ToolName:  part.Name,
				ToolInput: input,
			})
			calls = append(calls, agentToolCall{ID: part.ID, Name: part.Name, Input: input})
		}
	}

	return agentProviderResponse{
		Message:    message,
		Calls:      calls,
		StopReason: apiResp.StopReason,
		Usage: dag.TokenUsage{
			InputTokens:  apiResp.Usage.InputTokens,
			OutputTokens: apiResp.Usage.OutputTokens,
		},
		Raw: string(respBody),
	}, nil
}

func anthropicMessages(messages []agentMessage) []anthropicAgentMessage {
	out := make([]anthropicAgentMessage, 0, len(messages))
	for _, msg := range messages {
		am := anthropicAgentMessage{Role: msg.Role}
		if am.Role == "" {
			am.Role = "user"
		}
		for _, part := range msg.Content {
			switch part.Type {
			case contentPartTypeText:
				if strings.TrimSpace(part.Text) != "" {
					am.Content = append(am.Content, anthropicAgentContent{Type: contentPartTypeText, Text: part.Text})
				}
			case contentPartTypeToolUse:
				input := part.ToolInput
				if len(input) == 0 {
					input = json.RawMessage(`{}`)
				}
				am.Content = append(am.Content, anthropicAgentContent{
					Type:  contentPartTypeToolUse,
					ID:    part.ToolUseID,
					Name:  part.ToolName,
					Input: input,
				})
			case contentPartTypeToolResult:
				am.Content = append(am.Content, anthropicAgentContent{
					Type:      contentPartTypeToolResult,
					ToolUseID: part.ToolResultID,
					Content:   part.ToolOutput,
					IsError:   part.IsError,
				})
			}
		}
		if len(am.Content) > 0 {
			out = append(out, am)
		}
	}
	return out
}

type openAIChatMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content,omitempty"`
	ToolCalls  []openAIChatCall `json:"tool_calls,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
}

type openAIChatCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type openAIChatTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

func (c *agentProviderClient) chatOpenAICompletions(ctx context.Context, req agentProviderRequest) (agentProviderResponse, error) {
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = DefaultCompletionMaxTokens
	}
	payload := map[string]any{
		"model":                 req.Model,
		"messages":              openAIChatMessages(req.System, req.Messages),
		"tools":                 openAIChatTools(req.Tools),
		"max_completion_tokens": maxTokens,
	}
	if len(req.Tools) == 0 {
		delete(payload, "tools")
	}

	endpoint, err := c.provider.endpoint(LLMProviderOpenAI, req.Model)
	if err != nil {
		return agentProviderResponse{}, err
	}
	httpReq, err := newJSONRequest(ctx, http.MethodPost, endpoint, payload)
	if err != nil {
		return agentProviderResponse{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.provider.OpenAIAPIKey)

	respBody, err := doRequest(c.httpClient(), httpReq)
	if err != nil {
		return agentProviderResponse{}, err
	}

	var apiResp struct {
		Choices []struct {
			Message struct {
				Content   string           `json:"content"`
				ToolCalls []openAIChatCall `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return agentProviderResponse{}, fmt.Errorf("parse openai response: %w", err)
	}
	if len(apiResp.Choices) == 0 {
		return agentProviderResponse{}, fmt.Errorf("openai response contained no choices")
	}

	choice := apiResp.Choices[0]
	message := agentMessage{Role: "assistant"}
	if strings.TrimSpace(choice.Message.Content) != "" {
		message.Content = append(message.Content, agentContentPart{Type: contentPartTypeText, Text: choice.Message.Content})
	}
	calls := make([]agentToolCall, 0, len(choice.Message.ToolCalls))
	for _, tc := range choice.Message.ToolCalls {
		input := json.RawMessage(tc.Function.Arguments)
		if len(input) == 0 || !json.Valid(input) {
			input = json.RawMessage(`{}`)
		}
		message.Content = append(message.Content, agentContentPart{
			Type:      contentPartTypeToolUse,
			ToolUseID: tc.ID,
			ToolName:  tc.Function.Name,
			ToolInput: input,
		})
		calls = append(calls, agentToolCall{ID: tc.ID, Name: tc.Function.Name, Input: input})
	}

	return agentProviderResponse{
		Message:    message,
		Calls:      calls,
		StopReason: choice.FinishReason,
		Usage: dag.TokenUsage{
			InputTokens:  apiResp.Usage.PromptTokens,
			OutputTokens: apiResp.Usage.CompletionTokens,
		},
		Raw: string(respBody),
	}, nil
}

func openAIChatMessages(system string, messages []agentMessage) []openAIChatMessage {
	out := make([]openAIChatMessage, 0, len(messages)+1)
	if strings.TrimSpace(system) != "" {
		out = append(out, openAIChatMessage{Role: "system", Content: system})
	}
	for _, msg := range messages {
		if msg.Role == "assistant" {
			var calls []openAIChatCall
			var text []string
			for _, part := range msg.Content {
				switch part.Type {
				case contentPartTypeText:
					text = append(text, part.Text)
				case contentPartTypeToolUse:
					tc := openAIChatCall{ID: normalizeOpenAIToolID(part.ToolUseID), Type: "function"}
					tc.Function.Name = part.ToolName
					tc.Function.Arguments = string(defaultRawObject(part.ToolInput))
					calls = append(calls, tc)
				}
			}
			out = append(out, openAIChatMessage{Role: "assistant", Content: strings.Join(text, "\n"), ToolCalls: calls})
			continue
		}
		var text []string
		for _, part := range msg.Content {
			switch part.Type {
			case contentPartTypeText:
				text = append(text, part.Text)
			case contentPartTypeToolResult:
				out = append(out, openAIChatMessage{
					Role:       "tool",
					Content:    part.ToolOutput,
					ToolCallID: normalizeOpenAIToolID(part.ToolResultID),
				})
			}
		}
		if len(text) > 0 {
			out = append(out, openAIChatMessage{Role: "user", Content: strings.Join(text, "\n")})
		}
	}
	return out
}

func openAIChatTools(tools []agentToolDef) []openAIChatTool {
	out := make([]openAIChatTool, 0, len(tools))
	for _, t := range tools {
		ot := openAIChatTool{Type: "function"}
		ot.Function.Name = t.Name
		ot.Function.Description = t.Description
		ot.Function.Parameters = defaultSchemaObject(t.InputSchema)
		out = append(out, ot)
	}
	return out
}

type openAIResponsesInput struct {
	Type      string                   `json:"type,omitempty"`
	Role      string                   `json:"role,omitempty"`
	Content   []openAIResponsesContent `json:"content,omitempty"`
	CallID    string                   `json:"call_id,omitempty"`
	Name      string                   `json:"name,omitempty"`
	Arguments string                   `json:"arguments,omitempty"`
	Output    *string                  `json:"output,omitempty"`
}

type openAIResponsesContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type openAIResponsesTool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

func (c *agentProviderClient) chatOpenAIResponses(ctx context.Context, req agentProviderRequest) (agentProviderResponse, error) {
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = DefaultCompletionMaxTokens
	}
	payload := map[string]any{
		"model":             req.Model,
		"input":             openAIResponsesInputs(req.Messages),
		"instructions":      req.System,
		"tools":             openAIResponsesTools(req.Tools),
		"max_output_tokens": maxTokens,
		"store":             false,
	}
	if len(req.Tools) == 0 {
		delete(payload, "tools")
	}
	if strings.TrimSpace(req.System) == "" {
		delete(payload, "instructions")
	}
	if strings.TrimSpace(req.Reasoning) != "" && strings.ToLower(strings.TrimSpace(req.Reasoning)) != "none" {
		payload["reasoning"] = map[string]string{"effort": strings.TrimSpace(req.Reasoning), "summary": "auto"}
	}

	endpoint := openAIResponsesEndpoint(c.provider.OpenAIBaseURL)
	httpReq, err := newJSONRequest(ctx, http.MethodPost, endpoint, payload)
	if err != nil {
		return agentProviderResponse{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.provider.OpenAIAPIKey)

	respBody, err := doRequest(c.httpClient(), httpReq)
	if err != nil {
		return agentProviderResponse{}, err
	}

	var apiResp struct {
		Output []struct {
			Type      string `json:"type"`
			Role      string `json:"role"`
			CallID    string `json:"call_id"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
			Content   []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
		OutputText string `json:"output_text"`
		Usage      struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return agentProviderResponse{}, fmt.Errorf("parse openai responses response: %w", err)
	}

	message := agentMessage{Role: "assistant"}
	calls := make([]agentToolCall, 0)
	addedMessageText := false
	for _, item := range apiResp.Output {
		switch item.Type {
		case "message":
			for _, c := range item.Content {
				if (c.Type == "output_text" || c.Type == contentPartTypeText) && strings.TrimSpace(c.Text) != "" {
					message.Content = append(message.Content, agentContentPart{Type: contentPartTypeText, Text: c.Text})
					addedMessageText = true
				}
			}
		case "function_call":
			input := json.RawMessage(item.Arguments)
			if len(input) == 0 || !json.Valid(input) {
				input = json.RawMessage(`{}`)
			}
			callID := item.CallID
			if callID == "" {
				callID = item.Name
			}
			message.Content = append(message.Content, agentContentPart{
				Type:      contentPartTypeToolUse,
				ToolUseID: callID,
				ToolName:  item.Name,
				ToolInput: input,
			})
			calls = append(calls, agentToolCall{ID: callID, Name: item.Name, Input: input})
		}
	}
	if !addedMessageText && strings.TrimSpace(apiResp.OutputText) != "" {
		message.Content = append(message.Content, agentContentPart{Type: contentPartTypeText, Text: apiResp.OutputText})
	}

	return agentProviderResponse{
		Message: message,
		Calls:   calls,
		Usage: dag.TokenUsage{
			InputTokens:  apiResp.Usage.InputTokens,
			OutputTokens: apiResp.Usage.OutputTokens,
		},
		Raw: string(respBody),
	}, nil
}

func openAIResponsesEndpoint(baseURL string) string {
	baseURL = strings.TrimSpace(baseURL)
	switch {
	case baseURL == "", baseURL == defaultOpenAIBaseURL:
		return defaultOpenAIResponsesBaseURL
	case strings.HasSuffix(baseURL, "/chat/completions"):
		return strings.TrimSuffix(baseURL, "/chat/completions") + "/responses"
	default:
		return baseURL
	}
}

func openAIResponsesInputs(messages []agentMessage) []openAIResponsesInput {
	out := make([]openAIResponsesInput, 0, len(messages))
	for _, msg := range messages {
		switch msg.Role {
		case "assistant":
			var text []string
			for _, part := range msg.Content {
				switch part.Type {
				case contentPartTypeText:
					text = append(text, part.Text)
				case contentPartTypeToolUse:
					if len(text) > 0 {
						out = append(out, openAIResponsesInput{
							Type:    "message",
							Role:    "assistant",
							Content: []openAIResponsesContent{{Type: "output_text", Text: strings.Join(text, "\n")}},
						})
						text = nil
					}
					out = append(out, openAIResponsesInput{
						Type:      "function_call",
						CallID:    normalizeOpenAIToolID(part.ToolUseID),
						Name:      part.ToolName,
						Arguments: string(defaultRawObject(part.ToolInput)),
					})
				}
			}
			if len(text) > 0 {
				out = append(out, openAIResponsesInput{
					Type:    "message",
					Role:    "assistant",
					Content: []openAIResponsesContent{{Type: "output_text", Text: strings.Join(text, "\n")}},
				})
			}
		default:
			var content []openAIResponsesContent
			for _, part := range msg.Content {
				switch part.Type {
				case contentPartTypeText:
					content = append(content, openAIResponsesContent{Type: "input_text", Text: part.Text})
				case contentPartTypeToolResult:
					output := part.ToolOutput
					out = append(out, openAIResponsesInput{
						Type:   "function_call_output",
						CallID: normalizeOpenAIToolID(part.ToolResultID),
						Output: &output,
					})
				}
			}
			if len(content) > 0 {
				out = append(out, openAIResponsesInput{Type: "message", Role: "user", Content: content})
			}
		}
	}
	return out
}

func openAIResponsesTools(tools []agentToolDef) []openAIResponsesTool {
	out := make([]openAIResponsesTool, 0, len(tools))
	for _, t := range tools {
		out = append(out, openAIResponsesTool{
			Type:        "function",
			Name:        t.Name,
			Description: t.Description,
			Parameters:  defaultSchemaObject(t.InputSchema),
		})
	}
	return out
}

func agentOpenAIUsesResponses(model string) bool {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if strings.Contains(normalized, "codex") {
		return true
	}
	return agentOpenAIResponsesAPIModels[normalized]
}

func normalizeOpenAIToolID(id string) string {
	id = strings.TrimSpace(id)
	if strings.HasPrefix(id, "toolu_") {
		return "call_" + strings.TrimPrefix(id, "toolu_")
	}
	return id
}

type googleAgentFunctionDecl struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type googleAgentToolDecl struct {
	FunctionDeclarations []googleAgentFunctionDecl `json:"functionDeclarations"`
}

type googleAgentContent struct {
	Role  string            `json:"role,omitempty"`
	Parts []googleAgentPart `json:"parts"`
}

type googleAgentPart struct {
	Text             string                       `json:"text,omitempty"`
	FunctionCall     *googleAgentFunctionCall     `json:"functionCall,omitempty"`
	FunctionResponse *googleAgentFunctionResponse `json:"functionResponse,omitempty"`
	ThoughtSignature string                       `json:"thoughtSignature,omitempty"`
}

type googleAgentFunctionCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}

type googleAgentFunctionResponse struct {
	Name     string `json:"name"`
	Response any    `json:"response"`
}

func (c *agentProviderClient) chatGoogle(ctx context.Context, req agentProviderRequest) (agentProviderResponse, error) {
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = DefaultCompletionMaxTokens
	}

	payload := map[string]any{
		"contents": googleMessages(req.Messages),
		"generationConfig": map[string]any{
			"maxOutputTokens": maxTokens,
		},
	}
	if strings.TrimSpace(req.System) != "" {
		payload["systemInstruction"] = map[string]any{
			"parts": []map[string]string{{"text": req.System}},
		}
	}
	if len(req.Tools) > 0 {
		decls := make([]googleAgentFunctionDecl, 0, len(req.Tools))
		for _, t := range req.Tools {
			decls = append(decls, googleAgentFunctionDecl{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  defaultSchemaObject(t.InputSchema),
			})
		}
		payload["tools"] = []googleAgentToolDecl{{FunctionDeclarations: decls}}
	}

	endpoint, err := c.provider.endpoint(LLMProviderGoogle, req.Model)
	if err != nil {
		return agentProviderResponse{}, err
	}
	httpReq, err := newJSONRequest(ctx, http.MethodPost, endpoint, payload)
	if err != nil {
		return agentProviderResponse{}, err
	}

	respBody, err := doRequest(c.httpClient(), httpReq)
	if err != nil {
		return agentProviderResponse{}, err
	}

	var apiResp struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text             string `json:"text"`
					ThoughtSignature string `json:"thoughtSignature"`
					FunctionCall     *struct {
						Name string          `json:"name"`
						Args json.RawMessage `json:"args"`
					} `json:"functionCall"`
				} `json:"parts"`
			} `json:"content"`
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount     int `json:"promptTokenCount"`
			CandidatesTokenCount int `json:"candidatesTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return agentProviderResponse{}, fmt.Errorf("parse google response: %w", err)
	}
	if len(apiResp.Candidates) == 0 {
		return agentProviderResponse{}, fmt.Errorf("google response contained no candidates")
	}

	message := agentMessage{Role: "assistant"}
	calls := make([]agentToolCall, 0)
	for idx, part := range apiResp.Candidates[0].Content.Parts {
		if strings.TrimSpace(part.Text) != "" {
			message.Content = append(message.Content, agentContentPart{Type: contentPartTypeText, Text: part.Text})
		}
		if part.FunctionCall != nil {
			input := part.FunctionCall.Args
			if len(input) == 0 || !json.Valid(input) {
				input = json.RawMessage(`{}`)
			}
			callID := fmt.Sprintf("gemini_call_%d", idx+1)
			message.Content = append(message.Content, agentContentPart{
				Type:         contentPartTypeToolUse,
				ToolUseID:    callID,
				ToolName:     part.FunctionCall.Name,
				ToolInput:    input,
				ProviderMeta: part.ThoughtSignature,
			})
			calls = append(calls, agentToolCall{ID: callID, Name: part.FunctionCall.Name, Input: input})
		}
	}

	stopReason := apiResp.Candidates[0].FinishReason
	return agentProviderResponse{
		Message:    message,
		Calls:      calls,
		StopReason: stopReason,
		Usage: dag.TokenUsage{
			InputTokens:  apiResp.UsageMetadata.PromptTokenCount,
			OutputTokens: apiResp.UsageMetadata.CandidatesTokenCount,
		},
		Raw: string(respBody),
	}, nil
}

func googleMessages(messages []agentMessage) []googleAgentContent {
	callNames := map[string]string{}
	for _, msg := range messages {
		for _, part := range msg.Content {
			if part.Type == contentPartTypeToolUse && part.ToolUseID != "" {
				callNames[part.ToolUseID] = part.ToolName
			}
		}
	}

	out := make([]googleAgentContent, 0, len(messages))
	for _, msg := range messages {
		content := googleAgentContent{Role: "user"}
		if msg.Role == "assistant" {
			content.Role = "model"
		}
		for _, part := range msg.Content {
			switch part.Type {
			case contentPartTypeText:
				if strings.TrimSpace(part.Text) != "" {
					content.Parts = append(content.Parts, googleAgentPart{Text: part.Text})
				}
			case contentPartTypeToolUse:
				content.Parts = append(content.Parts, googleAgentPart{
					ThoughtSignature: part.ProviderMeta,
					FunctionCall: &googleAgentFunctionCall{
						Name: part.ToolName,
						Args: defaultRawObject(part.ToolInput),
					},
				})
			case contentPartTypeToolResult:
				name := part.ToolName
				if name == "" {
					name = callNames[part.ToolResultID]
				}
				content.Parts = append(content.Parts, googleAgentPart{
					FunctionResponse: &googleAgentFunctionResponse{
						Name:     name,
						Response: googleFunctionResponse(part.ToolOutput),
					},
				})
			}
		}
		if len(content.Parts) > 0 {
			out = append(out, content)
		}
	}
	return out
}

func googleFunctionResponse(output string) any {
	var decoded any
	if err := json.Unmarshal([]byte(output), &decoded); err == nil {
		switch decoded.(type) {
		case map[string]any, []any:
			return decoded
		}
	}
	return map[string]any{"output": output}
}

func defaultRawObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || !json.Valid(raw) {
		return json.RawMessage(`{}`)
	}
	return raw
}

func defaultSchemaObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || !json.Valid(raw) {
		return json.RawMessage(`{"type":"object","properties":{}}`)
	}
	return raw
}
