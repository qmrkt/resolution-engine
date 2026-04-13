package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

const (
	LLMProviderAnthropic = "anthropic"
	LLMProviderOpenAI    = "openai"
	LLMProviderGoogle    = "google"

	defaultAnthropicModel   = "claude-sonnet-4-6"
	defaultOpenAIModel      = "gpt-5.4"
	defaultGoogleModel      = "gemini-3.1-pro-preview"
	defaultAnthropicBaseURL = "https://api.anthropic.com/v1/messages"
	defaultOpenAIBaseURL    = "https://api.openai.com/v1/chat/completions"
	defaultGoogleBaseURL    = "https://generativelanguage.googleapis.com/v1beta/models"
)

// LLMJudgeConfig is the node config for llm_judge steps.
type LLMJudgeConfig struct {
	Provider         string `json:"provider,omitempty"`          // anthropic | openai | google
	Model            string `json:"model,omitempty"`             // e.g. "claude-sonnet-4-6"
	Prompt           string `json:"prompt"`                      // template with {{evidence}}, {{market.question}}
	RequireCitations bool   `json:"require_citations,omitempty"` // require cited support in the judgment
	TimeoutSeconds   int    `json:"timeout_seconds,omitempty"`
	WebSearch        bool   `json:"web_search,omitempty"`        // enable Anthropic server-side web search
}

// LLMJudgeExecutorConfig configures API access for supported model providers.
type LLMJudgeExecutorConfig struct {
	AnthropicAPIKey  string
	AnthropicBaseURL string
	OpenAIAPIKey     string
	OpenAIBaseURL    string
	GoogleAPIKey     string
	GoogleBaseURL    string
	HTTPClient       *http.Client
}

// LLMJudgeExecutor calls an LLM to evaluate evidence and determine an outcome.
type LLMJudgeExecutor struct {
	AnthropicAPIKey  string
	AnthropicBaseURL string
	OpenAIAPIKey     string
	OpenAIBaseURL    string
	GoogleAPIKey     string
	GoogleBaseURL    string
	HTTPClient       *http.Client
}

type llmJudgment struct {
	OutcomeIndex int             `json:"outcome_index"`
	Confidence   json.RawMessage `json:"confidence"`
	Reasoning    string          `json:"reasoning"`
	Citations    []string        `json:"citations,omitempty"`
}

func (j llmJudgment) ConfidenceString() string {
	s := strings.Trim(string(j.Confidence), "\"")
	if s == "" || s == "null" {
		return "medium"
	}
	return s
}

func NewLLMJudgeExecutor(apiKey string) *LLMJudgeExecutor {
	return NewLLMJudgeExecutorWithConfig(LLMJudgeExecutorConfig{
		AnthropicAPIKey: apiKey,
	})
}

func NewLLMJudgeExecutorWithConfig(cfg LLMJudgeExecutorConfig) *LLMJudgeExecutor {
	return &LLMJudgeExecutor{
		AnthropicAPIKey:  cfg.AnthropicAPIKey,
		AnthropicBaseURL: defaultString(cfg.AnthropicBaseURL, defaultAnthropicBaseURL),
		OpenAIAPIKey:     cfg.OpenAIAPIKey,
		OpenAIBaseURL:    defaultString(cfg.OpenAIBaseURL, defaultOpenAIBaseURL),
		GoogleAPIKey:     cfg.GoogleAPIKey,
		GoogleBaseURL:    defaultString(cfg.GoogleBaseURL, defaultGoogleBaseURL),
		HTTPClient:       cfg.HTTPClient,
	}
}

func (e *LLMJudgeExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[LLMJudgeConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("llm_judge config: %w", err)
	}

	prompt := execCtx.Interpolate(cfg.Prompt)
	provider, model, err := normalizeLLMSelection(cfg.Provider, cfg.Model)
	if err != nil {
		return failureResult(err.Error()), nil
	}

	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	apiKey, endpoint, err := e.resolveProvider(provider, model)
	if err != nil {
		return failureResult(err.Error()), nil
	}
	if apiKey == "" {
		return failureResult(fmt.Sprintf("no %s API key configured", provider)), nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		text  string
		usage dag.TokenUsage
	)
	switch provider {
	case LLMProviderAnthropic:
		text, usage, err = e.callAnthropic(reqCtx, endpoint, apiKey, model, prompt, cfg.RequireCitations, cfg.WebSearch)
	case LLMProviderOpenAI:
		text, usage, err = e.callOpenAI(reqCtx, endpoint, apiKey, model, prompt, cfg.RequireCitations)
	case LLMProviderGoogle:
		text, usage, err = e.callGoogle(reqCtx, endpoint, apiKey, model, prompt, cfg.RequireCitations)
	default:
		return failureResult(fmt.Sprintf("unsupported llm provider %q", provider)), nil
	}
	if err != nil {
		return failureResult(err.Error()), nil
	}

	return parseJudgmentResult(text, usage, cfg.RequireCitations), nil
}

func (e *LLMJudgeExecutor) httpClient() *http.Client {
	if e.HTTPClient != nil {
		return e.HTTPClient
	}
	return http.DefaultClient
}

func (e *LLMJudgeExecutor) resolveProvider(provider string, model string) (string, string, error) {
	switch provider {
	case LLMProviderAnthropic:
		return e.AnthropicAPIKey, e.AnthropicBaseURL, nil
	case LLMProviderOpenAI:
		return e.OpenAIAPIKey, e.OpenAIBaseURL, nil
	case LLMProviderGoogle:
		base := strings.TrimRight(e.GoogleBaseURL, "/")
		return e.GoogleAPIKey, fmt.Sprintf("%s/%s:generateContent?key=%s", base, url.PathEscape(model), url.QueryEscape(e.GoogleAPIKey)), nil
	default:
		return "", "", fmt.Errorf("unsupported llm provider %q", provider)
	}
}

func (e *LLMJudgeExecutor) callAnthropic(
	ctx context.Context,
	endpoint string,
	apiKey string,
	model string,
	prompt string,
	requireCitations bool,
	webSearch bool,
) (string, dag.TokenUsage, error) {
	reqBody := map[string]any{
		"model":      model,
		"max_tokens": 4096,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"system": buildJudgeInstructions(requireCitations),
	}

	if webSearch {
		reqBody["tools"] = []map[string]any{
			{
				"type":     "web_search_20250305",
				"name":     "web_search",
				"max_uses": 5,
			},
		}
	}

	req, err := newJSONRequest(ctx, http.MethodPost, endpoint, reqBody)
	if err != nil {
		return "", dag.TokenUsage{}, err
	}
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	if webSearch {
		req.Header.Set("anthropic-beta", "web-search-2025-03-05")
	}

	body, err := doRequest(e.httpClient(), req)
	if err != nil {
		return "", dag.TokenUsage{}, err
	}

	var apiResp struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return "", dag.TokenUsage{}, fmt.Errorf("parse anthropic response: %w", err)
	}

	return firstAnthropicText(apiResp.Content), dag.TokenUsage{
		InputTokens:  apiResp.Usage.InputTokens,
		OutputTokens: apiResp.Usage.OutputTokens,
	}, nil
}

func (e *LLMJudgeExecutor) callOpenAI(
	ctx context.Context,
	endpoint string,
	apiKey string,
	model string,
	prompt string,
	requireCitations bool,
) (string, dag.TokenUsage, error) {
	reqBody := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": buildJudgeInstructions(requireCitations)},
			{"role": "user", "content": prompt},
		},
		"response_format": map[string]string{
			"type": "json_object",
		},
		"max_completion_tokens": 1024,
	}

	req, err := newJSONRequest(ctx, http.MethodPost, endpoint, reqBody)
	if err != nil {
		return "", dag.TokenUsage{}, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	body, err := doRequest(e.httpClient(), req)
	if err != nil {
		return "", dag.TokenUsage{}, err
	}

	var apiResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return "", dag.TokenUsage{}, fmt.Errorf("parse openai response: %w", err)
	}
	if len(apiResp.Choices) == 0 {
		return "", dag.TokenUsage{}, fmt.Errorf("openai response contained no choices")
	}

	return apiResp.Choices[0].Message.Content, dag.TokenUsage{
		InputTokens:  apiResp.Usage.PromptTokens,
		OutputTokens: apiResp.Usage.CompletionTokens,
	}, nil
}

func (e *LLMJudgeExecutor) callGoogle(
	ctx context.Context,
	endpoint string,
	_ string,
	model string,
	prompt string,
	requireCitations bool,
) (string, dag.TokenUsage, error) {
	reqBody := map[string]any{
		"systemInstruction": map[string]any{
			"parts": []map[string]string{
				{"text": buildJudgeInstructions(requireCitations)},
			},
		},
		"contents": []map[string]any{
			{
				"role": "user",
				"parts": []map[string]string{
					{"text": prompt},
				},
			},
		},
		"generationConfig": map[string]any{
			"responseMimeType": "application/json",
			"responseSchema":   buildGoogleResponseSchema(requireCitations),
			"maxOutputTokens":  1024,
		},
	}

	req, err := newJSONRequest(ctx, http.MethodPost, endpoint, reqBody)
	if err != nil {
		return "", dag.TokenUsage{}, err
	}

	body, err := doRequest(e.httpClient(), req)
	if err != nil {
		return "", dag.TokenUsage{}, err
	}

	var apiResp struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount     int `json:"promptTokenCount"`
			CandidatesTokenCount int `json:"candidatesTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return "", dag.TokenUsage{}, fmt.Errorf("parse google response: %w", err)
	}
	if len(apiResp.Candidates) == 0 {
		return "", dag.TokenUsage{}, fmt.Errorf("google response contained no candidates")
	}

	return firstGoogleText(apiResp.Candidates[0].Content.Parts), dag.TokenUsage{
		InputTokens:  apiResp.UsageMetadata.PromptTokenCount,
		OutputTokens: apiResp.UsageMetadata.CandidatesTokenCount,
	}, nil
}

func newJSONRequest(ctx context.Context, method string, endpoint string, payload map[string]any) (*http.Request, error) {
	reqJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(reqJSON))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func doRequest(client *http.Client, req *http.Request) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func parseJudgmentResult(raw string, usage dag.TokenUsage, requireCitations bool) dag.ExecutorResult {
	text := extractJSON(raw)
	var judgment llmJudgment
	if err := json.Unmarshal([]byte(text), &judgment); err != nil {
		return dag.ExecutorResult{
			Outputs: map[string]string{
				"status":  "success",
				"outcome": "inconclusive",
				"raw":     raw,
			},
			Usage: usage,
		}
	}

	outputs := map[string]string{
		"status":     "success",
		"outcome":    fmt.Sprintf("%d", judgment.OutcomeIndex),
		"confidence": judgment.ConfidenceString(),
		"reasoning":  judgment.Reasoning,
	}

	if len(judgment.Citations) > 0 {
		citationsJSON, _ := json.Marshal(judgment.Citations)
		outputs["citations_json"] = string(citationsJSON)
		outputs["citations_count"] = fmt.Sprintf("%d", len(judgment.Citations))
	}

	if requireCitations && len(judgment.Citations) == 0 {
		outputs["status"] = "failed"
		outputs["outcome"] = "inconclusive"
		outputs["error"] = "citations required but missing"
	}

	return dag.ExecutorResult{Outputs: outputs, Usage: usage}
}

func failureResult(message string) dag.ExecutorResult {
	return dag.ExecutorResult{Outputs: map[string]string{
		"status":  "failed",
		"error":   message,
		"outcome": "inconclusive",
	}}
}

func normalizeLLMSelection(provider string, model string) (string, string, error) {
	normalizedProvider := strings.TrimSpace(strings.ToLower(provider))
	normalizedModel := strings.TrimSpace(model)

	if normalizedProvider == "" {
		normalizedProvider = inferProviderFromModel(normalizedModel)
	}
	if normalizedProvider == "" {
		normalizedProvider = LLMProviderAnthropic
	}
	if !isSupportedProvider(normalizedProvider) {
		return "", "", fmt.Errorf("unsupported llm provider %q", provider)
	}

	if normalizedModel == "" {
		normalizedModel = defaultModelForProvider(normalizedProvider)
	}

	inferred := inferProviderFromModel(normalizedModel)
	if inferred == "" {
		return "", "", fmt.Errorf("unsupported llm model %q", normalizedModel)
	}
	if inferred != normalizedProvider {
		return "", "", fmt.Errorf("model %q is not supported by provider %q", normalizedModel, normalizedProvider)
	}

	return normalizedProvider, normalizedModel, nil
}

func inferProviderFromModel(model string) string {
	normalized := strings.TrimSpace(strings.ToLower(model))
	switch {
	case strings.HasPrefix(normalized, "claude-"):
		return LLMProviderAnthropic
	case strings.HasPrefix(normalized, "gpt-"), strings.HasPrefix(normalized, "o1-"), strings.HasPrefix(normalized, "o3-"), strings.HasPrefix(normalized, "o4-"):
		return LLMProviderOpenAI
	case strings.HasPrefix(normalized, "gemini-"):
		return LLMProviderGoogle
	default:
		return ""
	}
}

func isSupportedProvider(provider string) bool {
	switch provider {
	case LLMProviderAnthropic, LLMProviderOpenAI, LLMProviderGoogle:
		return true
	default:
		return false
	}
}

func defaultModelForProvider(provider string) string {
	switch provider {
	case LLMProviderOpenAI:
		return defaultOpenAIModel
	case LLMProviderGoogle:
		return defaultGoogleModel
	default:
		return defaultAnthropicModel
	}
}

func buildJudgeInstructions(requireCitations bool) string {
	base := "You are a resolution judge for a prediction market. " +
		"Determine the correct outcome from the provided evidence. " +
		"Respond with ONLY a JSON object containing outcome_index (integer), confidence (high|medium|low), and reasoning (string)."
	if requireCitations {
		base += " Include citations as a JSON array of supporting quotes, URLs, or evidence references."
	}
	return base
}

func buildGoogleResponseSchema(requireCitations bool) map[string]any {
	properties := map[string]any{
		"outcome_index": map[string]any{
			"type": "INTEGER",
		},
		"confidence": map[string]any{
			"type": "STRING",
			"enum": []string{"high", "medium", "low"},
		},
		"reasoning": map[string]any{
			"type": "STRING",
		},
	}
	required := []string{"outcome_index", "confidence", "reasoning"}
	if requireCitations {
		properties["citations"] = map[string]any{
			"type": "ARRAY",
			"items": map[string]any{
				"type": "STRING",
			},
		}
		required = append(required, "citations")
	}

	return map[string]any{
		"type":       "OBJECT",
		"properties": properties,
		"required":   required,
	}
}

// extractJSON finds and returns the last JSON object in the text.
// Handles code fences, prose mixed with JSON, and multi-block responses.
func extractJSON(raw string) string {
	// First try: the whole thing after trimming code fences
	trimmed := trimCodeFence(raw)
	if json.Valid([]byte(trimmed)) {
		return trimmed
	}
	// Find the last { ... } block that is valid JSON
	for i := len(trimmed) - 1; i >= 0; i-- {
		if trimmed[i] == '}' {
			for j := i; j >= 0; j-- {
				if trimmed[j] == '{' {
					candidate := trimmed[j : i+1]
					if json.Valid([]byte(candidate)) {
						return candidate
					}
				}
			}
		}
	}
	return trimmed
}

func trimCodeFence(raw string) string {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "```json")
	trimmed = strings.TrimPrefix(trimmed, "```")
	trimmed = strings.TrimSuffix(trimmed, "```")
	return strings.TrimSpace(trimmed)
}

func firstAnthropicText(content []struct {
	Text string `json:"text"`
}) string {
	// With web search, the final JSON is in the last text block.
	// Concatenate all text blocks so the JSON parser can find it.
	var builder strings.Builder
	for _, item := range content {
		if text := strings.TrimSpace(item.Text); text != "" {
			if builder.Len() > 0 {
				builder.WriteByte('\n')
			}
			builder.WriteString(text)
		}
	}
	return builder.String()
}

func firstGoogleText(parts []struct {
	Text string `json:"text"`
}) string {
	var builder strings.Builder
	for _, part := range parts {
		if text := strings.TrimSpace(part.Text); text != "" {
			if builder.Len() > 0 {
				builder.WriteByte('\n')
			}
			builder.WriteString(text)
		}
	}
	return builder.String()
}

func defaultString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
