package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

// LLMJudgeConfig is the node config for llm_judge steps.
type LLMJudgeConfig struct {
	Model          string `json:"model,omitempty"`           // e.g. "claude-sonnet-4-6"
	Prompt         string `json:"prompt"`                    // template with {{evidence}}, {{market.question}}
	RequireCitations bool `json:"require_citations,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

// LLMJudgeExecutor calls an LLM to evaluate evidence and determine an outcome.
type LLMJudgeExecutor struct {
	APIKey  string
	BaseURL string // Anthropic API base URL
}

func NewLLMJudgeExecutor(apiKey string) *LLMJudgeExecutor {
	return &LLMJudgeExecutor{
		APIKey:  apiKey,
		BaseURL: "https://api.anthropic.com/v1/messages",
	}
}

func (e *LLMJudgeExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[LLMJudgeConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("llm_judge config: %w", err)
	}

	prompt := execCtx.Interpolate(cfg.Prompt)
	model := cfg.Model
	if model == "" {
		model = "claude-sonnet-4-6"
	}

	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	if e.APIKey == "" {
		// Dry run — no API key configured
		return dag.ExecutorResult{Outputs: map[string]string{
			"status":  "failed",
			"error":   "no API key configured",
			"outcome": "inconclusive",
		}}, nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Call Anthropic Messages API
	reqBody := map[string]interface{}{
		"model":      model,
		"max_tokens": 1024,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
		"system": "You are a resolution judge for a prediction market. " +
			"Based on the evidence provided, determine the correct outcome. " +
			"Respond with ONLY a JSON object: {\"outcome_index\": N, \"confidence\": \"high\"|\"medium\"|\"low\", \"reasoning\": \"...\"}",
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(reqCtx, "POST", e.BaseURL, bytes.NewReader(reqJSON))
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", e.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed", "error": err.Error(), "outcome": "inconclusive",
		}}, nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed", "error": fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(body)), "outcome": "inconclusive",
		}}, nil
	}

	// Parse Anthropic response
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
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed", "error": "parse response: " + err.Error(), "outcome": "inconclusive",
		}}, nil
	}

	text := ""
	if len(apiResp.Content) > 0 {
		text = apiResp.Content[0].Text
	}

	// Try to parse structured outcome from LLM response
	var judgment struct {
		OutcomeIndex int    `json:"outcome_index"`
		Confidence   string `json:"confidence"`
		Reasoning    string `json:"reasoning"`
	}
	if err := json.Unmarshal([]byte(text), &judgment); err != nil {
		return dag.ExecutorResult{
			Outputs: map[string]string{
				"status":  "success",
				"outcome": "inconclusive",
				"raw":     text,
			},
			Usage: dag.TokenUsage{
				InputTokens:  apiResp.Usage.InputTokens,
				OutputTokens: apiResp.Usage.OutputTokens,
			},
		}, nil
	}

	return dag.ExecutorResult{
		Outputs: map[string]string{
			"status":     "success",
			"outcome":    fmt.Sprintf("%d", judgment.OutcomeIndex),
			"confidence": judgment.Confidence,
			"reasoning":  judgment.Reasoning,
		},
		Usage: dag.TokenUsage{
			InputTokens:  apiResp.Usage.InputTokens,
			OutputTokens: apiResp.Usage.OutputTokens,
		},
	}, nil
}
