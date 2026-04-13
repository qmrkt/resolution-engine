package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

func TestAPIFetchSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"price": "70123.45",
			},
		})
	}))
	defer server.Close()

	exec := &APIFetchExecutor{Client: http.DefaultClient, AllowLocal: true}
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]interface{}{
			"url":       server.URL,
			"json_path": "data.price",
			"outcome_mapping": map[string]string{
				"70123.45": "0",
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %s: %s", result.Outputs["status"], result.Outputs["error"])
	}
	if result.Outputs["extracted"] != "70123.45" {
		t.Fatalf("expected 70123.45, got %q", result.Outputs["extracted"])
	}
	if result.Outputs["outcome"] != "0" {
		t.Fatalf("expected outcome=0, got %q", result.Outputs["outcome"])
	}
}

func TestAPIFetchHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("server error"))
	}))
	defer server.Close()

	exec := &APIFetchExecutor{Client: http.DefaultClient, AllowLocal: true}
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]interface{}{
			"url":       server.URL,
			"json_path": "data",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed, got %s", result.Outputs["status"])
	}
}

func TestAPIFetchWithInterpolation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("q") == "" {
			t.Error("expected query parameter")
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"result": "yes"})
	}))
	defer server.Close()

	exec := &APIFetchExecutor{Client: http.DefaultClient, AllowLocal: true}
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]interface{}{
			"url":       server.URL + "?q={{market_question}}",
			"json_path": "result",
		},
	}

	ctx := dag.NewContext(map[string]string{"market_question": "will-btc-100k"})
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["extracted"] != "yes" {
		t.Fatalf("expected yes, got %q", result.Outputs["extracted"])
	}
}

func TestJSONPathExtraction(t *testing.T) {
	tests := []struct {
		json     string
		path     string
		expected string
		wantErr  bool
	}{
		{`{"a": "hello"}`, "a", "hello", false},
		{`{"a": {"b": 42}}`, "a.b", "42", false},
		{`{"x": {"y": {"z": "deep"}}}`, "x.y.z", "deep", false},
		{`{"a": "hello"}`, "missing", "", true},
		{`{"a": "hello"}`, "", `{"a": "hello"}`, false},
	}

	for _, tt := range tests {
		result, err := jsonPathExtract([]byte(tt.json), tt.path)
		if tt.wantErr {
			if err == nil {
				t.Errorf("path=%q: expected error", tt.path)
			}
			continue
		}
		if err != nil {
			t.Errorf("path=%q: %v", tt.path, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("path=%q: expected %q, got %q", tt.path, tt.expected, result)
		}
	}
}

func TestSubmitResultSuccess(t *testing.T) {
	exec := NewSubmitResultExecutor()
	ctx := dag.NewContext(nil)
	ctx.Set("judge.outcome", "2")

	node := dag.NodeDef{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}}
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["outcome"] != "2" {
		t.Fatalf("expected outcome=2, got %q", result.Outputs["outcome"])
	}
	if result.Outputs["evidence_hash"] == "" {
		t.Fatal("expected non-empty evidence_hash")
	}
	if result.Outputs["submitted"] != "true" {
		t.Fatal("expected submitted=true")
	}
}

func TestSubmitResultNoOutcome(t *testing.T) {
	exec := NewSubmitResultExecutor()
	ctx := dag.NewContext(nil)

	node := dag.NodeDef{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}}
	_, err := exec.Execute(context.Background(), node, ctx)
	if err == nil {
		t.Fatal("expected error when no outcome determined")
	}
}

func TestSubmitResultWithExplicitKey(t *testing.T) {
	exec := NewSubmitResultExecutor()
	ctx := dag.NewContext(nil)
	ctx.Set("my_custom.result", "1")

	node := dag.NodeDef{
		ID:   "submit",
		Type: "submit_result",
		Config: map[string]interface{}{
			"outcome_key": "my_custom.result",
		},
	}
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["outcome"] != "1" {
		t.Fatalf("expected outcome=1, got %q", result.Outputs["outcome"])
	}
}

func TestCancelMarketExecutor(t *testing.T) {
	exec := NewCancelMarketExecutor()
	ctx := dag.NewContext(nil)

	node := dag.NodeDef{
		ID:   "cancel",
		Type: "cancel_market",
		Config: map[string]interface{}{
			"reason": "evidence unavailable",
		},
	}
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["action"] != "cancel" {
		t.Fatalf("expected action=cancel, got %q", result.Outputs["action"])
	}
	if result.Outputs["reason"] != "evidence unavailable" {
		t.Fatalf("expected reason, got %q", result.Outputs["reason"])
	}
}

func TestOutcomeTerminalityExecutorUniqueWinner(t *testing.T) {
	exec := NewOutcomeTerminalityExecutor()
	ctx := dag.NewContext(nil)
	ctx.Set("fetch.status", "success")
	ctx.Set("fetch.extracted", "2026-04-01")

	node := dag.NodeDef{
		ID:   "term",
		Type: "outcome_terminality",
		Config: map[string]interface{}{
			"outcomes": []map[string]interface{}{
				{
					"index":       0,
					"winner_when": "fetch.status == 'success' && fetch.extracted != ''",
				},
				{
					"index":           1,
					"eliminated_when": "fetch.status == 'success' && fetch.extracted != ''",
				},
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["outcome_0_state"] != "winner" {
		t.Fatalf("expected outcome 0 winner, got %q", result.Outputs["outcome_0_state"])
	}
	if result.Outputs["outcome_1_state"] != "eliminated" {
		t.Fatalf("expected outcome 1 eliminated, got %q", result.Outputs["outcome_1_state"])
	}
	if result.Outputs["unique_winner"] != "true" || result.Outputs["winning_outcome"] != "0" {
		t.Fatalf("expected unique winner 0, got unique=%q winning=%q", result.Outputs["unique_winner"], result.Outputs["winning_outcome"])
	}
}

func TestOutcomeTerminalityExecutorConflictingStateFails(t *testing.T) {
	exec := NewOutcomeTerminalityExecutor()
	ctx := dag.NewContext(map[string]string{"fetch.status": "success"})

	node := dag.NodeDef{
		ID:   "term",
		Type: "outcome_terminality",
		Config: map[string]interface{}{
			"outcomes": []map[string]interface{}{
				{
					"index":           0,
					"winner_when":     "fetch.status == 'success'",
					"eliminated_when": "fetch.status == 'success'",
				},
			},
		},
	}

	if _, err := exec.Execute(context.Background(), node, ctx); err == nil {
		t.Fatal("expected conflicting winner/eliminated rule to fail")
	}
}

func TestOutcomeTerminalityExecutorMultipleWinnersFail(t *testing.T) {
	exec := NewOutcomeTerminalityExecutor()
	ctx := dag.NewContext(map[string]string{"flag": "yes"})

	node := dag.NodeDef{
		ID:   "term",
		Type: "outcome_terminality",
		Config: map[string]interface{}{
			"outcomes": []map[string]interface{}{
				{"index": 0, "winner_when": "flag == 'yes'"},
				{"index": 1, "winner_when": "flag == 'yes'"},
			},
		},
	}

	if _, err := exec.Execute(context.Background(), node, ctx); err == nil {
		t.Fatal("expected multiple winners to fail")
	}
}

func TestDeferResolutionExecutor(t *testing.T) {
	exec := NewDeferResolutionExecutor()
	result, err := exec.Execute(context.Background(), dag.NodeDef{
		ID:   "defer",
		Type: "defer_resolution",
		Config: map[string]interface{}{
			"reason": "not terminal yet",
		},
	}, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["deferred"] != "true" || result.Outputs["action"] != "defer" {
		t.Fatalf("expected defer action, got %#v", result.Outputs)
	}
	if result.Outputs["reason"] != "not terminal yet" {
		t.Fatalf("expected defer reason, got %q", result.Outputs["reason"])
	}
}

func TestWaitExecutor(t *testing.T) {
	exec := NewWaitExecutor()
	node := dag.NodeDef{
		ID:   "wait",
		Type: "wait",
		Config: map[string]interface{}{
			"duration_seconds": 1,
		},
	}

	start := time.Now()
	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %s", result.Outputs["status"])
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("expected ~1s wait, got %v", elapsed)
	}
}

func TestWaitExecutorCancellation(t *testing.T) {
	exec := NewWaitExecutor()
	node := dag.NodeDef{
		ID:   "wait",
		Type: "wait",
		Config: map[string]interface{}{
			"duration_seconds": 60,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := exec.Execute(ctx, node, dag.NewContext(nil))
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestWaitExecutorDeferMode(t *testing.T) {
	exec := NewWaitExecutor()
	node := dag.NodeDef{
		ID:   "wait",
		Type: "wait",
		Config: map[string]interface{}{
			"mode":             "defer",
			"start_from":       "resolution_pending_since",
			"duration_seconds": 3600,
		},
	}

	ctx := dag.NewContext(map[string]string{
		"market.now_ts":                   "1712347200",
		"market.resolution_pending_since": "1712343600",
	})

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %#v", result.Outputs)
	}
	if result.Outputs["ready_at"] != "1712347200" {
		t.Fatalf("ready_at = %q, want 1712347200", result.Outputs["ready_at"])
	}
}

func TestWaitExecutorDeferModeWaiting(t *testing.T) {
	exec := NewWaitExecutor()
	node := dag.NodeDef{
		ID:   "wait",
		Type: "wait",
		Config: map[string]interface{}{
			"mode":             "defer",
			"start_from":       "deadline",
			"duration_seconds": 43200,
		},
	}

	ctx := dag.NewContext(map[string]string{
		"market.now_ts":   "1712347200",
		"market.deadline": "1712340000",
	})

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "waiting" {
		t.Fatalf("expected waiting, got %#v", result.Outputs)
	}
	if result.Outputs["waiting"] != "true" {
		t.Fatalf("expected waiting=true, got %#v", result.Outputs)
	}
	if result.Outputs["remaining_seconds"] != "36000" {
		t.Fatalf("remaining_seconds = %q, want 36000", result.Outputs["remaining_seconds"])
	}
}

func TestMarketEvidenceExecutor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/markets/44/evidence" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"appId":                  44,
			"question":               "Did it happen?",
			"outcomes":               []string{"Yes", "No"},
			"submissionCount":        2,
			"claimedOutcomeSummary":  map[string]int{"0": 1, "unspecified": 1},
			"entries":                []map[string]any{{"submitterAddress": "A"}, {"submitterAddress": "B"}},
			"resolutionPendingSince": 1712340000,
		})
	}))
	defer server.Close()

	exec := NewMarketEvidenceExecutor(server.URL)
	node := dag.NodeDef{
		ID:     "evidence",
		Type:   "market_evidence",
		Config: map[string]interface{}{},
	}

	ctx := dag.NewContext(map[string]string{"market_app_id": "44"})
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %#v", result.Outputs)
	}
	if result.Outputs["count"] != "2" {
		t.Fatalf("count = %q, want 2", result.Outputs["count"])
	}
	if !strings.Contains(result.Outputs["entries_json"], "\"submitterAddress\":\"A\"") {
		t.Fatalf("entries_json = %q", result.Outputs["entries_json"])
	}
	if result.Outputs["claimed_summary"] != "{\"0\":1,\"unspecified\":1}" {
		t.Fatalf("claimed_summary = %q", result.Outputs["claimed_summary"])
	}
}

func TestLLMJudgeNoAPIKey(t *testing.T) {
	exec := NewLLMJudgeExecutor("") // no API key
	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_judge",
		Config: map[string]interface{}{
			"prompt": "What is the outcome?",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	// Should return inconclusive, not crash
	if result.Outputs["outcome"] != "inconclusive" {
		t.Fatalf("expected inconclusive without API key, got %q", result.Outputs["outcome"])
	}
}

func TestLLMJudgeWithMockServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-key" {
			w.WriteHeader(401)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"content": []map[string]string{
				{"text": `{"outcome_index": 0, "confidence": "high", "reasoning": "BTC was above 70k"}`},
			},
			"usage": map[string]int{
				"input_tokens":  150,
				"output_tokens": 50,
			},
		})
	}))
	defer server.Close()

	exec := NewLLMJudgeExecutor("test-key")
	exec.AnthropicBaseURL = server.URL

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_judge",
		Config: map[string]interface{}{
			"prompt": "Evidence: {{search.evidence}}\nQuestion: {{market_question}}",
		},
	}

	ctx := dag.NewContext(map[string]string{"market_question": "Did BTC hit 70k?"})
	ctx.Set("search.evidence", "BTC closed at $70,123")

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["outcome"] != "0" {
		t.Fatalf("expected outcome=0, got %q", result.Outputs["outcome"])
	}
	if result.Outputs["confidence"] != "high" {
		t.Fatalf("expected high confidence, got %q", result.Outputs["confidence"])
	}
	if result.Usage.InputTokens != 150 {
		t.Fatalf("expected 150 input tokens, got %d", result.Usage.InputTokens)
	}
}

func TestLLMJudgeOpenAIProvider(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer openai-key" {
			w.WriteHeader(401)
			return
		}

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload["model"] != "gpt-5.4" {
			t.Fatalf("expected gpt-5.4 model, got %#v", payload["model"])
		}

		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]string{
						"content": `{"outcome_index":1,"confidence":"medium","reasoning":"Public filings support outcome 1.","citations":["https://example.com/report"]}`,
					},
				},
			},
			"usage": map[string]int{
				"prompt_tokens":     80,
				"completion_tokens": 24,
			},
		})
	}))
	defer server.Close()

	exec := NewLLMJudgeExecutorWithConfig(LLMJudgeExecutorConfig{
		OpenAIAPIKey:  "openai-key",
		OpenAIBaseURL: server.URL,
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_judge",
		Config: map[string]any{
			"provider":          "openai",
			"model":             "gpt-5.4",
			"prompt":            "Judge this evidence.",
			"require_citations": true,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["outcome"] != "1" {
		t.Fatalf("expected outcome=1, got %q", result.Outputs["outcome"])
	}
	if result.Outputs["citations_count"] != "1" {
		t.Fatalf("expected one citation, got %#v", result.Outputs["citations_count"])
	}
	if result.Usage.OutputTokens != 24 {
		t.Fatalf("expected 24 output tokens, got %d", result.Usage.OutputTokens)
	}
}

func TestLLMJudgeGoogleProvider(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "key=google-key") {
			w.WriteHeader(401)
			return
		}

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		generationConfig, ok := payload["generationConfig"].(map[string]any)
		if !ok || generationConfig["responseMimeType"] != "application/json" {
			t.Fatalf("expected structured JSON response config, got %#v", payload["generationConfig"])
		}

		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{
				{
					"content": map[string]any{
						"parts": []map[string]string{
							{"text": `{"outcome_index":0,"confidence":"high","reasoning":"Primary source confirms outcome 0.","citations":["newswire"]}`},
						},
					},
				},
			},
			"usageMetadata": map[string]int{
				"promptTokenCount":     90,
				"candidatesTokenCount": 32,
			},
		})
	}))
	defer server.Close()

	exec := NewLLMJudgeExecutorWithConfig(LLMJudgeExecutorConfig{
		GoogleAPIKey:  "google-key",
		GoogleBaseURL: server.URL,
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_judge",
		Config: map[string]any{
			"provider":          "google",
			"model":             "gemini-3.1-pro-preview",
			"prompt":            "Judge this evidence.",
			"require_citations": true,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["outcome"] != "0" {
		t.Fatalf("expected outcome=0, got %q", result.Outputs["outcome"])
	}
	if result.Outputs["citations_json"] != `["newswire"]` {
		t.Fatalf("unexpected citations_json: %q", result.Outputs["citations_json"])
	}
	if result.Usage.InputTokens != 90 {
		t.Fatalf("expected 90 input tokens, got %d", result.Usage.InputTokens)
	}
}

func TestLLMJudgeCitationsRequired(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]string{
						"content": `{"outcome_index":1,"confidence":"low","reasoning":"No citation included."}`,
					},
				},
			},
			"usage": map[string]int{
				"prompt_tokens":     12,
				"completion_tokens": 8,
			},
		})
	}))
	defer server.Close()

	exec := NewLLMJudgeExecutorWithConfig(LLMJudgeExecutorConfig{
		OpenAIAPIKey:  "openai-key",
		OpenAIBaseURL: server.URL,
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_judge",
		Config: map[string]any{
			"provider":          "openai",
			"model":             "gpt-5.4",
			"prompt":            "Judge this evidence.",
			"require_citations": true,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status when citations are required, got %q", result.Outputs["status"])
	}
	if result.Outputs["error"] != "citations required but missing" {
		t.Fatalf("unexpected error: %q", result.Outputs["error"])
	}
}

func TestHumanJudgeExecutorResponded(t *testing.T) {
	var (
		mu        sync.Mutex
		judgments = map[string]humanJudgmentRecord{}
	)
	const runID = "run-77"
	const judgmentID = "77:run-77:judge"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var body struct {
				JudgmentID        string   `json:"judgmentId"`
				RunID             string   `json:"runId"`
				NodeID            string   `json:"nodeId"`
				Prompt            string   `json:"prompt"`
				AllowedResponders []string `json:"allowedResponders"`
				TimeoutAt         int64    `json:"timeoutAt"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}

			judgment := humanJudgmentRecord{
				JudgmentID:        body.JudgmentID,
				AppID:             77,
				RunID:             body.RunID,
				NodeID:            body.NodeID,
				Status:            "pending",
				Prompt:            body.Prompt,
				AllowedResponders: body.AllowedResponders,
				TimeoutAt:         body.TimeoutAt,
			}

			mu.Lock()
			judgments[body.JudgmentID] = judgment
			mu.Unlock()

			json.NewEncoder(w).Encode(map[string]any{"judgment": judgment})
		case http.MethodGet:
			mu.Lock()
			list := make([]humanJudgmentRecord, 0, len(judgments))
			for _, judgment := range judgments {
				list = append(list, judgment)
			}
			mu.Unlock()

			json.NewEncoder(w).Encode(map[string]any{"judgments": list})
		default:
			http.Error(w, "method not allowed", 405)
		}
	}))
	defer server.Close()

	exec := NewHumanJudgeExecutor(server.URL)
	exec.PollInterval = 20 * time.Millisecond

	go func() {
		time.Sleep(80 * time.Millisecond)
		mu.Lock()
		judgment := judgments[judgmentID]
		judgment.Status = "responded"
		judgment.ResponseOutcome = 1
		judgment.ResponseReason = "Creator resolved it"
		judgment.ResponderAddress = "CREATORADDR"
		judgment.ResponderRole = "creator"
		judgment.ResponseHash = "hash123"
		judgment.ResponseSubmittedAt = time.Now().UnixMilli()
		judgments[judgmentID] = judgment
		mu.Unlock()
	}()

	result, err := exec.Execute(context.Background(), dag.NodeDef{
		ID:   "judge",
		Type: "human_judge",
		Config: map[string]interface{}{
			"prompt":             "Resolve this market.",
			"allowed_responders": []string{"creator"},
			"timeout_seconds":    2,
		},
	}, dag.NewContext(map[string]string{"market_app_id": "77", "resolution_run_id": runID}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "responded" {
		t.Fatalf("expected responded, got %q", result.Outputs["status"])
	}
	if result.Outputs["outcome"] != "1" {
		t.Fatalf("expected outcome=1, got %q", result.Outputs["outcome"])
	}
	if result.Outputs["responder_role"] != "creator" {
		t.Fatalf("expected responder_role=creator, got %q", result.Outputs["responder_role"])
	}
}

func TestHumanJudgeExecutorTimeout(t *testing.T) {
	var (
		mu        sync.Mutex
		judgments = map[string]humanJudgmentRecord{}
	)
	const runID = "run-78"
	const judgmentID = "78:run-78:judge"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var body struct {
				JudgmentID        string   `json:"judgmentId"`
				RunID             string   `json:"runId"`
				NodeID            string   `json:"nodeId"`
				Prompt            string   `json:"prompt"`
				AllowedResponders []string `json:"allowedResponders"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}

			judgment := humanJudgmentRecord{
				JudgmentID:        body.JudgmentID,
				AppID:             78,
				RunID:             body.RunID,
				NodeID:            body.NodeID,
				Status:            "pending",
				Prompt:            body.Prompt,
				AllowedResponders: body.AllowedResponders,
				TimeoutAt:         time.Now().Add(75 * time.Millisecond).UnixMilli(),
			}

			mu.Lock()
			judgments[body.JudgmentID] = judgment
			mu.Unlock()

			json.NewEncoder(w).Encode(map[string]any{"judgment": judgment})
		case http.MethodGet:
			mu.Lock()
			judgment := judgments[judgmentID]
			if judgment.Status == "pending" && judgment.TimeoutAt <= time.Now().UnixMilli() {
				judgment.Status = "timeout"
				judgments[judgmentID] = judgment
			}
			list := make([]humanJudgmentRecord, 0, len(judgments))
			for _, stored := range judgments {
				list = append(list, stored)
			}
			mu.Unlock()

			json.NewEncoder(w).Encode(map[string]any{"judgments": list})
		default:
			http.Error(w, "method not allowed", 405)
		}
	}))
	defer server.Close()

	exec := NewHumanJudgeExecutor(server.URL)
	exec.PollInterval = 20 * time.Millisecond

	result, err := exec.Execute(context.Background(), dag.NodeDef{
		ID:   "judge",
		Type: "human_judge",
		Config: map[string]interface{}{
			"prompt":             "Resolve this market.",
			"allowed_responders": []string{"creator"},
			"timeout_seconds":    1,
		},
	}, dag.NewContext(map[string]string{"market_app_id": "78", "resolution_run_id": runID}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "timeout" {
		t.Fatalf("expected timeout, got %q", result.Outputs["status"])
	}
}

// Integration test: full DAG with real executors
func TestFullResolutionDAGWithExecutors(t *testing.T) {
	// Mock API server
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"price": "70500",
		})
	}))
	defer apiServer.Close()

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("api_fetch", &APIFetchExecutor{Client: http.DefaultClient, AllowLocal: true})
	engine.RegisterExecutor("submit_result", NewSubmitResultExecutor())
	engine.RegisterExecutor("cancel_market", NewCancelMarketExecutor())

	bp := dag.Blueprint{
		ID:   "price-check",
		Name: "BTC Price Resolution",
		Nodes: []dag.NodeDef{
			{
				ID:   "fetch",
				Type: "api_fetch",
				Config: map[string]interface{}{
					"url":       apiServer.URL,
					"json_path": "price",
					"outcome_mapping": map[string]string{
						"70500": "0",
					},
				},
			},
			{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
			{ID: "fallback", Type: "cancel_market", Config: map[string]interface{}{
				"reason": "API unavailable",
			}},
		},
		Edges: []dag.EdgeDef{
			{From: "fetch", To: "submit", Condition: "fetch.status == 'success'"},
			{From: "fetch", To: "fallback", Condition: "fetch.status != 'success'"},
		},
		Budget: &dag.Budget{MaxTotalTimeSeconds: 30},
	}

	run, err := engine.Execute(context.Background(), bp, map[string]string{
		"market_app_id": "12345",
	})
	if err != nil {
		t.Fatal(err)
	}

	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if run.NodeStates["fetch"].Status != "completed" {
		t.Fatalf("fetch not completed: %s", run.NodeStates["fetch"].Status)
	}
	if run.NodeStates["submit"].Status != "completed" {
		t.Fatalf("submit not completed: %s", run.NodeStates["submit"].Status)
	}

	outcome := run.Context["submit.outcome"]
	if outcome != "0" {
		t.Fatalf("expected submit outcome=0, got %q", outcome)
	}

	evidenceHash := run.Context["submit.evidence_hash"]
	if evidenceHash == "" {
		t.Fatal("expected non-empty evidence hash")
	}

	// Fallback should NOT have run
	if run.NodeStates["fallback"].Status != "pending" {
		t.Fatalf("fallback should be pending, got %s", run.NodeStates["fallback"].Status)
	}

	fmt.Printf("Resolution complete: outcome=%s evidence=%s\n", outcome, evidenceHash[:16]+"...")
}

func TestHumanJudgeExecutorDesignatedAddress(t *testing.T) {
	var (
		mu                 sync.Mutex
		judgments          = map[string]humanJudgmentRecord{}
		capturedDesignated string
	)
	const runID = "run-90"
	const judgmentID = "90:run-90:judge"
	const designatedAddr = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRSTUV"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var body struct {
				JudgmentID        string   `json:"judgmentId"`
				RunID             string   `json:"runId"`
				NodeID            string   `json:"nodeId"`
				Prompt            string   `json:"prompt"`
				AllowedResponders []string `json:"allowedResponders"`
				DesignatedAddress string   `json:"designatedAddress"`
				TimeoutAt         int64    `json:"timeoutAt"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}

			mu.Lock()
			capturedDesignated = body.DesignatedAddress
			mu.Unlock()

			judgment := humanJudgmentRecord{
				JudgmentID:        body.JudgmentID,
				AppID:             90,
				RunID:             body.RunID,
				NodeID:            body.NodeID,
				Status:            "pending",
				Prompt:            body.Prompt,
				AllowedResponders: body.AllowedResponders,
				DesignatedAddress: body.DesignatedAddress,
				TimeoutAt:         body.TimeoutAt,
			}

			mu.Lock()
			judgments[body.JudgmentID] = judgment
			mu.Unlock()

			json.NewEncoder(w).Encode(map[string]any{"judgment": judgment})
		case http.MethodGet:
			mu.Lock()
			list := make([]humanJudgmentRecord, 0, len(judgments))
			for _, judgment := range judgments {
				list = append(list, judgment)
			}
			mu.Unlock()

			json.NewEncoder(w).Encode(map[string]any{"judgments": list})
		default:
			http.Error(w, "method not allowed", 405)
		}
	}))
	defer server.Close()

	exec := NewHumanJudgeExecutor(server.URL)
	exec.PollInterval = 20 * time.Millisecond

	go func() {
		time.Sleep(80 * time.Millisecond)
		mu.Lock()
		judgment := judgments[judgmentID]
		judgment.Status = "responded"
		judgment.ResponseOutcome = 0
		judgment.ResponseReason = "Designated judge resolved"
		judgment.ResponderAddress = designatedAddr
		judgment.ResponderRole = "designated"
		judgment.ResponseHash = "hash-designated"
		judgment.ResponseSubmittedAt = time.Now().UnixMilli()
		judgments[judgmentID] = judgment
		mu.Unlock()
	}()

	result, err := exec.Execute(context.Background(), dag.NodeDef{
		ID:   "judge",
		Type: "human_judge",
		Config: map[string]interface{}{
			"prompt":             "Resolve this market.",
			"allowed_responders": []string{"designated"},
			"designated_address": designatedAddr,
			"timeout_seconds":    2,
		},
	}, dag.NewContext(map[string]string{"market_app_id": "90", "resolution_run_id": runID}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "responded" {
		t.Fatalf("expected responded, got %q", result.Outputs["status"])
	}
	if result.Outputs["outcome"] != "0" {
		t.Fatalf("expected outcome=0, got %q", result.Outputs["outcome"])
	}
	if result.Outputs["responder_role"] != "designated" {
		t.Fatalf("expected responder_role=designated, got %q", result.Outputs["responder_role"])
	}
	if result.Outputs["responder_address"] != designatedAddr {
		t.Fatalf("expected responder_address=%s, got %q", designatedAddr, result.Outputs["responder_address"])
	}

	mu.Lock()
	defer mu.Unlock()
	if capturedDesignated != designatedAddr {
		t.Fatalf("expected designatedAddress in API payload to be %q, got %q", designatedAddr, capturedDesignated)
	}
}
