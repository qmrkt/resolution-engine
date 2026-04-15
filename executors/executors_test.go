package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestAPIFetchPostWithHeadersAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("X-API-Key") != "secret-123" {
			t.Fatalf("expected X-API-Key header, got %q", r.Header.Get("X-API-Key"))
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("expected application/json content type, got %q", r.Header.Get("Content-Type"))
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if string(body) != `{"question":"Will BTC hit 100k?"}` {
			t.Fatalf("unexpected body %q", string(body))
		}

		json.NewEncoder(w).Encode(map[string]any{"result": "ok"})
	}))
	defer server.Close()

	exec := &APIFetchExecutor{Client: http.DefaultClient, AllowLocal: true}
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]any{
			"url":       server.URL,
			"method":    "post",
			"body":      `{"question":"{{market_question}}"}`,
			"json_path": "result",
			"headers": map[string]string{
				"Content-Type": "application/json",
				"X-API-Key":    "{{api_key}}",
			},
		},
	}

	ctx := dag.NewContext(map[string]string{
		"market_question": "Will BTC hit 100k?",
		"api_key":         "secret-123",
	})
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %s", result.Outputs["status"])
	}
	if result.Outputs["extracted"] != "ok" {
		t.Fatalf("expected extracted=ok, got %q", result.Outputs["extracted"])
	}
	if result.Outputs["status_code"] != "200" {
		t.Fatalf("expected status_code=200, got %q", result.Outputs["status_code"])
	}
}

func TestAPIFetchBasicAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			t.Fatal("expected basic auth credentials")
		}
		if username != "resolver" || password != "s3cr3t" {
			t.Fatalf("unexpected credentials %q / %q", username, password)
		}
		json.NewEncoder(w).Encode(map[string]any{"result": "authorized"})
	}))
	defer server.Close()

	exec := &APIFetchExecutor{Client: http.DefaultClient, AllowLocal: true}
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]any{
			"url":       server.URL,
			"json_path": "result",
			"basic_auth": map[string]string{
				"username": "{{api_user}}",
				"password": "{{api_password}}",
			},
		},
	}

	ctx := dag.NewContext(map[string]string{
		"api_user":     "resolver",
		"api_password": "s3cr3t",
	})
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["extracted"] != "authorized" {
		t.Fatalf("expected authorized, got %q", result.Outputs["extracted"])
	}
}

func TestAPIFetchRawTextResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, "Yes")
	}))
	defer server.Close()

	exec := &APIFetchExecutor{Client: http.DefaultClient, AllowLocal: true}
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]any{
			"url": server.URL,
			"outcome_mapping": map[string]string{
				"Yes": "0",
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %s", result.Outputs["status"])
	}
	if result.Outputs["extracted"] != "Yes" {
		t.Fatalf("expected raw extracted value, got %q", result.Outputs["extracted"])
	}
	if result.Outputs["outcome"] != "0" {
		t.Fatalf("expected mapped outcome=0, got %q", result.Outputs["outcome"])
	}
	if !strings.Contains(result.Outputs["content_type"], "text/plain") {
		t.Fatalf("expected text/plain content type, got %q", result.Outputs["content_type"])
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

func TestLLMCallNoAPIKey(t *testing.T) {
	exec := NewLLMCallExecutor("") // no API key
	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
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

func TestLLMCallWithMockServer(t *testing.T) {
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

	exec := NewLLMCallExecutor("test-key")
	exec.AnthropicBaseURL = server.URL

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
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

func TestLLMCallOpenAIProvider(t *testing.T) {
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

	exec := NewLLMCallExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "openai-key",
		OpenAIBaseURL: server.URL,
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
		Config: map[string]any{
			"provider": "openai",
			"model":    "gpt-5.4",
			"prompt":   "Judge this evidence.",
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

func TestLLMCallGoogleProvider(t *testing.T) {
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

	exec := NewLLMCallExecutorWithConfig(LLMCallExecutorConfig{
		GoogleAPIKey:  "google-key",
		GoogleBaseURL: server.URL,
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
		Config: map[string]any{
			"provider": "google",
			"model":    "gemini-3.1-pro-preview",
			"prompt":   "Judge this evidence.",
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

func TestLLMCallSucceedsWithoutCitations(t *testing.T) {
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

	exec := NewLLMCallExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "openai-key",
		OpenAIBaseURL: server.URL,
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
		Config: map[string]any{
			"provider": "openai",
			"model":    "gpt-5.4",
			"prompt":   "Judge this evidence.",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success status, got %q", result.Outputs["status"])
	}
	if result.Outputs["outcome"] != "1" {
		t.Fatalf("expected outcome=1, got %q", result.Outputs["outcome"])
	}
	if _, ok := result.Outputs["citations_count"]; ok {
		t.Fatalf("expected citations_count to be omitted, got %q", result.Outputs["citations_count"])
	}
}

func TestLLMCallAcceptsOutcomeWithinAllowedRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]string{
						"content": `{"outcome_index":1,"confidence":"high","reasoning":"Outcome 1 is supported."}`,
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

	exec := NewLLMCallExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "openai-key",
		OpenAIBaseURL: server.URL,
	})
	ctx := dag.NewContext(map[string]string{
		"market.outcomes.json": `["Yes","No"]`,
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
		Config: map[string]any{
			"provider":             "openai",
			"model":                "gpt-5.4",
			"prompt":               "Judge this evidence.",
			"allowed_outcomes_key": "market.outcomes.json",
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success status, got %q", result.Outputs["status"])
	}
	if result.Outputs["outcome"] != "1" {
		t.Fatalf("expected outcome=1, got %q", result.Outputs["outcome"])
	}
}

func TestLLMCallRejectsOutcomeOutsideAllowedRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]string{
						"content": `{"outcome_index":3,"confidence":"low","reasoning":"Outcome 3 seems best."}`,
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

	exec := NewLLMCallExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "openai-key",
		OpenAIBaseURL: server.URL,
	})
	ctx := dag.NewContext(map[string]string{
		"market.outcomes.json": `["Yes","No"]`,
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
		Config: map[string]any{
			"provider":             "openai",
			"model":                "gpt-5.4",
			"prompt":               "Judge this evidence.",
			"allowed_outcomes_key": "market.outcomes.json",
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %q", result.Outputs["status"])
	}
	if result.Outputs["outcome"] != "inconclusive" {
		t.Fatalf("expected inconclusive outcome, got %q", result.Outputs["outcome"])
	}
	if !strings.Contains(result.Outputs["error"], "outside allowed outcome range") {
		t.Fatalf("unexpected error: %q", result.Outputs["error"])
	}
}

func TestLLMCallFailsWhenAllowedOutcomesKeyMissing(t *testing.T) {
	exec := NewLLMCallExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "openai-key",
		OpenAIBaseURL: "https://example.invalid",
	})

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
		Config: map[string]any{
			"provider":             "openai",
			"model":                "gpt-5.4",
			"prompt":               "Judge this evidence.",
			"allowed_outcomes_key": "market.outcomes.json",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %q", result.Outputs["status"])
	}
	if !strings.Contains(result.Outputs["error"], `allowed outcomes key "market.outcomes.json" not found in context`) {
		t.Fatalf("unexpected error: %q", result.Outputs["error"])
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

// --- cel_eval tests ---

func TestCelEvalBasicExpressions(t *testing.T) {
	exec := NewCelEvalExecutor()
	ctx := dag.NewContext(map[string]string{"name": "world"})

	node := dag.NodeDef{
		ID:   "eval",
		Type: "cel_eval",
		Config: map[string]interface{}{
			"expressions": map[string]interface{}{
				"greeting":  "'hello ' + name",
				"is_world":  "name == 'world'",
				"math_test": "1 + 2",
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["greeting"] != "hello world" {
		t.Fatalf("greeting = %q, want 'hello world'", result.Outputs["greeting"])
	}
	if result.Outputs["is_world"] != "true" {
		t.Fatalf("is_world = %q, want 'true'", result.Outputs["is_world"])
	}
	if result.Outputs["math_test"] != "3" {
		t.Fatalf("math_test = %q, want '3'", result.Outputs["math_test"])
	}
}

func TestCelEvalStringFunctions(t *testing.T) {
	exec := NewCelEvalExecutor()
	ctx := dag.NewContext(map[string]string{"value": "  Hello, World  "})

	node := dag.NodeDef{
		ID:   "eval",
		Type: "cel_eval",
		Config: map[string]interface{}{
			"expressions": map[string]interface{}{
				"trimmed": "value.trim()",
				"lower":   "value.trim().lowerAscii()",
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["trimmed"] != "Hello, World" {
		t.Fatalf("trimmed = %q, want 'Hello, World'", result.Outputs["trimmed"])
	}
	if result.Outputs["lower"] != "hello, world" {
		t.Fatalf("lower = %q, want 'hello, world'", result.Outputs["lower"])
	}
}

func TestCelEvalEmptyExpressionsFails(t *testing.T) {
	exec := NewCelEvalExecutor()
	node := dag.NodeDef{
		ID:   "eval",
		Type: "cel_eval",
		Config: map[string]interface{}{
			"expressions": map[string]interface{}{},
		},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err == nil {
		t.Fatal("expected error for empty expressions")
	}
}

func TestCelEvalInvalidExpressionFails(t *testing.T) {
	exec := NewCelEvalExecutor()
	node := dag.NodeDef{
		ID:   "eval",
		Type: "cel_eval",
		Config: map[string]interface{}{
			"expressions": map[string]interface{}{
				"bad": "(((",
			},
		},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err == nil {
		t.Fatal("expected error for invalid expression")
	}
}

// --- map executor tests ---

type mapTestExecutor struct {
	outputs map[string]string
	failOn  int // item index to fail on, -1 = never
}

func (e *mapTestExecutor) Execute(_ context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	if e.failOn >= 0 {
		idx := execCtx.Get("item_index")
		if idx == fmt.Sprintf("%d", e.failOn) {
			return dag.ExecutorResult{}, fmt.Errorf("deliberate failure on item %s", idx)
		}
	}
	outputs := make(map[string]string, len(e.outputs)+2)
	for k, v := range e.outputs {
		outputs[k] = v
	}
	outputs["item_echo"] = execCtx.Get("item")
	outputs["status"] = "success"
	return dag.ExecutorResult{Outputs: outputs}, nil
}

func newMapTestEngine(failOn int) *dag.Engine {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("step", &mapTestExecutor{
		outputs: map[string]string{"processed": "true"},
		failOn:  failOn,
	})
	return engine
}

func simpleInlineBlueprint() *dag.Blueprint {
	return &dag.Blueprint{
		Nodes: []dag.NodeDef{
			{ID: "step", Type: "step", Config: map[string]interface{}{}},
		},
	}
}

func TestMapExecutorParallel(t *testing.T) {
	engine := newMapTestEngine(-1)
	exec := NewMapExecutor(engine)
	ctx := dag.NewContext(nil)
	ctx.Set("items", `["apple","banana","cherry"]`)

	node := dag.NodeDef{
		ID:   "mapper",
		Type: "map",
		Config: map[string]interface{}{
			"items_key": "items",
			"inline":    simpleInlineBlueprint(),
			"mode":      "parallel",
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("status = %q, want success", result.Outputs["status"])
	}
	if result.Outputs["completed_count"] != "3" {
		t.Fatalf("completed_count = %q, want 3", result.Outputs["completed_count"])
	}
	if result.Outputs["total_count"] != "3" {
		t.Fatalf("total_count = %q, want 3", result.Outputs["total_count"])
	}

	var results []mapItemResult
	if err := json.Unmarshal([]byte(result.Outputs["results"]), &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Status != "completed" {
			t.Fatalf("item %d status = %q, want completed", r.Index, r.Status)
		}
	}
}

func TestMapExecutorSequential(t *testing.T) {
	engine := newMapTestEngine(-1)
	exec := NewMapExecutor(engine)
	ctx := dag.NewContext(nil)
	ctx.Set("items", `[1, 2, 3]`)

	node := dag.NodeDef{
		ID:   "mapper",
		Type: "map",
		Config: map[string]interface{}{
			"items_key": "items",
			"inline":    simpleInlineBlueprint(),
			"mode":      "sequential",
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["completed_count"] != "3" {
		t.Fatalf("completed_count = %q, want 3", result.Outputs["completed_count"])
	}
}

func TestMapExecutorMaxItems(t *testing.T) {
	engine := newMapTestEngine(-1)
	exec := NewMapExecutor(engine)
	ctx := dag.NewContext(nil)
	items := make([]int, 200)
	for i := range items {
		items[i] = i
	}
	data, _ := json.Marshal(items)
	ctx.Set("items", string(data))

	node := dag.NodeDef{
		ID:   "mapper",
		Type: "map",
		Config: map[string]interface{}{
			"items_key": "items",
			"inline":    simpleInlineBlueprint(),
			"max_items": 5,
		},
	}

	_, err := exec.Execute(context.Background(), node, ctx)
	if err == nil {
		t.Fatal("expected error for exceeding max_items")
	}
	if !strings.Contains(err.Error(), "exceeds max_items") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMapExecutorOnErrorFail(t *testing.T) {
	engine := newMapTestEngine(1) // fail on item index 1
	exec := NewMapExecutor(engine)
	ctx := dag.NewContext(nil)
	ctx.Set("items", `["a","b","c"]`)

	node := dag.NodeDef{
		ID:   "mapper",
		Type: "map",
		Config: map[string]interface{}{
			"items_key": "items",
			"inline":    simpleInlineBlueprint(),
			"mode":      "sequential",
			"on_error":  "fail",
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["failed_count"] == "0" {
		t.Fatal("expected at least one failure")
	}
	if result.Outputs["first_error"] == "" {
		t.Fatal("expected first_error to be set")
	}
}

func TestMapExecutorOnErrorContinue(t *testing.T) {
	engine := newMapTestEngine(1) // fail on item index 1
	exec := NewMapExecutor(engine)
	ctx := dag.NewContext(nil)
	ctx.Set("items", `["a","b","c"]`)

	node := dag.NodeDef{
		ID:   "mapper",
		Type: "map",
		Config: map[string]interface{}{
			"items_key": "items",
			"inline":    simpleInlineBlueprint(),
			"mode":      "sequential",
			"on_error":  "continue",
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["completed_count"] != "2" {
		t.Fatalf("completed_count = %q, want 2", result.Outputs["completed_count"])
	}
	if result.Outputs["failed_count"] != "1" {
		t.Fatalf("failed_count = %q, want 1", result.Outputs["failed_count"])
	}
	if result.Outputs["status"] != "partial" {
		t.Fatalf("status = %q, want partial", result.Outputs["status"])
	}
}

func TestMapExecutorEmptyArray(t *testing.T) {
	engine := newMapTestEngine(-1)
	exec := NewMapExecutor(engine)
	ctx := dag.NewContext(nil)
	ctx.Set("items", `[]`)

	node := dag.NodeDef{
		ID:   "mapper",
		Type: "map",
		Config: map[string]interface{}{
			"items_key": "items",
			"inline":    simpleInlineBlueprint(),
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["total_count"] != "0" {
		t.Fatalf("total_count = %q, want 0", result.Outputs["total_count"])
	}
}

func TestMapExecutorInputMappings(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("step", &mapTestExecutor{outputs: map[string]string{}, failOn: -1})
	exec := NewMapExecutor(engine)
	ctx := dag.NewContext(map[string]string{"shared_config": "abc123"})
	ctx.Set("items", `["x"]`)

	node := dag.NodeDef{
		ID:   "mapper",
		Type: "map",
		Config: map[string]interface{}{
			"items_key": "items",
			"inline":    simpleInlineBlueprint(),
			"input_mappings": map[string]interface{}{
				"config": "shared_config",
			},
			"output_keys": []interface{}{"step.item_echo", "step.status"},
		},
	}

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	var results []mapItemResult
	if err := json.Unmarshal([]byte(result.Outputs["results"]), &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "completed" {
		t.Fatalf("expected 1 completed result, got %+v", results)
	}
}

func TestMapExecutorRejectsNestedMap(t *testing.T) {
	engine := dag.NewEngine(nil)
	exec := NewMapExecutor(engine)
	ctx := dag.NewContext(nil)
	ctx.Set("items", `[1]`)

	node := dag.NodeDef{
		ID:   "mapper",
		Type: "map",
		Config: map[string]interface{}{
			"items_key": "items",
			"inline": &dag.Blueprint{
				Nodes: []dag.NodeDef{
					{ID: "nested", Type: "map", Config: map[string]interface{}{}},
				},
			},
		},
	}

	_, err := exec.Execute(context.Background(), node, ctx)
	if err == nil {
		t.Fatal("expected error for nested map node")
	}
	if !strings.Contains(err.Error(), "must not contain map nodes") {
		t.Fatalf("unexpected error: %v", err)
	}
}
