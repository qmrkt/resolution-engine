package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/question-market/resolution-engine/dag"
	"github.com/question-market/resolution-engine/executors"
)

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// newTestRunner creates a Runner with real synchronous executors for tests.
func newTestRunner(anthropicKey, dataDir string) *Runner {
	engine := dag.NewEngine(nil)

	apiFetchExec := executors.NewAPIFetchExecutor()
	apiFetchExec.AllowLocal = true

	engine.RegisterExecutor("api_fetch", apiFetchExec)
	engine.RegisterExecutor("llm_call", executors.NewLLMCallExecutorWithConfig(executors.LLMCallExecutorConfig{
		AnthropicAPIKey: anthropicKey,
	}))
	engine.RegisterExecutor("submit_result", executors.NewSubmitResultExecutor())
	engine.RegisterExecutor("cancel_market", executors.NewCancelMarketExecutor())
	engine.RegisterExecutor("defer_resolution", executors.NewDeferResolutionExecutor())
	engine.RegisterExecutor("wait", executors.NewWaitExecutor())
	engine.RegisterExecutor("cel_eval", executors.NewCelEvalExecutor())
	engine.RegisterExecutor("map", executors.NewMapExecutor(engine))

	os.MkdirAll(dataDir, 0o755)

	return &Runner{
		engine:  engine,
		dataDir: dataDir,
	}
}

func buildAPIFetchBlueprint(fetchURL string) []byte {
	bp := dag.Blueprint{
		ID:      "api-fetch-preset",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "fetch",
				Type: "api_fetch",
				Config: map[string]interface{}{
					"url":       fetchURL,
					"json_path": "result.winner",
					"outcome_mapping": map[string]string{
						"Yes": "0",
						"No":  "1",
					},
				},
			},
			{
				ID:   "submit",
				Type: "submit_result",
				Config: map[string]interface{}{
					"outcome_key": "fetch.outcome",
				},
			},
			{
				ID:   "cancel",
				Type: "cancel_market",
				Config: map[string]interface{}{
					"reason": "api fetch did not succeed",
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "fetch", To: "submit", Condition: "fetch.status == 'success'"},
			{From: "fetch", To: "cancel", Condition: "fetch.status != 'success'"},
		},
	}
	data, _ := json.Marshal(bp)
	return data
}

func buildLLMCallBlueprint() []byte {
	bp := dag.Blueprint{
		ID:      "llm-call-preset",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "llm",
				Type: "llm_call",
				Config: map[string]interface{}{
					"prompt":          "Evaluate the evidence and determine the outcome.",
					"timeout_seconds": 10,
				},
			},
			{
				ID:   "submit",
				Type: "submit_result",
				Config: map[string]interface{}{
					"outcome_key": "llm.outcome",
				},
			},
			{
				ID:   "cancel",
				Type: "cancel_market",
				Config: map[string]interface{}{
					"reason": "LLM call did not succeed",
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "llm", To: "submit", Condition: "llm.status == 'success'"},
			{From: "llm", To: "cancel", Condition: "llm.status != 'success'"},
		},
	}
	data, _ := json.Marshal(bp)
	return data
}

func TestE2EAPIFetchResolution(t *testing.T) {
	// Start a mock API server returning a result
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]interface{}{
			"result": map[string]string{
				"winner": "Yes",
			},
		})
	}))
	defer apiServer.Close()

	tmpDir, err := os.MkdirTemp("", "e2e-api-fetch-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	runner := newTestRunner("", tmpDir)
	bp := buildAPIFetchBlueprint(apiServer.URL)

	appID := 1003
	run, err := runner.RunBlueprint(appID, bp, nil)
	if err != nil {
		t.Fatalf("execution error: %v", err)
	}

	if run.Status != "completed" {
		t.Fatalf("expected status=completed, got %s", run.Status)
	}

	// Submit node should have run
	submitState := run.NodeStates["submit"]
	if submitState.Status != "completed" {
		t.Fatalf("expected submit node completed, got %s", submitState.Status)
	}

	// Cancel should not have run
	cancelState := run.NodeStates["cancel"]
	if cancelState.Status == "completed" {
		t.Fatal("cancel node should not run on success")
	}

	// Outcome should be "0" (Yes -> 0 via mapping)
	outcome := run.Context["submit.outcome"]
	if outcome != "0" {
		t.Fatalf("expected outcome=0, got %q", outcome)
	}

	// Verify the raw extracted value
	extracted := run.Context["fetch.extracted"]
	if extracted != "Yes" {
		t.Fatalf("expected extracted=Yes, got %q", extracted)
	}
}

// --------------------------------------------------------------------------
// Test 4: LLM call resolves via mock Anthropic API
// --------------------------------------------------------------------------

func TestE2ELLMCallResolution(t *testing.T) {
	// Mock Anthropic Messages API
	anthropicServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request has correct headers
		if r.Header.Get("x-api-key") != "test-key" {
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		if r.Header.Get("anthropic-version") != "2023-06-01" {
			writeJSON(w, 400, map[string]string{"error": "missing anthropic-version"})
			return
		}

		// Return a structured response
		writeJSON(w, 200, map[string]interface{}{
			"content": []map[string]string{
				{"type": "text", "text": `{"outcome_index": 1, "confidence": "high", "reasoning": "Based on the evidence, outcome 1 is correct."}`},
			},
			"usage": map[string]int{
				"input_tokens":  100,
				"output_tokens": 50,
			},
		})
	}))
	defer anthropicServer.Close()

	tmpDir, err := os.MkdirTemp("", "e2e-llm-call-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Build runner with real executors, override LLM base URL
	runner := newTestRunner("test-key", tmpDir)

	// Override the LLM executor's BaseURL to point to our mock
	llmExec := executors.NewLLMCallExecutor("test-key")
	llmExec.AnthropicBaseURL = anthropicServer.URL
	runner.engine.RegisterExecutor("llm_call", llmExec)

	bp := buildLLMCallBlueprint()

	appID := 1004
	run, err := runner.RunBlueprint(appID, bp, nil)
	if err != nil {
		t.Fatalf("execution error: %v", err)
	}

	if run.Status != "completed" {
		t.Fatalf("expected status=completed, got %s", run.Status)
	}

	// Submit should have run
	submitState := run.NodeStates["submit"]
	if submitState.Status != "completed" {
		t.Fatalf("expected submit node completed, got %s", submitState.Status)
	}

	// Outcome should be "1" (from outcome_index)
	outcome := run.Context["submit.outcome"]
	if outcome != "1" {
		t.Fatalf("expected outcome=1, got %q", outcome)
	}

	// Verify LLM metadata in context
	confidence := run.Context["llm.confidence"]
	if confidence != "high" {
		t.Fatalf("expected confidence=high, got %q", confidence)
	}

	reasoning := run.Context["llm.reasoning"]
	if reasoning == "" {
		t.Fatal("expected non-empty reasoning")
	}
}

// --------------------------------------------------------------------------
// Legacy dual-path runner orchestration moved out of this repo.
// Richer controller/orchestrator fallback tests belong in the monorepo.
// --------------------------------------------------------------------------
