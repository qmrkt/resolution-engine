package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
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
	agentExec := executors.NewAgentLoopExecutorWithConfig(executors.LLMCallExecutorConfig{
		AnthropicAPIKey: anthropicKey,
	}, engine)
	agentExec.AllowLocalSourceFetch = true
	engine.RegisterExecutor("agent_loop", agentExec)
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
	llmExec := executors.NewLLMCallExecutorWithConfig(executors.LLMCallExecutorConfig{
		AnthropicAPIKey:  "test-key",
		AnthropicBaseURL: anthropicServer.URL,
	})
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

func TestE2EYOLOAutoResolutionExample(t *testing.T) {
	validChild := dag.Blueprint{
		ID:      "child-valid",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 30, MaxTotalTokens: 2000},
		Nodes: []dag.NodeDef{
			{
				ID:   "pick",
				Type: "cel_eval",
				Config: map[string]any{
					"expressions": map[string]string{
						"outcome": "'1'",
					},
				},
			},
			{
				ID:   "submit",
				Type: "submit_result",
				Config: map[string]any{
					"outcome_key": "pick.outcome",
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "pick", To: "submit"}},
	}
	invalidChild := dag.Blueprint{
		ID:      "child-invalid",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "pick",
				Type: "cel_eval",
				Config: map[string]any{
					"expressions": map[string]string{
						"outcome": "'1'",
					},
				},
			},
		},
	}
	validChildJSON := mustBlueprintJSONMain(t, validChild)
	invalidChildJSON := mustBlueprintJSONMain(t, invalidChild)

	var openAIRequests atomic.Int32
	openAIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("expected /responses endpoint, got %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-openai-key" {
			t.Fatalf("authorization header = %q, want Bearer test-openai-key", got)
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode openai request: %v", err)
		}

		switch openAIRequests.Add(1) {
		case 1:
			writeJSON(w, http.StatusOK, map[string]any{
				"output": []map[string]any{
					{
						"type":    "function_call",
						"call_id": "call_draft",
						"name":    "record_output",
						"arguments": mustJSONString(t, map[string]any{
							"blueprint_json":   validChildJSON,
							"strategy_summary": "use a minimal deterministic child blueprint",
							"assumptions":      []string{"primary resolution is deterministic in this test"},
						}),
					},
				},
				"usage": map[string]int{"input_tokens": 20, "output_tokens": 8},
			})
		case 2:
			writeJSON(w, http.StatusOK, map[string]any{
				"output": []map[string]any{
					{
						"type":    "function_call",
						"call_id": "call_repair",
						"name":    "record_output",
						"arguments": mustJSONString(t, map[string]any{
							"blueprint_json": validChildJSON,
							"repair_notes":   []string{"restore terminal node and bounded budget"},
						}),
					},
				},
				"usage": map[string]int{"input_tokens": 24, "output_tokens": 9},
			})
		default:
			t.Fatalf("unexpected extra openai request")
		}
	}))
	defer openAIServer.Close()

	var anthropicRequests atomic.Int32
	anthropicServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-api-key"); got != "test-anthropic-key" {
			t.Fatalf("x-api-key = %q, want test-anthropic-key", got)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"content": []map[string]any{
				{
					"type": "tool_use",
					"id":   "call_redteam",
					"name": "record_output",
					"input": map[string]any{
						"blueprint_json": invalidChildJSON,
						"attack_summary": "remove the terminal node to force repair",
						"residual_risks": []string{"child blueprint is intentionally invalid"},
					},
				},
			},
			"usage": map[string]int{"input_tokens": 18, "output_tokens": 7},
		})
		anthropicRequests.Add(1)
	}))
	defer anthropicServer.Close()

	rawBlueprint, err := os.ReadFile("docs/examples/yolo-auto-resolution/blueprint.json")
	if err != nil {
		t.Fatal(err)
	}

	tmpDir, err := os.MkdirTemp("", "e2e-yolo-auto-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	runner := NewRunner(executors.LLMCallExecutorConfig{
		AnthropicAPIKey:  "test-anthropic-key",
		AnthropicBaseURL: anthropicServer.URL,
		OpenAIAPIKey:     "test-openai-key",
		OpenAIBaseURL:    openAIServer.URL + "/chat/completions",
	}, "", tmpDir, "")

	run, err := runner.RunBlueprint(2001, rawBlueprint, map[string]string{
		"market.question":              "Will the test outcome resolve to No?",
		"market.outcomes_json":         `["Yes","No"]`,
		"market.resolution_rules":      "Resolve to the most truthful outcome. Defer if evidence is inconclusive.",
		"market.context_json":          `{"test_mode":true}`,
		"market.sources_json":          `[]`,
		"yolo.allowed_node_types_json": `["api_fetch","llm_call","agent_loop","wait","defer_resolution","submit_result","cel_eval","map"]`,
		"yolo.examples_json":           `[]`,
		"yolo.source_packs_json":       `[]`,
	})
	if err != nil {
		t.Fatalf("execution error: %v", err)
	}

	if run.Status != "completed" {
		t.Fatalf("expected status=completed, got %s", run.Status)
	}
	if openAIRequests.Load() != 2 {
		t.Fatalf("openai request count = %d, want 2", openAIRequests.Load())
	}
	if anthropicRequests.Load() != 1 {
		t.Fatalf("anthropic request count = %d, want 1", anthropicRequests.Load())
	}
	if run.NodeStates["repair"].Status != "completed" {
		t.Fatalf("expected repair node completed, got %+v", run.NodeStates["repair"])
	}
	if run.NodeStates["run_child"].Status != "completed" {
		t.Fatalf("expected gadget node completed, got %+v", run.NodeStates["run_child"])
	}
	if run.NodeStates["submit"].Status != "completed" {
		t.Fatalf("expected submit node completed, got %+v", run.NodeStates["submit"])
	}
	if run.NodeStates["defer_invalid"].Status == "completed" || run.NodeStates["defer_runtime"].Status == "completed" {
		t.Fatalf("unexpected defer path in successful yolo run: %+v", run.NodeStates)
	}
	if run.Context["candidate.source"] != "repair" {
		t.Fatalf("expected repaired child blueprint to be selected, got %q", run.Context["candidate.source"])
	}
	if run.Context["run_child.submitted"] != "true" {
		t.Fatalf("expected gadget to propagate child submission, got %q", run.Context["run_child.submitted"])
	}
	if run.Context["submit.outcome"] != "1" {
		t.Fatalf("expected final outcome 1, got %q", run.Context["submit.outcome"])
	}
	if run.Context["validate.valid"] != "true" {
		t.Fatalf("expected final validate.valid=true, got %q", run.Context["validate.valid"])
	}
	if run.Context["validate._runs"] == "" {
		t.Fatal("expected validate node history to show at least one repair loop")
	}
}

func mustJSONString(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json string: %v", err)
	}
	return string(raw)
}

// --------------------------------------------------------------------------
// Legacy dual-path runner orchestration moved out of this repo.
// Richer controller/orchestrator fallback tests belong in the monorepo.
// --------------------------------------------------------------------------
