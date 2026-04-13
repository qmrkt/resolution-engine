package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/question-market/resolution-engine/dag"
	"github.com/question-market/resolution-engine/executors"
)

// mockIndexer simulates the indexer's human judgment endpoints.
type mockIndexer struct {
	mu        sync.Mutex
	judgments map[string]*indexerJudgment // keyed by judgmentId
}

type indexerJudgment struct {
	JudgmentID          string   `json:"judgmentId"`
	AppID               int      `json:"appId"`
	RunID               string   `json:"runId"`
	NodeID              string   `json:"nodeId"`
	Status              string   `json:"status"`
	Prompt              string   `json:"prompt"`
	AllowedResponders   []string `json:"allowedResponders"`
	ResponseNonce       string   `json:"responseNonce"`
	TimeoutAt           int64    `json:"timeoutAt"`
	ResponseOutcome     int      `json:"responseOutcome"`
	ResponseReason      string   `json:"responseReason"`
	ResponderAddress    string   `json:"responderAddress"`
	ResponderRole       string   `json:"responderRole"`
	ResponseHash        string   `json:"responseHash"`
	ResponseSubmittedAt int64    `json:"responseSubmittedAt"`
	RequireReason       bool     `json:"requireReason"`
	AllowCancel         bool     `json:"allowCancel"`
}

func newMockIndexer() *mockIndexer {
	return &mockIndexer{
		judgments: make(map[string]*indexerJudgment),
	}
}

func (m *mockIndexer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	method := r.Method

	// Route: POST /markets/{id}/human-judgments (create)
	// Route: GET  /markets/{id}/human-judgments (list)
	// Route: POST /markets/{id}/human-judgments/{jid}/respond

	parts := strings.Split(strings.Trim(path, "/"), "/")

	if len(parts) >= 3 && parts[0] == "markets" && parts[2] == "human-judgments" {
		if len(parts) == 3 {
			switch method {
			case http.MethodPost:
				m.handleCreate(w, r)
				return
			case http.MethodGet:
				m.handleList(w, r)
				return
			}
		}
		if len(parts) == 5 && parts[4] == "respond" && method == http.MethodPost {
			m.handleRespond(w, r, parts[3])
			return
		}
	}

	http.NotFound(w, r)
}

func (m *mockIndexer) handleCreate(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		JudgmentID        string   `json:"judgmentId"`
		RunID             string   `json:"runId"`
		NodeID            string   `json:"nodeId"`
		Prompt            string   `json:"prompt"`
		AllowedResponders []string `json:"allowedResponders"`
		TimeoutAt         int64    `json:"timeoutAt"`
		RequireReason     bool     `json:"requireReason"`
		AllowCancel       bool     `json:"allowCancel"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	j := &indexerJudgment{
		JudgmentID:        payload.JudgmentID,
		RunID:             payload.RunID,
		NodeID:            payload.NodeID,
		Status:            "pending",
		Prompt:            payload.Prompt,
		AllowedResponders: payload.AllowedResponders,
		TimeoutAt:         payload.TimeoutAt,
		RequireReason:     payload.RequireReason,
		AllowCancel:       payload.AllowCancel,
	}
	m.judgments[j.JudgmentID] = j

	writeJSON(w, 200, map[string]interface{}{"judgment": j})
}

func (m *mockIndexer) handleList(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for timed-out judgments and mark them
	now := time.Now().UnixMilli()
	for _, j := range m.judgments {
		if j.Status == "pending" && j.TimeoutAt > 0 && now >= j.TimeoutAt {
			j.Status = "timeout"
		}
	}

	list := make([]*indexerJudgment, 0, len(m.judgments))
	for _, j := range m.judgments {
		list = append(list, j)
	}

	writeJSON(w, 200, map[string]interface{}{"judgments": list})
}

func (m *mockIndexer) handleRespond(w http.ResponseWriter, r *http.Request, judgmentID string) {
	var payload struct {
		Outcome          int    `json:"outcome"`
		Reason           string `json:"reason"`
		ResponderAddress string `json:"responderAddress"`
		ResponderRole    string `json:"responderRole"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.judgments[judgmentID]
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "judgment not found"})
		return
	}

	j.Status = "responded"
	j.ResponseOutcome = payload.Outcome
	j.ResponseReason = payload.Reason
	j.ResponderAddress = payload.ResponderAddress
	j.ResponderRole = payload.ResponderRole
	j.ResponseHash = "mock-hash"
	j.ResponseSubmittedAt = time.Now().UnixMilli()

	writeJSON(w, 200, map[string]interface{}{"judgment": j})
}

// simulateResponse directly updates a judgment to "responded" status.
func (m *mockIndexer) simulateResponse(judgmentID string, outcome int, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.judgments[judgmentID]
	if !ok {
		return
	}
	j.Status = "responded"
	j.ResponseOutcome = outcome
	j.ResponseReason = reason
	j.ResponderAddress = "MOCK_ADDR"
	j.ResponderRole = "creator"
	j.ResponseHash = "mock-response-hash"
	j.ResponseSubmittedAt = time.Now().UnixMilli()
}

// waitForJudgment polls until a judgment with the given prefix appears.
func (m *mockIndexer) waitForJudgment(prefix string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		for id := range m.judgments {
			if strings.Contains(id, prefix) {
				m.mu.Unlock()
				return id
			}
		}
		m.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// setPollInterval configures a short poll interval on the Runner's human judge executors.
func setPollInterval(runner *Runner, d time.Duration) {
	engine := runner.engine
	// Access executors by re-registering with modified poll intervals is not possible
	// directly, but we can use the exported fields. We need to reach into the engine.
	// Instead, we build the runner manually with customized executors.
	_ = engine
}

// newTestRunner creates a Runner with short poll intervals for testing.
func newTestRunner(anthropicKey, indexerURL, dataDir string) *Runner {
	engine := dag.NewEngine(nil)

	hjExec := executors.NewHumanJudgeExecutor(indexerURL)
	hjExec.PollInterval = 50 * time.Millisecond

	apiFetchExec := executors.NewAPIFetchExecutor()
	apiFetchExec.AllowLocal = true

	engine.RegisterExecutor("api_fetch", apiFetchExec)
	engine.RegisterExecutor("llm_judge", executors.NewLLMJudgeExecutorWithConfig(executors.LLMJudgeExecutorConfig{
		AnthropicAPIKey: anthropicKey,
	}))
	engine.RegisterExecutor("human_judge", hjExec)
	engine.RegisterExecutor("submit_result", executors.NewSubmitResultExecutor())
	engine.RegisterExecutor("cancel_market", executors.NewCancelMarketExecutor())
	engine.RegisterExecutor("outcome_terminality", executors.NewOutcomeTerminalityExecutor())
	engine.RegisterExecutor("defer_resolution", executors.NewDeferResolutionExecutor())
	engine.RegisterExecutor("wait", executors.NewWaitExecutor())

	os.MkdirAll(dataDir, 0o755)

	return &Runner{
		engine:  engine,
		dataDir: dataDir,
	}
}

// buildHumanJudgeBlueprint builds a human_judge preset blueprint:
//
//	judge -> submit (on success) / cancel (on non-success)
func buildHumanJudgeBlueprint(timeoutSeconds int) []byte {
	bp := dag.Blueprint{
		ID:      "human-judge-preset",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "judge",
				Type: "human_judge",
				Config: map[string]interface{}{
					"prompt":             "Please resolve this market.",
					"allowed_responders": []string{"creator", "protocol_admin"},
					"timeout_seconds":    timeoutSeconds,
				},
			},
			{
				ID:   "submit",
				Type: "submit_result",
				Config: map[string]interface{}{
					"outcome_key": "judge.outcome",
				},
			},
			{
				ID:   "cancel",
				Type: "cancel_market",
				Config: map[string]interface{}{
					"reason": "human judgment did not succeed",
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "judge", To: "submit", Condition: "judge.status == 'responded'"},
			{From: "judge", To: "cancel", Condition: "judge.status != 'responded'"},
		},
	}
	data, _ := json.Marshal(bp)
	return data
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

func buildLLMJudgeBlueprint() []byte {
	bp := dag.Blueprint{
		ID:      "llm-judge-preset",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "llm",
				Type: "llm_judge",
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
					"reason": "LLM judge did not succeed",
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

// --------------------------------------------------------------------------
// Test 1: Human judge responds successfully, submit path taken
// --------------------------------------------------------------------------

func TestE2EHumanJudgeResolution(t *testing.T) {
	mock := newMockIndexer()
	indexerServer := httptest.NewServer(mock)
	defer indexerServer.Close()

	tmpDir, err := os.MkdirTemp("", "e2e-human-judge-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	runner := newTestRunner("", indexerServer.URL, tmpDir)
	bp := buildHumanJudgeBlueprint(5)

	appID := 1001

	// Run execution in background
	type result struct {
		run *dag.RunState
		err error
	}
	ch := make(chan result, 1)
	go func() {
		run, err := runner.RunBlueprint(appID, bp, nil)
		ch <- result{run, err}
	}()

	// Wait for the judgment to appear, then simulate a human response
	jID := mock.waitForJudgment(fmt.Sprintf("%d:", appID), 5*time.Second)
	if jID == "" {
		t.Fatal("timed out waiting for judgment to be created")
	}

	mock.simulateResponse(jID, 1, "Market clearly resolved to outcome 1")

	// Wait for execution to complete
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("execution error: %v", res.err)
		}
		run := res.run
		if run.Status != "completed" {
			t.Fatalf("expected status=completed, got %s", run.Status)
		}

		// Submit node should have run
		submitState := run.NodeStates["submit"]
		if submitState.Status != "completed" {
			t.Fatalf("expected submit node completed, got %s", submitState.Status)
		}

		// Cancel node should not have run (should be pending)
		cancelState := run.NodeStates["cancel"]
		if cancelState.Status == "completed" {
			t.Fatal("cancel node should not have run on successful judgment")
		}

		// Check outcome
		outcome := run.Context["submit.outcome"]
		if outcome != "1" {
			t.Fatalf("expected outcome=1, got %q", outcome)
		}

	case <-time.After(10 * time.Second):
		t.Fatal("execution timed out")
	}
}

// --------------------------------------------------------------------------
// Test 2: Human judge times out, cancel path taken
// --------------------------------------------------------------------------

func TestE2EHumanJudgeTimeout(t *testing.T) {
	mock := newMockIndexer()
	indexerServer := httptest.NewServer(mock)
	defer indexerServer.Close()

	tmpDir, err := os.MkdirTemp("", "e2e-human-timeout-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	runner := newTestRunner("", indexerServer.URL, tmpDir)

	// Use a 1-second timeout so the judgment times out quickly
	bp := buildHumanJudgeBlueprint(1)

	appID := 1002

	type result struct {
		run *dag.RunState
		err error
	}
	ch := make(chan result, 1)
	go func() {
		run, err := runner.RunBlueprint(appID, bp, nil)
		ch <- result{run, err}
	}()

	// Do NOT respond. The mock indexer will mark the judgment as "timeout"
	// once its timeoutAt timestamp passes.

	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("execution error: %v", res.err)
		}
		run := res.run
		if run.Status != "completed" {
			t.Fatalf("expected status=completed, got %s", run.Status)
		}

		// Cancel node should have run
		cancelState := run.NodeStates["cancel"]
		if cancelState.Status != "completed" {
			t.Fatalf("expected cancel node completed, got %s", cancelState.Status)
		}

		// Submit should not have run
		submitState := run.NodeStates["submit"]
		if submitState.Status == "completed" {
			t.Fatal("submit node should not run on timeout")
		}

		// Context should have cancel marker
		if run.Context["cancel.cancelled"] != "true" {
			t.Fatalf("expected cancel.cancelled=true, got %q", run.Context["cancel.cancelled"])
		}

	case <-time.After(15 * time.Second):
		t.Fatal("execution timed out waiting for timeout path")
	}
}

// --------------------------------------------------------------------------
// Test 3: API fetch resolves successfully
// --------------------------------------------------------------------------

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

	runner := newTestRunner("", "", tmpDir)
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
// Test 4: LLM judge resolves via mock Anthropic API
// --------------------------------------------------------------------------

func TestE2ELLMJudgeResolution(t *testing.T) {
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

	tmpDir, err := os.MkdirTemp("", "e2e-llm-judge-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Build runner with real executors, override LLM base URL
	runner := newTestRunner("test-key", "", tmpDir)

	// Override the LLM executor's BaseURL to point to our mock
	llmExec := executors.NewLLMJudgeExecutor("test-key")
	llmExec.AnthropicBaseURL = anthropicServer.URL
	runner.engine.RegisterExecutor("llm_judge", llmExec)

	bp := buildLLMJudgeBlueprint()

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
