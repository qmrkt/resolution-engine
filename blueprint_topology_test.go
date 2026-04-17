package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
)

// ---------------------------------------------------------------------------
// Mock executors
// ---------------------------------------------------------------------------

// configAwareExecutor reads node config to decide behavior.
type configAwareExecutor struct {
	handler func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error)
}

func (e *configAwareExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	cfg, _ := node.Config.(map[string]interface{})
	outputs, err := e.handler(cfg, inv)
	if err != nil {
		return dag.ExecutorResult{}, err
	}
	return dag.ExecutorResult{Outputs: outputs}, nil
}

// trackingExecutor records which node IDs ran.
type trackingExecutor struct {
	mu      sync.Mutex
	ran     []string
	outputs map[string]string
}

func (e *trackingExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	e.mu.Lock()
	e.ran = append(e.ran, node.ID)
	e.mu.Unlock()
	return dag.ExecutorResult{Outputs: e.outputs}, nil
}

func (e *trackingExecutor) didRun(nodeID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, id := range e.ran {
		if id == nodeID {
			return true
		}
	}
	return false
}

// retryAwareExecutor tracks call count per node and returns different results.
type retryAwareExecutor struct {
	mu       sync.Mutex
	calls    map[string]int
	behavior func(nodeID string, callNum int, inv *dag.Invocation) (map[string]string, error)
}

func newRetryAwareExecutor(behavior func(string, int, *dag.Invocation) (map[string]string, error)) *retryAwareExecutor {
	return &retryAwareExecutor{
		calls:    make(map[string]int),
		behavior: behavior,
	}
}

func (e *retryAwareExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	e.mu.Lock()
	e.calls[node.ID]++
	callNum := e.calls[node.ID]
	e.mu.Unlock()

	outputs, err := e.behavior(node.ID, callNum, inv)
	if err != nil {
		return dag.ExecutorResult{}, err
	}
	return dag.ExecutorResult{Outputs: outputs}, nil
}

// ---------------------------------------------------------------------------
// 1. TestPresetAwaitSignal
// ---------------------------------------------------------------------------

func TestPresetAwaitSignal(t *testing.T) {
	t.Run("success_path", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("await_signal", &mockExecutor{
			outputs: map[string]string{"status": "success", "outcome": "1", "reason": "Judge confirmed outcome"},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"status": "success", "submitted": "true"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "await-signal-preset",
			Name: "Await Signal",
			Nodes: []dag.NodeDef{
				{ID: "judge", Type: "await_signal", Config: map[string]interface{}{
					"signal_type":      "human_judgment.responded",
					"required_payload": []string{"outcome"},
					"default_outputs":  map[string]string{"status": "success"},
					"timeout_seconds":  3600,
				}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit", Condition: "results.judge.status == 'success'"},
				{From: "judge", To: "cancel", Condition: "results.judge.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["judge"].Status != "completed" {
			t.Fatalf("expected judge completed, got %s", run.NodeStates["judge"].Status)
		}
		if run.NodeStates["submit"].Status != "completed" {
			t.Fatalf("expected submit completed, got %s", run.NodeStates["submit"].Status)
		}
		if run.NodeStates["cancel"].Status != "pending" {
			t.Fatalf("expected cancel pending (not activated), got %s", run.NodeStates["cancel"].Status)
		}
		if testResultValue(run, "judge", "outcome") != "1" {
			t.Fatalf("expected judge.outcome=1, got %q", testResultValue(run, "judge", "outcome"))
		}
	})

	t.Run("timeout_cancel_path", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("await_signal", &mockExecutor{
			outputs: map[string]string{"status": "cancelled", "reason": "Judge timed out"},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"status": "success", "submitted": "true"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true", "reason": "No judgment received"},
		})

		bp := dag.Blueprint{
			ID:   "await-signal-preset-cancel",
			Name: "Await Signal Cancel",
			Nodes: []dag.NodeDef{
				{ID: "judge", Type: "await_signal", Config: map[string]interface{}{
					"signal_type":      "human_judgment.responded",
					"required_payload": []string{"outcome"},
					"default_outputs":  map[string]string{"status": "cancelled"},
					"timeout_seconds":  3600,
				}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit", Condition: "results.judge.status == 'success'"},
				{From: "judge", To: "cancel", Condition: "results.judge.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["cancel"].Status != "completed" {
			t.Fatalf("expected cancel completed, got %s", run.NodeStates["cancel"].Status)
		}
		if run.NodeStates["submit"].Status != "pending" {
			t.Fatalf("expected submit pending (not activated), got %s", run.NodeStates["submit"].Status)
		}
		if testResultValue(run, "cancel", "cancelled") != "true" {
			t.Fatalf("expected cancel.cancelled=true, got %q", testResultValue(run, "cancel", "cancelled"))
		}
	})
}

// ---------------------------------------------------------------------------
// 2. TestPresetAPIFetch
// ---------------------------------------------------------------------------

func TestPresetAPIFetch(t *testing.T) {
	t.Run("success_maps_outcome", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("api_fetch", &mockExecutor{
			outputs: map[string]string{
				"status":  "success",
				"outcome": "0",
				"data":    `{"price": 99500, "source": "coingecko"}`,
			},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-preset",
			Name: "API Fetch",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{
					"url":    "https://api.coingecko.com/api/v3/simple/price",
					"method": "GET",
				}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "results.fetch.status == 'success'"},
				{From: "fetch", To: "cancel", Condition: "results.fetch.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["submit"].Status != "completed" {
			t.Fatalf("expected submit completed, got %s", run.NodeStates["submit"].Status)
		}
		if run.NodeStates["cancel"].Status != "pending" {
			t.Fatalf("expected cancel pending, got %s", run.NodeStates["cancel"].Status)
		}
		if testResultValue(run, "fetch", "outcome") != "0" {
			t.Fatalf("expected fetch.outcome=0, got %q", testResultValue(run, "fetch", "outcome"))
		}
	})

	t.Run("api_500_cancels", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("api_fetch", &mockExecutor{
			outputs: map[string]string{
				"status": "failed",
				"error":  "HTTP 500: Internal Server Error",
			},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true", "reason": "API failure"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-fail",
			Name: "API Fetch Failure",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{
					"url": "https://api.example.com/broken",
				}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "results.fetch.status == 'success'"},
				{From: "fetch", To: "cancel", Condition: "results.fetch.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["cancel"].Status != "completed" {
			t.Fatalf("expected cancel completed, got %s", run.NodeStates["cancel"].Status)
		}
		if run.NodeStates["submit"].Status != "pending" {
			t.Fatalf("expected submit pending, got %s", run.NodeStates["submit"].Status)
		}
	})
}

// ---------------------------------------------------------------------------
// 3. TestPresetLLMCall
// ---------------------------------------------------------------------------

func TestPresetLLMCall(t *testing.T) {
	t.Run("success_outcome", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("llm_call", &mockExecutor{
			outputs: map[string]string{
				"status":    "success",
				"outcome":   "1",
				"reasoning": "Based on available evidence, outcome 1 is correct",
			},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "llm-call-preset",
			Name: "LLM Judge",
			Nodes: []dag.NodeDef{
				{ID: "judge", Type: "llm_call", Config: map[string]interface{}{
					"model":  "gpt-4o",
					"prompt": "Analyze the evidence and determine the outcome",
				}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit", Condition: "results.judge.status == 'success'"},
				{From: "judge", To: "cancel", Condition: "results.judge.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["submit"].Status != "completed" {
			t.Fatalf("expected submit completed, got %s", run.NodeStates["submit"].Status)
		}
		if run.NodeStates["cancel"].Status != "pending" {
			t.Fatalf("expected cancel pending, got %s", run.NodeStates["cancel"].Status)
		}
		if testResultValue(run, "judge", "reasoning") != "Based on available evidence, outcome 1 is correct" {
			t.Fatalf("expected reasoning in context, got %q", testResultValue(run, "judge", "reasoning"))
		}
	})

	t.Run("inconclusive_cancels", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("llm_call", &mockExecutor{
			outputs: map[string]string{
				"status":    "inconclusive",
				"outcome":   "inconclusive",
				"reasoning": "Insufficient evidence to determine outcome",
			},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true", "reason": "LLM inconclusive"},
		})

		bp := dag.Blueprint{
			ID:   "llm-call-inconclusive",
			Name: "LLM Judge Inconclusive",
			Nodes: []dag.NodeDef{
				{ID: "judge", Type: "llm_call", Config: map[string]interface{}{
					"model": "gpt-4o",
				}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit", Condition: "results.judge.status == 'success'"},
				{From: "judge", To: "cancel", Condition: "results.judge.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["cancel"].Status != "completed" {
			t.Fatalf("expected cancel completed, got %s", run.NodeStates["cancel"].Status)
		}
		if run.NodeStates["submit"].Status != "pending" {
			t.Fatalf("expected submit pending, got %s", run.NodeStates["submit"].Status)
		}
		if testResultValue(run, "cancel", "reason") != "LLM inconclusive" {
			t.Fatalf("expected cancel reason, got %q", testResultValue(run, "cancel", "reason"))
		}
	})
}

// ---------------------------------------------------------------------------
// 4. TestPresetAPIFetchLLM
// ---------------------------------------------------------------------------

func TestPresetAPIFetchLLM(t *testing.T) {
	// Two-stage pipeline: api_fetch -> llm_call -> submit
	// Each stage has its own cancel node to avoid multi-incoming-edge
	// blocking when an upstream stage is never activated.

	t.Run("api_success_llm_calls", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("api_fetch", &mockExecutor{
			outputs: map[string]string{
				"status": "success",
				"data":   `{"price": 105000, "timestamp": "2025-01-15T00:00:00Z"}`,
			},
		})
		engine.RegisterExecutor("llm_call", &configAwareExecutor{
			handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
				// LLM sees the fetched data in context
				data := inv.Lookup("results.fetch.data")
				if data == "" {
					return map[string]string{"status": "failed", "error": "no data to judge"}, nil
				}
				return map[string]string{
					"status":    "success",
					"outcome":   "1",
					"reasoning": "Price exceeds 100k based on API data: " + data,
				}, nil
			},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-llm",
			Name: "API Fetch then LLM Judge",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{
					"url": "https://api.coingecko.com/api/v3/simple/price",
				}},
				{ID: "judge", Type: "llm_call", Config: map[string]interface{}{
					"model": "gpt-4o",
				}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel_fetch", Type: "mock_cancel", Config: map[string]interface{}{}},
				{ID: "cancel_judge", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "judge", Condition: "results.fetch.status == 'success'"},
				{From: "fetch", To: "cancel_fetch", Condition: "results.fetch.status != 'success'"},
				{From: "judge", To: "submit", Condition: "results.judge.status == 'success'"},
				{From: "judge", To: "cancel_judge", Condition: "results.judge.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["fetch"].Status != "completed" {
			t.Fatalf("expected fetch completed, got %s", run.NodeStates["fetch"].Status)
		}
		if run.NodeStates["judge"].Status != "completed" {
			t.Fatalf("expected judge completed, got %s", run.NodeStates["judge"].Status)
		}
		if run.NodeStates["submit"].Status != "completed" {
			t.Fatalf("expected submit completed, got %s", run.NodeStates["submit"].Status)
		}
		if testResultValue(run, "judge", "outcome") != "1" {
			t.Fatalf("expected judge.outcome=1, got %q", testResultValue(run, "judge", "outcome"))
		}
		// Verify the LLM saw the API data
		if !strings.Contains(testResultValue(run, "judge", "reasoning"), "105000") {
			t.Fatalf("expected reasoning to reference API data, got %q", testResultValue(run, "judge", "reasoning"))
		}
		// Neither cancel node should have fired
		if run.NodeStates["cancel_fetch"].Status != "pending" {
			t.Fatalf("expected cancel_fetch pending, got %s", run.NodeStates["cancel_fetch"].Status)
		}
		if run.NodeStates["cancel_judge"].Status != "pending" {
			t.Fatalf("expected cancel_judge pending, got %s", run.NodeStates["cancel_judge"].Status)
		}
	})

	t.Run("api_success_llm_fails_cancel", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("api_fetch", &mockExecutor{
			outputs: map[string]string{
				"status": "success",
				"data":   `{"price": null, "error": "no data available"}`,
			},
		})
		engine.RegisterExecutor("llm_call", &mockExecutor{
			outputs: map[string]string{
				"status":    "failed",
				"reasoning": "Cannot determine outcome from null price data",
			},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-llm-fail",
			Name: "API Fetch LLM Fails",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "judge", Type: "llm_call", Config: map[string]interface{}{}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel_fetch", Type: "mock_cancel", Config: map[string]interface{}{}},
				{ID: "cancel_judge", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "judge", Condition: "results.fetch.status == 'success'"},
				{From: "fetch", To: "cancel_fetch", Condition: "results.fetch.status != 'success'"},
				{From: "judge", To: "submit", Condition: "results.judge.status == 'success'"},
				{From: "judge", To: "cancel_judge", Condition: "results.judge.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["fetch"].Status != "completed" {
			t.Fatalf("expected fetch completed, got %s", run.NodeStates["fetch"].Status)
		}
		if run.NodeStates["judge"].Status != "completed" {
			t.Fatalf("expected judge completed, got %s", run.NodeStates["judge"].Status)
		}
		if run.NodeStates["cancel_judge"].Status != "completed" {
			t.Fatalf("expected cancel_judge completed, got %s", run.NodeStates["cancel_judge"].Status)
		}
		if run.NodeStates["submit"].Status != "pending" {
			t.Fatalf("expected submit pending, got %s", run.NodeStates["submit"].Status)
		}
		if run.NodeStates["cancel_fetch"].Status != "pending" {
			t.Fatalf("expected cancel_fetch pending (fetch succeeded), got %s", run.NodeStates["cancel_fetch"].Status)
		}
	})

	t.Run("api_fails_llm_never_runs", func(t *testing.T) {
		tracker := &trackingExecutor{outputs: map[string]string{"cancelled": "true"}}

		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("api_fetch", &mockExecutor{
			outputs: map[string]string{
				"status": "failed",
				"error":  "connection timeout",
			},
		})
		engine.RegisterExecutor("llm_call", tracker)
		engine.RegisterExecutor("mock_submit", tracker)
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-llm-no-run",
			Name: "API Fails, LLM Skipped",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "judge", Type: "llm_call", Config: map[string]interface{}{}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "cancel_fetch", Type: "mock_cancel", Config: map[string]interface{}{}},
				{ID: "cancel_judge", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "judge", Condition: "results.fetch.status == 'success'"},
				{From: "fetch", To: "cancel_fetch", Condition: "results.fetch.status != 'success'"},
				{From: "judge", To: "submit", Condition: "results.judge.status == 'success'"},
				{From: "judge", To: "cancel_judge", Condition: "results.judge.status != 'success'"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		// Fetch cancel should fire (API failed)
		if run.NodeStates["cancel_fetch"].Status != "completed" {
			t.Fatalf("expected cancel_fetch completed, got %s", run.NodeStates["cancel_fetch"].Status)
		}
		// Judge should never have been activated
		if run.NodeStates["judge"].Status != "pending" {
			t.Fatalf("expected judge pending (never activated), got %s", run.NodeStates["judge"].Status)
		}
		if tracker.didRun("judge") {
			t.Fatal("LLM call should not have executed when API failed")
		}
		if tracker.didRun("submit") {
			t.Fatal("submit should not have executed when API failed")
		}
		// Judge's cancel should also remain pending (judge never ran)
		if run.NodeStates["cancel_judge"].Status != "pending" {
			t.Fatalf("expected cancel_judge pending, got %s", run.NodeStates["cancel_judge"].Status)
		}
	})
}

// ---------------------------------------------------------------------------
// 5. TestPresetAPIFetchWait (retry loop with back-edge)
// ---------------------------------------------------------------------------

func TestPresetAPIFetchWait(t *testing.T) {
	t.Run("first_fetch_succeeds", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("api_fetch", &mockExecutor{
			outputs: map[string]string{"status": "success", "outcome": "1", "data": "result"},
		})
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("wait", &mockExecutor{
			outputs: map[string]string{"status": "ready"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-wait-ok",
			Name: "API Fetch with Retry - Success",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "wait", Type: "wait", Config: map[string]interface{}{"delay_seconds": 5}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "results.fetch.status == 'success'"},
				{From: "fetch", To: "wait", Condition: "results.fetch.status != 'success'"},
				{From: "wait", To: "fetch", MaxTraversals: 2},
				{From: "wait", To: "cancel"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}
		if run.NodeStates["submit"].Status != "completed" {
			t.Fatalf("expected submit completed, got %s", run.NodeStates["submit"].Status)
		}
		// Wait should never have been activated (fetch succeeded first time)
		if run.NodeStates["wait"].Status != "pending" {
			t.Fatalf("expected wait pending, got %s", run.NodeStates["wait"].Status)
		}
	})

	t.Run("first_fails_retry_succeeds", func(t *testing.T) {
		fetchExec := newRetryAwareExecutor(func(nodeID string, callNum int, inv *dag.Invocation) (map[string]string, error) {
			if nodeID == "fetch" {
				if callNum == 1 {
					return map[string]string{"status": "failed", "error": "API temporarily unavailable"}, nil
				}
				return map[string]string{"status": "success", "outcome": "1", "data": "retry worked"}, nil
			}
			return nil, fmt.Errorf("unexpected node: %s", nodeID)
		})

		engine := dag.NewEngine(slog.Default())
		engine.RegisterExecutor("api_fetch", fetchExec)
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("wait", &mockExecutor{
			outputs: map[string]string{"status": "ready"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-wait-retry",
			Name: "API Fetch Retry Success",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "wait", Type: "wait", Config: map[string]interface{}{"delay_seconds": 1}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "results.fetch.status == 'success'"},
				{From: "fetch", To: "wait", Condition: "results.fetch.status != 'success'"},
				{From: "wait", To: "fetch", MaxTraversals: 2},
				{From: "wait", To: "cancel"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}

		// Fetch ran twice (initial fail + retry success)
		fetchExec.mu.Lock()
		fetchCalls := fetchExec.calls["fetch"]
		fetchExec.mu.Unlock()
		if fetchCalls != 2 {
			t.Fatalf("expected fetch to run 2 times, got %d", fetchCalls)
		}

		// Submit should have completed after retry
		if run.NodeStates["submit"].Status != "completed" {
			t.Fatalf("expected submit completed, got %s", run.NodeStates["submit"].Status)
		}
		if testResultValue(run, "fetch", "outcome") != "1" {
			t.Fatalf("expected fetch.outcome=1, got %q", testResultValue(run, "fetch", "outcome"))
		}
	})

	t.Run("both_attempts_fail_cancel", func(t *testing.T) {
		fetchExec := newRetryAwareExecutor(func(nodeID string, callNum int, inv *dag.Invocation) (map[string]string, error) {
			if nodeID == "fetch" {
				return map[string]string{"status": "failed", "error": fmt.Sprintf("attempt %d failed", callNum)}, nil
			}
			return nil, fmt.Errorf("unexpected node: %s", nodeID)
		})

		engine := dag.NewEngine(slog.Default())
		engine.RegisterExecutor("api_fetch", fetchExec)
		engine.RegisterExecutor("mock_submit", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("wait", &mockExecutor{
			outputs: map[string]string{"status": "ready"},
		})
		engine.RegisterExecutor("mock_cancel", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-wait-exhaust",
			Name: "API Fetch Retry Exhausted",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
				{ID: "wait", Type: "wait", Config: map[string]interface{}{"delay_seconds": 1}},
				{ID: "cancel", Type: "mock_cancel", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "results.fetch.status == 'success'"},
				{From: "fetch", To: "wait", Condition: "results.fetch.status != 'success'"},
				{From: "wait", To: "fetch", MaxTraversals: 2},
				{From: "wait", To: "cancel"},
			},
		}

		run, err := engine.Execute(context.Background(), bp, nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected completed, got %s", run.Status)
		}

		// Cancel should have run after retries exhausted
		if run.NodeStates["cancel"].Status != "completed" {
			t.Fatalf("expected cancel completed, got %s", run.NodeStates["cancel"].Status)
		}
		if testResultValue(run, "cancel", "cancelled") != "true" {
			t.Fatalf("expected cancel.cancelled=true, got %q", testResultValue(run, "cancel", "cancelled"))
		}

		// Submit should NOT have run
		if run.NodeStates["submit"].Status == "completed" {
			t.Fatal("submit should not have completed when all fetch attempts failed")
		}

		// Verify multiple fetch attempts occurred
		fetchExec.mu.Lock()
		fetchCalls := fetchExec.calls["fetch"]
		fetchExec.mu.Unlock()
		if fetchCalls < 2 {
			t.Fatalf("expected at least 2 fetch attempts, got %d", fetchCalls)
		}
	})
}

// ---------------------------------------------------------------------------
// 6. TestDiamondTopology
// ---------------------------------------------------------------------------

func TestDiamondTopology(t *testing.T) {
	tracker := &trackingExecutor{outputs: map[string]string{"done": "yes"}}

	engine := dag.NewEngine(slog.Default())
	engine.RegisterExecutor("start_node", &mockExecutor{
		outputs: map[string]string{"status": "success"},
	})
	engine.RegisterExecutor("path_node", tracker)
	engine.RegisterExecutor("merge_node", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			// Merge sees outputs from both paths
			a := inv.Lookup("results.path_a.done")
			b := inv.Lookup("results.path_b.done")
			return map[string]string{
				"merged":    "true",
				"saw_a":     a,
				"saw_b":     b,
				"all_paths": fmt.Sprintf("a=%s,b=%s", a, b),
			}, nil
		},
	})
	engine.RegisterExecutor("mock_submit", &mockExecutor{
		outputs: map[string]string{"submitted": "true"},
	})

	bp := dag.Blueprint{
		ID:   "diamond",
		Name: "Diamond Topology",
		Nodes: []dag.NodeDef{
			{ID: "start", Type: "start_node", Config: map[string]interface{}{}},
			{ID: "path_a", Type: "path_node", Config: map[string]interface{}{}},
			{ID: "path_b", Type: "path_node", Config: map[string]interface{}{}},
			{ID: "merge", Type: "merge_node", Config: map[string]interface{}{}},
			{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "start", To: "path_a"},
			{From: "start", To: "path_b"},
			{From: "path_a", To: "merge"},
			{From: "path_b", To: "merge"},
			{From: "merge", To: "submit"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}

	// All nodes should complete
	for _, nid := range []string{"start", "path_a", "path_b", "merge", "submit"} {
		if run.NodeStates[nid].Status != "completed" {
			t.Fatalf("expected %s completed, got %s", nid, run.NodeStates[nid].Status)
		}
	}

	// Merge should have run exactly once
	mergeCount := 0
	for _, nid := range tracker.ran {
		if nid == "merge" {
			mergeCount++
		}
	}
	// merge uses configAwareExecutor, not tracker; check via context
	if testResultValue(run, "merge", "merged") != "true" {
		t.Fatalf("expected merge.merged=true, got %q", testResultValue(run, "merge", "merged"))
	}
	if testResultValue(run, "merge", "saw_a") != "yes" {
		t.Fatalf("merge should see path_a output, got %q", testResultValue(run, "merge", "saw_a"))
	}
	if testResultValue(run, "merge", "saw_b") != "yes" {
		t.Fatalf("merge should see path_b output, got %q", testResultValue(run, "merge", "saw_b"))
	}

	// Both parallel paths ran
	if !tracker.didRun("path_a") {
		t.Fatal("path_a should have run")
	}
	if !tracker.didRun("path_b") {
		t.Fatal("path_b should have run")
	}
}

// ---------------------------------------------------------------------------
// 7. TestLongChain
// ---------------------------------------------------------------------------

func TestLongChain(t *testing.T) {
	engine := dag.NewEngine(slog.Default())

	// Each node reads the previous node's output and appends to it
	chainExec := &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			label, _ := config["label"].(string)
			prevKey, _ := config["prev_key"].(string)

			accumulated := ""
			if prevKey != "" {
				accumulated = inv.Lookup(prevKey)
			}
			if accumulated != "" {
				accumulated += "," + label
			} else {
				accumulated = label
			}
			return map[string]string{
				"status": "success",
				"chain":  accumulated,
				"step":   label,
			}, nil
		},
	}

	engine.RegisterExecutor("chain_step", chainExec)
	engine.RegisterExecutor("mock_submit", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			finalChain := inv.Lookup("results.d.chain")
			return map[string]string{
				"submitted":   "true",
				"final_chain": finalChain,
			}, nil
		},
	})

	bp := dag.Blueprint{
		ID:   "long-chain",
		Name: "Five Node Chain",
		Nodes: []dag.NodeDef{
			{ID: "a", Type: "chain_step", Config: map[string]interface{}{"label": "a", "prev_key": ""}},
			{ID: "b", Type: "chain_step", Config: map[string]interface{}{"label": "b", "prev_key": "results.a.chain"}},
			{ID: "c", Type: "chain_step", Config: map[string]interface{}{"label": "c", "prev_key": "results.b.chain"}},
			{ID: "d", Type: "chain_step", Config: map[string]interface{}{"label": "d", "prev_key": "results.c.chain"}},
			{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "a", To: "b"},
			{From: "b", To: "c"},
			{From: "c", To: "d"},
			{From: "d", To: "submit"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}

	// All nodes should complete
	for _, nid := range []string{"a", "b", "c", "d", "submit"} {
		if run.NodeStates[nid].Status != "completed" {
			t.Fatalf("expected %s completed, got %s", nid, run.NodeStates[nid].Status)
		}
	}

	// Context should accumulate outputs from each step
	if testResultValue(run, "a", "chain") != "a" {
		t.Fatalf("expected a.chain=a, got %q", testResultValue(run, "a", "chain"))
	}
	if testResultValue(run, "b", "chain") != "a,b" {
		t.Fatalf("expected b.chain=a,b, got %q", testResultValue(run, "b", "chain"))
	}
	if testResultValue(run, "c", "chain") != "a,b,c" {
		t.Fatalf("expected c.chain=a,b,c, got %q", testResultValue(run, "c", "chain"))
	}
	if testResultValue(run, "d", "chain") != "a,b,c,d" {
		t.Fatalf("expected d.chain=a,b,c,d, got %q", testResultValue(run, "d", "chain"))
	}

	// Submit should capture the full chain
	if testResultValue(run, "submit", "final_chain") != "a,b,c,d" {
		t.Fatalf("expected submit.final_chain=a,b,c,d, got %q", testResultValue(run, "submit", "final_chain"))
	}
}

// ---------------------------------------------------------------------------
// 8. TestErrorContinueChain
// ---------------------------------------------------------------------------

func TestErrorContinueChain(t *testing.T) {
	engine := dag.NewEngine(slog.Default())

	engine.RegisterExecutor("api_fetch", &mockExecutor{
		err: fmt.Errorf("DNS resolution failed"),
	})
	engine.RegisterExecutor("llm_call", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			// Judge runs even though fetch failed (on_error=continue)
			fetchStatus := inv.Lookup("results.fetch.status")
			fetchError := inv.Lookup("results.fetch.error")
			return map[string]string{
				"status":             "success",
				"outcome":            "inconclusive",
				"reasoning":          "Fetch failed, judging with limited context",
				"upstream_status":    fetchStatus,
				"upstream_error_msg": fetchError,
			}, nil
		},
	})
	engine.RegisterExecutor("mock_submit", &mockExecutor{
		outputs: map[string]string{"submitted": "true"},
	})

	bp := dag.Blueprint{
		ID:   "error-continue",
		Name: "Error with Continue",
		Nodes: []dag.NodeDef{
			{ID: "fetch", Type: "api_fetch", OnError: "continue", Config: map[string]interface{}{}},
			{ID: "judge", Type: "llm_call", Config: map[string]interface{}{}},
			{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "fetch", To: "judge"},
			{From: "judge", To: "submit"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}

	// Fetch should be marked as failed in node states
	if run.NodeStates["fetch"].Status != "failed" {
		t.Fatalf("expected fetch failed, got %s", run.NodeStates["fetch"].Status)
	}

	// But judge still ran because on_error=continue
	if run.NodeStates["judge"].Status != "completed" {
		t.Fatalf("expected judge completed, got %s", run.NodeStates["judge"].Status)
	}
	if run.NodeStates["submit"].Status != "completed" {
		t.Fatalf("expected submit completed, got %s", run.NodeStates["submit"].Status)
	}

	// Judge should have seen the failure context
	if testResultValue(run, "fetch", "status") != "failed" {
		t.Fatalf("expected fetch.status=failed in context, got %q", testResultValue(run, "fetch", "status"))
	}
	if testResultValue(run, "fetch", "error") != "DNS resolution failed" {
		t.Fatalf("expected fetch.error in context, got %q", testResultValue(run, "fetch", "error"))
	}
	if testResultValue(run, "judge", "upstream_status") != "failed" {
		t.Fatalf("expected judge to see upstream failure, got %q", testResultValue(run, "judge", "upstream_status"))
	}
	if testResultValue(run, "judge", "upstream_error_msg") != "DNS resolution failed" {
		t.Fatalf("expected judge to see upstream error msg, got %q", testResultValue(run, "judge", "upstream_error_msg"))
	}
}

// ---------------------------------------------------------------------------
// 9. TestContextInterpolation (end-to-end with engine)
// ---------------------------------------------------------------------------

func TestContextInterpolationBlueprint(t *testing.T) {
	engine := dag.NewEngine(slog.Default())

	// api_fetch reads its config URL and interpolates it using the context
	engine.RegisterExecutor("api_fetch", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			urlTemplate, _ := config["url"].(string)
			interpolatedURL := inv.Interpolate(urlTemplate)

			return map[string]string{
				"status":            "success",
				"outcome":           "1",
				"url_used":          interpolatedURL,
				"original_template": urlTemplate,
			}, nil
		},
	})
	engine.RegisterExecutor("mock_submit", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			urlUsed := inv.Lookup("results.fetch.url_used")
			return map[string]string{
				"submitted":  "true",
				"url_logged": urlUsed,
			}, nil
		},
	})

	bp := dag.Blueprint{
		ID:   "interpolation-test",
		Name: "Context Interpolation",
		Nodes: []dag.NodeDef{
			{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{
				"url": "https://api.example.com/resolve?q={{inputs.market_question}}&id={{inputs.market_app_id}}",
			}},
			{ID: "submit", Type: "mock_submit", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "fetch", To: "submit"},
		},
		Inputs: []dag.InputDef{
			{Name: "market_question", Label: "Market Question", Required: true},
			{Name: "market_app_id", Label: "Market App ID", Required: true},
		},
	}

	run, err := engine.Execute(context.Background(), bp, map[string]string{
		"market_question": "Will BTC hit 100k?",
		"market_app_id":   "42",
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}

	// Verify the URL was interpolated correctly
	expectedURL := "https://api.example.com/resolve?q=Will BTC hit 100k?&id=42"
	if testResultValue(run, "fetch", "url_used") != expectedURL {
		t.Fatalf("expected interpolated URL %q, got %q", expectedURL, testResultValue(run, "fetch", "url_used"))
	}

	// Verify the original template was preserved
	if testResultValue(run, "fetch", "original_template") != "https://api.example.com/resolve?q={{inputs.market_question}}&id={{inputs.market_app_id}}" {
		t.Fatalf("expected original template preserved, got %q", testResultValue(run, "fetch", "original_template"))
	}

	// Verify submit saw the interpolated URL
	if testResultValue(run, "submit", "url_logged") != expectedURL {
		t.Fatalf("expected submit to see interpolated URL, got %q", testResultValue(run, "submit", "url_logged"))
	}

	// Inputs live on Run.Inputs, not on a synthetic "input" node.
	if run.Inputs["market_question"] != "Will BTC hit 100k?" {
		t.Fatalf("expected market_question input on Run.Inputs, got %q", run.Inputs["market_question"])
	}
}

// ---------------------------------------------------------------------------
// Additional: TestContextInterpolationChained
// Verify {{node.output}} interpolation works across multiple stages
// ---------------------------------------------------------------------------

func TestContextInterpolationChained(t *testing.T) {
	engine := dag.NewEngine(slog.Default())

	engine.RegisterExecutor("api_fetch", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			urlTemplate, _ := config["url"].(string)
			interpolatedURL := inv.Interpolate(urlTemplate)
			return map[string]string{
				"status":   "success",
				"url_used": interpolatedURL,
				"price":    "105000",
			}, nil
		},
	})
	engine.RegisterExecutor("llm_call", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			promptTemplate, _ := config["prompt"].(string)
			interpolatedPrompt := inv.Interpolate(promptTemplate)
			return map[string]string{
				"status":      "success",
				"outcome":     "1",
				"prompt_used": interpolatedPrompt,
			}, nil
		},
	})

	bp := dag.Blueprint{
		ID:   "chained-interp",
		Name: "Chained Interpolation",
		Nodes: []dag.NodeDef{
			{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{
				"url": "https://api.example.com/price?q={{inputs.market_question}}",
			}},
			{ID: "judge", Type: "llm_call", Config: map[string]interface{}{
				"prompt": "The price is {{results.fetch.price}}. Did BTC hit 100k? Question: {{inputs.market_question}}",
			}},
		},
		Edges: []dag.EdgeDef{
			{From: "fetch", To: "judge"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, map[string]string{
		"market_question": "Will BTC hit 100k?",
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}

	// Judge prompt should contain the fetched price and the original input
	expectedPrompt := "The price is 105000. Did BTC hit 100k? Question: Will BTC hit 100k?"
	if testResultValue(run, "judge", "prompt_used") != expectedPrompt {
		t.Fatalf("expected chained interpolation %q, got %q", expectedPrompt, testResultValue(run, "judge", "prompt_used"))
	}
}

func TestMapTopologyBatchedSubmit(t *testing.T) {
	engine := dag.NewEngine(slog.Default())
	engine.RegisterExecutor("score_batch", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			return map[string]string{
				"status":     "success",
				"batch_size": inv.Lookup("inputs.batch_item_count"),
				"start":      inv.Lookup("inputs.batch_start_index"),
				"end":        inv.Lookup("inputs.batch_end_index"),
			}, nil
		},
	})
	engine.RegisterExecutor("return", executors.NewReturnExecutor())
	engine.RegisterExecutor("map", executors.NewMapExecutor(engine))

	inline := &dag.Blueprint{
		Nodes: []dag.NodeDef{
			{ID: "score", Type: "score_batch", Config: map[string]interface{}{}},
			{ID: "out", Type: "return", Config: map[string]interface{}{
				"value": map[string]interface{}{
					"status":     "success",
					"batch_size": "{{results.score.batch_size}}",
					"start":      "{{results.score.start}}",
					"end":        "{{results.score.end}}",
				},
			}},
		},
		Edges: []dag.EdgeDef{{From: "score", To: "out"}},
	}
	bp := dag.Blueprint{
		ID: "batched-map-submit",
		Nodes: []dag.NodeDef{
			{ID: "claims", Type: "map", Config: map[string]interface{}{
				"items_key":       "inputs.claims_json",
				"batch_size":      2,
				"max_concurrency": 1,
				"inline":          inline,
			}},
			{ID: "submit", Type: "return", Config: map[string]interface{}{
				"value": map[string]interface{}{
					"status":            "success",
					"completed_batches": "{{results.claims.completed_batches}}",
				},
			}},
		},
		Edges: []dag.EdgeDef{{From: "claims", To: "submit", Condition: "results.claims.status == 'success'"}},
	}

	run, err := engine.Execute(context.Background(), bp, map[string]string{
		"claims_json": `["a","b","c","d","e"]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("run status = %s, want completed", run.Status)
	}
	if testResultValue(run, "claims", "total_batches") != "3" {
		t.Fatalf("total_batches = %q, want 3", testResultValue(run, "claims", "total_batches"))
	}
	if testResultValue(run, "claims", "total_items") != "5" {
		t.Fatalf("total_items = %q, want 5", testResultValue(run, "claims", "total_items"))
	}
	if len(run.Return) == 0 {
		t.Fatalf("expected non-empty run return: %+v", run.Results)
	}
	var payload map[string]any
	if err := json.Unmarshal(run.Return, &payload); err != nil {
		t.Fatalf("unmarshal return: %v", err)
	}
	if payload["completed_batches"] != "3" {
		t.Fatalf("payload.completed_batches = %v, want 3", payload["completed_batches"])
	}
}

func TestMapTopologyNestedMapWithBackEdgeRetry(t *testing.T) {
	engine := dag.NewEngine(slog.Default())
	engine.RegisterExecutor("map", executors.NewMapExecutor(engine))
	engine.RegisterExecutor("extract_claims", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			var outerBatch []json.RawMessage
			if err := json.Unmarshal([]byte(inv.Lookup("inputs.batch")), &outerBatch); err != nil {
				return nil, err
			}
			if len(outerBatch) != 1 {
				return nil, fmt.Errorf("expected one group per outer batch, got %d", len(outerBatch))
			}
			return map[string]string{
				"status":      "success",
				"claims_json": string(outerBatch[0]),
			}, nil
		},
	})
	engine.RegisterExecutor("process_claim", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			if inv.Lookup("results.process.history") == "" {
				return map[string]string{"status": "retry", "attempt": "1"}, nil
			}
			return map[string]string{"status": "success", "attempt": "2"}, nil
		},
	})
	engine.RegisterExecutor("retry_gate", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			if inv.Lookup("results.process.status") == "retry" {
				return map[string]string{"status": "retry", "retry": "true"}, nil
			}
			return map[string]string{"status": "success", "retry": "false"}, nil
		},
	})
	engine.RegisterExecutor("done", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			return map[string]string{
				"status":       "success",
				"process_runs": inv.Lookup("results.process.history"),
			}, nil
		},
	})
	engine.RegisterExecutor("group_done", &configAwareExecutor{
		handler: func(config map[string]interface{}, inv *dag.Invocation) (map[string]string, error) {
			return map[string]string{
				"status":          "success",
				"inner_completed": inv.Lookup("results.inner.completed_batches"),
				"inner_results":   inv.Lookup("results.inner.results"),
			}, nil
		},
	})
	engine.RegisterExecutor("return", executors.NewReturnExecutor())

	innerInline := &dag.Blueprint{
		Nodes: []dag.NodeDef{
			{ID: "process", Type: "process_claim", Config: map[string]interface{}{}},
			{ID: "gate", Type: "retry_gate", Config: map[string]interface{}{}},
			{ID: "done", Type: "done", Config: map[string]interface{}{}},
			{ID: "out", Type: "return", Config: map[string]interface{}{
				"value": map[string]interface{}{
					"status":         "success",
					"process_status": "{{results.process.status}}",
					"process_runs":   "{{results.process.history}}",
				},
			}},
		},
		Edges: []dag.EdgeDef{
			{From: "process", To: "gate"},
			{From: "gate", To: "process", Condition: "results.gate.retry == 'true'", MaxTraversals: 1},
			{From: "gate", To: "done", Condition: "results.gate.retry != 'true'"},
			{From: "done", To: "out"},
		},
	}
	outerInline := &dag.Blueprint{
		Nodes: []dag.NodeDef{
			{ID: "extract", Type: "extract_claims", Config: map[string]interface{}{}},
			{ID: "inner", Type: "map", Config: map[string]interface{}{
				"items_key":       "results.extract.claims_json",
				"batch_size":      1,
				"max_concurrency": 2,
				"inline":          innerInline,
			}},
			{ID: "group_done", Type: "group_done", Config: map[string]interface{}{}},
			{ID: "out", Type: "return", Config: map[string]interface{}{
				"value": map[string]interface{}{
					"status":          "success",
					"inner_completed": "{{results.inner.completed_batches}}",
					"inner_results":   "{{results.inner.results}}",
				},
			}},
		},
		Edges: []dag.EdgeDef{
			{From: "extract", To: "inner"},
			{From: "inner", To: "group_done"},
			{From: "group_done", To: "out"},
		},
	}
	bp := dag.Blueprint{
		ID: "nested-map-retry",
		Nodes: []dag.NodeDef{
			{ID: "groups", Type: "map", Config: map[string]interface{}{
				"items_key":       "inputs.groups_json",
				"batch_size":      1,
				"max_concurrency": 0,
				"inline":          outerInline,
			}},
			{ID: "submit", Type: "return", Config: map[string]interface{}{
				"value": map[string]interface{}{
					"status":            "success",
					"completed_batches": "{{results.groups.completed_batches}}",
				},
			}},
		},
		Edges: []dag.EdgeDef{{From: "groups", To: "submit"}},
	}

	run, err := engine.Execute(context.Background(), bp, map[string]string{
		"groups_json": `[["a","b"],["c"]]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("run status = %s, want completed", run.Status)
	}
	if testResultValue(run, "groups", "completed_batches") != "2" {
		t.Fatalf("outer completed_batches = %q, want 2", testResultValue(run, "groups", "completed_batches"))
	}

	var outerResults []map[string]interface{}
	if err := json.Unmarshal([]byte(testResultValue(run, "groups", "results")), &outerResults); err != nil {
		t.Fatal(err)
	}
	if len(outerResults) != 2 {
		t.Fatalf("outer result count = %d, want 2", len(outerResults))
	}
	expectedInnerCounts := []string{"2", "1"}
	for i, outer := range outerResults {
		ret, ok := outer["return"].(map[string]interface{})
		if !ok {
			t.Fatalf("outer result %d missing return payload: %+v", i, outer)
		}
		if ret["inner_completed"] != expectedInnerCounts[i] {
			t.Fatalf("outer result %d inner_completed = %v, want %s", i, ret["inner_completed"], expectedInnerCounts[i])
		}
		innerRaw, ok := ret["inner_results"].(string)
		if !ok || innerRaw == "" {
			t.Fatalf("outer result %d missing inner results: %+v", i, ret)
		}
		var innerResults []map[string]interface{}
		if err := json.Unmarshal([]byte(innerRaw), &innerResults); err != nil {
			t.Fatal(err)
		}
		for j, inner := range innerResults {
			innerRet, ok := inner["return"].(map[string]interface{})
			if !ok {
				t.Fatalf("inner result %d/%d missing return payload: %+v", i, j, inner)
			}
			if innerRet["process_status"] != "success" {
				t.Fatalf("inner result %d/%d process_status = %v, want success", i, j, innerRet["process_status"])
			}
			if innerRet["process_runs"] == "" {
				t.Fatalf("inner result %d/%d missing retry history", i, j)
			}
		}
	}
}
