package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/question-market/resolution-engine/dag"
	"github.com/question-market/resolution-engine/executors"
)

// ---------------------------------------------------------------------------
// Mock executors
// ---------------------------------------------------------------------------

// configAwareExecutor reads node config to decide behavior.
type configAwareExecutor struct {
	handler func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error)
}

func (e *configAwareExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, _ := node.Config.(map[string]interface{})
	outputs, err := e.handler(cfg, execCtx)
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

func (e *trackingExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
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
	behavior func(nodeID string, callNum int, execCtx *dag.Context) (map[string]string, error)
}

func newRetryAwareExecutor(behavior func(string, int, *dag.Context) (map[string]string, error)) *retryAwareExecutor {
	return &retryAwareExecutor{
		calls:    make(map[string]int),
		behavior: behavior,
	}
}

func (e *retryAwareExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	e.mu.Lock()
	e.calls[node.ID]++
	callNum := e.calls[node.ID]
	e.mu.Unlock()

	outputs, err := e.behavior(node.ID, callNum, execCtx)
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"status": "success", "submitted": "true"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
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
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit", Condition: "judge.status == 'success'"},
				{From: "judge", To: "cancel", Condition: "judge.status != 'success'"},
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
		if run.Context["judge.outcome"] != "1" {
			t.Fatalf("expected judge.outcome=1, got %q", run.Context["judge.outcome"])
		}
	})

	t.Run("timeout_cancel_path", func(t *testing.T) {
		engine := dag.NewEngine(slog.Default())

		engine.RegisterExecutor("await_signal", &mockExecutor{
			outputs: map[string]string{"status": "cancelled", "reason": "Judge timed out"},
		})
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"status": "success", "submitted": "true"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
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
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit", Condition: "judge.status == 'success'"},
				{From: "judge", To: "cancel", Condition: "judge.status != 'success'"},
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
		if run.Context["cancel.cancelled"] != "true" {
			t.Fatalf("expected cancel.cancelled=true, got %q", run.Context["cancel.cancelled"])
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
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
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "fetch.status == 'success'"},
				{From: "fetch", To: "cancel", Condition: "fetch.status != 'success'"},
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
		if run.Context["fetch.outcome"] != "0" {
			t.Fatalf("expected fetch.outcome=0, got %q", run.Context["fetch.outcome"])
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
			outputs: map[string]string{"cancelled": "true", "reason": "API failure"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-fail",
			Name: "API Fetch Failure",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{
					"url": "https://api.example.com/broken",
				}},
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "fetch.status == 'success'"},
				{From: "fetch", To: "cancel", Condition: "fetch.status != 'success'"},
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
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
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit", Condition: "judge.status == 'success'"},
				{From: "judge", To: "cancel", Condition: "judge.status != 'success'"},
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
		if run.Context["judge.reasoning"] != "Based on available evidence, outcome 1 is correct" {
			t.Fatalf("expected reasoning in context, got %q", run.Context["judge.reasoning"])
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
			outputs: map[string]string{"cancelled": "true", "reason": "LLM inconclusive"},
		})

		bp := dag.Blueprint{
			ID:   "llm-call-inconclusive",
			Name: "LLM Judge Inconclusive",
			Nodes: []dag.NodeDef{
				{ID: "judge", Type: "llm_call", Config: map[string]interface{}{
					"model": "gpt-4o",
				}},
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit", Condition: "judge.status == 'success'"},
				{From: "judge", To: "cancel", Condition: "judge.status != 'success'"},
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
		if run.Context["cancel.reason"] != "LLM inconclusive" {
			t.Fatalf("expected cancel reason, got %q", run.Context["cancel.reason"])
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
			handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
				// LLM sees the fetched data in context
				data := execCtx.Get("fetch.data")
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
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
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel_fetch", Type: "cancel_market", Config: map[string]interface{}{}},
				{ID: "cancel_judge", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "judge", Condition: "fetch.status == 'success'"},
				{From: "fetch", To: "cancel_fetch", Condition: "fetch.status != 'success'"},
				{From: "judge", To: "submit", Condition: "judge.status == 'success'"},
				{From: "judge", To: "cancel_judge", Condition: "judge.status != 'success'"},
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
		if run.Context["judge.outcome"] != "1" {
			t.Fatalf("expected judge.outcome=1, got %q", run.Context["judge.outcome"])
		}
		// Verify the LLM saw the API data
		if !strings.Contains(run.Context["judge.reasoning"], "105000") {
			t.Fatalf("expected reasoning to reference API data, got %q", run.Context["judge.reasoning"])
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-llm-fail",
			Name: "API Fetch LLM Fails",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "judge", Type: "llm_call", Config: map[string]interface{}{}},
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel_fetch", Type: "cancel_market", Config: map[string]interface{}{}},
				{ID: "cancel_judge", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "judge", Condition: "fetch.status == 'success'"},
				{From: "fetch", To: "cancel_fetch", Condition: "fetch.status != 'success'"},
				{From: "judge", To: "submit", Condition: "judge.status == 'success'"},
				{From: "judge", To: "cancel_judge", Condition: "judge.status != 'success'"},
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
		engine.RegisterExecutor("submit_result", tracker)
		engine.RegisterExecutor("cancel_market", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-llm-no-run",
			Name: "API Fails, LLM Skipped",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "judge", Type: "llm_call", Config: map[string]interface{}{}},
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "cancel_fetch", Type: "cancel_market", Config: map[string]interface{}{}},
				{ID: "cancel_judge", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "judge", Condition: "fetch.status == 'success'"},
				{From: "fetch", To: "cancel_fetch", Condition: "fetch.status != 'success'"},
				{From: "judge", To: "submit", Condition: "judge.status == 'success'"},
				{From: "judge", To: "cancel_judge", Condition: "judge.status != 'success'"},
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("wait", &mockExecutor{
			outputs: map[string]string{"status": "ready"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-wait-ok",
			Name: "API Fetch with Retry - Success",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "wait", Type: "wait", Config: map[string]interface{}{"delay_seconds": 5}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "fetch.status == 'success'"},
				{From: "fetch", To: "wait", Condition: "fetch.status != 'success'"},
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
		fetchExec := newRetryAwareExecutor(func(nodeID string, callNum int, execCtx *dag.Context) (map[string]string, error) {
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
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("wait", &mockExecutor{
			outputs: map[string]string{"status": "ready"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-wait-retry",
			Name: "API Fetch Retry Success",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "wait", Type: "wait", Config: map[string]interface{}{"delay_seconds": 1}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "fetch.status == 'success'"},
				{From: "fetch", To: "wait", Condition: "fetch.status != 'success'"},
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
		if run.Context["fetch.outcome"] != "1" {
			t.Fatalf("expected fetch.outcome=1, got %q", run.Context["fetch.outcome"])
		}
	})

	t.Run("both_attempts_fail_cancel", func(t *testing.T) {
		fetchExec := newRetryAwareExecutor(func(nodeID string, callNum int, execCtx *dag.Context) (map[string]string, error) {
			if nodeID == "fetch" {
				return map[string]string{"status": "failed", "error": fmt.Sprintf("attempt %d failed", callNum)}, nil
			}
			return nil, fmt.Errorf("unexpected node: %s", nodeID)
		})

		engine := dag.NewEngine(slog.Default())
		engine.RegisterExecutor("api_fetch", fetchExec)
		engine.RegisterExecutor("submit_result", &mockExecutor{
			outputs: map[string]string{"submitted": "true"},
		})
		engine.RegisterExecutor("wait", &mockExecutor{
			outputs: map[string]string{"status": "ready"},
		})
		engine.RegisterExecutor("cancel_market", &mockExecutor{
			outputs: map[string]string{"cancelled": "true"},
		})

		bp := dag.Blueprint{
			ID:   "api-fetch-wait-exhaust",
			Name: "API Fetch Retry Exhausted",
			Nodes: []dag.NodeDef{
				{ID: "fetch", Type: "api_fetch", Config: map[string]interface{}{}},
				{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
				{ID: "wait", Type: "wait", Config: map[string]interface{}{"delay_seconds": 1}},
				{ID: "cancel", Type: "cancel_market", Config: map[string]interface{}{}},
			},
			Edges: []dag.EdgeDef{
				{From: "fetch", To: "submit", Condition: "fetch.status == 'success'"},
				{From: "fetch", To: "wait", Condition: "fetch.status != 'success'"},
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
		if run.Context["cancel.cancelled"] != "true" {
			t.Fatalf("expected cancel.cancelled=true, got %q", run.Context["cancel.cancelled"])
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
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			// Merge sees outputs from both paths
			a := execCtx.Get("path_a.done")
			b := execCtx.Get("path_b.done")
			return map[string]string{
				"merged":    "true",
				"saw_a":     a,
				"saw_b":     b,
				"all_paths": fmt.Sprintf("a=%s,b=%s", a, b),
			}, nil
		},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{
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
			{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
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
	if run.Context["merge.merged"] != "true" {
		t.Fatalf("expected merge.merged=true, got %q", run.Context["merge.merged"])
	}
	if run.Context["merge.saw_a"] != "yes" {
		t.Fatalf("merge should see path_a output, got %q", run.Context["merge.saw_a"])
	}
	if run.Context["merge.saw_b"] != "yes" {
		t.Fatalf("merge should see path_b output, got %q", run.Context["merge.saw_b"])
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
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			label, _ := config["label"].(string)
			prevKey, _ := config["prev_key"].(string)

			accumulated := ""
			if prevKey != "" {
				accumulated = execCtx.Get(prevKey)
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
	engine.RegisterExecutor("submit_result", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			finalChain := execCtx.Get("d.chain")
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
			{ID: "b", Type: "chain_step", Config: map[string]interface{}{"label": "b", "prev_key": "a.chain"}},
			{ID: "c", Type: "chain_step", Config: map[string]interface{}{"label": "c", "prev_key": "b.chain"}},
			{ID: "d", Type: "chain_step", Config: map[string]interface{}{"label": "d", "prev_key": "c.chain"}},
			{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
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
	if run.Context["a.chain"] != "a" {
		t.Fatalf("expected a.chain=a, got %q", run.Context["a.chain"])
	}
	if run.Context["b.chain"] != "a,b" {
		t.Fatalf("expected b.chain=a,b, got %q", run.Context["b.chain"])
	}
	if run.Context["c.chain"] != "a,b,c" {
		t.Fatalf("expected c.chain=a,b,c, got %q", run.Context["c.chain"])
	}
	if run.Context["d.chain"] != "a,b,c,d" {
		t.Fatalf("expected d.chain=a,b,c,d, got %q", run.Context["d.chain"])
	}

	// Submit should capture the full chain
	if run.Context["submit.final_chain"] != "a,b,c,d" {
		t.Fatalf("expected submit.final_chain=a,b,c,d, got %q", run.Context["submit.final_chain"])
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
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			// Judge runs even though fetch failed (on_error=continue)
			fetchStatus := execCtx.Get("fetch.status")
			fetchError := execCtx.Get("fetch.error")
			return map[string]string{
				"status":             "success",
				"outcome":            "inconclusive",
				"reasoning":          "Fetch failed, judging with limited context",
				"upstream_status":    fetchStatus,
				"upstream_error_msg": fetchError,
			}, nil
		},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{
		outputs: map[string]string{"submitted": "true"},
	})

	bp := dag.Blueprint{
		ID:   "error-continue",
		Name: "Error with Continue",
		Nodes: []dag.NodeDef{
			{ID: "fetch", Type: "api_fetch", OnError: "continue", Config: map[string]interface{}{}},
			{ID: "judge", Type: "llm_call", Config: map[string]interface{}{}},
			{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
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
	if run.Context["fetch.status"] != "failed" {
		t.Fatalf("expected fetch.status=failed in context, got %q", run.Context["fetch.status"])
	}
	if run.Context["fetch.error"] != "DNS resolution failed" {
		t.Fatalf("expected fetch.error in context, got %q", run.Context["fetch.error"])
	}
	if run.Context["judge.upstream_status"] != "failed" {
		t.Fatalf("expected judge to see upstream failure, got %q", run.Context["judge.upstream_status"])
	}
	if run.Context["judge.upstream_error_msg"] != "DNS resolution failed" {
		t.Fatalf("expected judge to see upstream error msg, got %q", run.Context["judge.upstream_error_msg"])
	}
}

// ---------------------------------------------------------------------------
// 9. TestContextInterpolation (end-to-end with engine)
// ---------------------------------------------------------------------------

func TestContextInterpolationBlueprint(t *testing.T) {
	engine := dag.NewEngine(slog.Default())

	// api_fetch reads its config URL and interpolates it using the context
	engine.RegisterExecutor("api_fetch", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			urlTemplate, _ := config["url"].(string)
			interpolatedURL := execCtx.Interpolate(urlTemplate)

			return map[string]string{
				"status":            "success",
				"outcome":           "1",
				"url_used":          interpolatedURL,
				"original_template": urlTemplate,
			}, nil
		},
	})
	engine.RegisterExecutor("submit_result", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			urlUsed := execCtx.Get("fetch.url_used")
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
				"url": "https://api.example.com/resolve?q={{market_question}}&id={{market_app_id}}",
			}},
			{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
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
	if run.Context["fetch.url_used"] != expectedURL {
		t.Fatalf("expected interpolated URL %q, got %q", expectedURL, run.Context["fetch.url_used"])
	}

	// Verify the original template was preserved
	if run.Context["fetch.original_template"] != "https://api.example.com/resolve?q={{market_question}}&id={{market_app_id}}" {
		t.Fatalf("expected original template preserved, got %q", run.Context["fetch.original_template"])
	}

	// Verify submit saw the interpolated URL
	if run.Context["submit.url_logged"] != expectedURL {
		t.Fatalf("expected submit to see interpolated URL, got %q", run.Context["submit.url_logged"])
	}

	// Inputs should also be available under input. prefix
	if run.Context["input.market_question"] != "Will BTC hit 100k?" {
		t.Fatalf("expected input.market_question in context, got %q", run.Context["input.market_question"])
	}
}

// ---------------------------------------------------------------------------
// Additional: TestContextInterpolationChained
// Verify {{node.output}} interpolation works across multiple stages
// ---------------------------------------------------------------------------

func TestContextInterpolationChained(t *testing.T) {
	engine := dag.NewEngine(slog.Default())

	engine.RegisterExecutor("api_fetch", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			urlTemplate, _ := config["url"].(string)
			interpolatedURL := execCtx.Interpolate(urlTemplate)
			return map[string]string{
				"status":   "success",
				"url_used": interpolatedURL,
				"price":    "105000",
			}, nil
		},
	})
	engine.RegisterExecutor("llm_call", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			promptTemplate, _ := config["prompt"].(string)
			interpolatedPrompt := execCtx.Interpolate(promptTemplate)
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
				"url": "https://api.example.com/price?q={{market_question}}",
			}},
			{ID: "judge", Type: "llm_call", Config: map[string]interface{}{
				"prompt": "The price is {{fetch.price}}. Did BTC hit 100k? Question: {{market_question}}",
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
	if run.Context["judge.prompt_used"] != expectedPrompt {
		t.Fatalf("expected chained interpolation %q, got %q", expectedPrompt, run.Context["judge.prompt_used"])
	}
}

func TestMapTopologyBatchedSubmit(t *testing.T) {
	engine := dag.NewEngine(slog.Default())
	engine.RegisterExecutor("score_batch", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			return map[string]string{
				"status":     "success",
				"batch_size": execCtx.Get("batch_item_count"),
				"start":      execCtx.Get("batch_start_index"),
				"end":        execCtx.Get("batch_end_index"),
			}, nil
		},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{outputs: map[string]string{"submitted": "true"}})
	engine.RegisterExecutor("map", executors.NewMapExecutor(engine))

	inline := &dag.Blueprint{
		Nodes: []dag.NodeDef{{ID: "score", Type: "score_batch", Config: map[string]interface{}{}}},
	}
	bp := dag.Blueprint{
		ID: "batched-map-submit",
		Nodes: []dag.NodeDef{
			{ID: "claims", Type: "map", Config: map[string]interface{}{
				"items_key":       "claims_json",
				"batch_size":      2,
				"max_concurrency": 1,
				"inline":          inline,
				"output_keys":     []string{"score.batch_size", "score.start", "score.end"},
			}},
			{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{{From: "claims", To: "submit", Condition: "claims.status == 'success'"}},
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
	if run.Context["claims.total_batches"] != "3" {
		t.Fatalf("total_batches = %q, want 3", run.Context["claims.total_batches"])
	}
	if run.Context["claims.total_items"] != "5" {
		t.Fatalf("total_items = %q, want 5", run.Context["claims.total_items"])
	}
	if run.Context["submit.submitted"] != "true" {
		t.Fatalf("submit did not run: %+v", run.Context)
	}
}

func TestMapTopologyNestedMapWithBackEdgeRetry(t *testing.T) {
	engine := dag.NewEngine(slog.Default())
	engine.RegisterExecutor("map", executors.NewMapExecutor(engine))
	engine.RegisterExecutor("extract_claims", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			var outerBatch []json.RawMessage
			if err := json.Unmarshal([]byte(execCtx.Get("batch")), &outerBatch); err != nil {
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
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			if execCtx.Get("process._runs") == "" {
				return map[string]string{"status": "retry", "attempt": "1"}, nil
			}
			return map[string]string{"status": "success", "attempt": "2"}, nil
		},
	})
	engine.RegisterExecutor("retry_gate", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			if execCtx.Get("process.status") == "retry" {
				return map[string]string{"status": "retry", "retry": "true"}, nil
			}
			return map[string]string{"status": "success", "retry": "false"}, nil
		},
	})
	engine.RegisterExecutor("done", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			return map[string]string{
				"status":       "success",
				"process_runs": execCtx.Get("process._runs"),
			}, nil
		},
	})
	engine.RegisterExecutor("group_done", &configAwareExecutor{
		handler: func(config map[string]interface{}, execCtx *dag.Context) (map[string]string, error) {
			return map[string]string{
				"status":          "success",
				"inner_completed": execCtx.Get("inner.completed_batches"),
				"inner_results":   execCtx.Get("inner.results"),
			}, nil
		},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{outputs: map[string]string{"submitted": "true"}})

	innerInline := &dag.Blueprint{
		Nodes: []dag.NodeDef{
			{ID: "process", Type: "process_claim", Config: map[string]interface{}{}},
			{ID: "gate", Type: "retry_gate", Config: map[string]interface{}{}},
			{ID: "done", Type: "done", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "process", To: "gate"},
			{From: "gate", To: "process", Condition: "gate.retry == 'true'", MaxTraversals: 1},
			{From: "gate", To: "done", Condition: "gate.retry != 'true'"},
		},
	}
	outerInline := &dag.Blueprint{
		Nodes: []dag.NodeDef{
			{ID: "extract", Type: "extract_claims", Config: map[string]interface{}{}},
			{ID: "inner", Type: "map", Config: map[string]interface{}{
				"items_key":       "extract.claims_json",
				"batch_size":      1,
				"max_concurrency": 2,
				"inline":          innerInline,
				"output_keys":     []string{"done.status", "process.status", "process._runs"},
			}},
			{ID: "group_done", Type: "group_done", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "extract", To: "inner"},
			{From: "inner", To: "group_done"},
		},
	}
	bp := dag.Blueprint{
		ID: "nested-map-retry",
		Nodes: []dag.NodeDef{
			{ID: "groups", Type: "map", Config: map[string]interface{}{
				"items_key":       "groups_json",
				"batch_size":      1,
				"max_concurrency": 0,
				"inline":          outerInline,
				"output_keys":     []string{"group_done.inner_completed", "group_done.inner_results"},
			}},
			{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
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
	if run.Context["groups.completed_batches"] != "2" {
		t.Fatalf("outer completed_batches = %q, want 2", run.Context["groups.completed_batches"])
	}

	var outerResults []map[string]interface{}
	if err := json.Unmarshal([]byte(run.Context["groups.results"]), &outerResults); err != nil {
		t.Fatal(err)
	}
	if len(outerResults) != 2 {
		t.Fatalf("outer result count = %d, want 2", len(outerResults))
	}
	expectedInnerCounts := []string{"2", "1"}
	for i, outer := range outerResults {
		outputs, ok := outer["outputs"].(map[string]interface{})
		if !ok {
			t.Fatalf("outer result %d missing outputs: %+v", i, outer)
		}
		if outputs["group_done.inner_completed"] != expectedInnerCounts[i] {
			t.Fatalf("outer result %d inner_completed = %v, want %s", i, outputs["group_done.inner_completed"], expectedInnerCounts[i])
		}
		innerRaw, ok := outputs["group_done.inner_results"].(string)
		if !ok || innerRaw == "" {
			t.Fatalf("outer result %d missing inner results: %+v", i, outputs)
		}
		var innerResults []map[string]interface{}
		if err := json.Unmarshal([]byte(innerRaw), &innerResults); err != nil {
			t.Fatal(err)
		}
		for j, inner := range innerResults {
			innerOutputs, ok := inner["outputs"].(map[string]interface{})
			if !ok {
				t.Fatalf("inner result %d/%d missing outputs: %+v", i, j, inner)
			}
			if innerOutputs["process.status"] != "success" {
				t.Fatalf("inner result %d/%d process.status = %v, want success", i, j, innerOutputs["process.status"])
			}
			if innerOutputs["process._runs"] == "" {
				t.Fatalf("inner result %d/%d missing retry history", i, j)
			}
		}
	}
}
