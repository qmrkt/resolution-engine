package executors

import (
	"strings"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

func TestAwaitSignalExecutorCanSuspend(t *testing.T) {
	if !NewAwaitSignalExecutor().CanSuspend(dag.NodeDef{Type: "await_signal"}) {
		t.Fatal("await_signal must always be suspension-capable")
	}
}

func TestWaitExecutorCanSuspend(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]any
		want bool
	}{
		{"long_sleep", map[string]any{"duration_seconds": 30}, true},
		{"short_inline_sleep", map[string]any{"duration_seconds": 5}, false},
		{"zero_duration", map[string]any{"duration_seconds": 0}, false},
		{"custom_cap_forces_suspension", map[string]any{"duration_seconds": 5, "max_inline_seconds": 2}, true},
		{"defer_mode_is_runtime_dependent", map[string]any{"mode": "defer", "start_from": "deadline", "duration_seconds": 5}, true},
	}
	exec := NewWaitExecutor()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := dag.NodeDef{ID: "w", Type: "wait", Config: tc.cfg}
			if got := exec.CanSuspend(node); got != tc.want {
				t.Fatalf("WaitExecutor.CanSuspend(%+v) = %v, want %v", tc.cfg, got, tc.want)
			}
		})
	}
}

func TestAgentLoopExecutorCanSuspend(t *testing.T) {
	exec := &AgentLoopExecutor{}
	cases := []struct {
		name   string
		config any
		want   bool
	}{
		{"sync", map[string]any{"prompt": "p"}, false},
		{"async_true", map[string]any{"prompt": "p", "async": true}, true},
		{"invalid_config_defaults_to_false", "not-a-map", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := exec.CanSuspend(dag.NodeDef{Type: "agent_loop", Config: tc.config}); got != tc.want {
				t.Fatalf("CanSuspend = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEngineValidateNoSuspensionCapableNodesEndToEnd registers the real
// suspension-capable executors, then asserts the engine-level validator
// enumerates the expected offenders and leaves non-suspending nodes alone.
// This is the closest thing to a cross-engine conformance check — add a new
// suspending executor without implementing CanSuspend and it falls out.
func TestEngineValidateNoSuspensionCapableNodesEndToEnd(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("wait", NewWaitExecutor())
	engine.RegisterExecutor("await_signal", NewAwaitSignalExecutor())
	engine.RegisterExecutor("return", NewReturnExecutor())
	engine.RegisterExecutor("agent_loop", &AgentLoopExecutor{})
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())

	bp := dag.Blueprint{
		Nodes: []dag.NodeDef{
			{ID: "zeta", Type: "wait", Config: map[string]any{"duration_seconds": 30}},
			{ID: "alpha", Type: "agent_loop", Config: map[string]any{"prompt": "p", "async": true}},
			{ID: "gamma", Type: "cel_eval"},
			{ID: "omega", Type: "agent_loop", Config: map[string]any{"prompt": "p"}},
			{ID: "mu", Type: "return", Config: map[string]any{"value": map[string]any{"status": "deferred"}}},
		},
	}
	err := engine.ValidateNoSuspensionCapableNodes(bp)
	if err == nil {
		t.Fatal("expected error listing suspending offenders")
	}
	msg := err.Error()
	if !strings.Contains(msg, "alpha (agent_loop), zeta (wait)") {
		t.Fatalf("offender list missing or out of order: %q", msg)
	}
	for _, id := range []string{"gamma", "omega", "mu"} {
		if strings.Contains(msg, id) {
			t.Fatalf("non-suspending node %q reported as offender: %q", id, msg)
		}
	}

	clean := dag.Blueprint{
		Nodes: []dag.NodeDef{
			{ID: "ok1", Type: "cel_eval"},
			{ID: "ok2", Type: "agent_loop", Config: map[string]any{"prompt": "p"}},
		},
	}
	if err := engine.ValidateNoSuspensionCapableNodes(clean); err != nil {
		t.Fatalf("unexpected error on clean blueprint: %v", err)
	}
}
