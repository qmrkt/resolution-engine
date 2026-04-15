package main

import (
	"encoding/json"
	"testing"

	"github.com/question-market/resolution-engine/dag"
)

func mustBlueprint(t *testing.T, text string) (dag.Blueprint, []byte) {
	t.Helper()
	raw := []byte(text)
	var bp dag.Blueprint
	if err := json.Unmarshal(raw, &bp); err != nil {
		t.Fatalf("unmarshal blueprint: %v", err)
	}
	return bp, raw
}

func TestValidateResolutionBlueprintAcceptsValidBlueprint(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"valid",
	  "version":1,
	  "nodes":[
	    {"id":"judge","type":"await_signal","config":{"signal_type":"human_judgment.responded","required_payload":["outcome"],"default_outputs":{"status":"responded"},"timeout_seconds":3600}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"judge.outcome"}}
	  ],
	  "edges":[{"from":"judge","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintRejectsDuplicateNodeID(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"dup",
	  "version":1,
	  "nodes":[
	    {"id":"judge","type":"await_signal","config":{"signal_type":"human_judgment.responded","required_payload":["outcome"],"default_outputs":{"status":"responded"},"timeout_seconds":3600}},
	    {"id":"judge","type":"submit_result","config":{"outcome_key":"judge.outcome"}}
	  ],
	  "edges":[]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected duplicate node blueprint to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsBackEdgeWithoutMaxTraversals(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"loop",
	  "version":1,
	  "nodes":[
	    {"id":"fetch","type":"api_fetch","config":{"url":"https://example.com","json_path":"result.value"}},
	    {"id":"wait","type":"wait","config":{"duration_seconds":30,"mode":"sleep"}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"fetch.outcome"}}
	  ],
	  "edges":[
	    {"from":"fetch","to":"wait"},
	    {"from":"wait","to":"fetch"},
	    {"from":"fetch","to":"submit","condition":"fetch.status == 'success'"}
	  ]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected loop blueprint to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsNoTerminalPath(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"noterminal",
	  "version":1,
	  "nodes":[
	    {"id":"judge","type":"await_signal","config":{"signal_type":"human_judgment.responded","required_payload":["outcome"],"default_outputs":{"status":"responded"},"timeout_seconds":3600}},
	    {"id":"other","type":"llm_call","config":{"prompt":"Judge this."}}
	  ],
	  "edges":[{"from":"judge","to":"other"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected blueprint without terminal path to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsAwaitSignalMissingSignalType(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"await",
	  "version":1,
	  "nodes":[
	    {"id":"judge","type":"await_signal","config":{"required_payload":["outcome"],"timeout_seconds":3600}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"judge.outcome"}}
	  ],
	  "edges":[{"from":"judge","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected await_signal without signal_type to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsWaitNowWithDeferMode(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"wait-invalid",
	  "version":1,
	  "nodes":[
	    {"id":"wait","type":"wait","config":{"duration_seconds":30,"mode":"defer","start_from":"now"}},
	    {"id":"defer","type":"defer_resolution","config":{"reason":"later"}}
	  ],
	  "edges":[{"from":"wait","to":"defer","condition":"wait.status == 'waiting'"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected invalid wait blueprint to be invalid")
	}
}

func TestValidateResolutionBlueprintAcceptsLLMCallAllowedOutcomesKey(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"llm-allowed-outcomes",
	  "version":1,
	  "nodes":[
	    {"id":"judge","type":"llm_call","config":{"prompt":"Judge this.","allowed_outcomes_key":"market.outcomes.json"}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"judge.outcome"}}
	  ],
	  "edges":[{"from":"judge","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintAcceptsAPIFetchWithoutJSONPath(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"api-raw",
	  "version":1,
	  "nodes":[
	    {"id":"fetch","type":"api_fetch","config":{"url":"https://example.com","method":"POST","body":"hello"}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"fetch.outcome"}}
	  ],
	  "edges":[{"from":"fetch","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid raw api_fetch blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintRejectsUnsupportedAPIFetchMethod(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"api-method-invalid",
	  "version":1,
	  "nodes":[
	    {"id":"fetch","type":"api_fetch","config":{"url":"https://example.com","method":"TRACE"}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"fetch.outcome"}}
	  ],
	  "edges":[{"from":"fetch","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected unsupported method blueprint to be invalid")
	}
}
