package main

import (
	"encoding/json"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
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

func TestValidateResolutionBlueprintAcceptsAgentLoopResolution(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"agent-loop",
	  "version":1,
	  "nodes":[
	    {"id":"agent","type":"agent_loop","config":{"prompt":"Resolve {{market.question}}.","output_mode":"resolution","allowed_outcomes_key":"market.outcomes.json"}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"agent.outcome"}}
	  ],
	  "edges":[{"from":"agent","to":"submit","condition":"agent.status == 'success'"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid agent_loop blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintRejectsAgentLoopStructuredWithoutSchema(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"agent-loop-structured",
	  "version":1,
	  "nodes":[
	    {"id":"agent","type":"agent_loop","config":{"prompt":"Extract a result.","output_mode":"structured"}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"agent.output.outcome"}}
	  ],
	  "edges":[{"from":"agent","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected structured agent_loop without schema to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsUnknownAgentLoopBuiltin(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"agent-loop-tool",
	  "version":1,
	  "nodes":[
	    {"id":"agent","type":"agent_loop","config":{"prompt":"Resolve.","tools":[{"name":"made_up","kind":"builtin"}]}},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"agent failed"}}
	  ],
	  "edges":[{"from":"agent","to":"cancel"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected unknown agent_loop builtin to be invalid")
	}
}

func TestValidateResolutionBlueprintAcceptsAgentLoopBlueprintToolWithSubagent(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"agent-loop-subagent-tool",
	  "version":1,
	  "nodes":[
	    {"id":"agent","type":"agent_loop","config":{
	      "prompt":"Resolve.",
	      "tools":[
	        {
	          "name":"delegate",
	          "kind":"blueprint",
	          "max_depth":2,
	          "inline":{
	            "id":"delegate-child",
	            "version":1,
	            "nodes":[
	              {"id":"child_agent","type":"agent_loop","config":{"prompt":"Inspect this."}}
	            ],
	            "edges":[]
	          }
	        }
	      ]
	    }},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"agent failed"}}
	  ],
	  "edges":[{"from":"agent","to":"cancel"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintRejectsNegativeAgentLoopToolMaxDepth(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"agent-loop-tool-max-depth",
	  "version":1,
	  "nodes":[
	    {"id":"agent","type":"agent_loop","config":{
	      "prompt":"Resolve.",
	      "tools":[
	        {
	          "name":"delegate",
	          "kind":"blueprint",
	          "max_depth":-1,
	          "inline":{
	            "id":"delegate-child",
	            "version":1,
	            "nodes":[
	              {"id":"step","type":"cel_eval","config":{"expressions":{"ok":"'true'"}}}
	            ],
	            "edges":[]
	          }
	        }
	      ]
	    }},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"agent failed"}}
	  ],
	  "edges":[{"from":"agent","to":"cancel"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected negative tool max_depth blueprint to be invalid")
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

func TestValidateResolutionBlueprintAcceptsBatchedMap(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"map-batches",
	  "version":1,
	  "nodes":[
	    {"id":"claims","type":"map","config":{
	      "items_key":"claims_json",
	      "batch_size":10,
	      "max_concurrency":4,
	      "inline":{"nodes":[{"id":"score","type":"cel_eval","config":{"expressions":{"ok":"'true'"}}}]}
	    }},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"claims.status"}}
	  ],
	  "edges":[{"from":"claims","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid batched map blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintRejectsRemovedMapMode(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"map-mode",
	  "version":1,
	  "nodes":[
	    {"id":"claims","type":"map","config":{
	      "items_key":"claims_json",
	      "mode":"parallel",
	      "inline":{"nodes":[{"id":"score","type":"cel_eval","config":{"expressions":{"ok":"'true'"}}}]}
	    }},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"claims.status"}}
	  ],
	  "edges":[{"from":"claims","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected map mode blueprint to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsNegativeMapConcurrency(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"map-concurrency",
	  "version":1,
	  "nodes":[
	    {"id":"claims","type":"map","config":{
	      "items_key":"claims_json",
	      "max_concurrency":-1,
	      "inline":{"nodes":[{"id":"score","type":"cel_eval","config":{"expressions":{"ok":"'true'"}}}]}
	    }},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"claims.status"}}
	  ],
	  "edges":[{"from":"claims","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected negative map concurrency blueprint to be invalid")
	}
}

func TestValidateResolutionBlueprintAcceptsValidateBlueprintNode(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"validate-blueprint-node",
	  "version":1,
	  "nodes":[
	    {"id":"generator","type":"agent_loop","config":{"prompt":"Generate a blueprint.","output_mode":"structured","output_tool":{"parameters":{"type":"object","properties":{"blueprint_json":{"type":"string"}},"required":["blueprint_json"]}}}},
	    {"id":"check","type":"validate_blueprint","config":{"blueprint_json_key":"generator.output.blueprint_json"}},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"example"}}
	  ],
	  "edges":[
	    {"from":"generator","to":"check"},
	    {"from":"check","to":"cancel"}
	  ]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintRejectsValidateBlueprintMissingKey(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"validate-blueprint-node-missing-key",
	  "version":1,
	  "nodes":[
	    {"id":"check","type":"validate_blueprint","config":{}},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"example"}}
	  ],
	  "edges":[{"from":"check","to":"cancel"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected validate_blueprint without blueprint_json_key to be invalid")
	}
}

func TestValidateResolutionBlueprintAcceptsGadgetNode(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"gadget-parent",
	  "version":1,
	  "nodes":[
	    {"id":"run_child","type":"gadget","config":{"blueprint_json_key":"input.child_blueprint_json","propagate_terminal":true}},
	    {"id":"submit","type":"submit_result","config":{"outcome_key":"run_child.outcome"}},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"child run failed"}}
	  ],
	  "edges":[
	    {"from":"run_child","to":"submit","condition":"run_child.submitted == 'true'"},
	    {"from":"run_child","to":"cancel","condition":"run_child.submitted != 'true'"}
	  ]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid gadget blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintRejectsGadgetMissingSource(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"gadget-missing-source",
	  "version":1,
	  "nodes":[
	    {"id":"run_child","type":"gadget","config":{}},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"child run failed"}}
	  ],
	  "edges":[{"from":"run_child","to":"cancel"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected gadget without a child blueprint source to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsGadgetNegativeMaxDepth(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"gadget-negative-depth",
	  "version":1,
	  "nodes":[
	    {"id":"run_child","type":"gadget","config":{"blueprint_json_key":"input.child_blueprint_json","max_depth":-1}},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"child run failed"}}
	  ],
	  "edges":[{"from":"run_child","to":"cancel"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected gadget with negative max_depth to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsInvalidInlineGadgetBlueprint(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"gadget-inline-invalid",
	  "version":1,
	  "nodes":[
	    {"id":"run_child","type":"gadget","config":{
	      "inline":{
	        "id":"child",
	        "version":1,
	        "nodes":[
	          {"id":"submit","type":"submit_result","config":{}}
	        ],
	        "edges":[]
	      }
	    }},
	    {"id":"cancel","type":"cancel_market","config":{"reason":"child run failed"}}
	  ],
	  "edges":[{"from":"run_child","to":"cancel"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected gadget with invalid inline child blueprint to be invalid")
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
