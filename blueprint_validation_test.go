package main

import (
	"encoding/json"
	"strings"
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
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.judge.outcome}}"}}}
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
	    {"id":"judge","type":"return","config":{"value":{"status":"success","outcome":"{{results.judge.outcome}}"}}}
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
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.fetch.outcome}}"}}}
	  ],
	  "edges":[
	    {"from":"fetch","to":"wait"},
	    {"from":"wait","to":"fetch"},
	    {"from":"fetch","to":"submit","condition":"results.fetch.status == 'success'"}
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
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.judge.outcome}}"}}}
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
	    {"id":"defer","type":"return","config":{"value":{"status":"deferred","reason":"later"}}}
	  ],
	  "edges":[{"from":"wait","to":"defer","condition":"results.wait.status == 'waiting'"}]
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
	    {"id":"judge","type":"llm_call","config":{"prompt":"Judge this.","allowed_outcomes_key":"inputs.market.outcomes.json"}},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.judge.outcome}}"}}}
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
	    {"id":"agent","type":"agent_loop","config":{"prompt":"Resolve {{inputs.market.question}}.","output_mode":"resolution","allowed_outcomes_key":"inputs.market.outcomes.json"}},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.agent.outcome}}"}}}
	  ],
	  "edges":[{"from":"agent","to":"submit","condition":"results.agent.status == 'success'"}]
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
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.agent.output.outcome}}"}}}
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
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"agent failed"}}}
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
	              {"id":"child_agent","type":"agent_loop","config":{"prompt":"Inspect this."}},
	              {"id":"done","type":"return","config":{"value":{"status":"success","final":"{{results.child_agent.output.final}}"}}}
	            ],
	            "edges":[{"from":"child_agent","to":"done"}]
	          }
	        }
	      ]
	    }},
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"agent failed"}}}
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
	              {"id":"step","type":"cel_eval","config":{"expressions":{"ok":"'true'"}}},
	              {"id":"done","type":"return","config":{"value":{"status":"success","ok":"{{results.step.ok}}"}}}
	            ],
	            "edges":[{"from":"step","to":"done"}]
	          }
	        }
	      ]
	    }},
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"agent failed"}}}
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
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.fetch.outcome}}"}}}
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
	      "items_key":"inputs.claims_json",
	      "batch_size":10,
	      "max_concurrency":4,
	      "inline":{"nodes":[{"id":"score","type":"cel_eval","config":{"expressions":{"ok":"'true'"}}},{"id":"done","type":"return","config":{"value":{"status":"success","ok":"{{results.score.ok}}"}}}],"edges":[{"from":"score","to":"done"}]}
	    }},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.claims.status}}"}}}
	  ],
	  "edges":[{"from":"claims","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected valid batched map blueprint, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintRejectsNegativeMapConcurrency(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"map-concurrency",
	  "version":1,
	  "nodes":[
	    {"id":"claims","type":"map","config":{
	      "items_key":"inputs.claims_json",
	      "max_concurrency":-1,
	      "inline":{"nodes":[{"id":"score","type":"cel_eval","config":{"expressions":{"ok":"'true'"}}},{"id":"done","type":"return","config":{"value":{"status":"success","ok":"{{results.score.ok}}"}}}],"edges":[{"from":"score","to":"done"}]}
	    }},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.claims.status}}"}}}
	  ],
	  "edges":[{"from":"claims","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected negative map concurrency blueprint to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsUnknownMapConfigField(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"map-unknown-field",
	  "version":1,
	  "nodes":[
	    {"id":"claims","type":"map","config":{
	      "items_key":"inputs.claims_json",
	      "mode":"parallel",
	      "inline":{"nodes":[{"id":"score","type":"cel_eval","config":{"expressions":{"ok":"'true'"}}},{"id":"done","type":"return","config":{"value":{"status":"success","ok":"{{results.score.ok}}"}}}],"edges":[{"from":"score","to":"done"}]}
	    }},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.claims.status}}"}}}
	  ],
	  "edges":[{"from":"claims","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected unknown map field blueprint to be invalid")
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Code == "MAP_CONFIG_INVALID" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected MAP_CONFIG_INVALID, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintAcceptsValidateBlueprintNode(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"validate-blueprint-node",
	  "version":1,
	  "nodes":[
	    {"id":"generator","type":"agent_loop","config":{"prompt":"Generate a blueprint.","output_mode":"structured","output_tool":{"parameters":{"type":"object","properties":{"blueprint_json":{"type":"string"}},"required":["blueprint_json"]}}}},
	    {"id":"check","type":"validate_blueprint","config":{"blueprint_json_key":"results.generator.output.blueprint_json"}},
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"example"}}}
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
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"example"}}}
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
	    {"id":"run_child","type":"gadget","config":{"blueprint_json_key":"inputs.child_blueprint_json"}},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.run_child.outcome}}"}}},
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"child run failed"}}}
	  ],
	  "edges":[
	    {"from":"run_child","to":"submit","condition":"results.run_child.submitted == 'true'"},
	    {"from":"run_child","to":"cancel","condition":"results.run_child.submitted != 'true'"}
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
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"child run failed"}}}
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
	    {"id":"run_child","type":"gadget","config":{"blueprint_json_key":"inputs.child_blueprint_json","max_depth":-1}},
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"child run failed"}}}
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
	          {"id":"submit","type":"return","config":{"value":{"outcome":"yes"}}}
	        ],
	        "edges":[]
	      }
	    }},
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"child run failed"}}}
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
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.fetch.outcome}}"}}}
	  ],
	  "edges":[{"from":"fetch","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected unsupported method blueprint to be invalid")
	}
}

func TestValidateResolutionBlueprintRejectsWaitMaxInlineOverCap(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"wait-inline-cap-exceeded",
	  "version":1,
	  "nodes":[
	    {"id":"wait","type":"wait","config":{"duration_seconds":600,"max_inline_seconds":301}},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.wait.status}}"}}}
	  ],
	  "edges":[{"from":"wait","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if result.Valid {
		t.Fatal("expected max_inline_seconds above the cap to be rejected")
	}
	var codes []string
	for _, issue := range result.Issues {
		codes = append(codes, issue.Code)
	}
	hasCode := false
	for _, code := range codes {
		if code == "WAIT_MAX_INLINE_TOO_LARGE" {
			hasCode = true
			break
		}
	}
	if !hasCode {
		t.Fatalf("expected WAIT_MAX_INLINE_TOO_LARGE, got codes: %v", codes)
	}
}

func TestValidateResolutionBlueprintAcceptsWaitMaxInlineAtCap(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"wait-inline-cap-ok",
	  "version":1,
	  "nodes":[
	    {"id":"wait","type":"wait","config":{"duration_seconds":600,"max_inline_seconds":300}},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.wait.status}}"}}}
	  ],
	  "edges":[{"from":"wait","to":"submit"}]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected max_inline_seconds at the cap to be accepted, got issues: %+v", result.Issues)
	}
}

func TestValidateResolutionBlueprintFlagsUnknownOutputKeyWithSuggestion(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"typo",
	  "version":1,
	  "nodes":[
	    {"id":"fetch","type":"api_fetch","config":{"url":"https://example.com","json_path":"result","outcome_mapping":{"yes":"0"}}},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.fetch.outcome}}"}}},
	    {"id":"cancel","type":"return","config":{"value":{"status":"failed","reason":"fetch failed"}}}
	  ],
	  "edges":[
	    {"from":"fetch","to":"submit","condition":"results.fetch.outcom == '0'"},
	    {"from":"fetch","to":"cancel","condition":"results.fetch.status != 'success'"}
	  ]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("unknown-key diagnostic must be a warning (Valid stays true), got issues: %+v", result.Issues)
	}
	var warn *BlueprintValidationIssue
	for i := range result.Issues {
		if result.Issues[i].Code == "EDGE_UNKNOWN_OUTPUT_KEY" {
			warn = &result.Issues[i]
			break
		}
	}
	if warn == nil {
		t.Fatalf("expected EDGE_UNKNOWN_OUTPUT_KEY, got: %+v", result.Issues)
	}
	if warn.Severity != "warning" {
		t.Fatalf("severity = %q, want warning", warn.Severity)
	}
	if !strings.Contains(warn.Message, `fetch.outcom`) {
		t.Fatalf("message should cite the bad identifier, got: %q", warn.Message)
	}
	if !strings.Contains(warn.Message, `fetch.outcome`) {
		t.Fatalf("message should suggest fetch.outcome, got: %q", warn.Message)
	}
}

func TestValidateResolutionBlueprintAllowsContextAndReservedReferences(t *testing.T) {
	bp, raw := mustBlueprint(t, `{
	  "id":"context-refs",
	  "version":1,
	  "nodes":[
	    {"id":"fetch","type":"api_fetch","config":{"url":"https://example.com","json_path":"result","outcome_mapping":{"yes":"0"}}},
	    {"id":"submit","type":"return","config":{"value":{"status":"success","outcome":"{{results.fetch.outcome}}"}}}
	  ],
	  "edges":[
	    {"from":"fetch","to":"submit","condition":"results.fetch.status == 'success' && inputs.market.deadline != '' && results.fetch.history.size() >= 0"}
	  ]
	}`)
	result := ValidateResolutionBlueprint(bp, raw)
	for _, issue := range result.Issues {
		if issue.Code == "EDGE_UNKNOWN_OUTPUT_KEY" {
			t.Fatalf("unexpected EDGE_UNKNOWN_OUTPUT_KEY on well-formed edge: %+v", issue)
		}
	}
}
