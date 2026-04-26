package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

func TestAgentLoopOpenAIResponsesResolutionWithContextTool(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("expected /responses endpoint for gpt-5 model, got %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-openai-key" {
			t.Fatalf("missing Authorization header")
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body["model"] != defaultOpenAIModel {
			t.Fatalf("expected default model %q, got %v", defaultOpenAIModel, body["model"])
		}
		tools := body["tools"].([]any)
		if len(tools) == 0 {
			t.Fatalf("expected tools in request")
		}

		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":      "function_call",
						"call_id":   "call_context",
						"name":      AgentBuiltinContextGet,
						"arguments": `{"keys":["market.question"]}`,
					},
				},
				"usage": map[string]int{"input_tokens": 10, "output_tokens": 4},
			})
		case 2:
			encoded, _ := json.Marshal(body["input"])
			if !strings.Contains(string(encoded), "function_call_output") {
				t.Fatalf("second request did not include tool output: %s", string(encoded))
			}
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":    "function_call",
						"call_id": "call_final",
						"name":    defaultResolutionToolName,
						"arguments": `{
							"status":"success",
							"outcome_index":1,
							"confidence":"high",
							"reasoning":"The provided market context supports outcome 1.",
							"citations":["market.question"]
						}`,
					},
				},
				"usage": map[string]int{"input_tokens": 8, "output_tokens": 6},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	engine := dag.NewEngine(nil)
	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, engine)
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":             LLMProviderOpenAI,
			"prompt":               "Resolve {{inputs.market.question}}",
			"output_mode":          AgentOutputModeResolution,
			"allowed_outcomes_key": "inputs.market.outcomes_json",
		},
	}
	ctx := dag.NewInvocationFromInputs(map[string]string{
		"market.question":      "Will the event resolve No?",
		"market.outcomes_json": `["Yes","No"]`,
	})

	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	if result.Outputs["outcome"] != "1" {
		t.Fatalf("expected outcome 1, got %q", result.Outputs["outcome"])
	}
	if result.Outputs["confidence"] != "high" {
		t.Fatalf("expected high confidence, got %q", result.Outputs["confidence"])
	}
	if result.Outputs["tool_calls_count"] != "2" {
		t.Fatalf("expected 2 tool calls, got %s", result.Outputs["tool_calls_count"])
	}
	if result.Usage.InputTokens != 18 || result.Usage.OutputTokens != 10 {
		t.Fatalf("unexpected usage: %+v", result.Usage)
	}
}

func TestAgentLoopExplicitEmptyToolsOnlyExposesOutputTool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("expected /responses endpoint for gpt-5 model, got %s", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		tools, ok := body["tools"].([]any)
		if !ok {
			t.Fatalf("expected tools array, got %+v", body["tools"])
		}
		if len(tools) != 1 {
			encoded, _ := json.Marshal(tools)
			t.Fatalf("expected only output tool, got %s", encoded)
		}
		encoded, _ := json.Marshal(tools[0])
		if !strings.Contains(string(encoded), defaultAgentOutputToolName) {
			t.Fatalf("expected output tool, got %s", encoded)
		}
		for _, forbidden := range []string{AgentBuiltinContextGet, AgentBuiltinContextList, AgentBuiltinSourceFetch, AgentBuiltinJSONExtract} {
			if strings.Contains(string(encoded), forbidden) {
				t.Fatalf("explicit empty tools leaked builtin %q: %s", forbidden, encoded)
			}
		}
		toolChoice, _ := json.Marshal(body["tool_choice"])
		if !strings.Contains(string(toolChoice), defaultAgentOutputToolName) {
			t.Fatalf("expected forced output tool choice, got %s", toolChoice)
		}
		writeAgentJSON(t, w, map[string]any{
			"output": []map[string]any{
				{
					"type":      "function_call",
					"call_id":   "call_final",
					"name":      defaultAgentOutputToolName,
					"arguments": `{"answer":"ok"}`,
				},
			},
		})
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderOpenAI,
			"prompt":      "Return structured output.",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"answer": map[string]any{"type": "string"}},
					"required":   []string{"answer"},
				},
			},
			"tools": []map[string]any{},
		},
	}
	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(map[string]string{
		"market.question": "hidden unless a context tool is explicitly configured",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["output.answer"] != "ok" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopStructuredOutputRequiresSchemaFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeAgentJSON(t, w, map[string]any{
			"output": []map[string]any{
				{
					"type":      "function_call",
					"call_id":   "call_final",
					"name":      defaultAgentOutputToolName,
					"arguments": `{}`,
				},
			},
		})
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderOpenAI,
			"prompt":      "Return structured output.",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{"answer": map[string]any{"type": "string"}},
					"required":             []string{"answer"},
					"additionalProperties": false,
				},
			},
			"tools": []map[string]any{},
		},
	}
	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed result, got %+v", result.Outputs)
	}
	if !strings.Contains(result.Outputs["error"], `missing required field "answer"`) {
		t.Fatalf("unexpected error: %+v", result.Outputs)
	}
}

func TestAgentLoopStructuredOutputRequiresToolChoiceWithAdditionalTools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body["tool_choice"] != "required" {
			t.Fatalf("expected required tool_choice, got %+v", body["tool_choice"])
		}
		writeAgentJSON(t, w, map[string]any{
			"output": []map[string]any{
				{
					"type":      "function_call",
					"call_id":   "call_final",
					"name":      defaultAgentOutputToolName,
					"arguments": `{"answer":"ok"}`,
				},
			},
		})
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderOpenAI,
			"prompt":      "Return structured output.",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"answer": map[string]any{"type": "string"}},
					"required":   []string{"answer"},
				},
			},
			"tools": []map[string]any{
				{"name": AgentBuiltinContextList, "kind": AgentToolKindBuiltin},
			},
		},
	}
	result, err := exec.Execute(context.Background(), node, dag.NewInvocation(dag.Run{ID: "run-tools"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["output.answer"] != "ok" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopAnthropicForcedOutputToolChoice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		choice, ok := body["tool_choice"].(map[string]any)
		if !ok || choice["type"] != "tool" || choice["name"] != defaultAgentOutputToolName {
			t.Fatalf("expected forced Anthropic output tool choice, got %#v", body["tool_choice"])
		}
		writeAgentJSON(t, w, map[string]any{
			"content": []map[string]any{{
				"type":  "tool_use",
				"id":    "toolu_final",
				"name":  defaultAgentOutputToolName,
				"input": map[string]any{"answer": "ok"},
			}},
			"stop_reason": "tool_use",
		})
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		AnthropicAPIKey:  "test-anthropic-key",
		AnthropicBaseURL: server.URL,
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderAnthropic,
			"model":       defaultAnthropicModel,
			"prompt":      "Return structured output.",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"answer": map[string]any{"type": "string"}},
					"required":   []string{"answer"},
				},
			},
			"tools": []map[string]any{},
		},
	}
	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["output.answer"] != "ok" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopAnthropicRequiresAnyToolWithAdditionalTools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		choice, ok := body["tool_choice"].(map[string]any)
		if !ok || choice["type"] != "any" {
			t.Fatalf("expected Anthropic any-tool choice, got %#v", body["tool_choice"])
		}
		writeAgentJSON(t, w, map[string]any{
			"content": []map[string]any{{
				"type":  "tool_use",
				"id":    "toolu_final",
				"name":  defaultAgentOutputToolName,
				"input": map[string]any{"answer": "ok"},
			}},
			"stop_reason": "tool_use",
		})
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		AnthropicAPIKey:  "test-anthropic-key",
		AnthropicBaseURL: server.URL,
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderAnthropic,
			"model":       defaultAnthropicModel,
			"prompt":      "Return structured output.",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"answer": map[string]any{"type": "string"}},
					"required":   []string{"answer"},
				},
			},
			"tools": []map[string]any{
				{"name": AgentBuiltinContextList, "kind": AgentToolKindBuiltin},
			},
		},
	}
	result, err := exec.Execute(context.Background(), node, dag.NewInvocation(dag.Run{ID: "run-anthropic-tools"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["output.answer"] != "ok" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopGoogleRequiresAnyToolWithAdditionalTools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		toolConfig, ok := body["toolConfig"].(map[string]any)
		if !ok {
			t.Fatalf("expected Google toolConfig, got %#v", body["toolConfig"])
		}
		fcc, ok := toolConfig["functionCallingConfig"].(map[string]any)
		if !ok || fcc["mode"] != "ANY" {
			t.Fatalf("expected Google ANY tool mode, got %#v", toolConfig["functionCallingConfig"])
		}
		if _, ok := fcc["allowedFunctionNames"]; ok {
			t.Fatalf("expected no allowedFunctionNames for any-tool mode, got %#v", fcc["allowedFunctionNames"])
		}
		writeAgentJSON(t, w, map[string]any{
			"candidates": []map[string]any{{
				"content": map[string]any{
					"parts": []map[string]any{{
						"functionCall": map[string]any{
							"name": defaultAgentOutputToolName,
							"args": map[string]any{"answer": "ok"},
						},
					}},
				},
				"finishReason": "STOP",
			}},
		})
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		GoogleAPIKey:  "test-google-key",
		GoogleBaseURL: server.URL,
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderGoogle,
			"model":       "gemini-3.1-pro-preview",
			"prompt":      "Return structured output.",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"answer": map[string]any{"type": "string"}},
					"required":   []string{"answer"},
				},
			},
			"tools": []map[string]any{
				{"name": AgentBuiltinContextList, "kind": AgentToolKindBuiltin},
			},
		},
	}
	result, err := exec.Execute(context.Background(), node, dag.NewInvocation(dag.Run{ID: "run-google-tools"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["output.answer"] != "ok" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopOpenAIChatCompletionsResolutionWithContextTool(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("expected chat completions endpoint, got %s", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body["model"] != "o4-mini" {
			t.Fatalf("expected o4-mini model, got %v", body["model"])
		}

		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"choices": []map[string]any{
					{
						"finish_reason": "tool_calls",
						"message": map[string]any{
							"tool_calls": []map[string]any{
								{
									"id":   "call_context",
									"type": "function",
									"function": map[string]any{
										"name":      AgentBuiltinContextGet,
										"arguments": `{"keys":["market.question"]}`,
									},
								},
							},
						},
					},
				},
				"usage": map[string]int{"prompt_tokens": 3, "completion_tokens": 2},
			})
		case 2:
			messagesJSON, _ := json.Marshal(body["messages"])
			if !strings.Contains(string(messagesJSON), `"role":"tool"`) {
				t.Fatalf("second request did not include tool role: %s", string(messagesJSON))
			}
			writeAgentJSON(t, w, map[string]any{
				"choices": []map[string]any{
					{
						"finish_reason": "tool_calls",
						"message": map[string]any{
							"tool_calls": []map[string]any{
								{
									"id":   "call_final",
									"type": "function",
									"function": map[string]any{
										"name":      defaultResolutionToolName,
										"arguments": `{"outcome_index":0,"confidence":"medium","reasoning":"Chat completions path resolved."}`,
									},
								},
							},
						},
					},
				},
				"usage": map[string]int{"prompt_tokens": 4, "completion_tokens": 3},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderOpenAI,
			"model":       "o4-mini",
			"prompt":      "Resolve with chat completions tools.",
			"output_mode": AgentOutputModeResolution,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(map[string]string{"market.question": "question"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["outcome"] != "0" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
	if result.Usage.InputTokens != 7 || result.Usage.OutputTokens != 5 {
		t.Fatalf("unexpected usage: %+v", result.Usage)
	}
}

func TestAgentLoopAnthropicResolutionToolCalling(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-anthropic-key" {
			t.Fatalf("missing x-api-key header")
		}
		if r.Header.Get("anthropic-version") != "2023-06-01" {
			t.Fatalf("missing anthropic-version header")
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"content": []map[string]any{
					{
						"type":  "tool_use",
						"id":    "toolu_context",
						"name":  AgentBuiltinContextGet,
						"input": map[string]any{"keys": []string{"market.question"}},
					},
				},
				"stop_reason": "tool_use",
				"usage":       map[string]int{"input_tokens": 5, "output_tokens": 3},
			})
		case 2:
			messagesJSON, _ := json.Marshal(body["messages"])
			if !strings.Contains(string(messagesJSON), "tool_result") {
				t.Fatalf("second request did not include Anthropic tool_result: %s", string(messagesJSON))
			}
			writeAgentJSON(t, w, map[string]any{
				"content": []map[string]any{
					{
						"type":  "tool_use",
						"id":    "toolu_final",
						"name":  defaultResolutionToolName,
						"input": map[string]any{"outcome_index": 0, "confidence": "medium", "reasoning": "Anthropic path resolved."},
					},
				},
				"stop_reason": "tool_use",
				"usage":       map[string]int{"input_tokens": 7, "output_tokens": 4},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		AnthropicAPIKey:  "test-anthropic-key",
		AnthropicBaseURL: server.URL,
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderAnthropic,
			"prompt":      "Resolve with tools.",
			"output_mode": AgentOutputModeResolution,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(map[string]string{"market.question": "question"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["outcome"] != "0" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
	if result.Usage.InputTokens != 12 || result.Usage.OutputTokens != 7 {
		t.Fatalf("unexpected usage: %+v", result.Usage)
	}
}

func TestAgentLoopGoogleResolutionToolCalling(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/gemini-3.1-pro-preview:generateContent") {
			t.Fatalf("unexpected Google path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("key") != "test-google-key" {
			t.Fatalf("missing Google key query")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"candidates": []map[string]any{
					{
						"content": map[string]any{
							"parts": []map[string]any{
								{"functionCall": map[string]any{"name": AgentBuiltinContextGet, "args": map[string]any{"keys": []string{"market.question"}}}},
							},
						},
						"finishReason": "STOP",
					},
				},
				"usageMetadata": map[string]int{"promptTokenCount": 6, "candidatesTokenCount": 2},
			})
		case 2:
			contentsJSON, _ := json.Marshal(body["contents"])
			if !strings.Contains(string(contentsJSON), "functionResponse") {
				t.Fatalf("second request did not include functionResponse: %s", string(contentsJSON))
			}
			writeAgentJSON(t, w, map[string]any{
				"candidates": []map[string]any{
					{
						"content": map[string]any{
							"parts": []map[string]any{
								{"functionCall": map[string]any{"name": defaultResolutionToolName, "args": map[string]any{"outcome_index": 1, "confidence": "low", "reasoning": "Gemini path resolved."}}},
							},
						},
						"finishReason": "STOP",
					},
				},
				"usageMetadata": map[string]int{"promptTokenCount": 7, "candidatesTokenCount": 3},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		GoogleAPIKey:  "test-google-key",
		GoogleBaseURL: server.URL,
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderGoogle,
			"prompt":      "Resolve with tools.",
			"output_mode": AgentOutputModeResolution,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(map[string]string{"market.question": "question"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["outcome"] != "1" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
	if result.Usage.InputTokens != 13 || result.Usage.OutputTokens != 5 {
		t.Fatalf("unexpected usage: %+v", result.Usage)
	}
}

func TestAgentLoopOpenAIResponsesProviderParity(t *testing.T) {
	tests := map[string]bool{
		"gpt-5-mini":       true,
		"gpt-5.4-mini":     true,
		"gpt-5.4-nano":     true,
		"gpt-5":            true,
		"gpt-5.2-codex":    true,
		"codex-experiment": true,
		"gpt-4o-mini":      false,
	}
	for model, want := range tests {
		if got := agentOpenAIUsesResponses(model); got != want {
			t.Fatalf("agentOpenAIUsesResponses(%q) = %t, want %t", model, got, want)
		}
	}

	inputs := openAIResponsesInputs([]agentMessage{{
		Role: agentRoleUser,
		Content: []agentContentPart{{
			Type:         "tool_result",
			ToolResultID: "call_empty",
			ToolOutput:   "",
		}},
	}})
	encoded, err := json.Marshal(inputs)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"output":""`) {
		t.Fatalf("empty tool output should be preserved for Responses API: %s", encoded)
	}
}

func TestAgentLoopOpenAIResponsesReasoningAndTextDeduplication(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		reasoning, ok := body["reasoning"].(map[string]any)
		if !ok {
			t.Fatalf("expected reasoning payload, got %#v", body["reasoning"])
		}
		if reasoning["effort"] != "medium" || reasoning["summary"] != "auto" {
			t.Fatalf("unexpected reasoning payload: %#v", reasoning)
		}

		writeAgentJSON(t, w, map[string]any{
			"output_text": "Final answer.",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{
						{"type": "output_text", "text": "Final answer."},
					},
				},
			},
			"usage": map[string]int{"input_tokens": 2, "output_tokens": 3},
		})
	}))
	defer server.Close()

	client := newAgentProviderClient(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL,
	})
	resp, err := client.chatOpenAIResponses(context.Background(), agentProviderRequest{
		Model:     defaultOpenAIModel,
		Messages:  []agentMessage{{Role: agentRoleUser, Content: []agentContentPart{{Type: "text", Text: "Say one thing."}}}},
		Reasoning: "medium",
	})
	if err != nil {
		t.Fatal(err)
	}
	if text := agentMessageText(resp.Message); text != "Final answer." {
		t.Fatalf("expected deduplicated output text, got %q", text)
	}
}

func TestAgentLoopAnthropicReasoningRequestMatchesMerak4(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		thinking, ok := body["thinking"].(map[string]any)
		if !ok || thinking["type"] != "adaptive" {
			t.Fatalf("expected adaptive thinking payload, got %#v", body["thinking"])
		}
		outputConfig, ok := body["output_config"].(map[string]any)
		if !ok || outputConfig["effort"] != "high" {
			t.Fatalf("expected high output_config effort, got %#v", body["output_config"])
		}
		if maxTokens, ok := body["max_tokens"].(float64); !ok || maxTokens < 16384 {
			t.Fatalf("expected max_tokens to be raised for Anthropic reasoning, got %#v", body["max_tokens"])
		}

		writeAgentJSON(t, w, map[string]any{
			"content": []map[string]any{
				{
					"type":  "tool_use",
					"id":    "toolu_final",
					"name":  defaultResolutionToolName,
					"input": map[string]any{"outcome_index": 0, "confidence": "high", "reasoning": "Reasoned path resolved."},
				},
			},
			"stop_reason": "tool_use",
		})
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		AnthropicAPIKey:  "test-anthropic-key",
		AnthropicBaseURL: server.URL,
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderAnthropic,
			"model":       defaultAnthropicModel,
			"prompt":      "Resolve with reasoning.",
			"reasoning":   "high",
			"output_mode": AgentOutputModeResolution,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["outcome"] != "0" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopGoogleThoughtSignatureRoundTrip(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"candidates": []map[string]any{
					{
						"content": map[string]any{
							"parts": []map[string]any{
								{
									"thoughtSignature": "sig-context",
									"functionCall":     map[string]any{"name": AgentBuiltinContextGet, "args": map[string]any{"keys": []string{"market.question"}}},
								},
							},
						},
					},
				},
			})
		case 2:
			contentsJSON, _ := json.Marshal(body["contents"])
			if !strings.Contains(string(contentsJSON), `"thoughtSignature":"sig-context"`) {
				t.Fatalf("second request did not preserve Gemini thoughtSignature: %s", string(contentsJSON))
			}
			if !strings.Contains(string(contentsJSON), "functionResponse") {
				t.Fatalf("second request did not include functionResponse: %s", string(contentsJSON))
			}
			writeAgentJSON(t, w, map[string]any{
				"candidates": []map[string]any{
					{
						"content": map[string]any{
							"parts": []map[string]any{
								{"functionCall": map[string]any{"name": defaultResolutionToolName, "args": map[string]any{"outcome_index": 1, "confidence": "medium", "reasoning": "Gemini signature path resolved."}}},
							},
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		GoogleAPIKey:  "test-google-key",
		GoogleBaseURL: server.URL,
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderGoogle,
			"prompt":      "Resolve with Gemini.",
			"output_mode": AgentOutputModeResolution,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(map[string]string{"market.question": "question"}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["outcome"] != "1" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopPredefinedBlueprintTool(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":      "function_call",
						"call_id":   "call_blueprint",
						"name":      "check_value",
						"arguments": `{"value":"abc"}`,
					},
				},
			})
		case 2:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			inputJSON, _ := json.Marshal(body["input"])
			if !strings.Contains(string(inputJSON), "abc-checked") {
				t.Fatalf("blueprint tool output was not returned to the model: %s", string(inputJSON))
			}
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":      "function_call",
						"call_id":   "call_final",
						"name":      defaultAgentOutputToolName,
						"arguments": `{"final":"abc-checked","ok":true}`,
					},
				},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("agent_test_echo", &agentLoopEchoExecutor{})
	engine.RegisterExecutor("return", NewReturnExecutor())
	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, engine)
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderOpenAI,
			"prompt":      "Check the value.",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"final": map[string]any{"type": "string"}, "ok": map[string]any{"type": "boolean"}},
					"required":   []string{"final", "ok"},
				},
			},
			"tools": []map[string]any{
				{
					"name":        "check_value",
					"kind":        AgentToolKindBlueprint,
					"description": "Check a value with a child blueprint.",
					"parameters": map[string]any{
						"type":       "object",
						"properties": map[string]any{"value": map[string]any{"type": "string"}},
						"required":   []string{"value"},
					},
					"input_mappings": map[string]string{"input_value": "$args.value"},
					"inline": dag.Blueprint{
						ID:      "child-check",
						Version: 1,
						Nodes: []dag.NodeDef{
							{ID: "echo", Type: "agent_test_echo", Config: map[string]any{}},
							{ID: "done", Type: "return", Config: map[string]any{
								"value": map[string]any{
									"status": "success",
									"value":  "{{results.echo.value}}",
								},
							}},
						},
						Edges: []dag.EdgeDef{{From: "echo", To: "done"}},
					},
				},
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("unexpected status: %+v", result.Outputs)
	}
	if result.Outputs["output.final"] != "abc-checked" || result.Outputs["output.ok"] != "true" {
		t.Fatalf("structured output was not flattened: %+v", result.Outputs)
	}
}

func TestAgentLoopPredefinedBlueprintToolSupportsSubagent(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":      "function_call",
						"call_id":   "call_blueprint",
						"name":      "delegate_check",
						"arguments": `{"value":"abc"}`,
					},
				},
			})
		case 2:
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":    "function_call",
						"call_id": "call_child_final",
						"name":    defaultAgentOutputToolName,
						"arguments": `{
							"final":"abc-subchecked"
						}`,
					},
				},
			})
		case 3:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			inputJSON, _ := json.Marshal(body["input"])
			if !strings.Contains(string(inputJSON), "abc-subchecked") {
				t.Fatalf("subagent blueprint tool output was not returned to the model: %s", string(inputJSON))
			}
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":      "function_call",
						"call_id":   "call_final",
						"name":      defaultAgentOutputToolName,
						"arguments": `{"final":"abc-subchecked","ok":true}`,
					},
				},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("return", NewReturnExecutor())
	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, engine)
	engine.RegisterExecutor("agent_loop", exec)
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderOpenAI,
			"prompt":      "Check the value with a delegated subagent.",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"final": map[string]any{"type": "string"}, "ok": map[string]any{"type": "boolean"}},
					"required":   []string{"final", "ok"},
				},
			},
			"tools": []map[string]any{
				{
					"name":        "delegate_check",
					"kind":        AgentToolKindBlueprint,
					"description": "Run a child blueprint that uses its own agent loop.",
					"max_depth":   1,
					"parameters": map[string]any{
						"type":       "object",
						"properties": map[string]any{"value": map[string]any{"type": "string"}},
						"required":   []string{"value"},
					},
					"input_mappings": map[string]string{"input_value": "$args.value"},
					"inline": dag.Blueprint{
						ID:      "child-subagent-check",
						Version: 1,
						Nodes: []dag.NodeDef{
							{
								ID:   "child_agent",
								Type: "agent_loop",
								Config: map[string]any{
									"provider":    LLMProviderOpenAI,
									"prompt":      "Check this delegated value: {{inputs.input_value}}",
									"output_mode": AgentOutputModeStructured,
									"output_tool": map[string]any{
										"parameters": map[string]any{
											"type":       "object",
											"properties": map[string]any{"final": map[string]any{"type": "string"}},
											"required":   []string{"final"},
										},
									},
								},
							},
							{
								ID:   "done",
								Type: "return",
								Config: map[string]any{
									"value": map[string]any{
										"status": "success",
										"final":  "{{results.child_agent.output.final}}",
									},
								},
							},
						},
						Edges: []dag.EdgeDef{{From: "child_agent", To: "done"}},
					},
				},
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("unexpected status: %+v", result.Outputs)
	}
	if result.Outputs["output.final"] != "abc-subchecked" || result.Outputs["output.ok"] != "true" {
		t.Fatalf("structured output was not flattened: %+v", result.Outputs)
	}
}

func TestBlueprintToolMaxDepthLimit(t *testing.T) {
	tool := &blueprintTool{
		name:     "delegate_check",
		inline:   &dag.Blueprint{ID: "child", Version: 1, Nodes: []dag.NodeDef{{ID: "step", Type: "cel_eval", Config: map[string]any{"expressions": map[string]string{"ok": "'true'"}}}}},
		maxDepth: 1,
		engine:   dag.NewEngine(nil),
		parentCtx: dag.NewInvocationFromInputs(map[string]string{
			"__agent_blueprint_depth": "1",
		}),
	}

	result, err := tool.Execute(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("expected tool error result, got %+v", result)
	}
	if !strings.Contains(result.Output, "exceeded max_depth 1") {
		t.Fatalf("unexpected tool output: %s", result.Output)
	}
}

func TestAgentLoopDynamicBlueprintTool(t *testing.T) {
	child := dag.Blueprint{
		ID:      "dynamic-child",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 10},
		Nodes: []dag.NodeDef{
			{ID: "calc", Type: "cel_eval", Config: map[string]any{"expressions": map[string]string{"answer": "'dynamic-ok'"}}},
			{ID: "done", Type: "return", Config: map[string]any{
				"value": map[string]any{
					"status": "success",
					"answer": "{{results.calc.answer}}",
				},
			}},
		},
		Edges: []dag.EdgeDef{{From: "calc", To: "done"}},
	}
	childJSON, err := json.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	runBlueprintArgs, _ := json.Marshal(map[string]any{
		"blueprint_json": string(childJSON),
	})

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":      "function_call",
						"call_id":   "call_dynamic",
						"name":      AgentBuiltinRunBlueprint,
						"arguments": string(runBlueprintArgs),
					},
				},
			})
		case 2:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			inputJSON, _ := json.Marshal(body["input"])
			if !strings.Contains(string(inputJSON), "dynamic-ok") {
				t.Fatalf("dynamic blueprint output was not returned to the model: %s", string(inputJSON))
			}
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{
					{
						"type":      "function_call",
						"call_id":   "call_final",
						"name":      defaultAgentOutputToolName,
						"arguments": `{"answer":"dynamic-ok"}`,
					},
				},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	engine.RegisterExecutor("return", NewReturnExecutor())
	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, engine)
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":                  LLMProviderOpenAI,
			"prompt":                    "Run a dynamic calculation.",
			"output_mode":               AgentOutputModeStructured,
			"enable_dynamic_blueprints": true,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"answer": map[string]any{"type": "string"}},
					"required":   []string{"answer"},
				},
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["output.answer"] != "dynamic-ok" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopTextModeCompleteTaskFinishes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		toolsJSON, _ := json.Marshal(body["tools"])
		if !strings.Contains(string(toolsJSON), AgentBuiltinCompleteTask) {
			t.Fatalf("expected complete_task tool, got %s", toolsJSON)
		}
		writeAgentJSON(t, w, map[string]any{
			"output": []map[string]any{{
				"type":      "function_call",
				"call_id":   "call_complete",
				"name":      AgentBuiltinCompleteTask,
				"arguments": `{"summary":"done cleanly"}`,
			}},
		})
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":         LLMProviderOpenAI,
			"prompt":           "Do a text-mode task.",
			"can_complete_key": "results.review.approved",
		},
	}
	inv := dag.NewInvocationFromInputs(nil)
	inv.Results.SetField("review", "approved", "true")
	result, err := exec.Execute(context.Background(), node, inv)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["text"] != "done cleanly" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestAgentLoopCompleteTaskHoldRequestsVerification(t *testing.T) {
	var requests atomic.Int32
	inv := dag.NewInvocationFromInputs(nil)
	inv.Results.SetField("review", "approved", "false")
	inv.Results.SetField("agent", "completion.feedback", "fix the missing proof")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{{
					"type":      "function_call",
					"call_id":   "call_hold",
					"name":      AgentBuiltinCompleteTask,
					"arguments": `{"summary":"first try"}`,
				}},
			})
		case 2:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			inputJSON, _ := json.Marshal(body["input"])
			if !strings.Contains(string(inputJSON), "fix the missing proof") {
				t.Fatalf("completion hold feedback was not returned to model: %s", inputJSON)
			}
			inv.Results.SetField("review", "approved", "true")
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{{
					"type":      "function_call",
					"call_id":   "call_done",
					"name":      AgentBuiltinCompleteTask,
					"arguments": `{"summary":"fixed and approved"}`,
				}},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":         LLMProviderOpenAI,
			"prompt":           "Do a gated task.",
			"can_complete_key": "results.review.approved",
		},
	}
	result, err := exec.Execute(context.Background(), node, inv)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["text"] != "fixed and approved" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
	requested, _ := inv.Results.Get("agent", "completion.requested")
	if requested != "false" {
		t.Fatalf("completion requested flag = %q, want false", requested)
	}
	attempt, _ := inv.Results.Get("agent", "completion.attempt")
	if attempt != "1" {
		t.Fatalf("completion attempt = %q, want 1", attempt)
	}
}

func TestAgentLoopExplicitYieldContinuesAfterProgressText(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{{
					"type": "message",
					"content": []map[string]any{{
						"type": "output_text",
						"text": "I'll inspect the inputs.",
					}},
				}},
			})
		case 2:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			inputJSON, _ := json.Marshal(body["input"])
			if !strings.Contains(string(inputJSON), AgentBuiltinYieldControl) {
				t.Fatalf("yield reminder missing from continuation input: %s", inputJSON)
			}
			writeAgentJSON(t, w, map[string]any{
				"output": []map[string]any{{
					"type":      "function_call",
					"call_id":   "call_yield",
					"name":      AgentBuiltinYieldControl,
					"arguments": `{"summary":"ready for the next agent"}`,
				}},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":               LLMProviderOpenAI,
			"prompt":                 "Do an explicit-yield task.",
			"require_explicit_yield": true,
			"max_continues":          3,
		},
	}
	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["text"] != "ready for the next agent" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want 2", requests.Load())
	}
}

func TestAgentLoopOpenAIResponsesSessionUsesPreviousResponseID(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		switch requests.Add(1) {
		case 1:
			if _, ok := body["previous_response_id"]; ok {
				t.Fatalf("first request unexpectedly had previous_response_id: %+v", body)
			}
			if body["store"] != true {
				t.Fatalf("session first request should store response, got %#v", body["store"])
			}
			writeAgentJSON(t, w, map[string]any{
				"id": "resp_1",
				"output": []map[string]any{{
					"type":      "function_call",
					"call_id":   "call_context",
					"name":      AgentBuiltinContextGet,
					"arguments": `{"keys":["market.question"]}`,
				}},
			})
		case 2:
			if body["previous_response_id"] != "resp_1" {
				t.Fatalf("previous_response_id = %#v, want resp_1", body["previous_response_id"])
			}
			inputJSON, _ := json.Marshal(body["input"])
			if strings.Contains(string(inputJSON), "Session question") {
				t.Fatalf("session continuation resent original prompt: %s", inputJSON)
			}
			if !strings.Contains(string(inputJSON), "function_call_output") {
				t.Fatalf("session continuation did not send tool output: %s", inputJSON)
			}
			writeAgentJSON(t, w, map[string]any{
				"id": "resp_2",
				"output": []map[string]any{{
					"type":      "function_call",
					"call_id":   "call_final",
					"name":      defaultAgentOutputToolName,
					"arguments": `{"answer":"session-ok"}`,
				}},
			})
		default:
			t.Fatalf("unexpected extra request")
		}
	}))
	defer server.Close()

	exec := NewAgentLoopExecutorWithConfig(LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: server.URL + "/chat/completions",
	}, dag.NewEngine(nil))
	node := dag.NodeDef{
		ID:   "agent",
		Type: "agent_loop",
		Config: map[string]any{
			"provider":    LLMProviderOpenAI,
			"prompt":      "Session question",
			"output_mode": AgentOutputModeStructured,
			"output_tool": map[string]any{
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"answer": map[string]any{"type": "string"}},
					"required":   []string{"answer"},
				},
			},
		},
	}
	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(map[string]string{
		"market.question": "Session question",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["output.answer"] != "session-ok" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

func TestValidateDynamicBlueprintAllowsInlineWaitByDefault(t *testing.T) {
	bp := dag.Blueprint{
		ID:      "dynamic-inline-wait",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5},
		Nodes: []dag.NodeDef{
			{
				ID:     "pause",
				Type:   "wait",
				Config: map[string]any{"duration_seconds": 0},
			},
			{
				ID:   "done",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{"status": "success"},
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "pause", To: "done"}},
	}

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("wait", NewWaitExecutor())
	if err := validateDynamicBlueprint(engine, bp, defaultDynamicBlueprintPolicy(nil), dag.NewInvocationFromInputs(nil)); err != nil {
		t.Fatalf("validateDynamicBlueprint rejected inline wait: %v", err)
	}
}

type agentLoopEchoExecutor struct{}

func (e *agentLoopEchoExecutor) Execute(_ context.Context, _ dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	return dag.ExecutorResult{Outputs: map[string]string{
		"status": "success",
		"value":  fmt.Sprintf("%s-checked", inv.Run.Inputs["input_value"]),
	}}, nil
}

func TestCompactAgentMessagesKeepsToolUseResultPairs(t *testing.T) {
	messages := []agentMessage{
		textAgentMessage(agentRoleUser, "prompt"),
		toolUseAgentMessage("call_1", "first"),
		toolResultAgentMessage("call_1", "first result"),
		textAgentMessage(agentRoleUser, "middle"),
		toolUseAgentMessage("call_2", "second"),
		toolResultAgentMessage("call_2", "second result"),
		textAgentMessage(agentRoleUser, "latest"),
	}

	got := compactAgentMessages(messages, 4, 1)
	if len(got) != 4 {
		t.Fatalf("len(compacted) = %d, want 4: %#v", len(got), got)
	}
	assertNoDanglingToolUses(t, got)
	if !agentMessageHasToolUseID(got[1], "call_2") || !agentMessageHasToolResultID(got[2], "call_2") {
		t.Fatalf("expected latest tool pair to be preserved, got %#v", got)
	}
	if got[3].Content[0].Text != "latest" {
		t.Fatalf("expected latest text message, got %#v", got[3])
	}
}

func TestCompactAgentMessagesDropsWholePairWhenMessageBudgetTooSmall(t *testing.T) {
	messages := []agentMessage{
		textAgentMessage(agentRoleUser, "prompt"),
		toolUseAgentMessage("call_1", "first"),
		toolResultAgentMessage("call_1", "first result"),
		textAgentMessage(agentRoleUser, "latest"),
	}

	got := compactAgentMessages(messages, 1, 1)
	if len(got) != 1 {
		t.Fatalf("len(compacted) = %d, want 1: %#v", len(got), got)
	}
	assertNoDanglingToolUses(t, got)
	if got[0].Content[0].Text != "latest" {
		t.Fatalf("expected latest message only, got %#v", got)
	}
}

func TestCompactAgentMessagesNeverSendsAnthropicDanglingToolUse(t *testing.T) {
	messages := []agentMessage{
		textAgentMessage(agentRoleUser, "prompt"),
		toolUseAgentMessage("toolu_old", "artifact_write"),
		toolResultAgentMessage("toolu_old", `{"status":"success"}`),
		toolUseAgentMessage("toolu_new", "artifact_write"),
		toolResultAgentMessage("toolu_new", `{"status":"success"}`),
		textAgentMessage(agentRoleUser, "continue"),
	}

	got := compactAgentMessages(messages, 4, 1)
	assertNoDanglingToolUses(t, got)
	anthropic := anthropicMessages(got)
	for i, msg := range anthropic {
		for _, part := range msg.Content {
			if part.Type != contentPartTypeToolUse {
				continue
			}
			if i+1 >= len(anthropic) {
				t.Fatalf("anthropic transcript ends with dangling tool_use %q: %#v", part.ID, anthropic)
			}
			if !anthropicMessageHasToolResultID(anthropic[i+1], part.ID) {
				t.Fatalf("anthropic transcript has tool_use %q without immediate result: %#v", part.ID, anthropic)
			}
		}
	}
	for _, msg := range anthropic {
		if anthropicMessageHasToolUseID(msg, "toolu_old") || anthropicMessageHasToolResultID(msg, "toolu_old") {
			t.Fatalf("old tool pair should be dropped as a pair, got %#v", anthropic)
		}
	}
	if !anthropicMessageHasToolUseID(anthropic[1], "toolu_new") || !anthropicMessageHasToolResultID(anthropic[2], "toolu_new") {
		t.Fatalf("new tool pair should remain adjacent, got %#v", anthropic)
	}
	if anthropic[3].Content[0].Text != "continue" {
		t.Fatalf("expected latest text message after tool pair, got %#v", anthropic)
	}
}

func textAgentMessage(role, text string) agentMessage {
	return agentMessage{Role: role, Content: []agentContentPart{{Type: contentPartTypeText, Text: text}}}
}

func toolUseAgentMessage(id, name string) agentMessage {
	return agentMessage{
		Role: "assistant",
		Content: []agentContentPart{{
			Type:      contentPartTypeToolUse,
			ToolUseID: id,
			ToolName:  name,
		}},
	}
}

func toolResultAgentMessage(id, output string) agentMessage {
	return agentMessage{
		Role: agentRoleUser,
		Content: []agentContentPart{{
			Type:         contentPartTypeToolResult,
			ToolResultID: id,
			ToolOutput:   output,
		}},
	}
}

func assertNoDanglingToolUses(t *testing.T, messages []agentMessage) {
	t.Helper()
	for i, msg := range messages {
		for _, part := range msg.Content {
			if part.Type != contentPartTypeToolUse {
				continue
			}
			if i+1 >= len(messages) || !agentMessageHasToolResultID(messages[i+1], part.ToolUseID) {
				t.Fatalf("dangling tool_use %q at index %d in %#v", part.ToolUseID, i, messages)
			}
		}
	}
}

func agentMessageHasToolUseID(msg agentMessage, id string) bool {
	for _, part := range msg.Content {
		if part.Type == contentPartTypeToolUse && part.ToolUseID == id {
			return true
		}
	}
	return false
}

func agentMessageHasToolResultID(msg agentMessage, id string) bool {
	for _, part := range msg.Content {
		if part.Type == contentPartTypeToolResult && part.ToolResultID == id {
			return true
		}
	}
	return false
}

func anthropicMessageHasToolUseID(msg anthropicAgentMessage, id string) bool {
	for _, part := range msg.Content {
		if part.Type == contentPartTypeToolUse && part.ID == id {
			return true
		}
	}
	return false
}

func anthropicMessageHasToolResultID(msg anthropicAgentMessage, id string) bool {
	for _, part := range msg.Content {
		if part.Type == contentPartTypeToolResult && part.ToolUseID == id {
			return true
		}
	}
	return false
}

func writeAgentJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("write response: %v", err)
	}
}
