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

	"github.com/question-market/resolution-engine/dag"
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
			"prompt":               "Resolve {{market.question}}",
			"output_mode":          AgentOutputModeResolution,
			"allowed_outcomes_key": "market.outcomes_json",
		},
	}
	ctx := dag.NewContext(map[string]string{
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

	result, err := exec.Execute(context.Background(), node, dag.NewContext(map[string]string{"market.question": "question"}))
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

	result, err := exec.Execute(context.Background(), node, dag.NewContext(map[string]string{"market.question": "question"}))
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

	result, err := exec.Execute(context.Background(), node, dag.NewContext(map[string]string{"market.question": "question"}))
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

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
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

	result, err := exec.Execute(context.Background(), node, dag.NewContext(map[string]string{"market.question": "question"}))
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
					"output_keys":    []string{"echo.value"},
					"inline": dag.Blueprint{
						ID:      "child-check",
						Version: 1,
						Nodes: []dag.NodeDef{
							{ID: "echo", Type: "agent_test_echo", Config: map[string]any{}},
						},
					},
				},
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
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

func TestAgentLoopDynamicBlueprintTool(t *testing.T) {
	child := dag.Blueprint{
		ID:      "dynamic-child",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 10},
		Nodes: []dag.NodeDef{
			{ID: "calc", Type: "cel_eval", Config: map[string]any{"expressions": map[string]string{"answer": "'dynamic-ok'"}}},
		},
	}
	childJSON, err := json.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	runBlueprintArgs, _ := json.Marshal(map[string]any{
		"blueprint_json": string(childJSON),
		"output_keys":    []string{"calc.answer"},
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

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" || result.Outputs["output.answer"] != "dynamic-ok" {
		t.Fatalf("unexpected result: %+v", result.Outputs)
	}
}

type agentLoopEchoExecutor struct{}

func (e *agentLoopEchoExecutor) Execute(_ context.Context, _ dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	return dag.ExecutorResult{Outputs: map[string]string{
		"status": "success",
		"value":  fmt.Sprintf("%s-checked", execCtx.Get("input_value")),
	}}, nil
}

func writeAgentJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("write response: %v", err)
	}
}
