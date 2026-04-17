package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

// ---------------------------------------------------------------------------
// SSRF Prevention
// ---------------------------------------------------------------------------

func TestAPIFetchSSRFBlocked(t *testing.T) {
	// api_fetch should block requests to private/loopback addresses
	exec := NewAPIFetchExecutor()

	ssrfURLs := []string{
		"http://localhost/admin",
		"http://127.0.0.1/admin",
		"http://[::1]/admin",
		"http://169.254.169.254/latest/meta-data/",
		"http://10.0.0.1/internal",
		"http://192.168.1.1/router",
	}

	for _, url := range ssrfURLs {
		node := dag.NodeDef{
			ID:   "fetch",
			Type: "api_fetch",
			Config: map[string]interface{}{
				"url":       url,
				"json_path": "",
			},
		}

		result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
		if err != nil {
			t.Fatalf("url=%s: unexpected error: %v", url, err)
		}
		// Should fail gracefully (connection refused or blocked), not crash
		if result.Outputs["status"] == "success" {
			t.Fatalf("url=%s: SSRF should not succeed", url)
		}
	}
}

func TestAPIFetchRedirectToLocalhostBlocked(t *testing.T) {
	// A server that redirects to localhost should be blocked by CheckRedirect
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"secret":"leaked"}`))
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:"+strings.TrimPrefix(target.URL, "http://127.0.0.1:"), http.StatusFound)
	}))
	defer redirector.Close()

	exec := NewAPIFetchExecutor() // uses safe client, AllowLocal=false
	// Override client to allow initial connection to test server but keep CheckRedirect
	exec.Client.Transport = http.DefaultTransport
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]interface{}{
			"url":       redirector.URL,
			"json_path": "secret",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("redirect to localhost should be blocked, got status=%s", result.Outputs["status"])
	}
	if !strings.Contains(result.Outputs["error"], "redirect blocked") && !strings.Contains(result.Outputs["error"], "blocked") {
		t.Fatalf("expected redirect blocked error, got: %s", result.Outputs["error"])
	}
}

func TestAPIFetchResponseBodyLimit(t *testing.T) {
	// A server returning more than 10 MB should be rejected
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Write just over 10 MB
		w.Write([]byte(`{"data":"`))
		chunk := make([]byte, 1024*1024) // 1 MB of 'x'
		for i := range chunk {
			chunk[i] = 'x'
		}
		for i := 0; i < 11; i++ {
			w.Write(chunk)
		}
		w.Write([]byte(`"}`))
	}))
	defer server.Close()

	exec := &APIFetchExecutor{Client: http.DefaultClient, AllowLocal: true}
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]interface{}{
			"url":       server.URL,
			"json_path": "data",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed for oversized response, got %s", result.Outputs["status"])
	}
	if !strings.Contains(result.Outputs["error"], "10 MB") {
		t.Fatalf("expected body limit error, got: %s", result.Outputs["error"])
	}
}

// ---------------------------------------------------------------------------
// Malformed executor configs
// ---------------------------------------------------------------------------

func TestAPIFetchEmptyURL(t *testing.T) {
	exec := NewAPIFetchExecutor()
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]interface{}{
			"url": "",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		// Error is acceptable for empty URL
		return
	}
	if result.Outputs["status"] == "success" {
		t.Fatal("empty URL should not succeed")
	}
}

func TestAPIFetchMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json at all"))
	}))
	defer server.Close()

	exec := NewAPIFetchExecutor()
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]interface{}{
			"url":       server.URL,
			"json_path": "data",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		return // acceptable
	}
	if result.Outputs["status"] == "success" {
		t.Fatal("malformed JSON should not succeed")
	}
}

func TestAPIFetchTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer server.Close()

	exec := NewAPIFetchExecutor()
	node := dag.NodeDef{
		ID:   "fetch",
		Type: "api_fetch",
		Config: map[string]interface{}{
			"url":             server.URL,
			"json_path":       "data",
			"timeout_seconds": 1,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		return
	}
	if result.Outputs["status"] == "success" {
		t.Fatal("timed out request should not succeed")
	}
}

// ---------------------------------------------------------------------------
// LLM Judge abuse
// ---------------------------------------------------------------------------

func TestLLMCallUnparseableResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return valid API response but non-JSON content
		json.NewEncoder(w).Encode(map[string]interface{}{
			"content": []map[string]string{
				{"text": "I think outcome 0 is correct but I won't format it as JSON"},
			},
			"usage": map[string]int{"input_tokens": 10, "output_tokens": 20},
		})
	}))
	defer server.Close()

	exec := NewLLMCallExecutor("test-key")
	exec.provider.AnthropicBaseURL = server.URL

	node := dag.NodeDef{
		ID:   "judge",
		Type: "llm_call",
		Config: map[string]interface{}{
			"prompt": "What is the outcome?",
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	// Should return inconclusive, not crash
	if result.Outputs["outcome"] != "inconclusive" {
		t.Fatalf("unparseable LLM response should be inconclusive, got %q", result.Outputs["outcome"])
	}
}

func TestLLMCallInjectionViaPrompt(t *testing.T) {
	// Verify that template interpolation doesn't allow code injection
	ctx := dag.NewInvocationFromInputs(map[string]string{
		"market_question": `"; DROP TABLE markets; --`,
	})

	result := ctx.Interpolate("Question: {{inputs.market_question}}")
	// The interpolation should be a simple string replacement, not eval
	if !strings.Contains(result, "DROP TABLE") {
		t.Fatal("interpolation should be literal, not filtered")
	}
	// The point: interpolation is safe because it's pure string replacement.
	// There's no eval, no shell execution, no SQL — just text into text.
}

// ---------------------------------------------------------------------------
// Full integration: malicious blueprint
// ---------------------------------------------------------------------------

func TestMaliciousBlueprintWithDeepNesting(t *testing.T) {
	// Blueprint with many nodes in a chain — should not stack overflow
	engine := dag.NewEngine(nil)

	passthrough := NewWaitExecutor() // reuse wait with 0 duration as passthrough
	engine.RegisterExecutor("step", passthrough)

	nodes := make([]dag.NodeDef, 50)
	edges := make([]dag.EdgeDef, 49)
	for i := 0; i < 50; i++ {
		nodes[i] = dag.NodeDef{
			ID:   fmt.Sprintf("n%03d", i),
			Type: "step",
			Config: map[string]interface{}{
				"duration_seconds": 0,
			},
		}
	}
	for i := 1; i < 50; i++ {
		edges[i-1] = dag.EdgeDef{From: nodes[i-1].ID, To: nodes[i].ID}
	}

	bp := dag.Blueprint{ID: "deep", Nodes: nodes, Edges: edges}
	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
}
