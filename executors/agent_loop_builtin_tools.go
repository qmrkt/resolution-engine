package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type contextGetTool struct {
	access contextAccess
}

func (t *contextGetTool) Name() string { return AgentBuiltinContextGet }
func (t *contextGetTool) Description() string {
	return "Read selected values from the current execution context. Internal and secret-like keys are hidden unless explicitly allowlisted."
}
func (t *contextGetTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"keys":{"type":"array","items":{"type":"string"},"description":"Context keys to read."}},"required":["keys"]}`)
}
func (t *contextGetTool) ReadOnly() bool { return true }
func (t *contextGetTool) Execute(_ context.Context, args json.RawMessage) (agentToolResult, error) {
	var input struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return toolErrorResult("invalid context_get input: %v", err), nil
	}
	values := map[string]string{}
	missing := make([]string, 0)
	denied := make([]string, 0)
	for _, key := range input.Keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if !t.access.readable(key) {
			denied = append(denied, key)
			continue
		}
		value := t.access.execCtx.Get(key)
		if value == "" {
			missing = append(missing, key)
			continue
		}
		values[key] = value
	}
	return jsonToolResult(map[string]any{
		"status":  "success",
		"values":  values,
		"missing": missing,
		"denied":  denied,
	}), nil
}

type contextListTool struct {
	access contextAccess
}

func (t *contextListTool) Name() string { return AgentBuiltinContextList }
func (t *contextListTool) Description() string {
	return "List readable execution context keys. Internal and secret-like keys are hidden unless explicitly allowlisted."
}
func (t *contextListTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}
func (t *contextListTool) ReadOnly() bool { return true }
func (t *contextListTool) Execute(_ context.Context, _ json.RawMessage) (agentToolResult, error) {
	keys := t.access.readableKeys()
	return jsonToolResult(map[string]any{"status": "success", "keys": keys}), nil
}

type sourceFetchTool struct {
	allowLocal bool
	client     *http.Client
}

func (t *sourceFetchTool) Name() string { return AgentBuiltinSourceFetch }
func (t *sourceFetchTool) Description() string {
	return "Fetch a public HTTP(S) source with SSRF protections, redirect limits, timeout, and response-size limits. Only GET is supported."
}
func (t *sourceFetchTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"url":{"type":"string","description":"Absolute HTTP(S) URL to fetch."},"json_path":{"type":"string","description":"Optional dot-notation path to extract from a JSON response."}},"required":["url"]}`)
}
func (t *sourceFetchTool) ReadOnly() bool { return true }
func (t *sourceFetchTool) Execute(ctx context.Context, args json.RawMessage) (agentToolResult, error) {
	var input struct {
		URL      string `json:"url"`
		JSONPath string `json:"json_path"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return toolErrorResult("invalid source_fetch input: %v", err), nil
	}
	input.URL = strings.TrimSpace(input.URL)
	if input.URL == "" {
		return toolErrorResult("source_fetch url is required"), nil
	}
	if !t.allowLocal {
		if err := validateURLSafety(input.URL); err != nil {
			return jsonToolResult(map[string]any{"status": "failed", "error": "blocked: " + err.Error()}), nil
		}
	}
	client := t.client
	if client == nil {
		if t.allowLocal {
			client = &http.Client{Timeout: DefaultAPIFetchTimeout}
		} else {
			client = newSafeClient()
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, input.URL, nil)
	if err != nil {
		return toolErrorResult("create request: %v", err), nil
	}
	resp, err := client.Do(req)
	if err != nil {
		return jsonToolResult(map[string]any{"status": "failed", "error": err.Error()}), nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return jsonToolResult(map[string]any{"status": "failed", "error": err.Error()}), nil
	}
	if int64(len(body)) > maxResponseBytes {
		return jsonToolResult(map[string]any{"status": "failed", "error": "response body exceeded 10 MB limit"}), nil
	}
	output := map[string]any{
		"status":       "success",
		"status_code":  resp.StatusCode,
		"content_type": resp.Header.Get("Content-Type"),
		"body":         string(body),
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		output["status"] = "failed"
		output["error"] = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	if strings.TrimSpace(input.JSONPath) != "" {
		extracted, err := jsonPathExtract(body, input.JSONPath)
		if err != nil {
			output["status"] = "failed"
			output["error"] = fmt.Sprintf("json_path %q: %v", input.JSONPath, err)
		} else {
			output["extracted"] = extracted
		}
	}
	return jsonToolResult(output), nil
}

type jsonExtractTool struct {
	access contextAccess
}

func (t *jsonExtractTool) Name() string { return AgentBuiltinJSONExtract }
func (t *jsonExtractTool) Description() string {
	return "Extract a value from JSON using simple dot-notation. Provide either json or context_key."
}
func (t *jsonExtractTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"json":{"type":"string","description":"JSON document."},"context_key":{"type":"string","description":"Readable context key containing JSON."},"path":{"type":"string","description":"Dot-notation path."}},"required":["path"]}`)
}
func (t *jsonExtractTool) ReadOnly() bool { return true }
func (t *jsonExtractTool) Execute(_ context.Context, args json.RawMessage) (agentToolResult, error) {
	var input struct {
		JSON       string `json:"json"`
		ContextKey string `json:"context_key"`
		Path       string `json:"path"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return toolErrorResult("invalid json_extract input: %v", err), nil
	}
	raw := input.JSON
	if strings.TrimSpace(input.ContextKey) != "" {
		key := strings.TrimSpace(input.ContextKey)
		if !t.access.readable(key) {
			return jsonToolResult(map[string]any{"status": "failed", "error": fmt.Sprintf("context key %q is not readable", key)}), nil
		}
		raw = t.access.execCtx.Get(key)
	}
	if strings.TrimSpace(raw) == "" {
		return jsonToolResult(map[string]any{"status": "failed", "error": "json or context_key is required"}), nil
	}
	value, err := jsonPathExtract([]byte(raw), input.Path)
	if err != nil {
		return jsonToolResult(map[string]any{"status": "failed", "error": err.Error()}), nil
	}
	return jsonToolResult(map[string]any{"status": "success", "value": value}), nil
}

type outputCaptureTool struct {
	name        string
	description string
	parameters  json.RawMessage
}

func (t *outputCaptureTool) Name() string { return t.name }
func (t *outputCaptureTool) Description() string {
	if strings.TrimSpace(t.description) != "" {
		return t.description
	}
	return "Record the final structured output and finish the agent loop."
}
func (t *outputCaptureTool) Parameters() json.RawMessage { return defaultSchemaObject(t.parameters) }
func (t *outputCaptureTool) ReadOnly() bool              { return false }
func (t *outputCaptureTool) Execute(_ context.Context, args json.RawMessage) (agentToolResult, error) {
	if len(args) == 0 || !json.Valid(args) {
		return agentToolResult{Output: `{"status":"failed","error":"invalid structured output"}`, IsError: true}, nil
	}
	return agentToolResult{Output: string(args), Complete: true, OutputJSON: append(json.RawMessage(nil), args...)}, nil
}
