package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

// APIFetchConfig is the node config for api_fetch steps.
type APIFetchConfig struct {
	URL             string            `json:"url"`
	Method          string            `json:"method,omitempty"` // default GET
	Headers         map[string]string `json:"headers,omitempty"`
	JSONPath        string            `json:"json_path"`        // dot-notation path to extract value
	OutcomeMapping  map[string]string `json:"outcome_mapping"`  // extracted_value → outcome_index
	TimeoutSeconds  int               `json:"timeout_seconds,omitempty"`
}

// APIFetchExecutor fetches a URL, extracts a value via JSONPath, and maps it to an outcome.
type APIFetchExecutor struct {
	Client      *http.Client
	AllowLocal  bool // For testing only — allows localhost URLs
}

func NewAPIFetchExecutor() *APIFetchExecutor {
	return &APIFetchExecutor{
		Client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (e *APIFetchExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[APIFetchConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("api_fetch config: %w", err)
	}

	rawURL := execCtx.Interpolate(cfg.URL)
	method := cfg.Method
	if method == "" {
		method = "GET"
	}

	// SSRF prevention: block private/loopback/link-local addresses
	if !e.AllowLocal {
		if err := validateURLSafety(rawURL); err != nil {
			return dag.ExecutorResult{Outputs: map[string]string{
				"status": "failed",
				"error":  fmt.Sprintf("blocked: %v", err),
			}}, nil
		}
	}

	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, rawURL, nil)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("create request: %w", err)
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, execCtx.Interpolate(v))
	}

	resp, err := e.Client.Do(req)
	if err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{"status": "failed", "error": err.Error()}}, nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{"status": "failed", "error": err.Error()}}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status":      "failed",
			"error":       fmt.Sprintf("HTTP %d", resp.StatusCode),
			"response":    string(body),
		}}, nil
	}

	// Extract value via dot-notation JSONPath
	extracted, err := jsonPathExtract(body, cfg.JSONPath)
	if err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  fmt.Sprintf("jsonpath %q: %v", cfg.JSONPath, err),
		}}, nil
	}

	// Map extracted value to outcome
	outcome := ""
	if cfg.OutcomeMapping != nil {
		outcome = cfg.OutcomeMapping[extracted]
	}
	if outcome == "" {
		outcome = extracted // pass through if no mapping
	}

	return dag.ExecutorResult{Outputs: map[string]string{
		"status":    "success",
		"extracted": extracted,
		"outcome":   outcome,
		"raw":       string(body),
	}}, nil
}

// validateURLSafety blocks requests to private, loopback, and link-local addresses.
func validateURLSafety(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("empty host")
	}

	// Block known dangerous hostnames
	if host == "localhost" || host == "metadata.google.internal" {
		return fmt.Errorf("blocked host: %s", host)
	}

	// Resolve and check IP
	ip := net.ParseIP(host)
	if ip == nil {
		// Try resolving hostname
		ips, err := net.LookupIP(host)
		if err == nil && len(ips) > 0 {
			ip = ips[0]
		}
	}

	if ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("blocked private/loopback address: %s", ip)
		}
		// Block AWS metadata endpoint
		if ip.Equal(net.ParseIP("169.254.169.254")) {
			return fmt.Errorf("blocked metadata endpoint")
		}
	}

	return nil
}

// jsonPathExtract extracts a value from JSON using simple dot notation.
// E.g., "data.price" extracts from {"data": {"price": "70000"}}
func jsonPathExtract(data []byte, path string) (string, error) {
	if path == "" {
		return string(data), nil
	}

	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return "", fmt.Errorf("parse JSON: %w", err)
	}

	parts := strings.Split(path, ".")
	current := obj
	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("expected object at %q, got %T", part, current)
		}
		current, ok = m[part]
		if !ok {
			return "", fmt.Errorf("key %q not found", part)
		}
	}

	return fmt.Sprintf("%v", current), nil
}
