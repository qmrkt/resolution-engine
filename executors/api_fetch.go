package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type APIFetchBasicAuth struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// APIFetchConfig is the node config for api_fetch steps.
type APIFetchConfig struct {
	URL            string             `json:"url"`
	Method         string             `json:"method,omitempty"` // default GET
	Headers        map[string]string  `json:"headers,omitempty"`
	Body           string             `json:"body,omitempty"`
	BasicAuth      *APIFetchBasicAuth `json:"basic_auth,omitempty"`
	JSONPath       string             `json:"json_path,omitempty"` // dot-notation path to extract value; empty returns raw body
	OutcomeMapping map[string]string  `json:"outcome_mapping"`     // extracted_value → outcome_index
	TimeoutSeconds int                `json:"timeout_seconds,omitempty"`
}

// APIFetchExecutor fetches a URL, extracts a value via JSONPath, and maps it to an outcome.
type APIFetchExecutor struct {
	Client     *http.Client
	AllowLocal bool // For testing only — allows localhost URLs
}

const maxResponseBytes int64 = MaxAPIResponseBytes

var supportedAPIFetchMethods = map[string]struct{}{
	http.MethodDelete: {},
	http.MethodGet:    {},
	http.MethodPatch:  {},
	http.MethodPost:   {},
	http.MethodPut:    {},
}

func NewAPIFetchExecutor() *APIFetchExecutor {
	return &APIFetchExecutor{
		Client: newSafeClient(),
	}
}

func newSafeClient() *http.Client {
	return &http.Client{
		Timeout: DefaultAPIFetchTimeout,
		Transport: &http.Transport{
			DialContext: safeDialContext,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if err := validateURLSafety(req.URL.String()); err != nil {
				return fmt.Errorf("redirect blocked: %w", err)
			}
			if len(via) >= MaxRedirects {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
}

func NormalizeAPIFetchMethod(method string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(method))
	if normalized == "" {
		normalized = http.MethodGet
	}
	if _, ok := supportedAPIFetchMethods[normalized]; !ok {
		return "", fmt.Errorf("unsupported method %q", method)
	}
	return normalized, nil
}

// safeDialContext rejects connections to private/loopback/link-local IPs at connect time,
// preventing DNS rebinding attacks that bypass pre-request URL validation.
func safeDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", addr, err)
	}

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", host, err)
	}

	for _, ipAddr := range ips {
		if isBlockedIP(ipAddr.IP) {
			continue
		}
		dialer := &net.Dialer{Timeout: DefaultDialTimeout}
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ipAddr.IP.String(), port))
		if err != nil {
			continue
		}
		return conn, nil
	}

	return nil, fmt.Errorf("no safe IP addresses for %q", host)
}

func (e *APIFetchExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[APIFetchConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("api_fetch config: %w", err)
	}

	rawURL := execCtx.Interpolate(cfg.URL)
	method, err := NormalizeAPIFetchMethod(cfg.Method)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("api_fetch config: %w", err)
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
		timeout = DefaultAPIFetchTimeout
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	requestBody := execCtx.Interpolate(cfg.Body)
	var bodyReader io.Reader
	if requestBody != "" {
		bodyReader = strings.NewReader(requestBody)
	}

	req, err := http.NewRequestWithContext(reqCtx, method, rawURL, bodyReader)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("create request: %w", err)
	}
	if cfg.BasicAuth != nil {
		req.SetBasicAuth(execCtx.Interpolate(cfg.BasicAuth.Username), execCtx.Interpolate(cfg.BasicAuth.Password))
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, execCtx.Interpolate(v))
	}

	client := e.Client
	if e.AllowLocal {
		client = &http.Client{Timeout: timeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{"status": "failed", "error": err.Error()}}, nil
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{"status": "failed", "error": err.Error()}}, nil
	}
	if int64(len(responseBody)) > maxResponseBytes {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  "response body exceeded 10 MB limit",
		}}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status":       "failed",
			"error":        fmt.Sprintf("HTTP %d", resp.StatusCode),
			"status_code":  strconv.Itoa(resp.StatusCode),
			"content_type": resp.Header.Get("Content-Type"),
			"response":     string(responseBody),
			"raw":          string(responseBody),
		}}, nil
	}

	// Extract value via dot-notation JSONPath
	extracted, err := jsonPathExtract(responseBody, cfg.JSONPath)
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
		"status":       "success",
		"status_code":  strconv.Itoa(resp.StatusCode),
		"content_type": resp.Header.Get("Content-Type"),
		"extracted":    extracted,
		"outcome":      outcome,
		"raw":          string(responseBody),
	}}, nil
}

// isBlockedIP returns true for loopback, private, link-local, and metadata IPs.
func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if ip.Equal(net.ParseIP("169.254.169.254")) {
		return true
	}
	return false
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
		ips, err := net.LookupIP(host)
		if err == nil && len(ips) > 0 {
			ip = ips[0]
		}
	}

	if isBlockedIP(ip) {
		return fmt.Errorf("blocked private/loopback address: %s", ip)
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
