package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

// MapConfig is the node config for map steps.
type MapConfig struct {
	ItemsKey              string            `json:"items_key"`
	Inline                *dag.Blueprint    `json:"inline"`
	ItemInputKey          string            `json:"item_input_key,omitempty"`
	IndexInputKey         string            `json:"index_input_key,omitempty"`
	Mode                  string            `json:"mode,omitempty"`
	MaxConcurrency        int               `json:"max_concurrency,omitempty"`
	OnError               string            `json:"on_error,omitempty"`
	MaxItems              int               `json:"max_items,omitempty"`
	PerItemTimeoutSeconds int               `json:"per_item_timeout_seconds,omitempty"`
	InputMappings         map[string]string `json:"input_mappings,omitempty"`
	OutputKeys            []string          `json:"output_keys,omitempty"`
}

type MapExecutor struct {
	engine *dag.Engine
}

func NewMapExecutor(engine *dag.Engine) *MapExecutor {
	return &MapExecutor{engine: engine}
}

type mapItemResult struct {
	Index   int               `json:"index"`
	Status  string            `json:"status"`
	Item    json.RawMessage   `json:"item,omitempty"`
	Outputs map[string]string `json:"outputs,omitempty"`
	Error   string            `json:"error,omitempty"`
	Usage   dag.TokenUsage    `json:"usage"`
}

func (e *MapExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[MapConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("map config: %w", err)
	}
	if err := e.validateConfig(cfg); err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("map config: %w", err)
	}

	itemInputKey := strings.TrimSpace(cfg.ItemInputKey)
	if itemInputKey == "" {
		itemInputKey = DefaultMapItemInputKey
	}
	indexInputKey := strings.TrimSpace(cfg.IndexInputKey)
	if indexInputKey == "" {
		indexInputKey = DefaultMapIndexInputKey
	}
	maxItems := cfg.MaxItems
	if maxItems <= 0 {
		maxItems = DefaultMapMaxItems
	}
	onError := strings.TrimSpace(cfg.OnError)
	if onError == "" {
		onError = DefaultMapOnError
	}

	// Parse items from context.
	raw := strings.TrimSpace(execCtx.Get(strings.TrimSpace(cfg.ItemsKey)))
	if raw == "" {
		return buildMapResult(nil, dag.TokenUsage{}), nil
	}
	var items []any
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("map items_key %q: expected JSON array: %w", cfg.ItemsKey, err)
	}
	if len(items) > maxItems {
		return dag.ExecutorResult{}, fmt.Errorf("map item count %d exceeds max_items %d", len(items), maxItems)
	}
	if len(items) == 0 {
		return buildMapResult(nil, dag.TokenUsage{}), nil
	}

	// Build child inputs template from InputMappings.
	baseInputs := make(map[string]string, len(cfg.InputMappings))
	for childKey, parentKey := range cfg.InputMappings {
		childKey = strings.TrimSpace(childKey)
		parentKey = strings.TrimSpace(parentKey)
		if childKey != "" && parentKey != "" {
			baseInputs[childKey] = execCtx.Get(parentKey)
		}
	}

	// Execute items.
	mode := strings.TrimSpace(cfg.Mode)
	if mode == "" {
		mode = DefaultMapMode
	}

	results := make([]mapItemResult, len(items))
	var totalUsage dag.TokenUsage

	switch mode {
	case "sequential":
		for i, item := range items {
			if ctx.Err() != nil {
				break
			}
			results[i] = e.executeItem(ctx, cfg, item, i, itemInputKey, indexInputKey, baseInputs)
			totalUsage.InputTokens += results[i].Usage.InputTokens
			totalUsage.OutputTokens += results[i].Usage.OutputTokens
			if onError == "fail" && results[i].Status != "completed" {
				break
			}
		}
	default: // parallel
		maxConc := cfg.MaxConcurrency
		if maxConc <= 0 {
			maxConc = DefaultMapMaxConcurrency
		}

		runCtx, cancelRun := context.WithCancel(ctx)
		defer cancelRun()

		sem := make(chan struct{}, maxConc)
		var mu sync.Mutex
		var wg sync.WaitGroup
		var firstErr bool

		for i, item := range items {
			if runCtx.Err() != nil {
				break
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(idx int, itm any) {
				defer func() { <-sem; wg.Done() }()
				result := e.executeItem(runCtx, cfg, itm, idx, itemInputKey, indexInputKey, baseInputs)
				mu.Lock()
				results[idx] = result
				totalUsage.InputTokens += result.Usage.InputTokens
				totalUsage.OutputTokens += result.Usage.OutputTokens
				if onError == "fail" && result.Status != "completed" && !firstErr {
					firstErr = true
					cancelRun()
				}
				mu.Unlock()
			}(i, item)
		}
		wg.Wait()
	}

	return buildMapResult(results, totalUsage), nil
}

func (e *MapExecutor) executeItem(
	ctx context.Context,
	cfg MapConfig,
	item any,
	index int,
	itemInputKey, indexInputKey string,
	baseInputs map[string]string,
) mapItemResult {
	// Serialize item to string.
	itemJSON, err := json.Marshal(item)
	if err != nil {
		return mapItemResult{Index: index, Status: "failed", Error: fmt.Sprintf("marshal item: %v", err)}
	}

	result := mapItemResult{Index: index, Item: itemJSON}

	// Build child inputs.
	childInputs := make(map[string]string, len(baseInputs)+2)
	for k, v := range baseInputs {
		childInputs[k] = v
	}
	childInputs[itemInputKey] = string(itemJSON)
	childInputs[indexInputKey] = strconv.Itoa(index)

	// Apply per-item timeout.
	childCtx := ctx
	if cfg.PerItemTimeoutSeconds > 0 {
		var cancel context.CancelFunc
		childCtx, cancel = context.WithTimeout(ctx, time.Duration(cfg.PerItemTimeoutSeconds)*time.Second)
		defer cancel()
	}

	// Execute child blueprint.
	run, err := e.engine.Execute(childCtx, *cfg.Inline, childInputs)
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		if run != nil {
			result.Usage = run.Usage
		}
		return result
	}
	if run == nil {
		result.Status = "failed"
		result.Error = "child run returned nil"
		return result
	}

	result.Usage = run.Usage
	if run.Status == "failed" {
		result.Status = "failed"
		result.Error = run.Error
		return result
	}

	result.Status = "completed"

	// Extract outputs.
	if len(cfg.OutputKeys) > 0 {
		outputs := make(map[string]string, len(cfg.OutputKeys))
		for _, key := range cfg.OutputKeys {
			key = strings.TrimSpace(key)
			if key != "" {
				if v, ok := run.Context[key]; ok {
					outputs[key] = v
				}
			}
		}
		result.Outputs = outputs
	} else if run.Context != nil {
		// No output_keys specified — return all non-internal context values.
		outputs := make(map[string]string, len(run.Context))
		for k, v := range run.Context {
			if !strings.HasPrefix(k, "__") && !strings.HasPrefix(k, "input.") {
				outputs[k] = v
			}
		}
		result.Outputs = outputs
	}

	return result
}

func (e *MapExecutor) validateConfig(cfg MapConfig) error {
	if strings.TrimSpace(cfg.ItemsKey) == "" {
		return fmt.Errorf("items_key is required")
	}
	if cfg.Inline == nil || len(cfg.Inline.Nodes) == 0 {
		return fmt.Errorf("inline blueprint is required")
	}
	if errs := dag.ValidateBlueprint(*cfg.Inline); len(errs) > 0 {
		return fmt.Errorf("inline blueprint: %w", errs[0])
	}
	for _, node := range cfg.Inline.Nodes {
		if node.Type == "map" {
			return fmt.Errorf("inline blueprint must not contain map nodes")
		}
	}
	mode := strings.TrimSpace(cfg.Mode)
	if mode != "" && mode != "parallel" && mode != "sequential" {
		return fmt.Errorf("mode must be \"parallel\" or \"sequential\"")
	}
	onError := strings.TrimSpace(cfg.OnError)
	if onError != "" && onError != "fail" && onError != "continue" {
		return fmt.Errorf("on_error must be \"fail\" or \"continue\"")
	}
	return nil
}

func buildMapResult(results []mapItemResult, usage dag.TokenUsage) dag.ExecutorResult {
	if results == nil {
		results = []mapItemResult{}
	}
	completedCount := 0
	failedCount := 0
	firstError := ""
	for _, r := range results {
		switch r.Status {
		case "completed":
			completedCount++
		case "failed":
			failedCount++
		}
		if firstError == "" && r.Error != "" {
			firstError = r.Error
		}
	}

	encoded, err := json.Marshal(results)
	if err != nil {
		encoded = []byte("[]")
	}

	status := "success"
	if failedCount > 0 && completedCount > 0 {
		status = "partial"
	} else if failedCount > 0 {
		status = "failed"
	}

	return dag.ExecutorResult{
		Outputs: map[string]string{
			"status":          status,
			"results":         string(encoded),
			"completed_count": strconv.Itoa(completedCount),
			"failed_count":    strconv.Itoa(failedCount),
			"total_count":     strconv.Itoa(len(results)),
			"first_error":     firstError,
		},
		Usage: usage,
	}
}
