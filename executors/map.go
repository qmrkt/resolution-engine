package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

const mapDepthKey = "__map_depth"

var deprecatedMapConfigFields = []string{
	"mode",
	"item_input_key",
	"index_input_key",
	"per_item_timeout_seconds",
}

// MapConfig is the node config for map steps.
type MapConfig struct {
	ItemsKey                string            `json:"items_key"`
	Inline                  *dag.Blueprint    `json:"inline"`
	BatchSize               int               `json:"batch_size,omitempty"`
	BatchInputKey           string            `json:"batch_input_key,omitempty"`
	BatchIndexInputKey      string            `json:"batch_index_input_key,omitempty"`
	BatchStartIndexInputKey string            `json:"batch_start_index_input_key,omitempty"`
	BatchEndIndexInputKey   string            `json:"batch_end_index_input_key,omitempty"`
	BatchItemCountInputKey  string            `json:"batch_item_count_input_key,omitempty"`
	MaxConcurrency          *int              `json:"max_concurrency,omitempty"`
	OnError                 string            `json:"on_error,omitempty"`
	MaxItems                int               `json:"max_items,omitempty"`
	MaxDepth                int               `json:"max_depth,omitempty"`
	PerBatchTimeoutSeconds  int               `json:"per_batch_timeout_seconds,omitempty"`
	InputMappings           map[string]string `json:"input_mappings,omitempty"`
	OutputKeys              []string          `json:"output_keys,omitempty"`
}

type MapExecutor struct {
	engine *dag.Engine
}

func NewMapExecutor(engine *dag.Engine) *MapExecutor {
	return &MapExecutor{engine: engine}
}

type mapSettings struct {
	batchSize               int
	batchInputKey           string
	batchIndexInputKey      string
	batchStartIndexInputKey string
	batchEndIndexInputKey   string
	batchItemCountInputKey  string
	maxConcurrency          int
	maxItems                int
	maxDepth                int
	onError                 string
	perBatchTimeoutSeconds  int
}

type mapBatch struct {
	Index      int
	StartIndex int
	EndIndex   int
	Items      []json.RawMessage
}

type mapBatchResult struct {
	BatchIndex      int               `json:"batch_index"`
	BatchStartIndex int               `json:"batch_start_index"`
	BatchEndIndex   int               `json:"batch_end_index"`
	BatchItemCount  int               `json:"batch_item_count"`
	Items           []json.RawMessage `json:"items,omitempty"`
	Status          string            `json:"status"`
	Outputs         map[string]string `json:"outputs,omitempty"`
	Error           string            `json:"error,omitempty"`
	Usage           dag.TokenUsage    `json:"usage"`
}

// DeprecatedMapConfigField reports removed map config fields so callers can
// reject stale blueprints before silently ignoring unknown JSON fields.
func DeprecatedMapConfigField(raw interface{}) (string, bool, error) {
	keys, err := rawConfigKeys(raw)
	if err != nil {
		return "", false, err
	}
	for _, field := range deprecatedMapConfigFields {
		if _, ok := keys[field]; ok {
			return field, true, nil
		}
	}
	return "", false, nil
}

func (e *MapExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	if field, ok, err := DeprecatedMapConfigField(node.Config); err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("map config: %w", err)
	} else if ok {
		return dag.ExecutorResult{}, fmt.Errorf("map config field %q is no longer supported; use batch_size and max_concurrency", field)
	}

	cfg, err := ParseConfig[MapConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("map config: %w", err)
	}
	if err := e.validateConfig(cfg); err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("map config: %w", err)
	}

	settings := resolveMapSettings(cfg)
	depth, err := currentMapDepth(execCtx)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("map depth: %w", err)
	}
	if depth >= settings.maxDepth {
		return dag.ExecutorResult{}, fmt.Errorf("map depth %d exceeds max_depth %d", depth+1, settings.maxDepth)
	}

	raw := strings.TrimSpace(execCtx.Get(strings.TrimSpace(cfg.ItemsKey)))
	if raw == "" {
		return buildMapResult(nil, dag.TokenUsage{}), nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("map items_key %q: expected JSON array: %w", cfg.ItemsKey, err)
	}
	if len(items) > settings.maxItems {
		return dag.ExecutorResult{}, fmt.Errorf("map item count %d exceeds max_items %d", len(items), settings.maxItems)
	}
	if len(items) == 0 {
		return buildMapResult(nil, dag.TokenUsage{}), nil
	}

	baseInputs := make(map[string]string, len(cfg.InputMappings))
	for childKey, parentKey := range cfg.InputMappings {
		childKey = strings.TrimSpace(childKey)
		parentKey = strings.TrimSpace(parentKey)
		if childKey != "" && parentKey != "" {
			baseInputs[childKey] = execCtx.Get(parentKey)
		}
	}

	batches := buildMapBatches(items, settings.batchSize)
	results, usage := e.executeBatches(ctx, cfg, settings, batches, baseInputs, depth)
	return buildMapResult(results, usage), nil
}

func (e *MapExecutor) executeBatches(
	ctx context.Context,
	cfg MapConfig,
	settings mapSettings,
	batches []mapBatch,
	baseInputs map[string]string,
	depth int,
) ([]mapBatchResult, dag.TokenUsage) {
	results := make([]mapBatchResult, len(batches))
	workerCount := mapWorkerCount(settings.maxConcurrency, len(batches))
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	sem := make(chan struct{}, workerCount)
	var wg sync.WaitGroup

	for _, batch := range batches {
		select {
		case sem <- struct{}{}:
		case <-runCtx.Done():
			results[batch.Index] = skippedMapBatchResult(batch)
			continue
		}

		if runCtx.Err() != nil {
			<-sem
			results[batch.Index] = skippedMapBatchResult(batch)
			continue
		}

		wg.Add(1)
		go func(batch mapBatch) {
			defer func() {
				<-sem
				wg.Done()
			}()

			result := e.executeBatch(runCtx, cfg, settings, batch, baseInputs, depth)
			results[batch.Index] = result
			if settings.onError == "fail" && result.Status != "completed" {
				cancelRun()
			}
		}(batch)
	}

	wg.Wait()

	var totalUsage dag.TokenUsage
	for _, batch := range batches {
		if results[batch.Index].Status == "" {
			results[batch.Index] = skippedMapBatchResult(batch)
		}
		totalUsage.InputTokens += results[batch.Index].Usage.InputTokens
		totalUsage.OutputTokens += results[batch.Index].Usage.OutputTokens
	}
	return results, totalUsage
}

func (e *MapExecutor) executeBatch(
	ctx context.Context,
	cfg MapConfig,
	settings mapSettings,
	batch mapBatch,
	baseInputs map[string]string,
	depth int,
) mapBatchResult {
	result := newMapBatchResult(batch)
	batchJSON, err := json.Marshal(batch.Items)
	if err != nil {
		result.Status = "failed"
		result.Error = fmt.Sprintf("marshal batch: %v", err)
		return result
	}

	childInputs := make(map[string]string, len(baseInputs)+6)
	for k, v := range baseInputs {
		childInputs[k] = v
	}
	childInputs[settings.batchInputKey] = string(batchJSON)
	childInputs[settings.batchIndexInputKey] = strconv.Itoa(batch.Index)
	childInputs[settings.batchStartIndexInputKey] = strconv.Itoa(batch.StartIndex)
	childInputs[settings.batchEndIndexInputKey] = strconv.Itoa(batch.EndIndex)
	childInputs[settings.batchItemCountInputKey] = strconv.Itoa(len(batch.Items))
	childInputs[mapDepthKey] = strconv.Itoa(depth + 1)

	childCtx := ctx
	if settings.perBatchTimeoutSeconds > 0 {
		var cancel context.CancelFunc
		childCtx, cancel = context.WithTimeout(ctx, time.Duration(settings.perBatchTimeoutSeconds)*time.Second)
		defer cancel()
	}

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
	result.Outputs = mapBatchOutputs(cfg.OutputKeys, run.Context)
	return result
}

func (e *MapExecutor) validateConfig(cfg MapConfig) error {
	if e == nil || e.engine == nil {
		return fmt.Errorf("map executor requires an engine")
	}
	if strings.TrimSpace(cfg.ItemsKey) == "" {
		return fmt.Errorf("items_key is required")
	}
	if cfg.Inline == nil || len(cfg.Inline.Nodes) == 0 {
		return fmt.Errorf("inline blueprint is required")
	}
	if errs := dag.ValidateBlueprint(*cfg.Inline); len(errs) > 0 {
		return fmt.Errorf("inline blueprint: %w", errs[0])
	}
	if cfg.BatchSize < 0 {
		return fmt.Errorf("batch_size must be non-negative")
	}
	if cfg.MaxConcurrency != nil && *cfg.MaxConcurrency < 0 {
		return fmt.Errorf("max_concurrency must be non-negative")
	}
	if cfg.MaxItems < 0 {
		return fmt.Errorf("max_items must be non-negative")
	}
	if cfg.MaxDepth < 0 {
		return fmt.Errorf("max_depth must be non-negative")
	}
	if cfg.PerBatchTimeoutSeconds < 0 {
		return fmt.Errorf("per_batch_timeout_seconds must be non-negative")
	}
	onError := strings.TrimSpace(cfg.OnError)
	if onError != "" && onError != "fail" && onError != "continue" {
		return fmt.Errorf("on_error must be \"fail\" or \"continue\"")
	}
	return nil
}

func resolveMapSettings(cfg MapConfig) mapSettings {
	maxConcurrency := DefaultMapMaxConcurrency
	if cfg.MaxConcurrency != nil {
		maxConcurrency = *cfg.MaxConcurrency
	}
	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = DefaultMapBatchSize
	}
	maxItems := cfg.MaxItems
	if maxItems == 0 {
		maxItems = DefaultMapMaxItems
	}
	maxDepth := cfg.MaxDepth
	if maxDepth == 0 {
		maxDepth = DefaultMapMaxDepth
	}
	onError := strings.TrimSpace(cfg.OnError)
	if onError == "" {
		onError = DefaultMapOnError
	}
	return mapSettings{
		batchSize:               batchSize,
		batchInputKey:           defaultString(strings.TrimSpace(cfg.BatchInputKey), DefaultMapBatchInputKey),
		batchIndexInputKey:      defaultString(strings.TrimSpace(cfg.BatchIndexInputKey), DefaultMapBatchIndexInputKey),
		batchStartIndexInputKey: defaultString(strings.TrimSpace(cfg.BatchStartIndexInputKey), DefaultMapBatchStartIndexInputKey),
		batchEndIndexInputKey:   defaultString(strings.TrimSpace(cfg.BatchEndIndexInputKey), DefaultMapBatchEndIndexInputKey),
		batchItemCountInputKey:  defaultString(strings.TrimSpace(cfg.BatchItemCountInputKey), DefaultMapBatchItemCountInputKey),
		maxConcurrency:          maxConcurrency,
		maxItems:                maxItems,
		maxDepth:                maxDepth,
		onError:                 onError,
		perBatchTimeoutSeconds:  cfg.PerBatchTimeoutSeconds,
	}
}

func buildMapBatches(items []json.RawMessage, batchSize int) []mapBatch {
	if len(items) == 0 {
		return nil
	}
	batches := make([]mapBatch, 0, (len(items)+batchSize-1)/batchSize)
	for start := 0; start < len(items); start += batchSize {
		endExclusive := start + batchSize
		if endExclusive > len(items) {
			endExclusive = len(items)
		}
		batches = append(batches, mapBatch{
			Index:      len(batches),
			StartIndex: start,
			EndIndex:   endExclusive - 1,
			Items:      append([]json.RawMessage(nil), items[start:endExclusive]...),
		})
	}
	return batches
}

func mapWorkerCount(maxConcurrency int, batchCount int) int {
	if batchCount <= 0 {
		return 0
	}
	if maxConcurrency == 0 || maxConcurrency > batchCount {
		return batchCount
	}
	return maxConcurrency
}

func currentMapDepth(execCtx *dag.Context) (int, error) {
	raw := strings.TrimSpace(execCtx.Get(mapDepthKey))
	if raw == "" {
		return 0, nil
	}
	depth, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	if depth < 0 {
		return 0, fmt.Errorf("negative depth %d", depth)
	}
	return depth, nil
}

func newMapBatchResult(batch mapBatch) mapBatchResult {
	return mapBatchResult{
		BatchIndex:      batch.Index,
		BatchStartIndex: batch.StartIndex,
		BatchEndIndex:   batch.EndIndex,
		BatchItemCount:  len(batch.Items),
		Items:           batch.Items,
	}
}

func skippedMapBatchResult(batch mapBatch) mapBatchResult {
	result := newMapBatchResult(batch)
	result.Status = "skipped"
	return result
}

func mapBatchOutputs(outputKeys []string, context map[string]string) map[string]string {
	if len(outputKeys) > 0 {
		outputs := make(map[string]string, len(outputKeys))
		for _, key := range outputKeys {
			key = strings.TrimSpace(key)
			if key != "" {
				if v, ok := context[key]; ok {
					outputs[key] = v
				}
			}
		}
		return outputs
	}
	if context == nil {
		return nil
	}
	outputs := make(map[string]string, len(context))
	for k, v := range context {
		if !strings.HasPrefix(k, "__") && !strings.HasPrefix(k, "input.") {
			outputs[k] = v
		}
	}
	return outputs
}

func buildMapResult(results []mapBatchResult, usage dag.TokenUsage) dag.ExecutorResult {
	if results == nil {
		results = []mapBatchResult{}
	}
	totalItems := 0
	completedBatches := 0
	failedBatches := 0
	skippedBatches := 0
	firstError := ""
	for _, r := range results {
		totalItems += r.BatchItemCount
		switch r.Status {
		case "completed":
			completedBatches++
		case "failed":
			failedBatches++
		case "skipped":
			skippedBatches++
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
	if failedBatches > 0 || skippedBatches > 0 {
		if completedBatches > 0 {
			status = "partial"
		} else {
			status = "failed"
		}
	}

	return dag.ExecutorResult{
		Outputs: map[string]string{
			"status":            status,
			"results":           string(encoded),
			"total_items":       strconv.Itoa(totalItems),
			"total_batches":     strconv.Itoa(len(results)),
			"completed_batches": strconv.Itoa(completedBatches),
			"failed_batches":    strconv.Itoa(failedBatches),
			"skipped_batches":   strconv.Itoa(skippedBatches),
			"first_error":       firstError,
		},
		Usage: usage,
	}
}

func rawConfigKeys(raw interface{}) (map[string]struct{}, error) {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("config is not an object")
	}
	keys := make(map[string]struct{}, len(m))
	for k := range m {
		keys[k] = struct{}{}
	}
	return keys, nil
}
