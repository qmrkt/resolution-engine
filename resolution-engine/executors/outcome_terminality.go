package executors

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/question-market/resolution-engine/dag"
)

type OutcomeTerminalityRule struct {
	Index          int    `json:"index"`
	WinnerWhen     string `json:"winner_when,omitempty"`
	EliminatedWhen string `json:"eliminated_when,omitempty"`
}

type OutcomeTerminalityConfig struct {
	Outcomes []OutcomeTerminalityRule `json:"outcomes"`
}

type OutcomeTerminalityExecutor struct{}

func NewOutcomeTerminalityExecutor() *OutcomeTerminalityExecutor {
	return &OutcomeTerminalityExecutor{}
}

func (e *OutcomeTerminalityExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[OutcomeTerminalityConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, err
	}
	if len(cfg.Outcomes) == 0 {
		return dag.ExecutorResult{}, fmt.Errorf("outcome_terminality requires at least one outcome rule")
	}

	rules := append([]OutcomeTerminalityRule(nil), cfg.Outcomes...)
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Index < rules[j].Index
	})

	outputs := map[string]string{
		"status":          "success",
		"winner_count":    "0",
		"open_count":      "0",
		"unique_winner":   "false",
		"winning_outcome": "",
	}

	seenIndices := make(map[int]struct{}, len(rules))
	winnerCount := 0
	openCount := 0
	winningOutcome := -1

	for _, rule := range rules {
		if rule.Index < 0 {
			return dag.ExecutorResult{}, fmt.Errorf("outcome index must be non-negative")
		}
		if _, exists := seenIndices[rule.Index]; exists {
			return dag.ExecutorResult{}, fmt.Errorf("duplicate outcome index %d", rule.Index)
		}
		seenIndices[rule.Index] = struct{}{}

		winner, err := evalOptionalCondition(rule.WinnerWhen, execCtx)
		if err != nil {
			return dag.ExecutorResult{}, fmt.Errorf("evaluate winner_when for outcome %d: %w", rule.Index, err)
		}
		eliminated, err := evalOptionalCondition(rule.EliminatedWhen, execCtx)
		if err != nil {
			return dag.ExecutorResult{}, fmt.Errorf("evaluate eliminated_when for outcome %d: %w", rule.Index, err)
		}
		if winner && eliminated {
			return dag.ExecutorResult{}, fmt.Errorf("outcome %d cannot be both winner and eliminated", rule.Index)
		}

		state := "open"
		switch {
		case winner:
			state = "winner"
			winnerCount++
			winningOutcome = rule.Index
		case eliminated:
			state = "eliminated"
		default:
			openCount++
		}

		outputs[fmt.Sprintf("outcome_%d_state", rule.Index)] = state
	}

	if winnerCount > 1 {
		return dag.ExecutorResult{}, fmt.Errorf("multiple outcomes are terminal winners")
	}

	outputs["winner_count"] = strconv.Itoa(winnerCount)
	outputs["open_count"] = strconv.Itoa(openCount)
	if winnerCount == 1 {
		outputs["unique_winner"] = "true"
		outputs["winning_outcome"] = strconv.Itoa(winningOutcome)
	}

	return dag.ExecutorResult{Outputs: outputs}, nil
}

func evalOptionalCondition(expression string, execCtx *dag.Context) (bool, error) {
	if expression == "" {
		return false, nil
	}
	return dag.EvalCondition(expression, execCtx)
}
