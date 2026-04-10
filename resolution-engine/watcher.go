package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Market status constants matching the on-chain state machine.
const (
	StatusCreated            = 0
	StatusActive             = 1
	StatusResolutionPending  = 2
	StatusResolutionProposed = 3
	StatusCancelled          = 4
	StatusResolved           = 5
	StatusDisputed           = 6
)

// OutcomePayload accepts either a legacy JSON-string-encoded outcome list
// or the current direct JSON array returned by the indexer.
type OutcomePayload string

func (o *OutcomePayload) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		*o = ""
		return nil
	}

	var direct []string
	if err := json.Unmarshal(data, &direct); err == nil {
		canonical, err := json.Marshal(direct)
		if err != nil {
			return err
		}
		*o = OutcomePayload(canonical)
		return nil
	}

	var legacy string
	if err := json.Unmarshal(data, &legacy); err == nil {
		*o = OutcomePayload(strings.TrimSpace(legacy))
		return nil
	}

	return fmt.Errorf("unsupported outcomes payload: %s", trimmed)
}

// MarketInfo is the market state from the indexer API.
type MarketInfo struct {
	AppID                  int            `json:"appId"`
	Status                 int            `json:"status"`
	Creator                string         `json:"creator"`
	Question               string         `json:"question"`
	Outcomes               OutcomePayload `json:"outcomes"`
	Deadline               int            `json:"deadline"`
	ResolutionPendingSince int64          `json:"resolutionPendingSince,omitempty"`
	ProposedOutcome        int            `json:"proposedOutcome,omitempty"`
	ProposalTimestamp      int            `json:"proposalTimestamp,omitempty"`
	ChallengeWindowSecs    int            `json:"challengeWindowSecs,omitempty"`
	Challenger             string         `json:"challenger,omitempty"`
	ChallengeReasonCode    int            `json:"challengeReasonCode,omitempty"`
	MarketAdmin            string         `json:"marketAdmin,omitempty"`
	ResolutionAuthority    string         `json:"resolutionAuthority,omitempty"`
}

// WatcherStats holds observable state for the health endpoint.
type WatcherStats struct {
	LastPollAt     time.Time
	MarketsWatched int
}

// Watcher polls the indexer for markets needing resolution.
type Watcher struct {
	indexerURL string
	interval   time.Duration
	client     *http.Client

	mu    sync.Mutex
	stats WatcherStats
}

func NewWatcher(indexerURL string, interval time.Duration) *Watcher {
	return &Watcher{
		indexerURL: indexerURL,
		interval:   interval,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// Stats returns a snapshot of the watcher's observable state.
func (w *Watcher) Stats() WatcherStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

func (w *Watcher) updateStats(totalMarkets int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stats.LastPollAt = time.Now()
	w.stats.MarketsWatched = totalMarkets
}

// PollActive returns markets in ACTIVE status (status=1).
func (w *Watcher) PollActive() ([]MarketInfo, error) {
	url := fmt.Sprintf("%s/markets?status=1", w.indexerURL)
	resp, err := w.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("poll indexer: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("indexer returned %d: %s", resp.StatusCode, string(body))
	}

	var markets []MarketInfo
	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, fmt.Errorf("parse markets: %w", err)
	}

	return markets, nil
}

// PollPending returns markets in RESOLUTION_PENDING status (status=2).
func (w *Watcher) PollPending() ([]MarketInfo, error) {
	url := fmt.Sprintf("%s/markets?status=2", w.indexerURL)
	resp, err := w.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("poll indexer: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("indexer returned %d: %s", resp.StatusCode, string(body))
	}

	var markets []MarketInfo
	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, fmt.Errorf("parse markets: %w", err)
	}

	return markets, nil
}

// PollProposed returns markets in RESOLUTION_PROPOSED status (status=3).
// The automation service performs the authoritative chain-time readiness check.
func (w *Watcher) PollProposed() ([]MarketInfo, error) {
	url := fmt.Sprintf("%s/markets?status=3", w.indexerURL)
	resp, err := w.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("poll indexer: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("indexer returned %d: %s", resp.StatusCode, string(body))
	}

	var markets []MarketInfo
	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, fmt.Errorf("parse markets: %w", err)
	}

	return markets, nil
}

// PollDisputed returns markets in DISPUTED status (status=6)
// that need dispute-path execution or adjudication.
func (w *Watcher) PollDisputed() ([]MarketInfo, error) {
	url := fmt.Sprintf("%s/markets?status=6", w.indexerURL)
	resp, err := w.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("poll indexer: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("indexer returned %d: %s", resp.StatusCode, string(body))
	}

	var markets []MarketInfo
	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, fmt.Errorf("parse markets: %w", err)
	}

	return markets, nil
}

// Run starts the watch loop. Calls onActive for each active market,
// onPending for each market needing resolution, onFinalizable for each
// market ready to finalize, and onDisputed for each market that has been
// challenged and needs dispute-path adjudication.
func (w *Watcher) Run(
	done <-chan struct{},
	onActive func(MarketInfo),
	onPending func(MarketInfo),
	onFinalizable func(MarketInfo),
	onDisputed func(MarketInfo),
) {
	for {
		totalMarkets := 0

		active, err := w.PollActive()
		if err != nil {
			slog.Error("poll active error", "component", "watcher", "error", err)
		} else {
			totalMarkets += len(active)
			for _, m := range active {
				onActive(m)
			}
		}

		pending, err := w.PollPending()
		if err != nil {
			slog.Error("poll pending error", "component", "watcher", "error", err)
		} else {
			totalMarkets += len(pending)
			for _, m := range pending {
				onPending(m)
			}
		}

		proposed, err := w.PollProposed()
		if err != nil {
			slog.Error("poll proposed error", "component", "watcher", "error", err)
		} else {
			for _, m := range proposed {
				if strings.TrimSpace(m.Challenger) != "" {
					continue
				}
				totalMarkets++
				onFinalizable(m)
			}
		}

		disputed, err := w.PollDisputed()
		if err != nil {
			slog.Error("poll disputed error", "component", "watcher", "error", err)
		} else {
			totalMarkets += len(disputed)
			for _, m := range disputed {
				onDisputed(m)
			}
		}

		w.updateStats(totalMarkets)

		select {
		case <-done:
			return
		case <-time.After(w.interval):
		}
	}
}
