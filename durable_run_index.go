package main

import "sync"

// durableRunIndex is an in-memory projection of the run store's hot-path
// metadata. Every successful persist updates it via Upsert; queries hit
// RAM instead of walking runs/*.json.
//
// The index holds the *minimum* data needed to answer the hot questions
// without loading a full record:
//   - "is there an active run for app X?" (Submit duplicate check)
//   - "how many queued runs?" (Submit queue cap)
//   - "how many non-terminal runs?" (ActiveCount, /health)
//   - "which runs have a due-timer waiting right now?" (wakeDueRuns)
//   - "which terminal runs still need callback delivery?" (deliverCallbacks)
//   - "what run ID services app X for a scoped signal?" (findSignalTarget)
//
// Callers that need the full record still call loadRun(runID) — the index
// just narrows the candidate set to O(1) lookup or a cheap per-run
// predicate, replacing the O(H) directory scan.
//
// Consistency: Upsert is gated on the record's Revision. A stale Upsert
// (older rev arriving after a newer one) is discarded, so concurrent
// persists that land at the store in revision order can't be undone by
// an out-of-order index write.
type durableRunIndex struct {
	mu   sync.RWMutex
	runs map[string]*durableIndexEntry
}

type durableIndexEntry struct {
	RunID             string
	AppID             int
	Status            RunStatus
	Revision          int64
	CallbackURL       string
	CallbackDelivered bool
	NextCallbackAt    string
	// EarliestDueTimer is the smallest ResumeAtUnix across the record's
	// waiting entries, or 0 if the record has no timer-bound waiting.
	EarliestDueTimer int64
	// HasWaiting is true if the record has any waiting entries at all —
	// used to distinguish "waiting but no timer" from "not waiting".
	HasWaiting bool
}

func newDurableRunIndex() *durableRunIndex {
	return &durableRunIndex{runs: make(map[string]*durableIndexEntry)}
}

// Upsert folds a record's current state into the index. Older revisions
// are ignored so out-of-order post-saveRun calls can't clobber newer
// state. Safe to call without the store lock held.
func (ix *durableRunIndex) Upsert(record *durableRunRecord) {
	if ix == nil || record == nil {
		return
	}
	runID := record.Request.RunID
	if runID == "" {
		return
	}
	entry := buildIndexEntry(record)

	ix.mu.Lock()
	defer ix.mu.Unlock()
	if existing, ok := ix.runs[runID]; ok && existing.Revision > entry.Revision {
		return
	}
	ix.runs[runID] = entry
}

func buildIndexEntry(record *durableRunRecord) *durableIndexEntry {
	entry := &durableIndexEntry{
		RunID:             record.Request.RunID,
		AppID:             record.Request.AppID,
		Status:            record.Result.Status,
		Revision:          record.Revision,
		CallbackURL:       record.Request.CallbackURL,
		CallbackDelivered: record.CallbackDelivered,
		NextCallbackAt:    record.NextCallbackAt,
	}
	var earliest int64
	for _, waiting := range record.Checkpoint.Waiting {
		entry.HasWaiting = true
		if waiting.ResumeAtUnix > 0 && (earliest == 0 || waiting.ResumeAtUnix < earliest) {
			earliest = waiting.ResumeAtUnix
		}
	}
	entry.EarliestDueTimer = earliest
	return entry
}

// ActiveCount returns the number of non-terminal runs.
func (ix *durableRunIndex) ActiveCount() int {
	if ix == nil {
		return 0
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	count := 0
	for _, entry := range ix.runs {
		if !isTerminalStatus(entry.Status) {
			count++
		}
	}
	return count
}

// QueuedCount returns the number of runs whose status is currently queued.
func (ix *durableRunIndex) QueuedCount() int {
	if ix == nil {
		return 0
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	count := 0
	for _, entry := range ix.runs {
		if entry.Status == RunStatusQueued {
			count++
		}
	}
	return count
}

// ActiveRunIDForApp returns the run_id of the non-terminal run for the
// given app, if any. Empty string when no active run exists.
func (ix *durableRunIndex) ActiveRunIDForApp(appID int) string {
	if ix == nil {
		return ""
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for _, entry := range ix.runs {
		if entry.AppID == appID && !isTerminalStatus(entry.Status) {
			return entry.RunID
		}
	}
	return ""
}

// DueTimerRunIDs returns run IDs with at least one due-timer waiting
// entry as of the now timestamp.
func (ix *durableRunIndex) DueTimerRunIDs(now int64) []string {
	if ix == nil {
		return nil
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var out []string
	for _, entry := range ix.runs {
		if isTerminalStatus(entry.Status) {
			continue
		}
		if entry.EarliestDueTimer > 0 && entry.EarliestDueTimer <= now {
			out = append(out, entry.RunID)
		}
	}
	return out
}

// PendingCallbackRunIDs returns terminal run IDs with an undelivered
// callback. Callers filter further (CallbackURL presence, NextCallbackAt
// time) after loading the full record.
func (ix *durableRunIndex) PendingCallbackRunIDs() []string {
	if ix == nil {
		return nil
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var out []string
	for _, entry := range ix.runs {
		if !isTerminalStatus(entry.Status) || entry.CallbackDelivered || entry.CallbackURL == "" {
			continue
		}
		out = append(out, entry.RunID)
	}
	return out
}

// ActiveRunIDsByApp returns every non-terminal run for the given app.
// Used by findSignalTarget when the caller scopes by app_id only.
func (ix *durableRunIndex) ActiveRunIDsByApp(appID int) []string {
	if ix == nil {
		return nil
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var out []string
	for _, entry := range ix.runs {
		if entry.AppID == appID && !isTerminalStatus(entry.Status) {
			out = append(out, entry.RunID)
		}
	}
	return out
}

// allActiveRunIDs returns every non-terminal run. Used by findSignalTarget
// when the caller has no app/run scoping — still O(active) instead of the
// previous O(all-history).
func (ix *durableRunIndex) allActiveRunIDs() []string {
	if ix == nil {
		return nil
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var out []string
	for _, entry := range ix.runs {
		if !isTerminalStatus(entry.Status) {
			out = append(out, entry.RunID)
		}
	}
	return out
}
