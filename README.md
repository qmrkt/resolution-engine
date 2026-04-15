# resolution-engine

Async blueprint execution service for [question.market](https://question.market).

Scoped to blueprint execution orchestration:

- accepts blueprint execution requests over HTTP
- runs DAG-based resolution logic
- exposes async run status and cancellation
- emits structured execution traces for observability
- owns the execution lifecycle for queued, running, waiting, and terminal runs

It does not own chain indexing or market read models. Those concerns belong to the indexer, which can feed the engine durable resume signals as chain facts change.

## Quick start

Requires Go 1.23+.

```bash
go mod download

# Required
export INDEXER_URL=http://localhost:3001
export LISTEN_PORT=3002

# LLM providers (at least one recommended)
export ANTHROPIC_API_KEY=...
export OPENAI_API_KEY=...
export GOOGLE_API_KEY=...

# Optional auth
export ENGINE_CONTROL_TOKEN=...
export ENGINE_CALLBACK_TOKEN=...
export TRACE_INGEST_TOKEN=...

go run .
```

## HTTP API

### POST /run

Submit a blueprint execution request.

```json
{
  "app_id": 123,
  "blueprint_json": {"id": "bp", "nodes": [], "edges": []},
  "inputs": {"market_question": "Will BTC hit 150k?"},
  "blueprint_path": "main",
  "initiator": "indexer:status-transition",
  "callback_url": "http://indexer.local/markets/123/resolution-result"
}
```

Returns `202 Accepted` with a `run_id` and durable status, usually `queued`.
Returns `409 Conflict` if the market already has an active run.
Returns `429 Too Many Requests` if the local workflow queue is full.

Invalid blueprints are rejected with `400 Bad Request` and a list of validation issues.

### GET /runs/{run_id}

Returns durable run state. Runs may be `queued`, `running`, `waiting`,
`completed`, `failed`, or `cancelled`.

### DELETE /runs/{run_id}

Cancels an in-flight run.

### POST /signals

Persist an idempotent resume signal from the indexer or another trusted caller.
Matching signals resume waiting workflow nodes such as `await_signal` and
`defer_resolution`.

```json
{
  "idempotency_key": "chain-event-123:market-456",
  "app_id": 456,
  "run_id": "optional-run-id",
  "signal_type": "human_judgment.responded",
  "correlation_key": "456:run-id:judge",
  "payload": {"outcome": "1", "reason": "Evidence is conclusive."}
}
```

### GET /health

Returns service health and active run count.

## Architecture

The engine executes resolution blueprints as DAGs with conditional branching (CEL expressions). Node types are handled by typed executors or durable suspension handlers:

| Executor              | Description                                                      |
| --------------------- | ---------------------------------------------------------------- |
| `llm_call`            | Multi-provider LLM call (Anthropic, OpenAI, Google)              |
| `agent_loop`          | Multi-provider agent loop with native and blueprint-backed tools |
| `api_fetch`           | External data source fetching                                    |
| `await_signal`        | Suspends until a correlated signal or timeout                    |
| `cel_eval`            | Evaluates CEL expressions into context outputs                   |
| `map`                 | Runs an inline child blueprint over a JSON array                 |
| `defer_resolution`    | Defers resolution to a later time                                |
| `wait`                | Pauses execution for a duration                                  |
| `submit_result`       | Marks the final outcome                                          |
| `cancel_market`       | Marks the market for cancellation                                |

Terminal results carry an action: `propose`, `finalize_dispute`, `cancel_market`, `defer`, or `none`.

## Blueprint semantics

A blueprint is a directed graph of steps. Each step writes values into a shared execution context, and later steps or edges can read those values.

### Inputs and context

The engine seeds your request inputs into the context twice:

- as plain keys like `market_question`
- as `input.*` keys like `input.market_question`

That means either style can be referenced by executors and conditions.

```text
POST /run
  inputs = {
    "market_question": "Will BTC hit 150k?",
    "main_outcome": "0"
  }

Context at start:
  market_question        = "Will BTC hit 150k?"
  input.market_question  = "Will BTC hit 150k?"
  main_outcome           = "0"
  input.main_outcome     = "0"
```

A node can then emit outputs like:

- `fetch.status = success`
- `fetch.outcome = 1`
- `judge.reason = ...`

### Conditional edges (CEL)

Edges can have conditions written in [CEL (Common Expression Language)](https://cel.dev). A target node only becomes reachable if the edge condition evaluates to true. See the [CEL language spec](https://github.com/google/cel-spec/blob/master/doc/langdef.md) for the full reference.

Context values are available as CEL variables using the dotted key convention (`nodeId.field`). Scalar values from executor outputs are strings. JSON arrays and objects stored in context (such as `_runs` history) are passed to CEL as native lists and maps, so standard operators work on them:

```cel
fetch.status == 'success'
fetch.status != 'success'
judge.outcome != 'inconclusive' && judge.outcome != ''
wait.status == 'success'

// List operations on node history (see below)
fetch._runs.size() > 0
fetch._runs.exists(r, r.status == 'success')

// Map field access on structured values
judge.details.confidence > 0.5
```

Typical pattern:
- success path goes to `submit_result`
- failure or inconclusive path goes somewhere else (`cancel_market`, `defer_resolution`, another judge, etc.)

### Node history (`_runs`)

When a node is re-executed via a back-edge loop, the engine snapshots its outputs before resetting. These snapshots accumulate in `nodeId._runs` as a JSON array, giving downstream nodes and edge conditions forensic access to all prior executions.

`nodeId.field` always holds the latest value (last-write-wins). `nodeId._runs` holds the history of all previous iterations (not including the current one).

Example: a node `fetch` is looped 3 times. After the run completes:
- `fetch.status` = output from iteration 3 (latest)
- `fetch._runs` = `[{iteration 1 outputs}, {iteration 2 outputs}]`

### Back edges and bounded loops

Blueprints can loop by using a back edge. Back edges must be bounded with `max_traversals`, otherwise they are treated as exhausted.

This gives you patterns like:
- fetch
- wait
- retry fetch up to N times
- then continue or fail

Loop example from the UI:

![Back edge loop example](docs/assets/back-edge-loop-example.png)

The engine records edge traversal counts, so loops are explicit and inspectable.

### Map batching

`map` runs an inline child blueprint over a JSON array from context. It is
configured with generic batching controls:

- `batch_size`: number of items passed to each child run; defaults to `1`
- `max_concurrency`: number of child batches that can run at once; defaults to `1`
- `max_concurrency: 0`: start all batches concurrently, subject to engine limits and cancellation

Each child run receives `batch`, `batch_index`, `batch_start_index`,
`batch_end_index`, and `batch_item_count` inputs. Nested `map` nodes are
allowed with a depth guard.

### A small example

Here is a simpler workflow from the UI:

![Simple workflow example](docs/assets/simple-workflow-example.png)

A common pattern is:
- gather some evidence or judgment input
- if the result is usable, flow into `submit_result`
- if the result is not usable, flow into `cancel_market`

In practice:

- nodes read inputs and context
- nodes write outputs back into context
- edges decide where execution goes next
- back edges allow bounded retry loops
- `submit_result` and `cancel_market` are the primary terminal actions

## Design notes

- Run state is durably persisted in the local data directory for single-node operation.
- `wait`, `await_signal`, and `defer_resolution` suspend runs instead of occupying workers.
- Resume signals are idempotent and correlated by run, app, or explicit key.
- Callback URLs are delivered through a durable retrying outbox.
- Traces are observability, not control-plane state.

## Tests

```bash
go test ./...
```

- `dag/` -- DAG engine, scheduling, CEL expressions
- `executors/` -- executor unit tests, LLM provider routing
- `run_manager_test.go` -- async run lifecycle
- `durable_manager_integration_test.go` -- durable queue, suspend/resume, signals, timers, restart recovery, outbox retry
- `server_test.go` -- HTTP API
- `runner_test.go` -- trace lifecycle, evidence persistence
- `dag/fuzz_test.go`, `executors/fuzz_test.go`, and `blueprint_fuzz_test.go` -- fuzz targets and property tests for CEL evaluation, interpolation, blueprint validation, scheduling, JSON extraction, JSON paths, and judgment parsing

Optional local TLA+ model checks live in `tla/`:

```bash
./tla/check-tla.sh
```

They model the single-node durable execution state machine and DAG frontier
bookkeeping: queueing, workers, suspension, timer/signal resume, cancellation,
restart recovery, callback delivery, back-edge reactivation, and duplicate-run
prevention for terminal DAG nodes.

For active fuzzing beyond the seed corpus run by `go test`, run one target at
a time:

```bash
go test ./dag -run '^$' -fuzz='^FuzzEvalCondition$' -fuzztime=30s
go test ./executors -run '^$' -fuzz='^FuzzExtractJSON$' -fuzztime=30s
```

## License

See [LICENSE](./LICENSE).
