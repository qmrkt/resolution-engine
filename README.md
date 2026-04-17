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
`agent_loop async=true`.

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

For a detailed walkthrough of the in-memory DAG host versus the durable
suspend/resume host, see [docs/execution-architecture.md](docs/execution-architecture.md).

| Executor              | Description                                                      |
| --------------------- | ---------------------------------------------------------------- |
| `llm_call`            | Multi-provider LLM call (Anthropic, OpenAI, Google)              |
| `agent_loop`          | Multi-provider agent loop with native and blueprint-backed tools |
| `api_fetch`           | External data source fetching                                    |
| `await_signal`        | Suspends until a correlated signal or timeout                    |
| `cel_eval`            | Evaluates CEL expressions into context outputs                   |
| `map`                 | Runs an inline child blueprint over a JSON array                 |
| `gadget`              | Validates and runs a child blueprint supplied at runtime         |
| `validate_blueprint`  | Validates blueprint JSON from context and emits issues           |
| `wait`                | Pauses execution for a duration                                  |
| `return`              | Emits a JSON value and short-circuits the run                    |

Every blueprint terminates by firing a `return` node. The emitted JSON
object (with a required `status` string) is the run's result, accessible
as `RunState.Return` and `RunResult.Return`. Caller code (the indexer)
interprets the payload — the engine itself does not know about
"propose", "cancel", or "defer"; those are caller-side conventions that
live in the `status` field.

## Blueprint semantics

A blueprint is a directed graph of steps. Each step writes values into a shared execution context, and later steps or edges can read those values.

### Inputs and results

The engine exposes invocation state under three namespaces. Templates and CEL
expressions must use one of these — bare keys are not resolved.

- `inputs.<key>` — the caller's request inputs
- `results.<node>.<field>` — outputs produced by completed nodes
- `results.<node>.history` — back-edge iteration history (JSON array)
- `run.id` / `run.blueprint_id` / `run.started_at` — run identity

```text
POST /run
  inputs = {
    "market_question": "Will BTC hit 150k?",
    "main_outcome": "0"
  }

References from inside the blueprint:
  inputs.market_question  = "Will BTC hit 150k?"
  inputs.main_outcome     = "0"
  results.fetch.status    = "success"     // after node "fetch" completes
  results.judge.reason    = "..."         // after node "judge" completes
  results.fetch.history   = [...]         // back-edge iterations of "fetch"
```

### Conditional edges (CEL)

Edges can have conditions written in [CEL (Common Expression Language)](https://cel.dev). A target node only becomes reachable if the edge condition evaluates to true. See the [CEL language spec](https://github.com/google/cel-spec/blob/master/doc/langdef.md) for the full reference.

CEL identifiers use the same namespaced surface as templates. Scalar values
from executor outputs are strings; JSON arrays and objects parse into native
CEL lists and maps so standard operators work on them:

```cel
results.fetch.status == 'success'
results.fetch.status != 'success'
results.judge.outcome != 'inconclusive' && results.judge.outcome != ''
inputs.market.deadline != ''

// List operations on node back-edge history (see below)
results.fetch.history.size() > 0
results.fetch.history.exists(r, r.status == 'success')

// Map field access on structured values
results.judge.details.confidence > 0.5
```

Typical pattern:
- success path goes to a `return` with `{"status": "success", "outcome": "..."}`
- failure or inconclusive path goes to a different `return` (`{"status": "cancelled"}`, `{"status": "deferred"}`, another judge, etc.)

### Node history (`results.<node>.history`)

When a node is re-executed via a back-edge loop, the engine snapshots its outputs before resetting. These snapshots accumulate under `results.<node>.history` as a JSON array, giving downstream nodes and edge conditions forensic access to all prior executions.

`results.<node>.<field>` always holds the latest value (last-write-wins). `results.<node>.history` holds the history of all previous iterations (not including the current one).

Example: a node `fetch` is looped 3 times. After the run completes:
- `results.fetch.status` = output from iteration 3 (latest)
- `results.fetch.history` = `[{iteration 1 outputs}, {iteration 2 outputs}]`

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
- if the result is usable, flow into a `return` with `{"status": "success", ...}`
- if the result is not usable, flow into a `return` with `{"status": "cancelled", ...}`

In practice:

- nodes read inputs and context
- nodes write outputs back into context
- edges decide where execution goes next
- back edges allow bounded retry loops
- the run terminates the moment any `return` node fires (first-completion-wins; siblings are cancelled)

## Design notes

- Run state is durably persisted in the local data directory for single-node operation.
- `wait` and `await_signal` suspend runs instead of occupying workers.
- Resume signals are idempotent and correlated by run, app, or explicit key.
- Callback URLs are delivered through a durable retrying outbox.
- Traces are observability, not control-plane state.

## Blueprint Authoring Skills

This repo ships a `blueprint-author` agent skill for authoring, reviewing,
and debugging resolution blueprints. It routes agents to the in-repo node
catalog, executor schemas, examples, and validator diagnostics instead of
making them guess.

The skill itself lives at [`skills/blueprint-author/`](skills/blueprint-author/). For the shared skill docs, see [`skills/README.md`](skills/README.md).

### Install for Claude Code

```bash
mkdir -p ~/.claude/skills
cp -r skills/blueprint-author ~/.claude/skills/
```

### Install for Codex

```bash
mkdir -p ~/.agents/skills
cp -r skills/blueprint-author ~/.agents/skills/
```

After installing, restart your session if the skill does not appear immediately.

## Security

Blueprints and LLM outputs cannot cause arbitrary code or command execution.
The engine does not spawn processes, load plugins, eval code, or render
templates — no `os/exec`, `syscall.Exec`, `plugin.Open`, `reflect.Call`,
`unsafe`, or `text/template`. The only sandboxed evaluator is
[CEL](https://cel.dev), a pure-expression language used for edge conditions
and `cel_eval`; it has no I/O, no user-defined functions, and is configured
with only the safe `ext.Strings` extension.

What blueprints _can_ do is bounded:

- **HTTP egress** via `api_fetch` and the agent-loop `source_fetch` tool is
  SSRF-protected (`safeDialContext` rejects loopback/private/link-local at
  connect time, redirects are re-validated, bodies are capped at 10 MB).
- **Filesystem writes** are confined to the configured data directory
  (durable run records, event logs, per-app evidence).
- **Dynamic/child blueprints** (via `gadget`, `map`, or the agent's
  `run_blueprint` tool) go through the same validator plus a
  `DynamicBlueprintPolicy` that caps nodes, edges, depth, time, and tokens
  and allowlists node types.
- **`run_id` path traversal** is rejected at the HTTP boundary — runIDs must
  match `^[A-Za-z0-9_-]{1,128}$`.

Operator responsibilities:

- Set `ENGINE_CONTROL_TOKEN` in production; when unset, every endpoint is
  open.
- Restrict network access to the indexer (firewall, k8s NetworkPolicy,
  mesh authz, or bind the listener to loopback) — the engine does not
  enforce caller identity beyond the bearer token.

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

## Agent Skills

This repo ships a [`blueprint-author`](skills/blueprint-author/) skill that
teaches coding agents how to author, review, and debug engine blueprints.
It references the in-repo [node catalog](docs/node-catalog.md),
[execution architecture](docs/execution-architecture.md), and
[examples](docs/examples/).

**Auto-loads in this repo** (no action needed):

- Claude Code picks it up from `.claude/skills/blueprint-author/` (symlink
  into `skills/`).
- Codex CLI picks it up from `.agents/skills/blueprint-author/` (symlink
  into `skills/`).

**Install in your profile or another project**:

```bash
# Claude Code (user-level, all projects)
cp -r skills/blueprint-author ~/.claude/skills/

# Codex CLI (user-level, all projects)
cp -r skills/blueprint-author ~/.agents/skills/

# Project-level: substitute ~/.claude or ~/.agents with
#   <project>/.claude or <project>/.agents
```

See [skills/README.md](skills/README.md) for details and verification steps.

## License

See [LICENSE](./LICENSE).
