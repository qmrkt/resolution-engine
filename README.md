# resolution-engine

Async blueprint execution service for [question.market](https://question.market).

Scoped to execution only:
- accepts blueprint execution requests over HTTP
- runs DAG-based resolution logic
- exposes async run status and cancellation
- emits structured execution traces for observability

It does not poll markets, decide lifecycle transitions, submit on-chain transactions, or own durable orchestration state. Those concerns belong to the indexer/orchestrator.

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

Returns `202 Accepted` with a `run_id`. Returns `409 Conflict` if the market already has an active run.

### GET /runs/{run_id}

Returns run state while the engine retains the run (bounded TTL).

### DELETE /runs/{run_id}

Cancels an in-flight run.

### GET /health

Returns service health and active run count.

## Architecture

The engine executes resolution blueprints as DAGs with conditional branching (CEL expressions). Each node runs a typed executor:

| Executor | Description |
|---|---|
| `llm_judge` | Multi-provider LLM evaluation (Anthropic, OpenAI, Google) |
| `api_fetch` | External data source fetching |
| `market_evidence` | On-chain market state and trade history |
| `human_judge` | Delegates to a human arbiter (configurable `allowed_responders`) |
| `outcome_terminality` | Checks if an outcome is terminal |
| `defer_resolution` | Defers resolution to a later time |
| `wait` | Pauses execution for a duration |
| `submit_result` | Marks the final outcome |
| `cancel_market` | Marks the market for cancellation |

Terminal results carry an action: `propose`, `finalize_dispute`, `cancel_market`, `defer`, or `none`.

## Blueprint semantics, in human terms

A blueprint is a small directed graph of steps. Each step writes values into a shared execution context, and later steps or edges can read those values.

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

### Conditional edges

Edges can have CEL conditions. A target node only becomes reachable if the edge condition evaluates to true.

```text
        +-----------------------------+
        | fetch.status == 'success'   |
        v                             |
   [fetch] -----------------------> [submit_result]
      |
      | fetch.status != 'success'
      v
   [cancel_market]
```

Typical pattern:
- success path goes to `submit_result`
- failure or inconclusive path goes somewhere else (`cancel_market`, `defer_resolution`, another judge, etc.)

### Back edges and bounded loops

Blueprints can loop by using a back edge. Back edges must be bounded with `max_traversals`, otherwise they are treated as exhausted.

```text
   [fetch] --success--> [submit]
      |
      +--retry_needed--> [wait]
                           |
                           | back edge
                           | max_traversals = 3
                           v
                         [fetch]
```

This gives you patterns like:
- fetch
- wait
- retry fetch up to N times
- then continue or fail

The engine records edge traversal counts, so loops are explicit and inspectable.

### Submit vs cancel

Two terminal-style node patterns matter most:

1. `submit_result`
- marks the chosen outcome
- sets `*.submitted = true`
- produces an evidence hash
- this becomes an actionable result like `propose` or `finalize_dispute`

```text
[fetch] -> [submit_result]
             outcome_key = fetch.outcome
```

2. `cancel_market`
- marks the run as asking for market cancellation
- sets `*.cancelled = true`
- carries a reason

```text
[judge] --timeout/failure--> [cancel_market]
                               reason = "human judgment did not succeed"
```

In other words:
- `submit_result` means "we have an outcome"
- `cancel_market` means "this run wants the orchestrator to cancel the market"

### A small example

```text
            +------------------------------+
            | fetch.status == 'success'    |
            v                              |
[start] -> [fetch] --------------------> [submit_result]
              |
              | fetch.status != 'success'
              v
            [wait]
              |
              | retry, max_traversals = 2
              +---------------------------> [fetch]

If all retries fail:

[fetch] -> [cancel_market]
```

That should be the main mental model:
- nodes read inputs/context
- nodes write outputs back into context
- edges decide where execution goes next
- back edges allow bounded retry loops
- `submit_result` and `cancel_market` produce the most important terminal actions

### A more realistic example from the UI

Below is a more complex blueprint from the visual editor. It combines a timing gate, evidence collection, an LLM-based decision, and multiple terminal branches.

![Complex workflow example](docs/assets/complex-workflow-example.png)

What is happening semantically:
- `Evidence Window` is a `wait`-style gate tied to a market lifecycle moment (`43200s from resolution pending` in the screenshot)
- if the waiting/evidence period is not over yet, the run exits through `Retry Later` using `defer_resolution`
- once the window has closed, `Participant Evidence` gathers signed participant submissions
- that evidence is then evaluated by an `LLM Judge`
- if the judge is confident, the workflow reaches `Submit Result`
- if the evidence or judgment is inconclusive, the workflow reaches `Cancel Market`

The useful thing about this shape is that every branch still ends in a terminal action:
- `defer_resolution`
- `submit_result`
- `cancel_market`

That makes the graph easy to reason about operationally: every run ends by either deferring, proposing a result, or explicitly cancelling.

You can think of it as a richer version of the smaller examples above:
- inputs still enter through the shared execution context
- edges are still conditional
- terminals are still `submit_result`, `cancel_market`, or `defer_resolution`
- the only difference is that the graph has more than one decision point

## Design notes

- Run state is in-memory and ephemeral. The indexer is the durable source of truth.
- Traces are observability, not control-plane state.
- Terminal results are retained for a bounded TTL, then cleaned up.
- Callback URLs receive the terminal result via POST when the run completes.

## Tests

```bash
go test ./...
```

- `dag/` -- DAG engine, scheduling, CEL expressions
- `executors/` -- executor unit tests, LLM provider routing
- `run_manager_test.go` -- async run lifecycle
- `server_test.go` -- HTTP API
- `runner_test.go` -- trace lifecycle, evidence persistence

## License

See [LICENSE](./LICENSE).
