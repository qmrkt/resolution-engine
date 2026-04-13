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
| `human_judge` | Delegates to a human arbiter |
| `ask_creator` | Requests market creator input |
| `ask_market_admin` | Requests market admin input |
| `outcome_terminality` | Checks if an outcome is terminal |
| `defer_resolution` | Defers resolution to a later time |
| `wait` | Pauses execution for a duration |
| `submit_result` | Marks the final outcome |
| `cancel_market` | Marks the market for cancellation |

Terminal results carry an action: `propose`, `finalize_dispute`, `cancel_market`, `defer`, or `none`.

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
