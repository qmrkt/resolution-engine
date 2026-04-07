# question.market blueprint engine

Resolution blueprint engine for [question.market](https://question.market). Executes DAG-based resolution logic to determine prediction market outcomes.

## Architecture

The engine has two layers:

**Resolution engine** (`resolution-engine/`) -- a Go service that:
- Watches for markets entering resolution via an indexer
- Executes resolution blueprints as DAGs with conditional branching
- Supports multiple executor types: LLM judge, API fetch, human judge, market evidence, creator/admin input
- Submits resolution proposals and dispute rulings on-chain
- Emits structured execution traces for observability

**Blueprints** (`blueprints/`) -- JSON blueprint definitions and supporting scripts for:
- Sequential plan execution
- Security hardening campaigns
- Simplify/refactor workflows
- Task-level execution with validation gates

## Quick start

### Resolution engine

```bash
cd resolution-engine

# Dependencies
go mod download

# Required environment
export ALGOD_SERVER=http://localhost
export ALGOD_PORT=4001
export ALGOD_TOKEN=aaaa...
export RESOLUTION_AUTHORITY_MNEMONIC="your 25-word mnemonic"
export ANTHROPIC_API_KEY=sk-...       # optional, for LLM judge
export INDEXER_URL=http://localhost:3001

# Run
go run .

# Tests
go test ./...
```

### Blueprints

Blueprints are JSON DAG definitions consumed by the engine. See `blueprints/README.md` for the full specification.

```bash
# Run blueprint tests
cd blueprints && python -m pytest tests/ -v
```

## DAG engine

The core DAG engine (`resolution-engine/dag/`) supports:

- **Nodes** with typed executors, input/output bindings, and retry policies
- **CEL expressions** for conditional edges and guard clauses
- **Parallel execution** with topological scheduling
- **Context propagation** across nodes via a flat key-value store
- **Token usage tracking** for LLM-backed executors

## Executor types

| Executor | Description |
|---|---|
| `llm_judge` | Sends evidence to Claude for outcome determination |
| `api_fetch` | Fetches external data sources for evidence gathering |
| `market_evidence` | Collects on-chain market state and trade history |
| `human_judge` | Delegates to a designated human arbiter |
| `ask_creator` | Requests input from the market creator |
| `ask_market_admin` | Requests input from the market admin |
| `outcome_terminality` | Checks if an outcome is terminal/final |
| `defer_resolution` | Defers resolution to a later time |
| `wait` | Pauses execution for a specified duration |
| `submit` | Submits the resolution result on-chain |

## License

See [LICENSE](./LICENSE).
