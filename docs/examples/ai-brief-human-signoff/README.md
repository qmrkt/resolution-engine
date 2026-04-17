# AI Brief + Human Signoff

Use this for human-judged markets where the human should not start from a blank page. The agent prepares a compact evidence brief; the final outcome still comes from a signed `human_judgment.responded` signal.

The expected signal payload includes:

- `outcome`: zero-based outcome index.
- `reason`: human explanation.

```mermaid
flowchart LR
  brief["Prepare Evidence Brief<br/>agent_loop"]
  human["Human Signoff<br/>await_signal"]
  submit["Submit Human Outcome<br/>return"]
  defer["Defer Resolution<br/>return"]

  brief --> human
  human -->|"responded with outcome"| submit
  human -->|"timeout or inconclusive"| defer
```

