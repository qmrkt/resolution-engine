# Dispute Challenge Review

Use this as a dispute path after someone challenges a proposed resolution. The agent is not merely resolving from scratch; it evaluates whether the challenge undermines the proposed outcome.

The reviewer can:

- reaffirm the proposed outcome,
- cancel when the market is unresolvable or invalid,
- escalate to a human market admin.

```mermaid
flowchart LR
  review["Challenge Reviewer<br/>agent_loop"]
  admin["Admin Fallback<br/>await_signal"]
  submitReview["Submit Reviewed Outcome<br/>return"]
  submitAdmin["Submit Admin Outcome<br/>return"]
  cancelReview["Cancel From Review<br/>return"]
  cancelAdmin["Cancel From Admin Timeout<br/>return"]

  review -->|"reaffirm"| submitReview
  review -->|"cancel"| cancelReview
  review -->|"escalate or failed"| admin
  admin -->|"admin outcome"| submitAdmin
  admin -->|"timeout or inconclusive"| cancelAdmin
```
