# ADR-0001: At-least-once delivery via offset acknowledgment loop

- Status: Accepted
- Date: 2026-06-15
- Deciders: project author

## Context and problem statement

setu decodes Postgres logical replication (WAL) events and delivers them as
HTTP notifications (webhook / Slack / Telegram). Two delivery guarantees were
considered:

1. **At-most-once** — confirm the offset to Postgres as soon as the event is
   read; a crash mid-delivery loses the event.
2. **At-least-once** — hold the offset until the destination returns 2xx; a
   crash causes replay on restart, so consumers may see duplicates.

An exactly-once guarantee was not achievable without transactional sinks
(two-phase commit across Postgres LSNs and HTTP endpoints), which no webhook
destination supports.

## Considered options

| Option | Guarantee | Loss window | Cost |
|---|---|---|---|
| Confirm offset on read (at-most-once) | At-most-once | Crash between read and delivery | None |
| Confirm on 2xx (at-least-once) | At-least-once | No loss; duplicates possible | Idempotent consumers required; offset held on permanent failure |
| Transactional sink | Exactly-once | None | Not possible over plain HTTP |

## Decision outcome

Chosen: **at-least-once delivery.** The offset (`source_offset`, a Postgres
LSN) travels with every `DbEvent` and is only confirmed to the source after
the Egress Agent receives a 2xx response — or immediately when the Filter
Agent drops a non-matching event. On restart, ingestion resumes from the last
confirmed offset.

The full mechanics (channel flow, `update_applied_lsn()`, manual resolution
of permanently failed deliveries) are documented in
[AGENTS.md §6](../../AGENTS.md).

## Consequences

- **Positive:** No event is silently lost. Loss would be worse than duplication
  for the alerting/activation use cases setu targets.
- **Positive:** A skipped (non-matching) event releases its offset instantly,
  so the slot's confirmed position advances even when no rule matches.
- **Negative:** Downstream handlers MUST be idempotent; duplicate payloads are
  expected after crashes and retries. Documented in the README ("use the
  table's primary key as a dedup token").
- **Negative:** A permanently failing destination (4xx) holds the offset and
  requires manual intervention; WAL accumulates on the source until resolved.
