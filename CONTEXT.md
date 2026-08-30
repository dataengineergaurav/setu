# CONTEXT

setu is a real-time data activation engine: it watches a database's change
stream and fires outbound notifications (webhook / Slack / Telegram) when
configured row changes happen. It runs as a single compiled Rust binary on a
multi-threaded tokio runtime.

## Glossary

- **Ingress Agent** (a.k.a. Ingress Source): connects to a database's change
  stream, decodes mutations into `DbEvent`, and pushes them down the pipeline.
  Pluggable behind the `IngressSource` trait; today only Postgres via logical
  replication (WAL) is implemented.
- **Filter Agent** (a.k.a. Routing Engine): consumes `DbEvent`, matches it
  against rules in `activation.yaml`, evaluates state-delta conditions between
  `old_row` / `new_row`, and builds the outbound `ActivationTask`. Events that
  match no rule are dropped and their offset released immediately.
- **Egress Agent** (a.k.a. Outbound Worker): consumes `ActivationTask` and
  dispatches the HTTP POST via a shared `reqwest::Client`. Offset is confirmed
  only on a 2xx response.
- **`DbEvent`**: the internal representation of a decoded row mutation
  (`source_offset`, `table_name`, `op_type`, `old_row`, `new_row`).
- **`ActivationTask`**: a filtered, payload-ready unit of outbound work
  (`source_offset`, `table_name`, `op_type`, `payload`, `destination`).
- **`source_offset`**: a source-agnostic position string (LSN for Postgres).
  Travels with every event so delivery can be acknowledged exactly once the
  target confirms.
- **Offset acknowledgment loop**: the at-least-once-delivery guarantee. The
  ingress source does not advance its confirmed position until the offset
  returns via `confirmed_offset_rx` (from the Filter Agent on drop, or the
  Egress Agent on 2xx). On restart it resumes from the last confirmed offset,
  so destinations must tolerate duplicate payloads.
- **Replication slot / publication**: Postgres logical-replication primitives
  setu auto-creates if missing (`pgoutput` decoder).
- **`activation.yaml`**: rule + source config. Supports a legacy flat format and
  a new `source:` block (source block wins when present).
- **`channels-env.yml`**: gitignored secrets (`${var}` references resolved at
  load time by `config.rs::resolve_string()`).

## Actors (single process, three tasks)

```
Ingress → event channel → Filter → task channel → Egress → webhook/slack/telegram
                                  │                  │
                                  └── confirmed_offset ──┘ (back to Ingress)
```

Bounded `tokio::mpsc` channels (capacity 1024) give natural backpressure: a slow
destination stalls Egress, then Filter, then Ingress.

## Key decisions

- **At-least-once delivery**: offset confirmed only after target 2xx. Downstream
  webhooks must be idempotent. (This is intentional; see AGENTS.md §6.)
- **`OffsetTracker` is currently unused**: `src/offset/tracker.rs` exists but is
  not wired into `main.rs`; offsets flow directly to the ingress source. Adopt it
  only if a central confirmed-position readout (metrics/health) is needed.

## ADRs

Decisions recorded so far: [ADR-0001](docs/adr/0001-at-least-once-delivery.md) (at-least-once delivery), [ADR-0002](docs/adr/0002-portfolio-project-not-a-product.md) (portfolio project, not a product).
