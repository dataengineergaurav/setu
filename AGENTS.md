# AGENTS.md: System Architecture, Data Flows, and Actor Responsibilities

This document defines the core runtime agents (asynchronous tasks), memory boundaries, and state transition mechanics for **setu** — a real-time data activation engine. Use this file as the single source of truth during development to prevent architectural drift.

---

## 1. Actor Architecture Overview

The system runs as a single compiled Rust binary using the multi-threaded `tokio` runtime. Concurrency is managed via three core autonomous actors communicating through bounded, non-blocking channels (`tokio::sync::mpsc`, capacity 1024 each).


```

+-----------------------------------------------------------------------------+
|                              RUST BINARY                                    |
|                                                                             |
|  +--------------------+     +--------------------+     +------------------+  |
|  |                    |     |                    |     |                  |  |
|  | 1. Ingress Source  | ==> | 2. Routing Engine  | ==> | 3. Outbound      |  | --> Webhooks
|  |  (Ingress Agent)   |(Ch) |   (Filter Agent)   |(Ch) |   Worker         |  | --> Slack
|  |                    |     |                    |     |   (Egress Agent) |  | --> Telegram
|  +--------------------+     +--------------------+     +------------------+  |
+-----------------------------------------------------------------------------+
^                   |                                     |
| (Replication      | (source_offset feedback loop)       | (offset confirmations)
|  stream)          v                                     v
+---------------+  +---------------------+              +--------------------+
|  PostgreSQL    |  |  IngressSource     |  (future)    |  Offset Tracker   |
|  (native)      |  |  trait + Factory   | <==========  |  (src/offset/)    |
+---------------+  +---------------------+              +--------------------+

```

### Architectural Principle: Pluggable Source Layer

The **source type** is abstracted behind `IngressSource` trait (`src/ingress/traits.rs`). The Filter and Egress agents are source-agnostic — they operate on `DbEvent` and `ActivationTask` which carry a generic `source_offset: String` rather than a database-specific position type.

### Component Breakdown

* **Ingress Agent (`IngressSource` trait):** Each implementation connects to a specific database type (e.g. Postgres via `PostgresSource` in `src/ingress/postgres.rs`), consumes the change stream, decodes mutations into `DbEvent`, and pushes them through `event_tx`. On reconnect, it auto-creates the replication slot and publication if missing.
* **Filter Agent (Routing Engine, `src/filter/engine.rs`):** Ingests raw `DbEvent` values, matches them against YAML configuration rules (`activation.yaml`), evaluates condition expressions on `old_row`/`new_row`, and constructs target JSON payloads. Non-matching events forward their offset directly to the confirmed-offset channel for immediate release.
* **Egress Agent (Outbound Worker, `src/main.rs`):** Manages a shared `reqwest::Client` (rustls-backed, 30s timeout), dispatches requests to Webhook/Slack/Telegram destinations via a match on `DestinationKind`, and forwards the offset on 2xx success. On delivery failure, the offset is NOT confirmed — manual intervention required.
* **Offset Tracker (`src/offset/tracker.rs`):** Defines `OffsetTracker` with `AtomicU64` max-confirmed tracking. Currently defined but **not wired into main.rs** — the ingress source receives confirmed offsets directly through `confirmed_offset_rx` and calls `client.update_applied_lsn()` in its run loop.

---

## 2. Source Factory & Config

Source creation happens through a factory pattern:

```
activation.yaml
      │
      ▼
  config.rs::ActivationConfig
      │  ├── source block (new format, optional)
      │  │     type: postgres
      │  │     connection: "host=..."
      │  │     replication_slot: "slot"
      │  │     publication: "pub"
      │  │
      │  └── flat fields (legacy format, backward compatible)
      │        pg_connection: "host=..."
      │        replication_slot: "slot"
      │        publication: "pub"
      │
      ▼
  ActivationConfig::to_source_config()
      │
      ▼
  ingress::SourceConfig::Postgres { ... }
      │
      ├──> ingress::create_source() → Box<dyn IngressSource>
      │         │
      │         └──> PostgresSource::new(pg_connection, slot, pub)
      │
      └──> ingress::spawn_source() → JoinHandle
                │
                └──> spawn_source_from(source, event_tx, confirmed_offset_rx)
                           │
                           ▼
                      tokio::spawn(source.run())
```

The `source` block takes priority when present. Future source types (MySQL, Kafka, etc.) add a new variant to `SourceDef` (config YAML) and `SourceConfig` (factory enum), plus a match arm in `create_source()`.

---

## 3. Ingress Source Contract

### The `IngressSource` trait (`src/ingress/traits.rs`)

```rust
#[async_trait]
pub trait IngressSource: Send + 'static {
    async fn run(
        self: Box<Self>,
        event_tx: mpsc::Sender<DbEvent>,
        confirmed_offset_rx: mpsc::Receiver<String>,
    ) -> anyhow::Result<()>;

    fn name(&self) -> &'static str;
}
```

### Data Contract (`src/types.rs`)

```rust
pub enum SourceKind { Postgres }

pub enum OpType { Insert, Update, Delete }

pub struct DbEvent {
    pub source_offset: String,       // LSN, binlog position, etc.
    pub source_kind: SourceKind,     // identifies the source implementation
    pub table_name: String,
    pub op_type: OpType,
    pub old_row: Option<serde_json::Value>,   // requires REPLICA IDENTITY FULL
    pub new_row: Option<serde_json::Value>,
}

pub struct ActivationTask {
    pub source_offset: String,
    pub table_name: String,
    pub op_type: OpType,
    pub payload: serde_json::Value,
    pub destination: DestinationConfig,
}

pub struct DestinationConfig {
    pub kind: DestinationKind,       // Webhook | Slack | Telegram
    pub url: String,
    pub headers: Vec<(String, String)>,  // custom headers + slack channel
}

pub enum DestinationKind {
    Webhook,
    Slack,
    Telegram,
}
```

---

## 4. Filter Agent (Routing Engine)

### Responsibilities

* Ingest `DbEvent` packages from the primary bounded channel.
* Parse conditions from `activation.yaml` and evaluate changes efficiently.
* Perform state delta matching between `old_row` and `new_row` structures.
* Construct the explicit payload expected by the downstream target.
* Forward `source_offset` for filtered-out events immediately to the confirmed-offset channel.

### Execution Path

1. Listen on `mpsc::Receiver<DbEvent>`.
2. Evaluate rules matching `table_name` and `op_type`.
3. If true, run structural conditions (e.g., check if a property switched states from `A` to `B`).
4. On a successful match, generate an `ActivationTask` and forward it to the outbound queue.
5. If the event is **filtered out** (no rule matched), immediately forward its `source_offset` to the confirmed-offset channel for release.

---

## 5. Egress Agent (Outbound Worker)

### Responsibilities

* Ingest validated `ActivationTask` structures.
* Execute HTTP POST calls against the destination using a shared, pooled `reqwest::Client` (30s timeout, rustls-backed).
* Enforce backpressure. If destination targets slow down, the channel bounds naturally slow down upstream agents.
* Execute retry loops on transient network failures (`429 Too Many Requests`, `5xx Server Error`).

### Dispatch Logic (in `src/main.rs`)

```rust
let success = match task.destination.kind {
    DestinationKind::Webhook  => egress::webhook::send(&task, &client).await,
    DestinationKind::Slack    => egress::slack::send(&task, &client).await,
    DestinationKind::Telegram => egress::telegram::send(&task, &client).await,
};

if success {
    // forward offset to be confirmed
    let _ = egress_offset_tx.send(task.source_offset.clone()).await;
} else {
    // offset NOT confirmed — manual intervention required
}
```

### Transient Failure Handling Blueprint

```
[HTTP Post] ───> Success (2xx) ───> Log Success ───> Forward offset to confirmed channel
     │
     └──> Transient Error (429/5xx)
              │
              └──> [Linear Backoff Delay] ───> Retry (Max 3 attempts)
                                                   │
                                                   └──> Exhausted ───> Log warning
                                                                    (offset NOT confirmed)
```

---

## 6. The Critical Offset Acknowledgment Loop

To guarantee **at-least-once delivery**, this project implements an explicit feedback loop for source-position offsets (Postgres LSN, MySQL binlog pos, etc.).

1. The **Ingress Source** reads an offset from the database but *does not* send a standby status update acknowledgement back — it waits for confirmation through `confirmed_offset_rx`.
2. The offset is attached as `source_offset` (a `String`) on the `DbEvent` and travels down the channel pipeline.
3. If an event is filtered out by the **Filter Agent**, its offset is immediately forwarded to `confirmed_offset_tx`.
4. If an event passes filtering, its offset stays locked inside the execution path until the **Egress Agent** receives a `200 OK` from the target.
5. The **Ingress Source** receives confirmed offsets through `confirmed_offset_rx` in a `tokio::select!` loop and calls `client.update_applied_lsn()` to send the standby status update to PostgreSQL.

> **Development Warning:** If an event crashes mid-flight or the network connection breaks before an explicit acknowledgment occurs, the system restarts from the last confirmed offset. This means downstream webhooks *must* handle potential duplicate payloads gracefully.

### Offset Channel Flow (Current Implementation)

```
                  ┌─────────────────────────────────────┐
                  │        confirmed_offset_tx           │
                  │     (mpsc::Sender<String>)           │
                  │     shared clone in main.rs          │
                  │                                      │
  Filter Agent ───┤  (forwards offset when filtered)    │
                  │                                      │
  Egress Agent ───┤  (forwards offset on 2xx success)   │
                  │                                      │
                  └──────────────┬───────────────────────┘
                                 │
                                 ▼
                        confirmed_offset_rx
                                 │
                                 ▼
                       Ingress Source (PostgresSource)
                    tokio::select! loop reading offsets
                                 │
                                 ▼
              client.update_applied_lsn(lsn.into())
```

### Offset Tracker (src/offset/tracker.rs) — Unused / Future Use

An `OffsetTracker` struct exists with `AtomicU64` tracking but is **not wired into main.rs**. It was designed as a decoupled alternative where the tracker sits between the agents and the ingress source:

```
[Filter/Egress] ──> OffsetTracker (AtomicU64 max) ──read──> Ingress Source
```

Currently, offsets go directly to the ingress source. The `OffsetTracker` can be adopted in the future if a central confirmed-position readout is needed (e.g., for metrics, health checks, or multiple ingress sources).

---

## 7. File Layout

```
src/
├── main.rs              # Entry point — spawns all 3 agents, wires channels, tokio::select! monitors
├── lib.rs               # Crate root, module exports (pub for integration tests)
├── config.rs            # activation.yaml parser + channels-env.yml env var resolution
├── types.rs             # Core types (DbEvent, ActivationTask, SourceKind, OpType,
│                        #   DestinationConfig, DestinationKind)
├── pgoutput.rs          # PostgreSQL pgoutput logical replication decoder
├── ingress/
│   ├── mod.rs           # SourceConfig enum, create_source(), spawn_source(), spawn_source_from()
│   ├── traits.rs        # IngressSource trait definition
│   └── postgres.rs      # PostgresSource: WAL consumer, slot/pub auto-creation, reconnection loop
├── filter/
│   ├── mod.rs
│   └── engine.rs        # Rule matching (table, op_type, conditions), payload building
├── egress/
│   ├── mod.rs
│   ├── webhook.rs       # Generic HTTP POST delivery with retry (reqwest)
│   ├── slack.rs         # Slack message formatting + dispatch
│   └── telegram.rs      # Telegram bot message formatting (HTML parse_mode) + dispatch
└── offset/
    ├── mod.rs
    └── tracker.rs       # OffsetTracker — AtomicU64 max-offset tracking (unused, for future)
```

---

## 8. Configuration Format (`activation.yaml`)

### Legacy format (backward compatible)

```yaml
pg_connection: "host=localhost port=5432 dbname=mydb user=postgres"
replication_slot: "my_slot"
publication: "my_pub"
rules:
  - table: "users"
    op_type: Update
    conditions:
      - field: "status"
        old_value: "active"
        new_value: "premium"
    destination:
      type: webhook
      url: "http://localhost:8080/hook"
      headers:
        X-Custom: "value"
```

### New `source` block format (preferred for multi-source)

```yaml
source:
  type: postgres
  connection: "host=localhost port=5432 dbname=mydb user=postgres"
  replication_slot: "my_slot"
  publication: "my_pub"
rules:
  - table: "users"
    op_type: Update
    conditions:
      - field: "status"
        old_value: "active"
        new_value: "premium"
    destination:
      type: webhook
      url: "http://localhost:8080/hook"
```

### Secrets via `channels-env.yml`

```yaml
# channels-env.yml (gitignored)
telegram_bot_token: "8817138382:ABCdef..."
telegram_chat_id: "123456789"
slack_webhook_url: "https://hooks.slack.com/services/T00/B00/xxxxx"
api_key: "whsec_..."
```

Referenced in `activation.yaml` with `${var}` syntax — resolved at load time by `config.rs::resolve_string()`.

---

## 9. Channel Capacities and Backpressure

| Channel | Capacity | Producer | Consumer |
|---|---|---|---|
| `event_tx` / `event_rx` | 1024 | Ingress Source | Filter Agent |
| `task_tx` / `task_rx` | 1024 | Filter Agent | Egress Agent |
| `confirmed_offset_tx` / `confirmed_offset_rx` | 1024 | Filter + Egress | Ingress Source |

Natural backpressure: if the Egress Agent slows down (slow HTTP target), `task_tx` backs up, which blocks the Filter Agent, which in turn blocks the Ingress Source via `event_tx`.

---

## 10. PostgresSource Auto-Creation and Reconnection

On start (and on connection loss), `PostgresSource::run()` loops and calls `try_connect()` which:

1. **Auto-creates** the publication (`CREATE PUBLICATION ... FOR ALL TABLES`) if missing.
2. **Auto-creates** the logical replication slot (`pg_create_logical_replication_slot`) if missing.
3. Connects a `ReplicationClient` and enters a `tokio::select!` loop:
   - **Branch A:** reads confirmed offsets from `confirmed_offset_rx` and calls `client.update_applied_lsn()`.
   - **Branch B:** reads `ReplicationEvent` values from the WAL stream, decodes them via `PgoutputDecoder`, and sends `DbEvent` values through `event_tx`.

On connection failure, waits 5 seconds and retries. On clean disconnect, waits 1 second and retries.

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues (via the `gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout: one `CONTEXT.md` at the root plus `docs/adr/` for decisions. See `docs/agents/domain.md`.
