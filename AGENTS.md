# AGENTS.md: System Architecture, Data Flows, and Actor Responsibilities

This document defines the core runtime agents (asynchronous tasks), memory boundaries, and state transition mechanics for this real-time data activation engine. Use this file as the single source of truth during development to prevent architectural drift.

---

## 1. Actor Architecture Overview

The system runs as a single compiled Rust binary using the multi-threaded `tokio` runtime. Concurrency is managed via three core autonomous actors communicating through bounded, non-blocking channels (`tokio::sync::mpsc`).


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
|  PostgreSQL /  |  |  IngressSource     |  (future)    |  Offset Tracker    |
|  MySQL (future)|  |  trait + Factory   | <==========  |  (src/offset/)     |
+---------------+  +---------------------+              +--------------------+

```

### Architectural Principle: Pluggable Source Layer

The **source type** is abstracted behind `IngressSource` trait (`src/ingress/traits.rs`). The Filter and Egress agents are source-agnostic — they operate on `DbEvent` and `ActivationTask` which carry a generic `source_offset: String` rather than a database-specific position type.

### Component Breakdown

* **Ingress Agent (`IngressSource` trait):** Each implementation connects to a specific database type (e.g. Postgres via `PostgresSource` in `src/ingress/postgres.rs`), consumes the change stream, decodes mutations into `DbEvent`, and pushes them through `event_tx`.
* **Filter Agent (Routing Engine, `src/filter/engine.rs`):** Ingests raw `DbEvent` values, matches them against YAML configuration rules (`activation.yaml`), evaluates condition expressions on `old_row`/`new_row`, and constructs target JSON payloads.
* **Egress Agent (Outbound Worker, `src/egress/`):** Manages HTTP connection pools (shared `reqwest::Client`), enforces backpressure, dispatches requests to Webhooks/Slack/Telegram destinations, and runs retry loops on transient failures.
* **Offset Tracker (`src/offset/tracker.rs`):** Receives confirmed offset values from the Filter (filtered-out events) and Egress (delivered events) agents and tracks the maximum confirmed position. The Ingress source reads this value to acknowledge it back to the source database. This decouples offset tracking from I/O.

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
      ▼
  ingress::create_source() → Box<dyn IngressSource>
      │
      ▼
  ingress::spawn_source() → JoinHandle
```

The `source` block takes priority when present. Future source types (MySQL, Kafka, etc.) add a new variant to `SourceDef` (config YAML) and `SourceConfig` (factory enum).

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
    pub old_row: Option<serde_json::Value>,
    pub new_row: Option<serde_json::Value>,
}

pub struct ActivationTask {
    pub source_offset: String,
    pub table_name: String,
    pub op_type: OpType,
    pub payload: serde_json::Value,
    pub destination: DestinationConfig,
}
```

---

## 4. Filter Agent (Routing Engine)

### Responsibilities

* Ingest `DbEvent` packages from the primary bounded channel.
* Parse conditions from `activation.yaml` and evaluate changes efficiently.
* Perform state delta matching between `old_row` and `new_row` structures.
* Construct the explicit payload expected by the downstream target.

### Execution Path

1. Listen on `mpsc::Receiver<DbEvent>`.
2. Evaluate rules matching `table_name` and `op_type`.
3. If true, run structural conditions (e.g., check if a property switched states from `A` to `B`).
4. On a successful match, generate an `ActivationTask` and forward it to the outbound queue.
5. If the event is **filtered out** (no rule matched), immediately forward its `source_offset` to the Offset Tracker for release.

---

## 5. Egress Agent (Outbound Worker)

### Responsibilities

* Ingest validated `ActivationTask` structures.
* Execute HTTP POST calls against the destination using a shared, pooled `reqwest::Client`.
* Enforce backpressure. If destination targets slow down, the channel bounds naturally slow down upstream agents.
* Execute retry loops on transient network failures (`429 Too Many Requests`, `5xx Server Error`).

### Transient Failure Handling Blueprint

```
[HTTP Post] ───> Success (2xx) ───> Log Success ───> Forward offset to Offset Tracker
     │
     └──> Transient Error (429/5xx)
              │
              └──> [Linear Backoff Delay] ───> Retry (Max 3 attempts)
                                                   │
                                                   └──> Exhausted ───> Dead Letter Log / Halt
                                                                    (offset NOT confirmed)
```

---

## 7. The Critical Offset Acknowledgment Loop

To guarantee **at-least-once delivery**, this project implements an explicit feedback loop for source-position offsets (Postgres LSN, MySQL binlog pos, etc.).

1. The **Ingress Source** reads an offset from the database but *does not* send a standby status update acknowledgement back.
2. The offset is attached as `source_offset` (a `String`) on the `DbEvent` and travels down the channel pipeline.
3. If an event is filtered out by the **Filter Agent**, its offset is immediately forwarded to the **Offset Tracker** for release.
4. If an event passes filtering, its offset stays locked inside the execution path until the **Egress Agent** receives a `200 OK` from the target.
5. Once confirmed, the **Offset Tracker** holds the maximum confirmed position. The Ingress source periodically reads this value and sends the heartbeat update (e.g. `WalReceiverStatusUpdate` for Postgres).

> **Development Warning:** If an event crashes mid-flight or the network connection breaks before an explicit acknowledgment occurs, the system restarts from the last confirmed offset. This means downstream webhooks *must* handle potential duplicate payloads gracefully.

### Offset Channel Flow

```
                  ┌─────────────────────────────────────┐
                  │            confirmed_offset_tx       │
                  │          (mpsc::Sender<String>)      │
                  │                                     │
  Filter Agent ───┤  (forwards offset when filtered)    │
                  │                                     │
  Egress Agent ───┤  (forwards offset on 2xx success)   │
                  │                                     │
                  └──────────────┬──────────────────────┘
                                 │
                                 ▼
                         Offset Tracker
                     (src/offset/tracker.rs)
                      AtomicU64 max position
                                 │
                                 │ (read by Ingress on each loop iteration)
                                 ▼
                        Ingress Source
                     calls update_applied_lsn()
```

---

## 8. File Layout

```
src/
├── main.rs              # Entry point, agent wiring
├── lib.rs               # Crate root, module exports
├── config.rs            # activation.yaml parser + env var resolution
├── types.rs             # Core types (DbEvent, ActivationTask, SourceKind, …)
├── pgoutput.rs          # PostgreSQL pgoutput logical replication decoder
├── ingress/
│   ├── mod.rs           # SourceConfig enum, create_source(), spawn_source()
│   ├── traits.rs        # IngressSource trait definition
│   └── postgres.rs      # PostgresSource: WAL consumer, slot/pub management
├── filter/
│   ├── mod.rs
│   └── engine.rs        # Rule matching, condition evaluation, payload building
├── egress/
│   ├── mod.rs
│   ├── webhook.rs       # Generic HTTP POST delivery
│   ├── slack.rs         # Slack message formatting
│   └── telegram.rs      # Telegram bot message formatting
└── offset/
    ├── mod.rs
    └── tracker.rs       # OffsetTracker — atomic max-offset tracking
```

---

## 9. Configuration Format (`activation.yaml`)

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
