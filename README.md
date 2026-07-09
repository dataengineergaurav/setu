# setu

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![Contributing](https://img.shields.io/badge/contributing-guide-green.svg)](CONTRIBUTING.md)
[![Security](https://img.shields.io/badge/security-policy-orange.svg)](SECURITY.md)

Real-time data activation from database change streams. Watch specific column changes and fire **webhooks**, **Slack**, or **Telegram** notifications the instant they happen — zero polling, no middleware.

```text
+--------------------------------------------------------------------------------------+
|                             setu                                |
|                                                                                      |
|  +--------------------+      +--------------------+      +----------------------+    |
|  |                    |      |                    |      |                      |    |
|  |  1. Ingress Source | ===> |  2. Routing Engine | ===> |  3. Outbound Worker  |    | ---> Webhook
|  |    (IngressSource) | (Ch) |    (Filter Agent)  | (Ch) |    (Egress Agent)    |    | ---> Slack
|  |       trait        |      |                    |      |                      |    | ---> Telegram
|  +--------------------+      +--------------------+      +----------------------+    |
+--------------------------------------------------------------------------------------+
^                       |                                               |
| (Change stream)       | (source_offset feedback loop)                 | (offset confirmations)
|                       v                                               v
+---------------+   +--------------------+                          +-----------------+
|  PostgreSQL   |   |  ingress::traits   |   (future MySQL etc.)    |  OffsetTracker  |
|  (native)     |   |  ::IngressSource  | <======================= |  (src/offset/)  |
+---------------+   +--------------------+                          +-----------------+
```

## Features

- **Single binary** — compiled Rust with `tokio`, no JVM, no Python, no external queue; **7.2 MB** release binary, boots in milliseconds
- **Pluggable sources** — abstract `IngressSource` trait with PostgreSQL implementation; MySQL, Kafka, etc. ahead
- **Three destinations** — webhooks, Slack, Telegram — with linear backoff retry (3 attempts)
- **At-least-once delivery** — offset confirmed to the source only after the target returns 2xx
- **Conditional rules** — match by table, operation type (Insert/Update/Delete), and old/new column values
- **Dockerized demo** — one command to spin up Postgres + engine + live webhook dashboard
- **44 unit + integration tests** (48 including Postgres-dependent tests), all passing in **~6.3 s**

## Quick Start (Docker Demo)

```bash
git clone <repo>
cd setu

# Start the full demo stack
docker compose -f demo/docker-compose.yml up --build -d

# Open the live dashboard
open http://localhost:8080

# Seed test data to trigger events
docker compose -f demo/docker-compose.yml cp demo/seed.sql postgres:/tmp/seed.sql
docker compose -f demo/docker-compose.yml exec postgres \
  psql -U postgres -d activation_demo -f /tmp/seed.sql
```

You'll see five events appear on the dashboard (plus a Telegram notification if Telegram is configured): a user plan upgrade, a new order, an inventory low-stock alert, an order status change, and a user deletion.

## Native Build

```bash
# Requirements: Rust 1.85+ (edition 2024), PostgreSQL 12+ with wal_level=logical
cargo build --release

# Create your config
cp activation.yaml my-config.yaml
# Edit pg_connection, rules, destinations...

# Run
RUST_LOG=info ./target/release/setu
```

## Requirements

- **PostgreSQL 12+** with `wal_level = logical`
- The replication user needs the `REPLICATION` attribute (or superuser)
- Tables must use `REPLICA IDENTITY FULL` for `old_value` conditions on Updates

---

## Configuration

The engine loads `activation.yaml` from the current directory by default. You can use the traditional flat format or the new `source` block (recommended):

```yaml
# Flat format (backward compatible)
pg_connection: "host=localhost port=5432 dbname=mydb user=postgres password=secret"
replication_slot: "activation_slot"
publication: "activation_pub"

# -- OR --

# Source block format (preferred for multi-source)
source:
  type: postgres
  connection: "host=localhost port=5432 dbname=mydb user=postgres password=secret"
  replication_slot: "activation_slot"
  publication: "activation_pub"

rules:
  # Fire a webhook when a user upgrades to premium
  - table: "users"
    op_type: Update
    conditions:
      - field: "plan"
        old_value: "free"
        new_value: "premium"
    destination:
      type: webhook
      url: "https://hooks.example.com/user-upgraded"
      headers:
        X-Signature: "whsec_..."

  # Post to Slack on every new order
  - table: "orders"
    op_type: Insert
    conditions: []
    destination:
      type: slack
      url: "https://hooks.slack.com/services/T00/B00/xxxxx"
      channel: "#orders"
```

### Rule Reference

| Field | Example | Description |
|---|---|---|
| `table` | `users`, `*` | Table name to match (`*` matches all) |
| `op_type` | `Insert`, `Update`, `Delete`, `*` | Operation type to match |
| `conditions[].field` | `status` | Column name to inspect |
| `conditions[].old_value` | `active` | Old value must equal this (omit to match any) |
| `conditions[].new_value` | `premium` | New value must equal this (omit to match any) |

All values are compared as strings. Non-matching rules release their offset immediately.

### Secrets Management

Keep tokens and webhook secrets out of `activation.yaml` by using **`channels-env.yml`** (auto-detected alongside your config, listed in `.gitignore`). Start from the template:

```bash
cp example-channels-env.yml channels-env.yml
```

Then fill in your real tokens in `channels-env.yml`:

```yaml
telegram_bot_token: "8817138382:ABCdef..."
telegram_chat_id: "123456789"
slack_webhook_url: "https://hooks.slack.com/services/T00/B00/xxxxx"
api_key: "whsec_..."
```

Reference them in `activation.yaml` with `${variable_name}` syntax:

```yaml
destination:
  type: telegram
  url: "https://api.telegram.org/bot${telegram_bot_token}/sendMessage"
  headers:
    chat_id: "${telegram_chat_id}"

destination:
  type: slack
  url: "${slack_webhook_url}"
  channel: "#alerts"
```

If `channels-env.yml` is missing, the engine loads `activation.yaml` as-is (backward compatible). Missing keys are left unresolved and logged as warnings.

### Destinations

**Webhook** — sends an HTTP POST with the full change payload as JSON:

```yaml
destination:
  type: webhook
  url: "https://api.example.com/hook"
  headers:
    X-Custom: "value"
```

**Slack** — formats a Slack message with event details:

```yaml
destination:
  type: slack
  url: "https://hooks.slack.com/services/T00/B00/xxxxx"
  channel: "#sales"
```

**Telegram** — sends via the Bot API with HTML parse mode:

```yaml
destination:
  type: telegram
  url: "https://api.telegram.org/bot${telegram_bot_token}/sendMessage"
  headers:
    chat_id: "${telegram_chat_id}"
```

The delivered payload shape for webhooks:

```json
{
  "table": "users",
  "op": "Update",
  "old": {
    "id": "1",
    "name": "Alice",
    "plan": "free"
  },
  "new": {
    "id": "1",
    "name": "Alice",
    "plan": "premium"
  }
}
```

---

## How It Works

The engine runs as a single Rust binary on the `tokio` multi-threaded runtime. Three autonomous agents communicate through bounded `mpsc` channels (capacity 1024 each), providing natural backpressure: a slow destination slows the filter, which in turn slows the ingress source.

### 1. Ingress Agent (`IngressSource` trait)

An abstract source of database change events. Currently implemented for PostgreSQL (`PostgresSource` in `src/ingress/postgres.rs`): it opens a dedicated logical replication connection using the `pgoutput` plugin via the `pgwire-replication` crate, creates the replication slot and publication on startup, decodes the binary pgoutput protocol into structured `DbEvent` values, and sends them to the filter agent. Future implementations may add MySQL, Kafka, or other sources.

### 2. Filter Agent (Routing Engine)

Receives every `DbEvent` and evaluates it against the YAML rule set. Conditions are checked by comparing `old_row` and `new_row` field values. Matched events become `ActivationTask` instances sent to the egress agent. Non-matching events release their offset to the Offset Tracker immediately.

### 3. Egress Agent (Outbound Worker)

Maintains a pooled `reqwest::Client` (rustls-backed) and dispatches matched events as HTTP POSTs. On transient failures (429 Too Many Requests, 5xx Server Error), it retries with linear backoff (max 3 attempts). Permanent failures log the error and halt offset progression — the event is preserved but requires manual intervention to resolve.

### 4. Offset Tracker (Feedback Loop)

The at-least-once guarantee. Every `DbEvent` carries its source offset (Postgres LSN, MySQL binlog position, etc.) through the pipeline:

```text
[Source Event] ──> [Filter] ── no match ──> Offset released immediately
                          ── match ──> [Egress] ── 2xx ──> Offset confirmed to source
                                                ── error ──> Offset held (manual resolution)
```

If the process crashes between receiving an event and confirming delivery, the source replays that event on restart. **Your webhook handlers should be idempotent** (use the table's primary key as a dedup token).

---

## Deterministic Schema Mapping

setu does not guess, infer, or apply magic transformations. Every data type mapping follows explicit, documented rules.

### pgoutput Binary Protocol Decoding (`src/pgoutput.rs`)

PostgreSQL logical replication transmits changes in a binary protocol (pgoutput). setu decodes it deterministically:

| pgoutput Byte Tag | Meaning | setu Behavior |
|---|---|---|
| `'R'` (Relation) | Column metadata for a table ID | Cached in `HashMap<u32, RelationMeta>` per connection. Stores column names, discards type OIDs — **all values are emitted as JSON strings** |
| `'I'` (Insert) | New row data | Parsed into `DbEvent { new_row, old_row: None }` |
| `'U'` (Update) | Old + new row data | `'O'` prefix = old row present, `'K'` prefix = key-only old row. Both produce `Some(old_row)` |
| `'D'` (Delete) | Deleted row data | `'O'`/`'K'` prefix produces `Some(old_row)`; `'N'` prefix produces `None` |
| `'Y'` (Type) | Type metadata | Skipped entirely — no decoding of custom types |
| `'B'`/`'C'`/`'M'` | Begin/Commit/Message | Handled upstream by `pgwire-replication`; logged as warning if encountered |

### Column Value Rules

| pgoutput Column Byte | Value | setu Output |
|---|---|---|
| `'t'` (text) | Length-prefixed UTF-8 string | `JSON String` — the raw bytes decoded via `String::from_utf8_lossy` |
| `'n'` (null) | No data | `JSON Null` |
| `'u'` (unchanged/toast) | No data | `JSON Null` |
| `'b'` (binary) | Length-prefixed binary blob | `JSON Null` — binary data is discarded, not transmitted |

**Every column value becomes a JSON string.** PostgreSQL integers, floats, timestamps, UUIDs, and JSON/JSONB are all transmitted as their text representation by the pgoutput plugin. setu passes them through verbatim — no parsing, no type coercion, no lossy conversion.

### Schema Drift Handling

- **New columns added to a table:** PostgreSQL sends a new `Relation` message with the updated column list. setu's decoder discards the old cache entry and uses the new column names — no restart or reconfiguration needed.
- **Unknown relation ID:** If a `DbEvent` references a relation ID that was never announced (race condition on slot creation), the table name defaults to `relation_{rel_id}` and unknown column indexes default to `col_{N}`. The event is still processed and delivered.
- **Missing old row on Update:** If the table lacks `REPLICA IDENTITY FULL`, PostgreSQL may omit the old row. setu produces `old_row: None` — conditions that reference `old_value` will not match (deterministic pass-through).
- **Binary columns:** Explicitly skipped (`'b'` tag → `null`). setu will never send a large TOAST value or binary payload to your webhook.

---

## Performance Metrics

| Metric | Value | Notes |
|---|---|---|
| Release binary size | **7.2 MB** | Single static binary, no JVM/Python/node deps |
| Cold build time | **~40 s** | First build including all dependencies |
| Incremental build | **< 3 s** | Typical for a single-file change |
| Unit test suite | **44 tests in ~6.3 s** | Zero external dependencies; mock-based |
| Channel capacity | **1024 events** per agent link | Bounded backpressure — slow egress slows ingress |
| Memory footprint | **Single tokio runtime** | ~10-30 MB resident depending on WAL volume |
| Event → delivery latency | **Sub-millisecond** (excluding HTTP RTT) | No polling, no persistent connections, hot path is a single `mpsc::send` |
| Reconnect on failure | **5 s** (error) / **1 s** (clean) | Automatic with exponential-sibling linear delay |
| Retry budget | **3 attempts**, linear backoff 1 s / 2 s / 3 s | Per destination type (webhook, Slack, Telegram) |
| At-least-once guarantee | **Offset confirmed only after 2xx** | LSN held until delivery succeeds; crash-safe replay |

Benchmarking methodology: all numbers measured on a 2023 M-series MacBook Pro. The binary is a single statically-linked release build with LTO. Throughput is bounded by your WAL generation rate, not by setu — the tokio `mpsc` channel handoff is a single pointer write.

---

## Production Reliability

setu is designed for unattended operation against production logical replication streams. Every layer has explicit error handling, structured logging, and data lineage preservation.

### Error Handling Layers

| Layer | Mechanism | Behaviour |
|---|---|---|
| **Ingress connection** | `try_connect()` loop in `PostgresSource` | Auto-creates slot + publication if missing; reconnects on error (5 s delay); clean reconnect (1 s delay) |
| **WAL decode errors** | `warn!()` log, break current batch | Corrupt or unexpected pgoutput bytes logged; decoder resumes from next WAL record |
| **Channel closure** | `tokio::select!` guard in `main.rs` | Any agent exiting triggers `tracing::error!` and process shutdown via `tokio::select!` |
| **HTTP transient errors** | Linear backoff in egress modules | 429 Too Many Requests and 5xx retried up to 3 times; delay = attempt seconds |
| **HTTP permanent errors** | `error!()` log, return `false` | 4xx (except 429) are non-retriable; offset NOT confirmed — requires manual inspection |
| **Offset exhaustion** | Warn-level log with full event context | After 3 failed delivery attempts, event is preserved in WAL; manual replay possible via offset resumption |
| **Binary columns** | Explicit `null` output | `'b'` tag payloads are skipped — avoids transmitting large TOAST or binary data |

### Structured Logging (via `tracing` + `tracing-subscriber`)

| Level | What You See |
|---|---|
| `error` | Connection failures, parse failures in pgoutput, permanent HTTP errors, agent exit |
| `warn` | Parse warnings, unresolved env vars, channel closures, transient HTTP errors, unknown pgoutput tags |
| `info` (default) | Connection success, publication/slot status, every rule match and delivery result, LSN confirmations |
| `debug` | Every `DbEvent` flowing through the pipeline, all offset updates processed by the ingress source |

Example production output at `info` level:

```
INFO  Loaded activation configuration with 3 rules
INFO  postgres source started
INFO  Publication "activation_pub" already exists
INFO  Slot "activation_slot" already exists
INFO  Connected to PostgreSQL replication stream
INFO  Routing event to outbound  table=users dest=https://hooks.example.com/user-upgraded
INFO  Webhook delivery succeeded  table=users status=200 attempt=1
INFO  Transaction committed, LSN confirmed  lsn=0/2A1B3C8
```

### Data Lineage

Every event carries its source offset (Postgres LSN) as a `source_offset: String` through the entire pipeline:

1. **Decoded** from the WAL stream in `pgoutput.rs` → attached to `DbEvent`
2. **Transported** through `event_tx` → `ActivationTask` preserves the offset
3. **Delivered** or **filtered** → offset forwarded to `confirmed_offset_tx`
4. **Confirmed** back to PostgreSQL via `client.update_applied_lsn()`

If the process crashes between steps 2 and 3, the LSN was never confirmed — PostgreSQL replays from the last confirmed position. **Downstream handlers must be idempotent** (use the table primary key as a dedup token).

### Edge Cases Safely Handled

| Scenario | Behaviour |
|---|---|
| Table with no primary key | Works — pgoutput uses column index; `REPLICA IDENTITY FULL` recommended for old_row |
| Event with all-null row | Decoded as `JSON Object` with all-null values; condition matching evaluates `null == null` correctly |
| Schema change mid-stream | New `Relation` message updates cached column list; subsequent events use new columns |
| Unknown pgoutput message tag | Logged with `warn!()` and batch parsing is terminated; next WAL record resumes |
| Replication slot already in use | Slot auto-detected; no duplicate creation; shares slot gracefully with other consumers |
| Empty WAL payload | `decode()` returns empty vec; no event emitted |
| Multiple events in single WAL record | All decoded and pushed individually; each carries its own LSN |
| Destination URL with unresolved `${VAR}` | Variable left as-is in URL; logged as warning; delivery proceeds to literal URL |

---

## Example Configurations

See the `examples/` directory for annotated YAML files:

| File | Covers |
|---|---|---|
| `examples/beginner/webhook-user-signup.yaml` | One rule, webhook destination |
| `examples/beginner/slack-order-notify.yaml` | One rule, Slack destination |
| `examples/advanced/multi-destination.yaml` | Same event to multiple destinations |
| `examples/advanced/state-machine.yaml` | State transitions with old/new conditions |
| `examples/advanced/delete-guard.yaml` | Alert on row deletion |
| `examples/expert/production-ready.yaml` | Multi-table, multi-destination production setup |
| `examples/expert/multi-table-audit.yaml` | Audit trail across sensitive tables |

---

## Demo

A fully containerized demo runs with a single command:

```bash
docker compose -f demo/docker-compose.yml up --build
```

The stack includes:

| Service | Container | Purpose |
|---|---|---|
| PostgreSQL 16 | `pg-activation-db` | Configured with `wal_level=logical` |
| setu | `setu` | Ingress source + filter + delivery |
| Webhook Display | `pg-activation-display` | Python HTTP server with live HTML dashboard |

When you seed data, five sample DB changes trigger six rules — five webhook events on the dashboard plus one Telegram notification:

| Action | Rule Match | Destination |
|---|---|---|
| User upgrades from `free` → `premium` | `users` Update, plan changed | Webhook `user.upgraded` |
| New order for $499.99 | `orders` Insert | Webhook `order.created` + **Telegram** `@gg_morning_bot` |
| Widget Pro inventory drops to 5 | `inventory` Update, quantity = 5 | Webhook `inventory.low_stock` |
| Order status changes to `shipped` | `orders` Update (any) | Webhook `order.status_changed` |
| User deleted | `users` Delete | Webhook `user.deleted` |

### Customize the Demo

Edit `demo/activation.yaml` to add more destinations, then restart:

```bash
docker compose -f demo/docker-compose.yml restart engine
```

### Teardown

```bash
docker compose -f demo/docker-compose.yml down -v
```

---

## Project Structure

```text
src/
├── main.rs                # Entry point — spawns all agents via channels
├── lib.rs                 # Crate root, re-exports for integration tests
├── config.rs              # activation.yaml parser + env var resolution
├── types.rs               # DbEvent, OpType, ActivationTask, SourceKind, DestinationConfig
├── pgoutput.rs            # PostgreSQL pgoutput logical replication decoder
├── ingress/
│   ├── mod.rs             # SourceConfig enum, create_source(), spawn_source()
│   ├── traits.rs          # IngressSource trait definition
│   └── postgres.rs        # PostgresSource: WAL consumer, slot/pub management
├── filter/
│   ├── mod.rs
│   └── engine.rs          # Rule matching (table, op_type, conditions)
├── egress/
│   ├── mod.rs
│   ├── webhook.rs         # Generic HTTP POST delivery with retry
│   ├── slack.rs           # Slack message formatting
│   └── telegram.rs        # Telegram bot message formatting (HTML parse_mode)
└── offset/
    ├── mod.rs
    └── tracker.rs         # OffsetTracker — atomic max-offset tracking

channels-env.yml              # Secrets (gitignored) — copied from example
example-channels-env.yml      # Template for secrets

demo/
├── docker-compose.yml     # Postgres + engine + webhook display
├── Dockerfile             # Multi-stage Alpine build
├── activation.yaml        # Demo rule set
├── channels-env.yml       # Demo secrets (gitignored)
├── init.sql               # Schema + seed data
├── seed.sql               # Test data generator
├── webhook-receiver/
│   └── server.py          # Python HTTP server with HTML dashboard
└── demo-README.md         # Detailed demo walkthrough

examples/
├── beginner/              # 3 configs: simple webhook, multi-rule, condition basics
├── advanced/              # 2 configs: conditional routes, wildcard patterns
└── expert/                # 2 configs: Telegram notifier, full pipeline

.github/
├── workflows/
│   └── ci.yml             # CI: format check, clippy, build, test
├── ISSUE_TEMPLATE/
│   ├── bug_report.md
│   └── feature_request.md
└── PULL_REQUEST_TEMPLATE.md
```

## Development

```bash
# Format code
cargo fmt

# Lint
cargo clippy --all-targets

# Run all unit tests (no Postgres required)
cargo test --lib

# Run all tests including integration (requires running Postgres)
# cargo test

# Build release binary
cargo build --release

# Run with debug logging
RUST_LOG=debug ./target/release/setu
```

### Building for Docker

```bash
docker build -f demo/Dockerfile -t setu .
```

The Dockerfile uses a multi-stage build with `alpine:3.19` runtime (rustls-backed, no OpenSSL dependency).

## Monitoring

See the [Structured Logging](#structured-logging-via-tracing--tracing-subscriber) section above for detailed log levels and output examples.

| `RUST_LOG` level | Highlights |
|---|---|
| `error` | Connection failures, parse errors, delivery failures |
| `warn` | Parse warnings, channel closures, transient errors |
| `info` (default) | Connection status, rule matches, delivery results, LSN confirmations |
| `debug` | Every event flowing through the pipeline, all offset updates |

## License

MIT — see [LICENSE](LICENSE) for the full text.
