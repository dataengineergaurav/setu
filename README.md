# setu

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

- **Single binary** — compiled Rust with `tokio`, no JVM, no Python, no external queue
- **Pluggable sources** — abstract `IngressSource` trait with PostgreSQL implementation; MySQL, Kafka, etc. ahead
- **Three destinations** — webhooks, Slack, Telegram — with linear backoff retry (3 attempts)
- **At-least-once delivery** — offset confirmed to the source only after the target returns 2xx
- **Conditional rules** — match by table, operation type (Insert/Update/Delete), and old/new column values
- **Dockerized demo** — one command to spin up Postgres + engine + live webhook dashboard
- **88 tests** — unit tests for every module, integration tests (4 require Postgres, ignored by default)

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

| `RUST_LOG` level | Output |
|---|---|
| `error` | Connection failures, parse errors, delivery failures |
| `warn` | Parse warnings, channel closures |
| `info` (default) | Connection status, rule matches, delivery results, offset confirmations |
| `debug` | Every event flowing through the pipeline, all offset updates |

## License

MIT
