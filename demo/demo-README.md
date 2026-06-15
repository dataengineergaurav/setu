# realtime-activation-engine Demo

A fully containerized demo that runs everything with a single command.

## Quick Start

```bash
# 1. Build and start everything
docker compose -f demo/docker-compose.yml up --build

# 2. Open the event dashboard
open http://localhost:8080

# 3. In another terminal, seed test data
docker compose -f demo/docker-compose.yml exec postgres \
  psql -U postgres -d activation_demo -f /docker-entrypoint-initdb.d/seed.sql
```

Watch events appear in real time at `http://localhost:8080`.

## What's Inside

| Service | Container | Purpose |
|---|---|---|
| PostgreSQL 16 | `pg-activation-db` | Database with `wal_level=logical` and demo tables |
| realtime-activation-engine | `realtime-activation-engine` | The WAL reader, filter, and delivery engine |
| Webhook Display | `pg-activation-display` | HTTP server showing received events in a web UI |

## Demo Walkthrough

### Step 1 — Start the stack

```bash
docker compose -f demo/docker-compose.yml up --build
```

This brings up Postgres (with logical replication), the engine, and the webhook display server. The engine will sit idle waiting for WAL events. Visit `http://localhost:8080` to see the empty dashboard.

### Step 2 — Seed data

In a second terminal:

```bash
docker compose -f demo/docker-compose.yml exec postgres \
  psql -U postgres -d activation_demo -f /docker-entrypoint-initdb.d/seed.sql
```

This executes the following changes, each of which triggers a WAL event:

| Action | Rule Match | Destination |
|---|---|---|
| User 1 upgrades from `free` → `premium` | `users` Update, `plan` changed | Webhook `user.upgraded` |
| New order for $499.99 | `orders` Insert (any) | Webhook `order.created` |
| Widget Pro inventory drops to 5 (below threshold 10) | `inventory` Update, `quantity` < 10 | Webhook `inventory.low_stock` |
| Order 2 status changes to `shipped` | `orders` Update (any) | Webhook `order.status_changed` |
| User 3 is deleted | `users` Delete (any) | Webhook `user.deleted` |

### Step 3 — Experiment

Seed again to generate another batch:
```bash
# Same command — generates another 5 events
docker compose -f demo/docker-compose.yml exec postgres \
  psql -U postgres -d activation_demo -f /docker-entrypoint-initdb.d/seed.sql
```

Or run ad-hoc SQL to trigger specific rules:
```bash
docker compose -f demo/docker-compose.yml exec postgres \
  psql -U postgres -d activation_demo -c \
  "UPDATE users SET plan = 'premium' WHERE id = 2;"
```

### Step 4 — Tear down

```bash
docker compose -f demo/docker-compose.yml down -v
```

The `-v` flag removes the Postgres data volume so next startup is fresh.

## Configuration

The demo rules are defined in `demo/activation.yaml`. By default, all events are sent to the local webhook display. You can extend it to send to real Slack or Telegram channels:

```yaml
rules:
  # Keep the local display
  - table: "users"
    op_type: Update
    conditions:
      - field: "plan"
        old_value: "free"
        new_value: "premium"
    destination:
      type: webhook
      url: "http://webhook-display:8080/events"

  # Add Slack notification (replace URL)
  - table: "users"
    op_type: Update
    conditions:
      - field: "plan"
        old_value: "free"
        new_value: "premium"
    destination:
      type: slack
      url: "https://hooks.slack.com/services/..."
      channel: "#sales"

  # Add Telegram notification (replace token + chat_id)
  - table: "orders"
    op_type: Insert
    conditions: []
    destination:
      type: telegram
      url: "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/sendMessage"
      headers:
        chat_id: "<YOUR_CHAT_ID>"
```

Edit `demo/activation.yaml`, then restart the engine:
```bash
docker compose -f demo/docker-compose.yml restart engine
```

## Architecture

```
                WAL stream
Postgres ──────────────────> realtime-activation-engine ──HTTP──> Webhook Display
  ▲                              │                          (localhost:8080)
  │                              │
  └──── LSN feedback loop ───────┘
```

The engine connects as a logical replication client, decodes the pgoutput protocol, evaluates each change against rules in `activation.yaml`, and delivers matched events as HTTP POSTs to the configured destinations.
