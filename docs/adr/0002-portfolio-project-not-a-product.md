# ADR-0002: setu is a portfolio project, not a product

- Status: Accepted
- Date: 2026-08-30
- Contextual decision framework: [ADR-0001](0001-at-least-once-delivery.md)

## Context and problem statement

In August 2026 we asked whether setu should be developed toward a business
(startup, managed offering, commercial OSS) or remain a portfolio project.
The competitive landscape:

| Competitor | What it is | Why it matters |
|---|---|---|
| **Debezium Server** (Red Hat) | Standalone Quarkus/JVM app, many sinks incl. a direct HTTP sink, Kafka-less deployments now first-class | setu's core wedge — "no JVM, no Kafka, straight to webhooks" — is now a checkbox on the incumbent's feature list ([debezium-server](https://github.com/debezium/debezium-server), [Kafka-less post](https://debezium.io/blog/2026/07/06/kafka-less-migration/)) |
| **Sequin** (VC-funded, OSS) | Postgres-native CDC to webhooks/SQS/Kafka with filtering, retries, backfill, cloud + Neon partnership | Same job as setu with a company, docs and cloud behind it; already owns "lightweight Postgres change streams" mindshare ([sequinstream.com](https://sequinstream.com/), [filters reference](https://sequinstream.com/docs/reference/filters)) |
| **Hightouch / Census (Fivetran)** | Warehouse-native reverse ETL, consolidated two-horse race | The commercial end of the market is mature, not open |
| **Supabase / Neon platform triggers** | "Webhook on row change" absorbed at the platform level | Easiest answer for their own hosted users; sets expectations for free |
| **Maxwell's Daemon** | The precedent: loved lightweight CDC tool, never a business | Most likely ceiling for an unsupported single-maintainer tool |

## Decision

**setu stays a portfolio project.** No managed offering, no commercialization
track, no growth features built to attract users rather than to serve the
project's own engineering quality.

We will:

- Keep it as a systems-engineering showcase: logical replication, pgoutput
  decoding, backpressured actor pipelines, at-least-once delivery semantics.
- Keep scope small: Postgres-first, three destinations, YAML rules. Adding
  MySQL/Kafka ingress is welcomed as a *trait-implementation exercise*, not a
  roadmap promise.
- Position honestly in the README: "the 7 MB self-hosted option" for
  homelab/self-hosted users who resent Debezium's JVM footprint — with a
  truthful comparison table against Debezium Server and Sequin.
- Recommend downstream users with scaling/ops needs (backfill, exactly-once,
  teams, support contracts) to Debezium or Sequin, and say so in the README.

We will not:

- Chase feature parity with funded competitors (backfill, exactly-once
  guarantees, management UI, multi-tenant cloud).
- Add features because a competitor has them.
- Court adoption metrics (stars, launches) as a goal in themselves.

## Consequences

- **Positive:** No pressure to grow scope. The codebase stays small, readable,
  and excellent at one job. Maintenance burden fits a single maintainer. The
  project remains a strong differentiator for its author's data-engineering
  profile.
- **Positive:** Honest positioning builds trust with the self-hosted niche it
  does serve; the README comparison table becomes a strength, not marketing.
- **Negative:** No revenue path, by choice. If usage grows, expect issues and
  feature requests beyond a single maintainer's scope; triage with
  `wontfix` generously (see `docs/agents/triage-labels.md`).
- **Negative:** Platform-level absorption (Supabase/Neon triggers) will keep
  shrinking the addressable niche over time. Accepted.
- **Mitigation:** If attention on the repo ever explodes, revisit this ADR
  with real evidence rather than enthusiasm.

## Links

- Recorded during the August 2026 competitive evaluation; see repo git history
  around this ADR for the working notes.
