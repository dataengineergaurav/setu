//! setu: real-time data activation engine — listen for database mutations, match
//! them against configurable rules, and deliver structured payloads to
//! webhooks, Slack, Telegram, or other HTTP endpoints.
//!
//! # Architecture
//!
//! The system runs three autonomous agents communicating via bounded
//! `tokio::sync::mpsc` channels:
//!
//! 1. **Ingress** (`ingress::traits::IngressSource`) — consumes change
//!    streams from a database and emits [`types::DbEvent`] values.
//! 2. **Filter** (`filter::engine`) — evaluates rules against each event,
//!    builds JSON payloads, and emits [`types::ActivationTask`] values.
//! 3. **Egress** (`egress::webhook`, `slack`, `telegram`) — dispatches
//!    tasks to the configured destination with retry logic.
//!
//! Offsets are confirmed back to the source only after delivery succeeds
//! (at-least-once semantics). See [`offset::tracker`] for details.

pub mod config;
pub mod egress;
pub mod filter;
pub mod ingress;
pub mod offset;
pub mod pgoutput;
pub mod types;
