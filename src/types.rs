//! Core data types shared across the pipeline.
//!
//! [`DbEvent`] is the raw change event produced by an ingress source.
//! [`ActivationTask`] is a matched event ready for delivery, produced by
//! the filter engine and consumed by the egress dispatcher.

use serde::Serialize;

/// Identifies which database source produced an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SourceKind {
    Postgres,
}

/// The type of database operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OpType {
    Insert,
    Update,
    Delete,
}

/// A raw change event produced by an [`IngressSource`](crate::ingress::traits::IngressSource).
///
/// Carries source-specific position tracking (`source_offset`) so the
/// pipeline can confirm delivery back to the originating database.
#[derive(Debug, Clone)]
pub struct DbEvent {
    /// Source-specific position (Postgres LSN, MySQL binlog offset, …).
    pub source_offset: String,
    /// Identifies which source implementation produced this event.
    #[allow(dead_code)]
    pub source_kind: SourceKind,
    pub table_name: String,
    pub op_type: OpType,
    /// Previous row state (requires REPLICA IDENTITY FULL on Postgres).
    pub old_row: Option<serde_json::Value>,
    pub new_row: Option<serde_json::Value>,
}

/// A matched event ready for delivery to a destination.
#[derive(Debug, Clone, Serialize)]
pub struct ActivationTask {
    pub source_offset: String,
    pub table_name: String,
    pub op_type: OpType,
    pub payload: serde_json::Value,
    pub destination: DestinationConfig,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationConfig {
    pub kind: DestinationKind,
    pub url: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
pub enum DestinationKind {
    Webhook,
    Slack,
    Telegram,
}
