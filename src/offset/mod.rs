//! Source-offset tracking for at-least-once delivery.
//!
//! [`tracker::OffsetTracker`] receives confirmed source positions from
//! the filter and egress agents and tracks the maximum confirmed value.
//! The ingress source reads this value periodically to acknowledge the
//! position back to the database.

pub mod tracker;
