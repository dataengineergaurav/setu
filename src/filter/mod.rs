//! Rule evaluation engine.
//!
//! Ingest [`DbEvent`](crate::types::DbEvent) values, match them against
//! YAML-defined rules, evaluate conditions, and produce
//! [`ActivationTask`](crate::types::ActivationTask) values for delivery.

pub mod engine;
