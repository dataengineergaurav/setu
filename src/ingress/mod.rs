//! Pluggable ingress sources for database change streams.
//!
//! This module defines the [`IngressSource`](traits::IngressSource) trait,
//! a [`SourceConfig`] enum for config-driven source creation, and the
//! [`create_source`] / [`spawn_source`] functions.
//!
//! Currently implemented:
//! - [`postgres::PostgresSource`] — logical replication WAL consumer.

pub mod postgres;
pub mod traits;

use crate::types::DbEvent;
use tokio::sync::mpsc;
use tracing::info;
use traits::IngressSource;

/// Configuration for creating an ingress source.
///
/// As new source types are added, new variants are introduced here. The
/// factory function [`create_source`] maps each variant to its corresponding
/// [`IngressSource`] implementation.
pub enum SourceConfig {
    /// PostgreSQL logical replication source.
    Postgres {
        pg_connection: String,
        replication_slot: String,
        publication: String,
    },
}

/// Create and return an [`IngressSource`] based on the given [`SourceConfig`].
pub fn create_source(config: SourceConfig) -> Box<dyn IngressSource> {
    match config {
        SourceConfig::Postgres {
            pg_connection,
            replication_slot,
            publication,
        } => Box::new(postgres::PostgresSource::new(
            pg_connection,
            replication_slot,
            publication,
        )),
    }
}

/// Spawn a source onto the tokio runtime and return a [`JoinHandle`].
pub fn spawn_source_from(
    source: Box<dyn IngressSource>,
    event_tx: mpsc::Sender<DbEvent>,
    confirmed_offset_rx: mpsc::Receiver<String>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move { source.run(event_tx, confirmed_offset_rx).await })
}

/// Convenience: create a source from [`SourceConfig`] and spawn it.
pub fn spawn_source(
    config: SourceConfig,
    event_tx: mpsc::Sender<DbEvent>,
    confirmed_offset_rx: mpsc::Receiver<String>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let source = create_source(config);
    info!("{} source started", source.name());
    spawn_source_from(source, event_tx, confirmed_offset_rx)
}
