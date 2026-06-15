use std::time::Duration;

use async_trait::async_trait;
use pgwire_replication::{ReplicationClient, ReplicationEvent};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::config::build_replication_config_from_parts;
use crate::pgoutput::PgoutputDecoder;
use crate::types::DbEvent;

use super::traits::IngressSource;

/// A PostgreSQL WAL source that implements the [`IngressSource`] trait.
///
/// Connects to a PostgreSQL database via logical replication, consumes the
/// WAL stream, and pushes decoded [`DbEvent`] values into the pipeline.
pub struct PostgresSource {
    pg_connection: String,
    replication_slot: String,
    publication: String,
}

impl PostgresSource {
    pub fn new(pg_connection: String, replication_slot: String, publication: String) -> Self {
        Self { pg_connection, replication_slot, publication }
    }
}

#[async_trait]
impl IngressSource for PostgresSource {
    async fn run(
        self: Box<Self>,
        event_tx: mpsc::Sender<DbEvent>,
        mut confirmed_offset_rx: mpsc::Receiver<String>,
    ) -> anyhow::Result<()> {
        loop {
            match try_connect(&self, &event_tx, &mut confirmed_offset_rx).await {
                Ok(()) => {
                    info!("Postgres source finished normally, reconnecting...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    error!(error = %e, "Postgres source connection lost, reconnecting in 5s");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    fn name(&self) -> &'static str {
        "postgres"
    }
}

async fn try_connect(
    source: &PostgresSource,
    event_tx: &mpsc::Sender<DbEvent>,
    confirmed_offset_rx: &mut mpsc::Receiver<String>,
) -> anyhow::Result<()> {
    ensure_slot_and_publication(source).await?;

    let repl_config = build_replication_config_from_parts(
        &source.pg_connection,
        &source.replication_slot,
        &source.publication,
    )?;
    let mut client = ReplicationClient::connect(repl_config).await?;
    info!("Connected to PostgreSQL replication stream");

    let mut decoder = PgoutputDecoder::new();

    loop {
        tokio::select! {
            confirmed = confirmed_offset_rx.recv() => {
                match confirmed {
                    Some(offset) => {
                        if let Ok(lsn) = offset.parse::<u64>() {
                            client.update_applied_lsn(lsn.into());
                        }
                    }
                    None => {
                        warn!("Confirmed offset channel closed, stopping ingress");
                        break;
                    }
                }
            }
            event = client.recv() => {
                match event {
                    Ok(Some(ReplicationEvent::XLogData { wal_end, data, .. })) => {
                        let lsn: u64 = wal_end.into();
                        let events = decoder.decode(&data, lsn);

                        if events.is_empty() {
                            continue;
                        }

                        for db_event in events {
                            if event_tx.send(db_event).await.is_err() {
                                error!("Filter agent channel closed, stopping");
                                return Ok(());
                            }
                        }
                    }
                    Ok(Some(ReplicationEvent::KeepAlive { .. })) => {}
                    Ok(Some(ReplicationEvent::Begin { .. })) => {}
                    Ok(Some(ReplicationEvent::Commit { end_lsn, .. })) => {
                        let lsn: u64 = end_lsn.into();
                        client.update_applied_lsn(lsn.into());
                        info!(lsn = %end_lsn, "Transaction committed, LSN confirmed");
                    }
                    Ok(Some(ReplicationEvent::Message { .. })) => {}
                    Ok(Some(ReplicationEvent::StoppedAt { reached })) => {
                        info!(reached = %reached, "Replication reached stop position");
                        break;
                    }
                    Ok(None) => {
                        info!("Replication stream ended cleanly");
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "Replication error");
                        return Err(e.into());
                    }
                }
            }
        }
    }

    Ok(())
}

async fn ensure_slot_and_publication(source: &PostgresSource) -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(&source.pg_connection, tokio_postgres::NoTls).await?;
    tokio::spawn(connection);

    let pub_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&source.publication],
        )
        .await?
        .get(0);

    if !pub_exists {
        client
            .simple_query(&format!(
                "CREATE PUBLICATION \"{}\" FOR ALL TABLES",
                source.publication
            ))
            .await?;
        info!(publication = %source.publication, "Publication created");
    } else {
        info!(publication = %source.publication, "Publication already exists");
    }

    let slot_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&source.replication_slot],
        )
        .await?
        .get(0);

    if !slot_exists {
        client
            .simple_query(&format!(
                "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
                source.replication_slot
            ))
            .await?;
        info!(slot = %source.replication_slot, "Replication slot created");
    } else {
        info!(slot = %source.replication_slot, "Replication slot already exists");
    }

    Ok(())
}
