use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::types::DbEvent;

/// A source of database change events.
///
/// Each implementation connects to a specific database type (PostgreSQL, MySQL,
/// etc.), listens for mutations, decodes them into [`DbEvent`] values, and
/// pushes them into the pipeline via `event_tx`.
///
/// The `confirmed_offset_rx` channel delivers source-position strings that have
/// been fully processed (either delivered downstream or filtered out). The
/// source implementation acknowledges those positions to the database so that
/// the system can resume from the correct point after a restart.
///
/// # At-least-once guarantee
///
/// Offsets are sent back *only* after an event reaches a terminal state:
/// delivered to the destination (2xx response) or explicitly filtered. If the
/// source crashes before processing an offset it will be replayed from the
/// last confirmed position.
#[async_trait]
pub trait IngressSource: Send + 'static {
    /// Run the source, consuming the configured connection.
    ///
    /// Implementations should run an infinite event loop that reads from the
    /// database, decodes changes, and sends [`DbEvent`] values through
    /// `event_tx`. The method should return when the stream ends (clean
    /// shutdown) or propagate an error on unrecoverable failure.
    async fn run(
        self: Box<Self>,
        event_tx: mpsc::Sender<DbEvent>,
        confirmed_offset_rx: mpsc::Receiver<String>,
    ) -> anyhow::Result<()>;

    /// Human-readable source name for logging and metrics.
    fn name(&self) -> &'static str;
}
