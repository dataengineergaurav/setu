use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Tracks the maximum confirmed source offset.
///
/// Receives offset values (e.g., Postgres LSN as a numeric string, or a MySQL
/// binlog position) from the filter and egress agents and tracks the highest
/// confirmed value. The ingress agent reads this to acknowledge the position
/// back to the source database.
///
/// This decouples the offset tracking from the I/O (which happens in the
/// ingress agent).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OffsetTracker {
    confirmed: Arc<AtomicU64>,
}

#[allow(dead_code)]
impl OffsetTracker {
    pub fn new() -> (Self, mpsc::Sender<String>) {
        let confirmed = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::channel::<String>(1024);
        let tracker = Self {
            confirmed: Arc::clone(&confirmed),
        };

        let confirm = Arc::clone(&confirmed);
        tokio::spawn(async move {
            while let Some(offset) = rx.recv().await {
                if let Ok(pos) = offset.parse::<u64>() {
                    let max_pos = confirm.fetch_max(pos, Ordering::AcqRel).max(pos);
                    debug!(offset = %offset, max = %max_pos, "Offset confirmed");
                }
            }
            info!("Offset tracker shutting down");
        });

        (tracker, tx)
    }

    pub fn last_confirmed(&self) -> u64 {
        self.confirmed.load(Ordering::Acquire)
    }
}
