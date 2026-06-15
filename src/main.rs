mod config;
mod egress;
mod filter;
mod ingress;
mod offset;
mod pgoutput;
mod types;

use tokio::sync::mpsc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = config::ActivationConfig::from_file("activation.yaml")?;
    info!(
        "Loaded activation configuration with {} rules",
        cfg.rules.len()
    );

    // Channel capacities enforce backpressure per architecture design
    let (event_tx, event_rx) = mpsc::channel::<types::DbEvent>(1024);
    let (task_tx, task_rx) = mpsc::channel::<types::ActivationTask>(1024);
    let (confirmed_offset_tx, confirmed_offset_rx) = mpsc::channel::<String>(1024);

    let rules = cfg.rules.clone();

    // Agent 1: Ingress — Source (PostgreSQL WAL Consumer by default)
    let ingress_source_cfg = cfg.to_source_config().expect("valid source config");
    let ingress_event_tx = event_tx.clone();
    let ingress_handle = ingress::spawn_source(ingress_source_cfg, ingress_event_tx, confirmed_offset_rx);

    // Agent 2: Filter — Routing Engine
    let filter_event_rx = event_rx;
    let filter_task_tx = task_tx.clone();
    let filter_offset_tx = confirmed_offset_tx.clone();
    let filter_rules = rules.clone();
    let filter_handle = tokio::spawn(async move {
        filter::engine::run(filter_event_rx, filter_task_tx, filter_offset_tx, filter_rules).await;
    });

    // Agent 3: Egress — Outbound Worker
    let egress_task_rx = task_rx;
    let egress_offset_tx = confirmed_offset_tx.clone();
    let egress_handle = tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        let mut task_rx = egress_task_rx;
        while let Some(task) = task_rx.recv().await {
            let success = match task.destination.kind {
                types::DestinationKind::Webhook => {
                    egress::webhook::send(&task, &client).await
                }
                types::DestinationKind::Slack => {
                    egress::slack::send(&task, &client).await
                }
                types::DestinationKind::Telegram => {
                    egress::telegram::send(&task, &client).await
                }
            };

            if success {
                let _ = egress_offset_tx.send(task.source_offset.clone()).await;
            } else {
                tracing::warn!(
                    table = %task.table_name,
                    dest = %task.destination.url,
                    "Delivery failed, offset {} will not be confirmed — manual intervention required",
                    task.source_offset
                );
            }
        }
    });

    // Monitor all agents
    tokio::select! {
        result = ingress_handle => {
            tracing::error!("Ingress agent exited: {:?}", result);
        }
        result = filter_handle => {
            tracing::error!("Filter agent exited: {:?}", result);
        }
        result = egress_handle => {
            tracing::error!("Egress agent exited: {:?}", result);
        }
    }

    Ok(())
}
