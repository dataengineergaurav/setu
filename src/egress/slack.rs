use std::time::Duration;

use reqwest::Client;
use serde_json::json;
use tracing::{error, info, warn};

use crate::types::ActivationTask;

const MAX_RETRIES: u32 = 3;

pub async fn send(task: &ActivationTask, client: &Client) -> bool {
    let text = format!(
        "`{:?}` event on `{}`\n```json\n{}\n```",
        task.op_type,
        task.table_name,
        serde_json::to_string_pretty(&task.payload).unwrap_or_default(),
    );

    let slack_payload = json!({
        "text": text,
        "channel": task.destination.headers.iter()
            .find(|(k, _)| k.to_lowercase() == "channel")
            .map(|(_, v)| v)
            .unwrap_or(&"#general".to_string()),
    });

    let mut last_error = String::new();

    for attempt in 1..=MAX_RETRIES {
        match client.post(&task.destination.url).json(&slack_payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    table = %task.table_name,
                    attempt,
                    "Slack notification sent"
                );
                return true;
            }
            Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                last_error = format!("HTTP {}", resp.status());
                warn!(status = %resp.status(), attempt, "Slack transient error, retrying");
                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
            Ok(resp) => {
                error!(status = %resp.status(), attempt, "Slack non-retriable error");
                return false;
            }
            Err(e) => {
                last_error = e.to_string();
                warn!(error = %e, attempt, "Slack request failed, retrying");
                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
        }
    }

    error!(
        table = %task.table_name,
        error = %last_error,
        "Slack delivery exhausted after {MAX_RETRIES} attempts"
    );
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DestinationConfig, DestinationKind};

    fn test_task() -> ActivationTask {
        ActivationTask {
            source_offset: "100".into(),
            table_name: "users".into(),
            op_type: crate::types::OpType::Insert,
            payload: serde_json::json!({"id": 1}),
            destination: DestinationConfig {
                kind: DestinationKind::Slack,
                url: "http://localhost:9999/nonexistent".into(),
                headers: vec![("channel".into(), "#test".into())],
            },
        }
    }

    #[test]
    fn retry_constant_is_three() {
        assert_eq!(MAX_RETRIES, 3);
    }

    #[tokio::test]
    async fn send_to_nonexistent_server_returns_false() {
        let client = Client::builder().timeout(Duration::from_millis(100)).build().unwrap();
        let ok = send(&test_task(), &client).await;
        assert!(!ok);
    }
}
