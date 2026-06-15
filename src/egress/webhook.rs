use std::time::Duration;

use reqwest::Client;
use tracing::{error, info, warn};

use crate::types::ActivationTask;

const MAX_RETRIES: u32 = 3;

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
                kind: DestinationKind::Webhook,
                url: "http://localhost:9999/nonexistent".into(),
                headers: vec![("X-Test".into(), "val".into())],
            },
        }
    }

    #[test]
    fn retry_constant_is_three() {
        assert_eq!(MAX_RETRIES, 3);
    }

    #[tokio::test]
    async fn send_to_nonexistent_server_returns_false() {
        let client = Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let ok = send(&test_task(), &client).await;
        assert!(!ok);
    }
}

pub async fn send(task: &ActivationTask, client: &Client) -> bool {
    let mut last_error = String::new();

    for attempt in 1..=MAX_RETRIES {
        let mut req = client.post(&task.destination.url).json(&task.payload);
        for (key, value) in &task.destination.headers {
            req = req.header(key.as_str(), value.as_str());
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    table = %task.table_name,
                    dest = %task.destination.url,
                    status = %resp.status(),
                    attempt,
                    "Webhook delivery succeeded"
                );
                return true;
            }
            Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                last_error = format!("HTTP {}", resp.status());
                warn!(
                    status = %resp.status(),
                    attempt,
                    "Transient error, retrying"
                );
                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
            Ok(resp) => {
                error!(
                    status = %resp.status(),
                    attempt,
                    "Non-retriable HTTP error"
                );
                return false;
            }
            Err(e) => {
                last_error = e.to_string();
                warn!(error = %e, attempt, "Request failed, retrying");
                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
        }
    }

    error!(
        table = %task.table_name,
        error = %last_error,
        "Webhook delivery exhausted after {MAX_RETRIES} attempts"
    );
    false
}
