use std::time::Duration;

use reqwest::Client;
use serde_json::json;
use tracing::{error, info, warn};

use crate::types::ActivationTask;

const MAX_RETRIES: u32 = 3;

pub async fn send(task: &ActivationTask, client: &Client) -> bool {
    let chat_id = task
        .destination
        .headers
        .iter()
        .find(|(k, _)| k.to_lowercase() == "chat_id")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    let op_str = format!("{:?}", task.op_type);
    let payload_str = serde_json::to_string_pretty(&task.payload).unwrap_or_default();
    let text = format!(
        "\u{1f514} <b>{}</b> on <code>{}</code>\n<pre>{}</pre>",
        op_str, task.table_name, payload_str,
    );

    let telegram_payload = json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    });

    let mut last_error = String::new();

    for attempt in 1..=MAX_RETRIES {
        match client.post(&task.destination.url).json(&telegram_payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    attempt,
                    chat_id = %chat_id,
                    "Telegram notification sent"
                );
                return true;
            }
            Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                last_error = format!("HTTP {}", resp.status());
                warn!(status = %resp.status(), attempt, "Telegram transient error, retrying");
                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
            Ok(resp) => {
                error!(status = %resp.status(), attempt, "Telegram non-retriable error");
                return false;
            }
            Err(e) => {
                last_error = e.to_string();
                warn!(error = %e, attempt, "Telegram request failed, retrying");
                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
        }
    }

    error!(
        error = %last_error,
        "Telegram delivery exhausted after {MAX_RETRIES} attempts"
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
            payload: serde_json::json!({"id": 1, "name": "Alice"}),
            destination: DestinationConfig {
                kind: DestinationKind::Telegram,
                url: "https://api.telegram.org/botTEST_TOKEN/sendMessage".into(),
                headers: vec![("chat_id".into(), "123456".into())],
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
