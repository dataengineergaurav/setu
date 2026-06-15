use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::{DestinationDef, Rule};
use crate::types::{ActivationTask, DbEvent, DestinationConfig, DestinationKind, OpType};

pub async fn run(
    mut event_rx: mpsc::Receiver<DbEvent>,
    task_tx: mpsc::Sender<ActivationTask>,
    filtered_lsn_tx: mpsc::Sender<String>,
    rules: Vec<Rule>,
) {
    while let Some(event) = event_rx.recv().await {
        let matched_rules: Vec<&Rule> = rules.iter().filter(|r| matches_rule(r, &event)).collect();

        if matched_rules.is_empty() {
            debug!(table = %event.table_name, "No rules matched, releasing LSN");
            let _ = filtered_lsn_tx.send(event.source_offset.clone()).await;
            continue;
        }

        for rule in matched_rules {
            let payload = build_payload(&event);
            let dest = dest_config_from_def(&rule.destination);

            let task = ActivationTask {
                source_offset: event.source_offset.clone(),
                table_name: event.table_name.clone(),
                op_type: event.op_type,
                payload,
                destination: dest,
            };

            info!(table = %task.table_name, dest = %rule.destination.url, "Routing event to outbound");
            if task_tx.send(task).await.is_err() {
                warn!("Outbound worker channel closed");
                return;
            }
        }
    }
}

fn matches_rule(rule: &Rule, event: &DbEvent) -> bool {
    if rule.table != "*" && rule.table != event.table_name {
        return false;
    }

    let rule_op = match rule.op_type.to_lowercase().as_str() {
        "insert" => OpType::Insert,
        "update" => OpType::Update,
        "delete" => OpType::Delete,
        "*" => return true,
        _ => return false,
    };
    if rule_op != event.op_type {
        return false;
    }

    for condition in &rule.conditions {
        let old_field = event.old_row.as_ref().and_then(|r| r.get(&condition.field));
        let new_field = event.new_row.as_ref().and_then(|r| r.get(&condition.field));

        let old_matches = condition
            .old_value
            .as_ref()
            .map(|v| old_field == Some(v))
            .unwrap_or(true);
        let new_matches = condition
            .new_value
            .as_ref()
            .map(|v| new_field == Some(v))
            .unwrap_or(true);

        if !old_matches || !new_matches {
            return false;
        }
    }

    true
}

fn build_payload(event: &DbEvent) -> serde_json::Value {
    let mut payload = serde_json::Map::new();
    payload.insert("table".into(), event.table_name.clone().into());
    payload.insert("op".into(), format!("{:?}", event.op_type).into());

    if let Some(ref old) = event.old_row {
        payload.insert("old".into(), old.clone());
    }
    if let Some(ref new) = event.new_row {
        payload.insert("new".into(), new.clone());
    }

    serde_json::Value::Object(payload)
}

fn dest_config_from_def(def: &DestinationDef) -> DestinationConfig {
    let kind = match def.kind.to_lowercase().as_str() {
        "slack" => DestinationKind::Slack,
        "telegram" => DestinationKind::Telegram,
        _ => DestinationKind::Webhook,
    };

    let mut headers: Vec<(String, String)> = def
        .headers
        .as_ref()
        .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    if let Some(ref channel) = def.channel {
        headers.push(("channel".into(), channel.clone()));
    }

    DestinationConfig {
        kind,
        url: def.url.clone(),
        headers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Condition;
    use crate::types::SourceKind;

    fn make_event(table: &str, op: OpType, old: Option<serde_json::Value>, new: Option<serde_json::Value>) -> DbEvent {
        DbEvent {
            source_offset: "0".into(),
            source_kind: SourceKind::Postgres,
            table_name: table.into(),
            op_type: op,
            old_row: old,
            new_row: new,
        }
    }

    fn make_rule(table: &str, op: &str, conditions: Vec<Condition>) -> Rule {
        Rule {
            table: table.into(),
            op_type: op.into(),
            conditions,
            destination: DestinationDef {
                kind: "webhook".into(),
                url: "http://example.com/hook".into(),
                headers: None,
                channel: None,
            },
        }
    }

    fn cond(field: &str, old: Option<&str>, new: Option<&str>) -> Condition {
        Condition {
            field: field.into(),
            old_value: old.map(|s| serde_json::Value::String(s.into())),
            new_value: new.map(|s| serde_json::Value::String(s.into())),
        }
    }

    #[test]
    fn exact_table_match() {
        let event = make_event("users", OpType::Insert, None, None);
        let rule = make_rule("users", "Insert", vec![]);
        assert!(matches_rule(&rule, &event));
    }

    #[test]
    fn table_mismatch() {
        let event = make_event("orders", OpType::Insert, None, None);
        let rule = make_rule("users", "Insert", vec![]);
        assert!(!matches_rule(&rule, &event));
    }

    #[test]
    fn wildcard_table() {
        let event = make_event("any_table", OpType::Update, None, None);
        let rule = make_rule("*", "Update", vec![]);
        assert!(matches_rule(&rule, &event));
    }

    #[test]
    fn wildcard_op() {
        let event = make_event("users", OpType::Delete, None, None);
        let rule = make_rule("users", "*", vec![]);
        assert!(matches_rule(&rule, &event));
    }

    #[test]
    fn op_mismatch() {
        let event = make_event("users", OpType::Insert, None, None);
        let rule = make_rule("users", "Delete", vec![]);
        assert!(!matches_rule(&rule, &event));
    }

    #[test]
    fn condition_new_value_matches() {
        let new = serde_json::json!({"status": "premium"});
        let event = make_event("users", OpType::Update, None, Some(new));
        let rule = make_rule("users", "Update", vec![cond("status", None, Some("premium"))]);
        assert!(matches_rule(&rule, &event));
    }

    #[test]
    fn condition_new_value_mismatch() {
        let new = serde_json::json!({"status": "active"});
        let event = make_event("users", OpType::Update, None, Some(new));
        let rule = make_rule("users", "Update", vec![cond("status", None, Some("premium"))]);
        assert!(!matches_rule(&rule, &event));
    }

    #[test]
    fn condition_old_and_new_both_match() {
        let old = serde_json::json!({"status": "active"});
        let new = serde_json::json!({"status": "premium"});
        let event = make_event("users", OpType::Update, Some(old), Some(new));
        let rule = make_rule("users", "Update", vec![cond("status", Some("active"), Some("premium"))]);
        assert!(matches_rule(&rule, &event));
    }

    #[test]
    fn condition_old_mismatch() {
        let old = serde_json::json!({"status": "suspended"});
        let new = serde_json::json!({"status": "premium"});
        let event = make_event("users", OpType::Update, Some(old), Some(new));
        let rule = make_rule("users", "Update", vec![cond("status", Some("active"), Some("premium"))]);
        assert!(!matches_rule(&rule, &event));
    }

    #[test]
    fn no_conditions_always_matches() {
        let event = make_event("users", OpType::Insert, None, Some(serde_json::json!({"x": "y"})));
        let rule = make_rule("users", "Insert", vec![]);
        assert!(matches_rule(&rule, &event));
    }

    #[test]
    fn field_not_in_event_still_passes() {
        let event = make_event("users", OpType::Update, None, Some(serde_json::json!({"name": "bob"})));
        let rule = make_rule("users", "Update", vec![cond("status", None, None)]);
        // old_value and new_value are both None, so both conditions implicitly match
        assert!(matches_rule(&rule, &event));
    }

    #[test]
    fn build_payload_contains_expected_fields() {
        let old = serde_json::json!({"id": 1, "status": "active"});
        let new = serde_json::json!({"id": 1, "status": "premium"});
        let event = make_event("users", OpType::Update, Some(old), Some(new.clone()));
        let payload = build_payload(&event);
        assert_eq!(payload.get("table").and_then(|v| v.as_str()), Some("users"));
        assert_eq!(payload.get("op").and_then(|v| v.as_str()), Some("Update"));
        assert_eq!(payload.get("new"), Some(&new));
    }

    #[test]
    fn dest_config_defaults_to_webhook() {
        let def = DestinationDef {
            kind: "unknown".into(),
            url: "http://ex.com".into(),
            headers: None,
            channel: None,
        };
        let cfg = dest_config_from_def(&def);
        assert!(matches!(cfg.kind, DestinationKind::Webhook));
    }

    #[test]
    fn dest_config_slack_kind() {
        let def = DestinationDef {
            kind: "Slack".into(),
            url: "http://slack.com/hook".into(),
            headers: None,
            channel: Some("#test".into()),
        };
        let cfg = dest_config_from_def(&def);
        assert!(matches!(cfg.kind, DestinationKind::Slack));
        assert!(cfg.headers.iter().any(|(k, v)| k == "channel" && v == "#test"));
    }

    #[test]
    fn dest_config_telegram_kind() {
        let def = DestinationDef {
            kind: "Telegram".into(),
            url: "https://api.telegram.org/botTOKEN/sendMessage".into(),
            headers: None,
            channel: None,
        };
        let cfg = dest_config_from_def(&def);
        assert!(matches!(cfg.kind, DestinationKind::Telegram));
    }
}
