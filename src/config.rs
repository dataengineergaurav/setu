//! Configuration parsing for `activation.yaml`.
//!
//! The top-level [`ActivationConfig`] struct deserializes the YAML file,
//! resolves `${var}` substitutions from an optional `channels-env.yml`
//! secrets file, and can produce an [`ingress::SourceConfig`] for the
//! ingress factory.

use std::collections::HashMap;
use std::path::Path;
use serde::Deserialize;
use tracing::{info, warn};

use crate::ingress::SourceConfig;

/// Describes the database source in `activation.yaml`.
///
/// Each variant carries the connection details specific to that source type.
/// When this block is present, it takes priority over the top-level
/// `pg_connection` / `replication_slot` / `publication` fields.
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceDef {
    Postgres {
        connection: String,
        replication_slot: String,
        publication: String,
    },
}

/// Top-level configuration loaded from `activation.yaml`.
///
/// Supports both the legacy flat fields (`pg_connection`, `replication_slot`,
/// `publication`) and a newer `source` block. When `source` is present, the
/// flat fields are ignored.
#[derive(Debug, Deserialize, Clone)]
pub struct ActivationConfig {
    #[serde(default)]
    pub source: Option<SourceDef>,
    #[serde(default)]
    pub pg_connection: Option<String>,
    #[serde(default)]
    pub replication_slot: Option<String>,
    #[serde(default)]
    pub publication: Option<String>,
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Rule {
    pub table: String,
    pub op_type: String,
    pub conditions: Vec<Condition>,
    pub destination: DestinationDef,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Condition {
    pub field: String,
    pub old_value: Option<serde_json::Value>,
    pub new_value: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DestinationDef {
    #[serde(rename = "type")]
    pub kind: String,
    pub url: String,
    pub headers: Option<HashMap<String, String>>,
    pub channel: Option<String>,
}

impl ActivationConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut config: ActivationConfig = serde_yaml::from_str(&content)?;

        // Look for channels-env.yml alongside activation.yaml
        let config_dir = Path::new(path).parent().unwrap_or(Path::new("."));
        let env_path = config_dir.join("channels-env.yml");
        if env_path.exists() {
            let env_content = std::fs::read_to_string(&env_path)?;
            let env_vars: HashMap<String, String> = serde_yaml::from_str(&env_content)?;
            config.resolve_env_vars(&env_vars);
            info!(path = %env_path.display(), "Loaded secrets from channels-env.yml");
        }

        Ok(config)
    }

    /// Resolve the connection info, preferring the `source` block over flat fields.
    pub fn pg_connection(&self) -> Option<&str> {
        match &self.source {
            Some(SourceDef::Postgres { connection, .. }) => Some(connection.as_str()),
            None => self.pg_connection.as_deref(),
        }
    }

    /// Resolve the replication slot name.
    pub fn replication_slot(&self) -> Option<&str> {
        match &self.source {
            Some(SourceDef::Postgres { replication_slot, .. }) => Some(replication_slot.as_str()),
            None => self.replication_slot.as_deref(),
        }
    }

    /// Resolve the publication name.
    pub fn publication(&self) -> Option<&str> {
        match &self.source {
            Some(SourceDef::Postgres { publication, .. }) => Some(publication.as_str()),
            None => self.publication.as_deref(),
        }
    }

    /// Build an [`ingress::SourceConfig`] from this config.
    ///
    /// Returns an error if the source type is unsupported or if required
    /// connection fields are missing.
    pub fn to_source_config(&self) -> anyhow::Result<SourceConfig> {
        match &self.source {
            Some(SourceDef::Postgres { connection, replication_slot, publication }) => {
                Ok(SourceConfig::Postgres {
                    pg_connection: connection.clone(),
                    replication_slot: replication_slot.clone(),
                    publication: publication.clone(),
                })
            }
            None => {
                let pg_connection = self.pg_connection
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!(
                        "missing pg_connection and no source block in activation.yaml"
                    ))?
                    .clone();
                let replication_slot = self.replication_slot
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!(
                        "missing replication_slot and no source block in activation.yaml"
                    ))?
                    .clone();
                let publication = self.publication
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!(
                        "missing publication and no source block in activation.yaml"
                    ))?
                    .clone();
                Ok(SourceConfig::Postgres { pg_connection, replication_slot, publication })
            }
        }
    }

    fn resolve_env_vars(&mut self, env: &HashMap<String, String>) {
        match &mut self.source {
            Some(SourceDef::Postgres { connection, replication_slot, publication }) => {
                *connection = resolve_string(connection, env);
                *replication_slot = resolve_string(replication_slot, env);
                *publication = resolve_string(publication, env);
            }
            None => {
                if let Some(v) = &mut self.pg_connection {
                    *v = resolve_string(v, env);
                }
                if let Some(v) = &mut self.replication_slot {
                    *v = resolve_string(v, env);
                }
                if let Some(v) = &mut self.publication {
                    *v = resolve_string(v, env);
                }
            }
        }

        for rule in &mut self.rules {
            rule.destination.url = resolve_string(&rule.destination.url, env);
            rule.destination.channel = rule
                .destination
                .channel
                .as_ref()
                .map(|c| resolve_string(c, env));
            if let Some(ref mut headers) = rule.destination.headers {
                *headers = headers
                    .drain()
                    .map(|(k, v)| (resolve_string(&k, env), resolve_string(&v, env)))
                    .collect();
            }
        }
    }

    pub fn to_replication_config(&self) -> anyhow::Result<pgwire_replication::ReplicationConfig> {
        let pg_connection = self.pg_connection()
            .ok_or_else(|| anyhow::anyhow!("no pg_connection configured"))?;
        let slot = self.replication_slot()
            .ok_or_else(|| anyhow::anyhow!("no replication_slot configured"))?;
        let pub_name = self.publication()
            .ok_or_else(|| anyhow::anyhow!("no publication configured"))?;
        build_replication_config_from_parts(pg_connection, slot, pub_name)
    }
}

pub(crate) fn build_replication_config_from_parts(
    pg_connection: &str,
    replication_slot: &str,
    publication: &str,
) -> anyhow::Result<pgwire_replication::ReplicationConfig> {
    let host = parse_host(pg_connection).unwrap_or("localhost");
    let port = parse_port(pg_connection).unwrap_or(5432);
    let dbname = parse_dbname(pg_connection).unwrap_or("postgres");
    let user = parse_user(pg_connection).unwrap_or("postgres");
    let password = parse_password(pg_connection).unwrap_or("");

    Ok(pgwire_replication::ReplicationConfig::new(
        host,
        user,
        password,
        dbname,
        replication_slot,
        publication,
    )
    .with_port(port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_yaml() {
        let yaml = "pg_connection: host=db.example.com port=5432 dbname=proddb user=admin password=secret
replication_slot: my_slot
publication: my_pub
rules:
  - table: users
    op_type: Update
    conditions:
      - field: status
        old_value: active
        new_value: premium
    destination:
      type: webhook
      url: https://hook.example.com/upgrade
      headers:
        X-Key: abc123
  - table: orders
    op_type: Insert
    conditions: []
    destination:
      type: slack
      url: https://hooks.slack.com/hook
      channel: '#sales'
";
        let cfg: ActivationConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.pg_connection.as_deref(), Some("host=db.example.com port=5432 dbname=proddb user=admin password=secret"));
        assert_eq!(cfg.replication_slot.as_deref(), Some("my_slot"));
        assert_eq!(cfg.publication.as_deref(), Some("my_pub"));
        assert_eq!(cfg.rules.len(), 2);

        let first = &cfg.rules[0];
        assert_eq!(first.table, "users");
        assert_eq!(first.op_type, "Update");
        assert_eq!(first.conditions.len(), 1);
        assert_eq!(first.conditions[0].field, "status");
        assert_eq!(first.conditions[0].old_value.as_ref().and_then(|v| v.as_str()), Some("active"));
        assert_eq!(first.conditions[0].new_value.as_ref().and_then(|v| v.as_str()), Some("premium"));
        assert_eq!(first.destination.kind, "webhook");
        assert_eq!(first.destination.url, "https://hook.example.com/upgrade");
        assert_eq!(first.destination.headers.as_ref().unwrap().get("X-Key").unwrap(), "abc123");

        let second = &cfg.rules[1];
        assert_eq!(second.table, "orders");
        assert_eq!(second.op_type, "Insert");
        assert!(second.conditions.is_empty());
        assert_eq!(second.destination.kind, "slack");
        assert_eq!(second.destination.channel.as_ref().unwrap(), "#sales");
    }

    #[test]
    fn parse_minimal_yaml() {
        let yaml = r#"
pg_connection: "host=localhost dbname=mydb user=postgres"
replication_slot: "slot"
publication: "pub"
rules:
  - table: "*"
    op_type: "*"
    conditions: []
    destination:
      type: webhook
      url: "http://localhost:8080"
"#;
        let cfg: ActivationConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.rules.len(), 1);
        assert_eq!(cfg.rules[0].table, "*");
    }

    #[test]
    fn parse_host_extracts_correctly() {
        let conn = "host=db.example.com port=5432 dbname=mydb user=admin";
        assert_eq!(parse_host(conn), Some("db.example.com"));
        assert_eq!(parse_port(conn), Some(5432));
        assert_eq!(parse_dbname(conn), Some("mydb"));
        assert_eq!(parse_user(conn), Some("admin"));
        assert_eq!(parse_password(conn), None);
    }

    #[test]
    fn parse_connection_with_password() {
        let conn = "host=localhost dbname=test user=u password=p@ss";
        assert_eq!(parse_host(conn), Some("localhost"));
        assert_eq!(parse_password(conn), Some("p@ss"));
    }

    #[test]
    fn parse_connection_missing_fields_use_defaults() {
        let conn = "host=localhost";
        assert_eq!(parse_port(conn), None);
        assert_eq!(parse_dbname(conn), None);
        assert_eq!(parse_user(conn), None);
    }

    #[test]
    fn resolve_env_vars_replaces_known_keys() {
        let yaml = r#"
pg_connection: "host=${PG_HOST} dbname=mydb user=admin"
replication_slot: "slot"
publication: "pub"
rules:
  - table: users
    op_type: Update
    conditions: []
    destination:
      type: webhook
      url: "https://${API_DOMAIN}/hook"
      headers:
        Authorization: "Bearer ${API_KEY}"
"#;
        let mut cfg: ActivationConfig = serde_yaml::from_str(yaml).unwrap();
        let mut env = HashMap::new();
        env.insert("PG_HOST".into(), "db.internal.com".into());
        env.insert("API_DOMAIN".into(), "api.example.com".into());
        env.insert("API_KEY".into(), "secret-123".into());
        cfg.resolve_env_vars(&env);

        assert_eq!(cfg.pg_connection.as_deref(), Some("host=db.internal.com dbname=mydb user=admin"));
        assert_eq!(cfg.rules[0].destination.url, "https://api.example.com/hook");
        let auth = cfg.rules[0].destination.headers.as_ref().unwrap().get("Authorization").unwrap();
        assert_eq!(auth, "Bearer secret-123");
    }

    #[test]
    fn resolve_env_vars_leaves_unknown_keys() {
        let yaml = r#"
pg_connection: "host=${UNKNOWN_KEY} dbname=mydb"
replication_slot: "slot"
publication: "pub"
rules:
  - table: users
    op_type: Update
    conditions: []
    destination:
      type: webhook
      url: "http://localhost:8080"
"#;
        let mut cfg: ActivationConfig = serde_yaml::from_str(yaml).unwrap();
        let env = HashMap::new();
        cfg.resolve_env_vars(&env);
        assert_eq!(cfg.pg_connection.as_deref(), Some("host=${UNKNOWN_KEY} dbname=mydb"));
    }

    #[test]
    fn resolve_env_vars_no_substitutions_leaves_unchanged() {
        let yaml = r#"
pg_connection: "host=localhost dbname=mydb"
replication_slot: "slot"
publication: "pub"
rules: []
"#;
        let mut cfg: ActivationConfig = serde_yaml::from_str(yaml).unwrap();
        let env = HashMap::new();
        cfg.resolve_env_vars(&env);
        assert_eq!(cfg.pg_connection.as_deref(), Some("host=localhost dbname=mydb"));
    }

    #[test]
    fn resolve_string_multiple_substitutions() {
        let mut env = HashMap::new();
        env.insert("A".into(), "foo".into());
        env.insert("B".into(), "bar".into());
        assert_eq!(resolve_string("${A}/${B}", &env), "foo/bar");
    }

    #[test]
    fn resolve_string_adjacent_substitutions() {
        let mut env = HashMap::new();
        env.insert("A".into(), "x".into());
        env.insert("B".into(), "y".into());
        assert_eq!(resolve_string("${A}${B}", &env), "xy");
    }

    #[test]
    fn to_replication_config_preserves_values() {
        let cfg = ActivationConfig {
            source: None,
            pg_connection: Some("host=pg.example.com port=5432 dbname=test user=replicator password=secret".into()),
            replication_slot: Some("slot1".into()),
            publication: Some("pub1".into()),
            rules: vec![],
        };
        let repl = cfg.to_replication_config().unwrap();
        assert_eq!(repl.host, "pg.example.com");
        assert_eq!(repl.port, 5432);
        assert_eq!(repl.database, "test");
        assert_eq!(repl.user, "replicator");
        assert_eq!(repl.password, "secret");
        assert_eq!(repl.slot, "slot1");
        assert_eq!(repl.publication, "pub1");
    }
}

fn resolve_string(s: &str, env: &HashMap<String, String>) -> String {
    let mut result = s.to_string();
    let mut start = 0;
    loop {
        let open = result[start..].find("${");
        let Some(open) = open else {
            break;
        };
        let open = start + open;
        let close = result[open..].find('}').map(|c| open + c);
        let Some(close) = close else {
            warn!("Unclosed env var reference in: {}", s);
            break;
        };
        let key = &result[open + 2..close];
        match env.get(key) {
            Some(val) => {
                result.replace_range(open..=close, val);
                start = open + val.len();
            }
            None => {
                warn!("Unresolved env var: ${{{}}} in: {}", key, s);
                start = close + 1;
            }
        }
    }
    result
}

pub(crate) fn parse_host(s: &str) -> Option<&str> {
    for part in s.split_whitespace() {
        if let Some(val) = part.strip_prefix("host=") {
            return Some(val);
        }
    }
    None
}

pub(crate) fn parse_port(s: &str) -> Option<u16> {
    for part in s.split_whitespace() {
        if let Some(val) = part.strip_prefix("port=") {
            return val.parse().ok();
        }
    }
    None
}

pub(crate) fn parse_dbname(s: &str) -> Option<&str> {
    for part in s.split_whitespace() {
        if let Some(val) = part.strip_prefix("dbname=") {
            return Some(val);
        }
    }
    None
}

pub(crate) fn parse_user(s: &str) -> Option<&str> {
    for part in s.split_whitespace() {
        if let Some(val) = part.strip_prefix("user=") {
            return Some(val);
        }
    }
    None
}

pub(crate) fn parse_password(s: &str) -> Option<&str> {
    for part in s.split_whitespace() {
        if let Some(val) = part.strip_prefix("password=") {
            return Some(val);
        }
    }
    None
}
