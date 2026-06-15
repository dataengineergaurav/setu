// ──────────────────────────────────────────────────────────────
// INTEGRATION TEST PATTERNS
// ──────────────────────────────────────────────────────────────
// These tests demonstrate how to verify the full pipeline
// end-to-end. They require a running PostgreSQL instance with
// logical replication enabled.
//
// Run with:
//   DATABASE_URL="host=localhost dbname=test user=postgres" cargo test --test integration_test -- --ignored
//
// To run ALL integration tests (including ignored):
//   cargo test -- --include-ignored

use pgwire_replication::lsn::Lsn;

// ── Helper: parse DATABASE_URL env var ──
fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "host=localhost port=5432 dbname=postgres user=postgres".into())
}

fn db_host() -> String {
    for part in db_url().split_whitespace() {
        if let Some(val) = part.strip_prefix("host=") {
            return val.to_string();
        }
    }
    "localhost".into()
}

fn db_port() -> u16 {
    for part in db_url().split_whitespace() {
        if let Some(val) = part.strip_prefix("port=") {
            return val.parse().unwrap_or(5432);
        }
    }
    5432
}

fn db_name() -> String {
    for part in db_url().split_whitespace() {
        if let Some(val) = part.strip_prefix("dbname=") {
            return val.to_string();
        }
    }
    "postgres".into()
}

fn db_user() -> String {
    for part in db_url().split_whitespace() {
        if let Some(val) = part.strip_prefix("user=") {
            return val.to_string();
        }
    }
    "postgres".into()
}

fn db_password() -> String {
    for part in db_url().split_whitespace() {
        if let Some(val) = part.strip_prefix("password=") {
            return val.to_string();
        }
    }
    "".into()
}

// ──────────────────────────────────────────────────────────────
// 1. INFRASTRUCTURE TEST
//    Verifies that PostgreSQL is reachable and has wal_level=logical
// ──────────────────────────────────────────────────────────────
#[ignore = "requires running PostgreSQL with logical replication"]
#[tokio::test]
async fn postgres_is_reachable() {
    let (client, conn) = tokio_postgres::connect(&db_url(), tokio_postgres::NoTls)
        .await
        .expect("Failed to connect to Postgres. Set DATABASE_URL env var.");
    tokio::spawn(conn);

    let wal_level: String = client.query_one("SHOW wal_level", &[]).await.unwrap().get(0);
    assert_eq!(
        wal_level, "logical",
        "wal_level must be 'logical'. Run: ALTER SYSTEM SET wal_level = logical; SELECT pg_reload_conf();"
    );
}

// ──────────────────────────────────────────────────────────────
// 2. SLOT & PUBLICATION MANAGEMENT TEST
//    Ensures slots/publications can be created and dropped
// ──────────────────────────────────────────────────────────────
#[ignore = "requires running PostgreSQL with logical replication"]
#[tokio::test]
async fn create_and_drop_replication_slot() {
    let (client, conn) = tokio_postgres::connect(&db_url(), tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);

    let slot_name = "test_slot_integration";

    // Create
    client
        .simple_query(&format!(
            "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
            slot_name
        ))
        .await
        .unwrap();

    // Verify exists
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await
        .unwrap()
        .get(0);
    assert!(exists);

    // Drop
    client
        .simple_query(&format!("SELECT pg_drop_replication_slot('{}')", slot_name))
        .await
        .unwrap();
}

// ──────────────────────────────────────────────────────────────
// 3. WAL STREAM CONNECTION TEST
//    Verifies the pgwire-replication client can connect and
//    start receiving events
// ──────────────────────────────────────────────────────────────
#[ignore = "requires running PostgreSQL with logical replication"]
#[tokio::test]
async fn replication_client_connects_and_streams() {
    let slot_name = "test_stream_integration";

    // Ensure slot exists
    {
        let (client, conn) = tokio_postgres::connect(&db_url(), tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(conn);

        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot_name],
            )
            .await
            .unwrap()
            .get(0);
        if !exists {
            client
                .simple_query(&format!(
                    "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
                    slot_name
                ))
                .await
                .unwrap();
        }

        client
            .simple_query("CREATE PUBLICATION IF NOT EXISTS test_pub_integration FOR ALL TABLES")
            .await
            .unwrap();
    }

    let config = pgwire_replication::ReplicationConfig::new(
        db_host(),
        db_user(),
        db_password(),
        db_name(),
        slot_name,
        "test_pub_integration",
    )
    .with_port(db_port());

    let mut client = pgwire_replication::ReplicationClient::connect(config)
        .await
        .expect("ReplicationClient::connect failed");

    // Receive events for up to 3 seconds
    let timeout = tokio::time::sleep(std::time::Duration::from_secs(3));
    tokio::pin!(timeout);

    let mut received_event = false;
    loop {
        tokio::select! {
            _ = &mut timeout => break,
            ev = client.recv() => {
                match ev {
                    Ok(Some(_)) => received_event = true,
                    Ok(None) => break,
                    Err(e) => panic!("Replication error: {e}"),
                }
            }
        }
    }

    assert!(received_event, "Expected at least one event (e.g., KeepAlive)");

    // Cleanup
    client.stop();
    let _ = client.shutdown().await;

    // Drop slot
    let (cleanup, conn) = tokio_postgres::connect(&db_url(), tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);
    let _ = cleanup
        .simple_query(&format!("SELECT pg_drop_replication_slot('{slot_name}')"))
        .await;
    let _ = cleanup
        .simple_query("DROP PUBLICATION IF EXISTS test_pub_integration")
        .await;
}

// ──────────────────────────────────────────────────────────────
// 4. PGOUTPUT PARSER TEST (data-level integration)
//    Verifies that pgoutput bytes produced by Postgres are
//    correctly decoded by our parser
// ──────────────────────────────────────────────────────────────
#[ignore = "requires running PostgreSQL with logical replication"]
#[tokio::test]
async fn pgoutput_decoder_integration() {
    use realtime_activation_engine::pgoutput::PgoutputDecoder;

    let slot_name = "test_parser_integration";

    // Setup: create table, slot, publication
    let (client, conn) = tokio_postgres::connect(&db_url(), tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);

    client
        .batch_execute(&format!(
            r#"
            DROP TABLE IF EXISTS test_cdc CASCADE;
            CREATE TABLE test_cdc (
                id SERIAL PRIMARY KEY,
                label TEXT,
                value INT
            );
            ALTER TABLE test_cdc REPLICA IDENTITY FULL;

            CREATE PUBLICATION IF NOT EXISTS test_parser_pub FOR ALL TABLES;

            SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput');
            "#
        ))
        .await
        .unwrap();

    // Connect replication client
    let config = pgwire_replication::ReplicationConfig::new(
        db_host(),
        db_user(),
        db_password(),
        db_name(),
        slot_name,
        "test_parser_pub",
    )
    .with_port(db_port())
    .with_start_lsn(Lsn::ZERO);

    let mut repl = pgwire_replication::ReplicationClient::connect(config).await.unwrap();

    // Insert a row to generate a WAL event
    client
        .simple_query("INSERT INTO test_cdc (label, value) VALUES ('hello', 42)")
        .await
        .unwrap();

    // Collect WAL events and decode them
    let mut decoder = PgoutputDecoder::new();
    let mut found_insert = false;

    let timeout = tokio::time::sleep(std::time::Duration::from_secs(5));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => break,
            ev = repl.recv() => {
                match ev {
                    Ok(Some(pgwire_replication::ReplicationEvent::XLogData { data, wal_end, .. })) => {
                        let events = decoder.decode(&data, wal_end.into());
                        for e in &events {
                            if e.table_name == "test_cdc"
                                && e.op_type == realtime_activation_engine::types::OpType::Insert
                            {
                                found_insert = true;
                                if let Some(ref new) = e.new_row {
                                    assert_eq!(new.get("label").and_then(|v| v.as_str()), Some("hello"));
                                }
                            }
                        }
                    }
                    Ok(Some(pgwire_replication::ReplicationEvent::Commit { end_lsn, .. })) => {
                        repl.update_applied_lsn(end_lsn);
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(e) => panic!("Replication error: {e}"),
                }
            }
        }
    }

    assert!(found_insert, "Expected to decode an Insert event on test_cdc");

    // Cleanup
    repl.stop();
    let _ = repl.shutdown().await;
    let _ = client
        .simple_query(&format!("SELECT pg_drop_replication_slot('{slot_name}')"))
        .await;
    let _ = client.simple_query("DROP PUBLICATION IF EXISTS test_parser_pub").await;
}

// ──────────────────────────────────────────────────────────────
// 5. CONFIGURATION PARSING (no Postgres needed)
//    End-to-end config parsing and replication config generation
// ──────────────────────────────────────────────────────────────
#[test]
fn config_round_trip() {
    let yaml = r#"
pg_connection: "host=localhost port=5432 dbname=mydb user=postgres"
replication_slot: "e2e_slot"
publication: "e2e_pub"
rules:
  - table: "users"
    op_type: Update
    conditions:
      - field: "status"
        old_value: "active"
        new_value: "premium"
    destination:
      type: webhook
      url: "https://hooks.example.com/upgrade"
"#;

    let cfg: realtime_activation_engine::config::ActivationConfig = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(cfg.rules.len(), 1);
    assert_eq!(cfg.rules[0].table, "users");

    let repl = cfg.to_replication_config().unwrap();
    assert_eq!(repl.host, "localhost");
    assert_eq!(repl.port, 5432);
    assert_eq!(repl.database, "mydb");
    assert_eq!(repl.slot, "e2e_slot");
    assert_eq!(repl.publication, "e2e_pub");
}

// ──────────────────────────────────────────────────────────────
// 6. FILTER ENGINE LOGIC (no Postgres needed)
//    Full pipeline: event → filter → activation task
// ──────────────────────────────────────────────────────────────
// Rule matching logic is thoroughly tested in src/filter/engine.rs unit tests.
// Config round-trip is validated above in parse_activation_yaml.
