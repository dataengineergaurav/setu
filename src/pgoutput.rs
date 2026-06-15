//! PostgreSQL pgoutput logical replication message decoder.
//!
//! Parses the binary pgoutput protocol to extract table mutations
//! (insert, update, delete) into [`DbEvent`](crate::types::DbEvent)
//! values with structured `old_row` / `new_row` JSON.

use bytes::Bytes;
use std::collections::HashMap;
use tracing::warn;

use crate::types::{DbEvent, OpType, SourceKind};

#[derive(Debug, Clone)]
struct RelationMeta {
    name: String,
    columns: Vec<ColumnMeta>,
}

#[derive(Debug, Clone)]
struct ColumnMeta {
    name: String,
}

pub struct PgoutputDecoder {
    relations: HashMap<u32, RelationMeta>,
}

impl PgoutputDecoder {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
        }
    }

    pub fn decode(&mut self, data: &Bytes, wal_end: u64) -> Vec<DbEvent> {
        let mut events = Vec::new();
        let mut p: &[u8] = &data[..];

        while !p.is_empty() {
            let tag = p[0];
            p = &p[1..];

            match tag {
                b'R' => {
                    if let Some(len) = self.parse_relation(p) {
                        p = &p[len..];
                    } else {
                        break;
                    }
                }
                b'I' => {
                    match self.parse_insert(p, wal_end) {
                        Ok((event, consumed)) => {
                            events.push(event);
                            p = &p[consumed..];
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to parse Insert message");
                            break;
                        }
                    }
                }
                b'U' => {
                    match self.parse_update(p, wal_end) {
                        Ok((event, consumed)) => {
                            events.push(event);
                            p = &p[consumed..];
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to parse Update message");
                            break;
                        }
                    }
                }
                b'D' => {
                    match self.parse_delete(p, wal_end) {
                        Ok((event, consumed)) => {
                            events.push(event);
                            p = &p[consumed..];
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to parse Delete message");
                            break;
                        }
                    }
                }
                b'Y' => {
                    // Type message - skip: flags(u8), oid(u32), namespace(null-str), name(null-str)
                    let mut offset = 0;
                    if p.len() < 5 { break; }
                    offset += 1 + 4; // flags + oid
                    offset += null_str_len(&p[offset..]);
                    offset += null_str_len(&p[offset..]);
                    p = &p[offset..];
                }
                b'O' => {
                    // Origin message - skip: lsn(u8*8=64bit), name(null-str)
                    let mut offset = 8;
                    offset += null_str_len(&p[offset..]);
                    p = &p[offset..];
                }
                b'T' => {
                    // Truncate message - skip for now
                    let mut offset = 0;
                    if p.len() < 5 { break; }
                    offset += 4; // rel_id
                    offset += 1; // flags
                    offset += 4; // n_rels
                    p = &p[offset..];
                }
                b'B' | b'C' | b'M' => {
                    // Begin/Commit/Message should already be parsed by pgwire-replication
                    // but handle gracefully if they appear in XLogData
                    warn!(tag = %(tag as char), "Unexpected boundary message in XLogData data");
                    break;
                }
                _ => {
                    warn!(tag = %tag, "Unknown pgoutput message tag");
                    break;
                }
            }
        }

        events
    }

    fn parse_relation(&mut self, data: &[u8]) -> Option<usize> {
        let mut offset = 0;

        if data.len() < 8 { return None; }
        let rel_id = u32::from_be_bytes(data[offset..offset + 4].try_into().ok()?);
        offset += 4;

        let ns_len = null_str_len(&data[offset..]);
        if ns_len == 0 { return None; }
        offset += ns_len;

        let name_len = null_str_len(&data[offset..]);
        if name_len == 0 { return None; }
        let name = String::from_utf8_lossy(&data[offset..offset + name_len - 1]).to_string();
        offset += name_len;

        if data.len() < offset + 1 { return None; }
        let _replica_identity = data[offset];
        offset += 1;

        if data.len() < offset + 2 { return None; }
        let n_cols = u16::from_be_bytes(data[offset..offset + 2].try_into().ok()?);
        offset += 2;

        let mut columns = Vec::with_capacity(n_cols as usize);
        for _ in 0..n_cols {
            if data.len() < offset + 1 { return None; }
            let _col_flags = data[offset];
            offset += 1;

            let col_name_len = null_str_len(&data[offset..]);
            if col_name_len == 0 { return None; }
            let col_name =
                String::from_utf8_lossy(&data[offset..offset + col_name_len - 1]).to_string();
            offset += col_name_len;

            if data.len() < offset + 6 { return None; }
            let _type_oid = u32::from_be_bytes(data[offset..offset + 4].try_into().ok()?);
            offset += 4;
            let _type_mod = i32::from_be_bytes(data[offset..offset + 4].try_into().ok()?);
            offset += 4;

            columns.push(ColumnMeta { name: col_name });
        }

        self.relations.insert(
            rel_id,
            RelationMeta {
                name,
                columns,
            },
        );

        Some(offset)
    }

    fn parse_insert(&self, data: &[u8], wal_end: u64) -> Result<(DbEvent, usize), String> {
        let mut offset = 0;
        if data.len() < 4 {
            return Err("Insert: truncated rel_id".into());
        }
        let rel_id = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // PG15+ pgoutput protocol adds 'N' prefix before new tuple
        if data.len() > offset && data[offset] == b'N' {
            offset += 1;
        }

        let (new_row, consumed) = self.parse_tuple_data(&data[offset..], rel_id)?;
        offset += consumed;

        let table_name = self.table_name(rel_id);
        Ok((
            DbEvent {
                source_offset: wal_end.to_string(),
                source_kind: SourceKind::Postgres,
                table_name,
                op_type: OpType::Insert,
                old_row: None,
                new_row: Some(new_row),
            },
            offset,
        ))
    }

    fn parse_update(&self, data: &[u8], wal_end: u64) -> Result<(DbEvent, usize), String> {
        let mut offset = 0;
        if data.len() < 4 {
            return Err("Update: truncated rel_id".into());
        }
        let rel_id = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let (old_row, _consumed) = if data.len() > offset {
            match data[offset] {
                b'O' | b'K' => {
                    offset += 1;
                    let (row, c) = self.parse_tuple_data(&data[offset..], rel_id)?;
                    offset += c;
                    (Some(row), c)
                }
                _ => (None, 0),
            }
        } else {
            (None, 0)
        };

        // PG15+ pgoutput protocol adds 'N' prefix before new tuple
        if data.len() > offset && data[offset] == b'N' {
            offset += 1;
        }

        let (new_row, consumed) = self.parse_tuple_data(&data[offset..], rel_id)?;
        offset += consumed;

        let table_name = self.table_name(rel_id);
        Ok((
            DbEvent {
                source_offset: wal_end.to_string(),
                source_kind: SourceKind::Postgres,
                table_name,
                op_type: OpType::Update,
                old_row,
                new_row: Some(new_row),
            },
            offset,
        ))
    }

    fn parse_delete(&self, data: &[u8], wal_end: u64) -> Result<(DbEvent, usize), String> {
        let mut offset = 0;
        if data.len() < 4 {
            return Err("Delete: truncated rel_id".into());
        }
        let rel_id = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let (old_row, _consumed) = if data.len() > offset {
            match data[offset] {
                b'O' | b'K' => {
                    offset += 1;
                    let (row, c) = self.parse_tuple_data(&data[offset..], rel_id)?;
                    offset += c;
                    (Some(row), c)
                }
                b'N' | b'n' => {
                    offset += 1;
                    (None, 0)
                }
                _ => (None, 0),
            }
        } else {
            (None, 0)
        };
        let table_name = self.table_name(rel_id);
        Ok((
            DbEvent {
                source_offset: wal_end.to_string(),
                source_kind: SourceKind::Postgres,
                table_name,
                op_type: OpType::Delete,
                old_row,
                new_row: None,
            },
            offset,
        ))
    }

    fn parse_tuple_data(
        &self,
        data: &[u8],
        rel_id: u32,
    ) -> Result<(serde_json::Value, usize), String> {
        if data.len() < 2 {
            return Err("TupleData: truncated n_cols".into());
        }
        let n_cols = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
        let mut offset = 2;

        let relation = self.relations.get(&rel_id);

        let mut map = serde_json::Map::new();
        for i in 0..n_cols {
            if data.len() <= offset {
                return Err("TupleData: truncated column".into());
            }
            let col_type = data[offset];
            offset += 1;

            let value = match col_type {
                b'n' | b'u' => serde_json::Value::Null,
                b't' => {
                    if data.len() < offset + 4 {
                        return Err("TupleData: truncated text length".into());
                    }
                    let len =
                        u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if data.len() < offset + len {
                        return Err("TupleData: truncated text data".into());
                    }
                    let s = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
                    offset += len;
                    serde_json::Value::String(s)
                }
                b'b' => {
                    if data.len() < offset + 4 {
                        return Err("TupleData: truncated binary length".into());
                    }
                    let len =
                        u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    offset += len;
                    serde_json::Value::Null
                }
                _ => {
                    return Err(format!("TupleData: unknown column type '{}'", col_type as char));
                }
            };

            let col_name = relation
                .and_then(|r| r.columns.get(i))
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("col_{}", i));

            map.insert(col_name, value);
        }

        Ok((serde_json::Value::Object(map), offset))
    }

    fn table_name(&self, rel_id: u32) -> String {
        self.relations
            .get(&rel_id)
            .map(|r| r.name.clone())
            .unwrap_or_else(|| format!("relation_{}", rel_id))
    }
}

fn null_str_len(data: &[u8]) -> usize {
    data.iter()
        .position(|&b| b == 0)
        .map(|pos| pos + 1)
        .unwrap_or(data.len())
}

#[cfg(test)]
fn build_relation_bytes(
    rel_id: u32,
    namespace: &str,
    name: &str,
    columns: &[(&str, u32, i32)],
) -> Vec<u8> {
    let mut buf = vec![b'R'];
    buf.extend_from_slice(&rel_id.to_be_bytes());
    buf.extend_from_slice(namespace.as_bytes());
    buf.push(0);
    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
    buf.push(0);
    buf.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for (col_name, type_oid, type_mod) in columns {
        buf.push(0);
        buf.extend_from_slice(col_name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&type_oid.to_be_bytes());
        buf.extend_from_slice(&type_mod.to_be_bytes());
    }
    buf
}

#[cfg(test)]
fn build_tuple_bytes(values: &[Option<&str>]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for val in values {
        match val {
            Some(s) => {
                buf.push(b't');
                buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            None => {
                buf.push(b'n');
            }
        }
    }
    buf
}

#[cfg(test)]
fn build_insert_bytes(rel_id: u32, values: &[Option<&str>]) -> Vec<u8> {
    let mut buf = vec![b'I'];
    buf.extend_from_slice(&rel_id.to_be_bytes());
    buf.push(b'N');
    buf.extend_from_slice(&build_tuple_bytes(values));
    buf
}

#[cfg(test)]
fn build_update_bytes(
    rel_id: u32,
    old_values: Option<&[Option<&str>]>,
    new_values: &[Option<&str>],
) -> Vec<u8> {
    let mut buf = vec![b'U'];
    buf.extend_from_slice(&rel_id.to_be_bytes());
    if let Some(old) = old_values {
        buf.push(b'O');
        buf.extend_from_slice(&build_tuple_bytes(old));
    }
    buf.push(b'N');
    buf.extend_from_slice(&build_tuple_bytes(new_values));
    buf
}

#[cfg(test)]
fn build_delete_bytes(rel_id: u32, old_values: Option<&[Option<&str>]>) -> Vec<u8> {
    let mut buf = vec![b'D'];
    buf.extend_from_slice(&rel_id.to_be_bytes());
    match old_values {
        Some(old) => {
            buf.push(b'O');
            buf.extend_from_slice(&build_tuple_bytes(old));
        }
        None => {
            buf.push(b'N');
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users_columns() -> Vec<(&'static str, u32, i32)> {
        vec![("id", 23, -1), ("name", 25, -1), ("email", 25, -1)]
    }

    fn setup_decoder() -> PgoutputDecoder {
        let mut d = PgoutputDecoder::new();
        d.parse_relation(&build_relation_bytes(1, "public", "users", &users_columns())[1..]);
        d
    }

    #[test]
    fn test_parse_insert() {
        let mut decoder = PgoutputDecoder::new();
        let rel = build_relation_bytes(1, "public", "users", &users_columns());
        let data = Bytes::from([rel.as_slice(), &build_insert_bytes(1, &[Some("1"), Some("Alice"), Some("alice@example.com")])].concat());
        let events = decoder.decode(&data, 100);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op_type, OpType::Insert);
        assert_eq!(events[0].table_name, "users");
        assert_eq!(events[0].source_offset, "100");
        let new = events[0].new_row.as_ref().unwrap();
        assert_eq!(new.get("id").and_then(|v| v.as_str()), Some("1"));
        assert_eq!(new.get("name").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(new.get("email").and_then(|v| v.as_str()), Some("alice@example.com"));
        assert!(events[0].old_row.is_none());
    }

    #[test]
    fn test_parse_update_with_old() {
        let mut decoder = setup_decoder();
        let data = Bytes::from(build_update_bytes(
            1,
            Some(&[Some("1"), Some("Alice"), Some("alice@example.com")]),
            &[Some("1"), Some("Alice"), Some("alice_new@example.com")],
        ));
        let events = decoder.decode(&data, 200);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op_type, OpType::Update);
        let old = events[0].old_row.as_ref().unwrap();
        assert_eq!(old.get("email").and_then(|v| v.as_str()), Some("alice@example.com"));
        let new = events[0].new_row.as_ref().unwrap();
        assert_eq!(new.get("email").and_then(|v| v.as_str()), Some("alice_new@example.com"));
    }

    #[test]
    fn test_parse_update_without_old() {
        let mut decoder = setup_decoder();
        let data = Bytes::from(build_update_bytes(1, None, &[Some("1"), Some("Bob"), Some("bob@example.com")]));
        let events = decoder.decode(&data, 300);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op_type, OpType::Update);
        assert!(events[0].old_row.is_none());
    }

    #[test]
    fn test_parse_delete_with_old() {
        let mut decoder = setup_decoder();
        let data = Bytes::from(build_delete_bytes(1, Some(&[Some("42"), Some("Charlie"), Some("charlie@example.com")])));
        let events = decoder.decode(&data, 400);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op_type, OpType::Delete);
        assert!(events[0].old_row.is_some());
        assert!(events[0].new_row.is_none());
    }

    #[test]
    fn test_parse_delete_without_old() {
        let mut decoder = setup_decoder();
        let data = Bytes::from(build_delete_bytes(1, None));
        let events = decoder.decode(&data, 500);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op_type, OpType::Delete);
        assert!(events[0].old_row.is_none());
        assert!(events[0].new_row.is_none());
    }

    #[test]
    fn test_multiple_events_in_one_batch() {
        let mut decoder = PgoutputDecoder::new();
        let rel = build_relation_bytes(2, "public", "orders", &[("id", 23, -1), ("total", 25, -1)]);
        let ins1 = build_insert_bytes(2, &[Some("1"), Some("49.99")]);
        let ins2 = build_insert_bytes(2, &[Some("2"), Some("129.00")]);
        let data = Bytes::from([rel.as_slice(), ins1.as_slice(), ins2.as_slice()].concat());
        let events = decoder.decode(&data, 600);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].table_name, "orders");
        assert_eq!(events[1].table_name, "orders");
    }

    #[test]
    fn test_null_values() {
        let mut decoder = setup_decoder();
        let data = Bytes::from(build_insert_bytes(1, &[Some("3"), None, Some("nullname@example.com")]));
        let events = decoder.decode(&data, 700);
        assert_eq!(events.len(), 1);
        let new = events[0].new_row.as_ref().unwrap();
        assert_eq!(new.get("name"), Some(&serde_json::Value::Null));
        assert_eq!(new.get("email").and_then(|v| v.as_str()), Some("nullname@example.com"));
    }

    #[test]
    fn test_unknown_relation_uses_fallback_name() {
        let mut decoder = PgoutputDecoder::new();
        let data = Bytes::from(build_insert_bytes(99, &[Some("x")]));
        let events = decoder.decode(&data, 800);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].table_name, "relation_99");
    }

    #[test]
    fn test_skip_type_message() {
        let mut decoder = setup_decoder();
        // Type: flags(u8) + oid(u32) + namespace\0 + name\0
        let mut type_msg = vec![b'Y', 0, 0, 0, 0, 1]; // flags=0, oid=1
        type_msg.extend_from_slice(b"public\0");
        type_msg.extend_from_slice(b"custom_type\0");
        // After it, an insert
        let ins = build_insert_bytes(1, &[Some("1"), Some("A"), Some("a@b.com")]);
        let data = Bytes::from([type_msg, ins].concat());
        let events = decoder.decode(&data, 900);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_empty_data_returns_no_events() {
        let mut decoder = PgoutputDecoder::new();
        let data = Bytes::new();
        let events = decoder.decode(&data, 0);
        assert!(events.is_empty());
    }

    #[test]
    fn test_builders_are_consistent() {
        let rel = build_relation_bytes(1, "s", "t", &[("c", 23, -1)]);
        assert_eq!(rel[0], b'R');
        let ins = build_insert_bytes(1, &[Some("v")]);
        assert_eq!(ins[0], b'I');
        let upd = build_update_bytes(1, Some(&[Some("v")]), &[Some("v2")]);
        assert_eq!(upd[0], b'U');
        let del = build_delete_bytes(1, None);
        assert_eq!(del[0], b'D');
    }
}

