//! Key-mode join & key-set validation (bd-3e0).
//!
//! Responsibilities:
//! - Build a key-tuple map from normalized records.
//! - Detect empty keys and duplicates.
//! - Compare key sets and surface mismatches.
//!
//! Memory note (v0):
//! - Key mode materializes one full side into a `HashMap<key_tuple, row>` before join.
//! - Peak RSS scales with key count and row width (roughly proportional to the
//!   loaded side plus HashMap overhead). Use row-order mode if RAM is tight.

use std::collections::HashMap;
use std::sync::Arc;

use crate::normalize::trim::ascii_trim;

pub type OwnedRecord = Vec<Vec<u8>>;
/// An exact, ordered tuple of ASCII-trimmed key component values.
///
/// Components remain separate so every possible byte sequence has an
/// unambiguous representation; delimiter-based flattening can collide.
pub type KeyValue = Arc<[Vec<u8>]>;

#[derive(Debug, Clone)]
pub struct KeyEntry {
    pub record_number: u64,
    pub fields: OwnedRecord,
}

#[derive(Debug, Clone)]
pub struct KeyMap {
    pub entries: HashMap<KeyValue, KeyEntry>,
}

#[derive(Debug, Clone)]
pub struct KeyAlignedRow {
    pub key: KeyValue,
    pub old: KeyEntry,
    pub new: KeyEntry,
}

/// Errors encountered in key-mode alignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyJoinError {
    EmptyKey {
        record_number: u64,
        component_index: usize,
    },
    DuplicateKey {
        key: KeyValue,
        first_record: u64,
        second_record: u64,
    },
    KeySetMismatch {
        missing_count: usize,
        extra_count: usize,
        missing_samples: Vec<KeyValue>,
        extra_samples: Vec<KeyValue>,
    },
}

/// Build a key-tuple map from normalized data records.
///
/// `records` should be data records only (header excluded) and already
/// normalized to header width. Every index is an ordered tuple component.
pub fn build_key_map<I>(records: I, key_indices: &[usize]) -> Result<KeyMap, KeyJoinError>
where
    I: IntoIterator<Item = (u64, OwnedRecord)>,
{
    debug_assert!(!key_indices.is_empty());
    let mut entries: HashMap<KeyValue, KeyEntry> = HashMap::new();
    for (record_number, record) in records.into_iter() {
        if is_blank_owned_record(&record) {
            continue;
        }
        let mut key = Vec::with_capacity(key_indices.len());
        for (component_index, &field_index) in key_indices.iter().enumerate() {
            let raw_component = record
                .get(field_index)
                .map(|value| value.as_slice())
                .unwrap_or(b"");
            let component = ascii_trim(raw_component);
            if component.is_empty() {
                return Err(KeyJoinError::EmptyKey {
                    record_number,
                    component_index,
                });
            }
            key.push(component.to_vec());
        }
        let key: KeyValue = key.into();
        if let Some(existing) = entries.get(&key) {
            return Err(KeyJoinError::DuplicateKey {
                key,
                first_record: existing.record_number,
                second_record: record_number,
            });
        }
        entries.insert(
            key,
            KeyEntry {
                record_number,
                fields: record,
            },
        );
    }
    Ok(KeyMap { entries })
}

/// Join two key maps by exact key match.
pub fn join_key_maps(old: KeyMap, new: KeyMap) -> Result<Vec<KeyAlignedRow>, KeyJoinError> {
    if let Some(mismatch) = compare_key_sets(&old.entries, &new.entries) {
        return Err(mismatch);
    }

    let mut keys: Vec<KeyValue> = old.entries.keys().cloned().collect();
    keys.sort();

    let mut old_entries = old.entries;
    let mut new_entries = new.entries;
    let mut aligned = Vec::with_capacity(keys.len());

    for key in keys {
        let old_entry = old_entries
            .remove(&key)
            .expect("key should exist in old map");
        let new_entry = new_entries
            .remove(&key)
            .expect("key should exist in new map");
        aligned.push(KeyAlignedRow {
            key,
            old: old_entry,
            new: new_entry,
        });
    }

    Ok(aligned)
}

fn compare_key_sets(
    old_entries: &HashMap<KeyValue, KeyEntry>,
    new_entries: &HashMap<KeyValue, KeyEntry>,
) -> Option<KeyJoinError> {
    let mut missing = Vec::new();
    let mut extra = Vec::new();

    for key in old_entries.keys() {
        if !new_entries.contains_key(key) {
            missing.push(key.clone());
        }
    }
    for key in new_entries.keys() {
        if !old_entries.contains_key(key) {
            extra.push(key.clone());
        }
    }

    if missing.is_empty() && extra.is_empty() {
        return None;
    }

    missing.sort();
    extra.sort();
    let missing_count = missing.len();
    let extra_count = extra.len();
    truncate_samples(&mut missing);
    truncate_samples(&mut extra);

    Some(KeyJoinError::KeySetMismatch {
        missing_count,
        extra_count,
        missing_samples: missing,
        extra_samples: extra,
    })
}

fn truncate_samples(samples: &mut Vec<KeyValue>) {
    const MAX_SAMPLES: usize = 10;
    if samples.len() > MAX_SAMPLES {
        samples.truncate(MAX_SAMPLES);
    }
}

fn is_blank_owned_record(record: &[Vec<u8>]) -> bool {
    record.iter().all(|field| ascii_trim(field).is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(fields: &[&[u8]]) -> OwnedRecord {
        fields.iter().map(|field| field.to_vec()).collect()
    }

    fn key(components: &[&[u8]]) -> KeyValue {
        components
            .iter()
            .map(|component| component.to_vec())
            .collect::<Vec<_>>()
            .into()
    }

    #[test]
    fn build_key_map_detects_empty_key() {
        let records = vec![(1, record(&[b"", b"1"]))];
        let err = build_key_map(records, &[0]).expect_err("empty key");
        assert_eq!(
            err,
            KeyJoinError::EmptyKey {
                record_number: 1,
                component_index: 0,
            }
        );
    }

    #[test]
    fn build_key_map_detects_duplicate_key() {
        let records = vec![(1, record(&[b"A", b"1"])), (2, record(&[b"A", b"2"]))];
        let err = build_key_map(records, &[0]).expect_err("duplicate");
        assert_eq!(
            err,
            KeyJoinError::DuplicateKey {
                key: key(&[b"A"]),
                first_record: 1,
                second_record: 2
            }
        );
    }

    #[test]
    fn build_key_map_skips_blank_records() {
        let records = vec![(1, record(&[b"", b""])), (2, record(&[b"A", b"1"]))];
        let map = build_key_map(records, &[0]).expect("map");
        assert_eq!(map.entries.len(), 1);
        assert!(map.entries.contains_key(&key(&[b"A"])));
    }

    #[test]
    fn join_key_maps_reports_mismatch() {
        let old = build_key_map(vec![(1, record(&[b"A"])), (2, record(&[b"B"]))], &[0]).unwrap();
        let new = build_key_map(vec![(1, record(&[b"A"])), (2, record(&[b"C"]))], &[0]).unwrap();
        let err = join_key_maps(old, new).expect_err("mismatch");
        assert_eq!(
            err,
            KeyJoinError::KeySetMismatch {
                missing_count: 1,
                extra_count: 1,
                missing_samples: vec![key(&[b"B"])],
                extra_samples: vec![key(&[b"C"])],
            }
        );
    }

    #[test]
    fn join_key_maps_orders_by_key_bytes() {
        let old = build_key_map(vec![(1, record(&[b"b"])), (2, record(&[b"a"]))], &[0]).unwrap();
        let new = build_key_map(vec![(1, record(&[b"b"])), (2, record(&[b"a"]))], &[0]).unwrap();
        let joined = join_key_maps(old, new).expect("joined");
        assert_eq!(joined.len(), 2);
        assert_eq!(joined[0].key, key(&[b"a"]));
        assert_eq!(joined[1].key, key(&[b"b"]));
    }

    #[test]
    fn composite_keys_keep_components_structural() {
        let records = vec![
            (1, record(&[b"a\xff", b"b", b"1"])),
            (2, record(&[b"a", b"\xffb", b"2"])),
        ];
        let map = build_key_map(records, &[0, 1]).expect("distinct tuple keys");
        assert_eq!(map.entries.len(), 2);
    }

    #[test]
    fn composite_key_requires_every_component() {
        let records = vec![(1, record(&[b"A", b"", b"1"]))];
        let err = build_key_map(records, &[0, 1]).expect_err("empty component");
        assert_eq!(
            err,
            KeyJoinError::EmptyKey {
                record_number: 1,
                component_index: 1,
            }
        );
    }
}
