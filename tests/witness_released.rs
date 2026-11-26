//! Ledger lines written by the released rvl 0.9.2 binary must be reproduced byte for byte, with
//! the same record ids. One line has `"tolerance":0.0`, which RFC 8785
//! (`cmdrvl_core::jcs`) would write as `0`; one carries a column registry hash. Do not
//! regenerate the fixture from the code under test.

use rvl::witness::record::{WitnessRecord, canonical_json};

const RELEASED_LEDGER: &str = include_str!("fixtures/witness/released-0.9.2.jsonl");

#[test]
fn released_ledger_lines_and_ids_are_reproduced() {
    assert_eq!(RELEASED_LEDGER.lines().count(), 2);
    assert!(RELEASED_LEDGER.contains("\"tolerance\":0.0}"));
    assert!(RELEASED_LEDGER.contains("\"column_registry_hash\":\"blake3:"));
    for line in RELEASED_LEDGER.lines() {
        let mut record: WitnessRecord = serde_json::from_str(line).expect("released line");
        assert_eq!(canonical_json(&record), line);

        let released_id = record.id.clone();
        record.compute_id();
        assert_eq!(record.id, released_id);
    }
}
