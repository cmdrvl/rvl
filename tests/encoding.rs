mod helpers;

use rvl::csv::input::{EncodingIssue, guard_input_bytes};

#[test]
fn utf16_bom_refused() {
    let bytes = helpers::read_fixture("utf16le_bom.csv");
    assert_eq!(guard_input_bytes(&bytes), Err(EncodingIssue::Utf16Or32Bom));
}

#[test]
fn utf32_bom_refused() {
    let bytes = helpers::read_fixture("utf32be_bom.csv");
    assert_eq!(guard_input_bytes(&bytes), Err(EncodingIssue::Utf16Or32Bom));
}

#[test]
fn nul_in_first_8k_refused() {
    let bytes = helpers::read_fixture("nul_in_8k.csv");
    assert_eq!(guard_input_bytes(&bytes), Err(EncodingIssue::NulByte));
}
