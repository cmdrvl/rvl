//! Value markers for the zero-retention (non-explicit) `--non-numeric` output.
//!
//! Without `--explicit`, the categorical old/new values and the affected-row identities are
//! never printed. They are replaced by one of two markers:
//!
//! - **Ordinal refs (default).** `redacted:ref:v1`, `v2`, ... for cell values and
//!   `redacted:ref:k1`, `k2`, ... for row identities (key tuples, or row positions in row-order
//!   mode). Values and rows are separate namespaces, so a value and a key holding equal bytes do
//!   not share a ref. Numbers follow first appearance over the entries that are emitted, in output
//!   order, so the output is byte-identical when the same inputs are compared again. No hash of any
//!   value is computed, so nothing can be recovered by a dictionary. A ref reveals only which
//!   emitted entries share a value and how many distinct values there are.
//! - **Keyed markers (opt-in).** When `RVL_REDACTION_KEY` holds a key of at least 16 bytes, each
//!   value or row identity becomes `redacted:hmac-sha256:<32 hex>`: HMAC-SHA256 of its raw bytes
//!   (row identities: the length-framed key components), truncated to 128 bits. Stable across runs
//!   and inputs for the key holder, unguessable without the key. A set-but-empty or short key is
//!   refused (`E_REDACTION_KEY_INVALID`), never silently downgraded to ordinal refs.
//!
//! There is deliberately no random per-run salt: it would make the output non-reproducible.

use std::collections::BTreeMap;
use std::ffi::OsString;

use hmac::{Hmac, Mac};
use sha2::Sha256;

pub const ENV_KEY: &str = "RVL_REDACTION_KEY";
pub const MIN_KEY_BYTES: usize = 16;
pub const REF_PREFIX: &str = "redacted:ref:";
pub const HMAC_PREFIX: &str = "redacted:hmac-sha256:";

/// Why `RVL_REDACTION_KEY` was refused. Never carries the key or its length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    Empty,
    TooShort,
}

impl KeyError {
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::TooShort => "too_short",
        }
    }
}

/// An operator-held redaction key. `Debug` never prints the bytes.
#[derive(Clone)]
pub struct RedactionKey(Vec<u8>);

impl std::fmt::Debug for RedactionKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedactionKey(<withheld>)")
    }
}

impl RedactionKey {
    /// `None` when the variable is unset (ordinal refs). Set-but-empty and too-short values are
    /// errors, so a misconfigured key cannot quietly fall back.
    pub fn from_env_value(value: Option<OsString>) -> Result<Option<Self>, KeyError> {
        let Some(value) = value else {
            return Ok(None);
        };
        #[cfg(unix)]
        let bytes = std::os::unix::ffi::OsStringExt::into_vec(value);
        #[cfg(not(unix))]
        let bytes = value.to_string_lossy().into_owned().into_bytes();
        if bytes.is_empty() {
            return Err(KeyError::Empty);
        }
        if bytes.len() < MIN_KEY_BYTES {
            return Err(KeyError::TooShort);
        }
        Ok(Some(Self(bytes)))
    }

    pub fn from_env() -> Result<Option<Self>, KeyError> {
        Self::from_env_value(std::env::var_os(ENV_KEY))
    }

    /// `redacted:hmac-sha256:<32 hex>` over `bytes`.
    pub fn marker(&self, bytes: &[u8]) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC accepts a key of any length");
        mac.update(bytes);
        let digest = mac.finalize().into_bytes();
        let mut hex = String::with_capacity(32);
        for byte in &digest[..16] {
            hex.push_str(&format!("{byte:02x}"));
        }
        format!("{HMAC_PREFIX}{hex}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Namespace {
    /// A cell value.
    Value,
    /// A row identity (key tuple or row position).
    Row,
}

/// Turns raw bytes into markers for one output. Create one per report and mark entries in output order.
pub struct Marker<'a> {
    key: Option<&'a RedactionKey>,
    values: BTreeMap<Vec<u8>, usize>,
    rows: BTreeMap<Vec<u8>, usize>,
}

impl<'a> Marker<'a> {
    pub fn new(key: Option<&'a RedactionKey>) -> Self {
        Self {
            key,
            values: BTreeMap::new(),
            rows: BTreeMap::new(),
        }
    }

    pub fn mark(&mut self, namespace: Namespace, bytes: &[u8]) -> String {
        if let Some(key) = self.key {
            return key.marker(bytes);
        }
        let (table, letter) = match namespace {
            Namespace::Value => (&mut self.values, 'v'),
            Namespace::Row => (&mut self.rows, 'k'),
        };
        let next = table.len() + 1;
        let ordinal = *table.entry(bytes.to_vec()).or_insert(next);
        format!("{REF_PREFIX}{letter}{ordinal}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(text: &str) -> RedactionKey {
        RedactionKey::from_env_value(Some(text.into()))
            .expect("valid key")
            .expect("key present")
    }

    #[test]
    fn ordinals_follow_first_appearance_and_equal_bytes_share_a_ref() {
        let mut marker = Marker::new(None);
        assert_eq!(marker.mark(Namespace::Value, b"open"), "redacted:ref:v1");
        assert_eq!(marker.mark(Namespace::Value, b"closed"), "redacted:ref:v2");
        assert_eq!(marker.mark(Namespace::Value, b"open"), "redacted:ref:v1");
    }

    #[test]
    fn values_and_rows_are_separate_namespaces() {
        let mut marker = Marker::new(None);
        assert_eq!(marker.mark(Namespace::Value, b"same"), "redacted:ref:v1");
        assert_eq!(marker.mark(Namespace::Row, b"same"), "redacted:ref:k1");
        assert_eq!(marker.mark(Namespace::Row, b"other"), "redacted:ref:k2");
    }

    #[test]
    fn ordinal_mode_computes_no_hash_text() {
        let mut marker = Marker::new(None);
        let text = marker.mark(Namespace::Value, b"open");
        assert!(!text.contains("blake3") && !text.contains("sha256") && !text.contains("hmac"));
    }

    #[test]
    fn key_validation_refuses_empty_and_short_and_accepts_16() {
        assert!(RedactionKey::from_env_value(None).unwrap().is_none());
        assert_eq!(
            RedactionKey::from_env_value(Some("".into())).unwrap_err(),
            KeyError::Empty
        );
        assert_eq!(
            RedactionKey::from_env_value(Some("0123456789abcde".into())).unwrap_err(),
            KeyError::TooShort
        );
        assert!(
            RedactionKey::from_env_value(Some("0123456789abcdef".into()))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn keyed_marker_matches_the_independent_openssl_known_answer() {
        // Computed independently of this crate with:
        //   printf '%s' open   | openssl dgst -sha256 -hmac 0123456789abcdef
        //     b0dd9e34e5ee47920ba0efa2d9a3e5be1a1f3d763901d569352d4e4c933ae33a
        //   printf '%s' closed | openssl dgst -sha256 -hmac 0123456789abcdef
        //     1fa915cc3c47c712fd41242d017fb1cf2ae23d20b03cbf656ed8d653c5f4b4c2
        //   printf '%s' open   | openssl dgst -sha256 -hmac 0123456789abcdeg
        //     9b1afebd8ed22ea4c3541275ccc465e00fceaf9c20b1488609cbae73ed827eee
        // The marker is the first 32 hex (128 bits) of each digest.
        let key = key("0123456789abcdef");
        let marker = key.marker(b"open");
        assert_eq!(
            marker,
            "redacted:hmac-sha256:b0dd9e34e5ee47920ba0efa2d9a3e5be"
        );
        assert_eq!(
            key.marker(b"closed"),
            "redacted:hmac-sha256:1fa915cc3c47c712fd41242d017fb1cf"
        );
        assert_eq!(
            self::key("0123456789abcdeg").marker(b"open"),
            "redacted:hmac-sha256:9b1afebd8ed22ea4c3541275ccc465e0"
        );
        assert_eq!(marker, self::key("0123456789abcdef").marker(b"open"));
        assert!(!format!("{key:?}").contains("0123456789abcdef"));
    }

    #[test]
    fn keyed_mode_ignores_ordinals_and_is_stable_across_markers() {
        let key = key("0123456789abcdef");
        let mut first = Marker::new(Some(&key));
        let mut second = Marker::new(Some(&key));
        let a = first.mark(Namespace::Value, b"open");
        first.mark(Namespace::Value, b"closed");
        assert_eq!(a, second.mark(Namespace::Value, b"open"));
        assert!(a.starts_with(HMAC_PREFIX));
    }
}
