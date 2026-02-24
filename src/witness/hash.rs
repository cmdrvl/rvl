use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::sync::OnceLock;

/// BLAKE3 hash of an in-memory byte slice. Returns hex-encoded hash string
/// (no `blake3:` prefix — callers add the prefix when building records).
pub fn hash_bytes(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

/// Streaming BLAKE3 hash of a file. Uses 16KB buffer reads to avoid loading
/// the entire file into memory. Returns hex-encoded hash string.
pub fn hash_file(path: &Path) -> io::Result<String> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// BLAKE3 hash of the running binary. Computed once and cached for the
/// lifetime of the process via `OnceLock`.
pub fn hash_self() -> io::Result<String> {
    static SELF_HASH: OnceLock<String> = OnceLock::new();
    if let Some(cached) = SELF_HASH.get() {
        return Ok(cached.clone());
    }
    let exe = std::env::current_exe()?;
    let h = hash_file(&exe)?;
    Ok(SELF_HASH.get_or_init(|| h).clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn hash_bytes_known_value() {
        // BLAKE3 of b"hello" is a well-known constant.
        let got = hash_bytes(b"hello");
        let expected = blake3::hash(b"hello").to_hex().to_string();
        assert_eq!(got, expected);
        assert_eq!(got.len(), 64, "BLAKE3 hex hash should be 64 chars");
    }

    #[test]
    fn hash_bytes_empty() {
        let got = hash_bytes(b"");
        let expected = blake3::hash(b"").to_hex().to_string();
        assert_eq!(got, expected);
    }

    #[test]
    fn hash_bytes_deterministic() {
        let a = hash_bytes(b"determinism test");
        let b = hash_bytes(b"determinism test");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_bytes_different_inputs_differ() {
        let a = hash_bytes(b"hello");
        let b = hash_bytes(b"world");
        assert_ne!(a, b);
    }

    #[test]
    fn hash_file_matches_hash_bytes() {
        let dir = std::env::temp_dir().join("rvl_test_hash_file");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test_file.bin");
        let content = b"hash file test content";
        std::fs::write(&path, content).unwrap();

        let file_hash = hash_file(&path).unwrap();
        let bytes_hash = hash_bytes(content);
        assert_eq!(file_hash, bytes_hash);

        std::fs::remove_file(&path).ok();
        std::fs::remove_dir(&dir).ok();
    }

    #[test]
    fn hash_file_empty_file() {
        let dir = std::env::temp_dir().join("rvl_test_hash_empty");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.bin");
        std::fs::write(&path, b"").unwrap();

        let file_hash = hash_file(&path).unwrap();
        let bytes_hash = hash_bytes(b"");
        assert_eq!(file_hash, bytes_hash);

        std::fs::remove_file(&path).ok();
        std::fs::remove_dir(&dir).ok();
    }

    #[test]
    fn hash_file_large_content() {
        // Test with content larger than the 16KB buffer to exercise streaming.
        let dir = std::env::temp_dir().join("rvl_test_hash_large");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("large.bin");
        let content: Vec<u8> = (0..50_000).map(|i| (i % 256) as u8).collect();
        std::fs::write(&path, &content).unwrap();

        let file_hash = hash_file(&path).unwrap();
        let bytes_hash = hash_bytes(&content);
        assert_eq!(file_hash, bytes_hash);

        std::fs::remove_file(&path).ok();
        std::fs::remove_dir(&dir).ok();
    }

    #[test]
    fn hash_file_nonexistent_returns_error() {
        let result = hash_file(Path::new("/nonexistent/path/to/file.bin"));
        assert!(result.is_err());
    }

    #[test]
    fn hash_self_returns_nonempty() {
        let h = hash_self().unwrap();
        assert!(!h.is_empty());
        assert_eq!(h.len(), 64, "BLAKE3 hex hash should be 64 chars");
    }

    #[test]
    fn hash_self_is_stable() {
        let a = hash_self().unwrap();
        let b = hash_self().unwrap();
        assert_eq!(a, b, "hash_self should return the same value across calls");
    }

    #[test]
    fn hash_has_no_prefix() {
        let h = hash_bytes(b"test");
        assert!(
            !h.starts_with("blake3:"),
            "hash functions should NOT include a prefix"
        );
        assert!(
            h.chars().all(|c| c.is_ascii_hexdigit()),
            "hash should be pure hex"
        );
    }

    #[test]
    fn hash_file_streaming_correctness() {
        // Write content in multiple chunks to verify streaming reads work.
        let dir = std::env::temp_dir().join("rvl_test_hash_chunks");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("chunks.bin");
        {
            let mut f = File::create(&path).unwrap();
            for i in 0..100 {
                writeln!(f, "chunk number {} with some padding data", i).unwrap();
            }
            f.flush().unwrap();
        }

        let file_hash = hash_file(&path).unwrap();
        let content = std::fs::read(&path).unwrap();
        let bytes_hash = hash_bytes(&content);
        assert_eq!(file_hash, bytes_hash);

        std::fs::remove_file(&path).ok();
        std::fs::remove_dir(&dir).ok();
    }
}
