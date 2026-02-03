use std::path::{Path, PathBuf};

pub fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

pub fn fixture_path(name: &str) -> PathBuf {
    fixtures_dir().join(name)
}

pub fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixture_path(name)).expect("fixture should be readable")
}
