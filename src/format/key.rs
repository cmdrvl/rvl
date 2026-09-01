use crate::format::ident_human::render_identifier_human;
use crate::format::ident_json::encode_identifier_json;

/// Render a key tuple for human-facing labels. The label is display-only;
/// machine consumers use the structured component array in JSON.
pub fn render_key_human(components: &[Vec<u8>]) -> String {
    components
        .iter()
        .map(|component| render_identifier_human(component))
        .collect::<Vec<_>>()
        .join(" + ")
}

/// Render the backward-compatible JSON label for a key tuple.
pub fn encode_key_label_json(components: &[Vec<u8>]) -> String {
    encode_key_components_json(components).join(" + ")
}

/// Encode each tuple component independently so composite keys remain
/// machine-readable and unambiguous.
pub fn encode_key_components_json(components: &[Vec<u8>]) -> Vec<String> {
    components
        .iter()
        .map(|component| encode_identifier_json(component))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_composite_keys_component_by_component() {
        let key = vec![b"A".to_vec(), vec![0xff]];
        assert_eq!(render_key_human(&key), "A + hex:ff");
        assert_eq!(encode_key_label_json(&key), "u8:A + hex:ff");
        assert_eq!(
            encode_key_components_json(&key),
            vec!["u8:A".to_string(), "hex:ff".to_string()]
        );
    }
}
