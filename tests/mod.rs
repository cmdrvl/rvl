mod helpers;

#[test]
fn fixtures_are_present_and_readable() {
    let old = helpers::read_fixture("basic_old.csv");
    let new = helpers::read_fixture("basic_new.csv");

    assert!(old.starts_with(b"id,value"));
    assert!(new.starts_with(b"id,value"));
    assert_ne!(old, new);
}
