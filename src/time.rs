/// Gets the unix timestamp as a duration
pub fn unix_timestamp() -> std::time::Duration {
    let now = std::time::SystemTime::now();

    // NOTE: Expect is trivial
    #[allow(clippy::expect_used)]
    now.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("time went backwards")
}
