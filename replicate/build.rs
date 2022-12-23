fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute("replication.net.connect.HeartbeatRequest", "#[replicate_macro::add_correlation_id]")
        .compile(&["src/net/connect/proto/heartbeat.proto"], &["src/net/connect/proto/"])
        .unwrap();
    Ok(())
}