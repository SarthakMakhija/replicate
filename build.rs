fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .compile(&["src/net/connect/proto/heartbeat.proto", "src/net/connect/proto/heartbeat.proto"], &["src/net/connect/proto/"])
        .unwrap();
    Ok(())
}