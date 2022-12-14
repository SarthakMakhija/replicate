fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute("examples.quorum.CorrelatingGetValueByKeyRequest", "#[raft_macro::add_correlation_id]")
        .type_attribute("examples.quorum.GetValueByKeyResponse", "#[raft_macro::add_correlation_id]")
        .compile(&["src/quorum/proto/quorum.proto"], &["src/quorum/proto/"])
        .unwrap();
    Ok(())
}