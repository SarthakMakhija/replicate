fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute("examples.quorum.CorrelatingGetValueByKeyRequest", "#[replicate_macro::add_correlation_id]")
        .type_attribute("examples.quorum.GetValueByKeyResponse", "#[replicate_macro::add_correlation_id]")
        .type_attribute("examples.quorum.VersionedPutKeyValueRequest", "#[replicate_macro::add_correlation_id]")
        .type_attribute("examples.quorum.PutKeyValueResponse", "#[replicate_macro::add_correlation_id]")
        .compile(&["src/quorum/proto/quorum.proto"], &["src/quorum/proto/"])
        .unwrap();
    Ok(())
}