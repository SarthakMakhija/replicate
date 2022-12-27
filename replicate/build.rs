fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute("replicate.tests.echo.EchoRequest", "#[replicate_macro::add_correlation_id]")
        .type_attribute("replicate.tests.echo.EchoResponse", "#[replicate_macro::add_correlation_id]")
        .compile(&["tests/proto/echo.proto"], &["tests/proto/"])
        .unwrap();
    Ok(())
}