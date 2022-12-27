fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute("raft.election.RequestVote", "#[replicate_macro::add_correlation_id]")
        .type_attribute("raft.election.RequestVoteResponse", "#[replicate_macro::add_correlation_id]")
        .type_attribute("raft.election.AppendEntries", "#[replicate_macro::add_correlation_id]")
        .compile(&["src/net/proto/raft.proto"], &["src/net/proto/"])
        .unwrap();
    Ok(())
}