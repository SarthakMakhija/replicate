pub type CorrelationId = u64;

pub trait CorrelationIdGenerator {
    fn generate(&self) -> CorrelationId;
}
