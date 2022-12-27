pub type CorrelationId = u64;

pub const RESERVED_CORRELATION_ID: CorrelationId = 0;

pub trait CorrelationIdGenerator {
    fn generate(&self) -> CorrelationId;
}

