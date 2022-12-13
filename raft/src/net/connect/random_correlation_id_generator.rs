use rand::{Rng, thread_rng};
use crate::net::connect::correlation_id::{CorrelationId, CorrelationIdGenerator};

pub struct RandomCorrelationIdGenerator {}

impl CorrelationIdGenerator for RandomCorrelationIdGenerator {
    fn generate(&self) -> CorrelationId {
        return thread_rng().gen();
    }
}

impl RandomCorrelationIdGenerator {
    pub fn new() -> Self {
        return RandomCorrelationIdGenerator {};
    }
}

#[cfg(test)]
#[allow(unused_comparisons)]
mod tests {
    use crate::net::connect::correlation_id::CorrelationIdGenerator;
    use crate::net::connect::random_correlation_id_generator::RandomCorrelationIdGenerator;

    #[test]
    fn generate_correlation_id() {
        let generator = RandomCorrelationIdGenerator::new();
        let correlation_id = generator.generate();
        assert!(correlation_id >= 0);
    }
}