use std::ops::Range;
use rand::distributions::uniform::SampleRange;
use rand::prelude::*;

pub type CorrelationId = u64;

pub struct CorrelationIdGenerator {
    thread_local_generator: ThreadRng,
}

impl CorrelationIdGenerator {
    const FIXED_CORRELATION_ID: CorrelationId = 100;

    pub fn new() -> CorrelationIdGenerator {
        let mut thread_local_generator = thread_rng();
        return CorrelationIdGenerator {
            thread_local_generator
        };
    }

    pub fn generate(&mut self) -> CorrelationId {
        return self.thread_local_generator.gen::<CorrelationId>();
    }

    pub fn generate_in_range<R: SampleRange<CorrelationId>>(&mut self, range: R) -> CorrelationId {
        return self.thread_local_generator.gen_range(range);
    }

    pub(crate) fn fixed() -> CorrelationId {
        return Self::FIXED_CORRELATION_ID;
    }
}

#[cfg(test)]
mod tests {
    use crate::net::connect::correlation_id::CorrelationIdGenerator;

    #[test]
    fn generate_correlation_id_in_exclusive_range() {
        let mut generator = CorrelationIdGenerator::new();
        let correlation_id = generator.generate_in_range(1..10);
        assert!(correlation_id >= 1 && correlation_id < 10);
    }

    #[test]
    fn generate_correlation_id_in_inclusive_range() {
        let mut generator = CorrelationIdGenerator::new();
        let correlation_id = generator.generate_in_range(1..=10);
        assert!(correlation_id >= 1 && correlation_id <= 10);
    }
}