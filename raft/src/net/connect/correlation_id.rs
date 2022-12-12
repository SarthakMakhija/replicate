use rand::prelude::*;

pub type DefaultCorrelationIdType = u64;

pub struct CorrelationIdGenerator {
    thread_local_generator: ThreadRng,
}

impl CorrelationIdGenerator {

    pub fn new() -> CorrelationIdGenerator {
        let thread_local_generator = thread_rng();
        return CorrelationIdGenerator {
            thread_local_generator
        };
    }

    pub fn generate(&mut self) -> DefaultCorrelationIdType {
        return self.thread_local_generator.gen::<DefaultCorrelationIdType>();
    }
}

#[cfg(test)]
#[allow(unused_comparisons)]
mod tests {
    use crate::net::connect::correlation_id::CorrelationIdGenerator;

    #[test]
    fn generate_correlation_id() {
        let mut generator = CorrelationIdGenerator::new();
        let correlation_id = generator.generate();
        assert!(correlation_id >= 0);
    }
}