use rand::prelude::*;

pub type CorrelationId = u64;

pub struct RandomCorrelationIdGenerator {
    thread_local_generator: ThreadRng,
}

impl RandomCorrelationIdGenerator {

    pub fn new() -> RandomCorrelationIdGenerator {
        let thread_local_generator = thread_rng();
        return RandomCorrelationIdGenerator {
            thread_local_generator
        };
    }

    pub fn generate(&mut self) -> CorrelationId {
        return self.thread_local_generator.gen::<CorrelationId>();
    }
}

#[cfg(test)]
#[allow(unused_comparisons)]
mod tests {
    use crate::net::connect::correlation_id::RandomCorrelationIdGenerator;

    #[test]
    fn generate_correlation_id() {
        let mut generator = RandomCorrelationIdGenerator::new();
        let correlation_id = generator.generate();
        assert!(correlation_id >= 0);
    }
}