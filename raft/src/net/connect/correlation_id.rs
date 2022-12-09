use rand::distributions::Standard;
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

    pub fn generate<T>(&mut self) -> T
        where Standard: Distribution<T> {
        return self.thread_local_generator.gen::<T>();
    }
}

#[cfg(test)]
#[allow(unused_comparisons)]
mod tests {
    use crate::net::connect::correlation_id::{CorrelationIdGenerator, DefaultCorrelationIdType};

    #[test]
    fn generate_correlation_id_of_default_correlation_type() {
        let mut generator = CorrelationIdGenerator::new();
        let correlation_id = generator.generate::<DefaultCorrelationIdType>();
        assert!(correlation_id >= 0);
    }

    #[test]
    fn generate_correlation_id_of_u8() {
        let mut generator = CorrelationIdGenerator::new();
        let correlation_id = generator.generate::<u8>();
        assert!(correlation_id >= 0 && correlation_id <= 255);
    }
}