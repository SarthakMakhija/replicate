use std::cell::RefCell;
use std::ops::DerefMut;

use rand::prelude::*;

pub type CorrelationId = u64;

pub struct RandomCorrelationIdGenerator {
    thread_local_generator: RefCell<ThreadRng>,
}

pub trait CorrelationIdGenerator {
    fn generate(&self) -> CorrelationId;
}

impl CorrelationIdGenerator for RandomCorrelationIdGenerator {
    fn generate(&self) -> CorrelationId {
        return self.thread_local_generator.borrow_mut().deref_mut().gen();
    }
}

impl RandomCorrelationIdGenerator {
    pub fn new() -> Self {
        let thread_local_generator = thread_rng();
        return RandomCorrelationIdGenerator {
            thread_local_generator: RefCell::new(thread_local_generator)
        };
    }
}

#[cfg(test)]
#[allow(unused_comparisons)]
mod tests {
    use crate::net::connect::correlation_id::{CorrelationIdGenerator, RandomCorrelationIdGenerator};

    #[test]
    fn generate_correlation_id() {
        let generator = RandomCorrelationIdGenerator::new();
        let correlation_id = generator.generate();
        assert!(correlation_id >= 0);
    }
}