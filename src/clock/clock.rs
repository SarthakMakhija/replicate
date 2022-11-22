use std::time::Instant;

pub(crate) struct SystemClock {}

pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        return Instant::now();
    }
}

impl SystemClock {
    pub fn new() -> SystemClock {
        return SystemClock {};
    }
}