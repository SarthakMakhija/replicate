use std::time::Instant;

pub(crate) struct SystemClock {}

pub(crate) trait Clock {
    fn now(&self) -> Instant;
}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        return Instant::now();
    }
}