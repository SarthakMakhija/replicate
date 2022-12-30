use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct SystemClock {}

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;

    fn now_seconds(&self) -> u64 {
        return self.now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    }

    fn duration_since(&self, time: SystemTime) -> Duration {
        return self.now().duration_since(time).unwrap();
    }
}

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        return SystemTime::now();
    }
}

impl SystemClock {
    pub fn new() -> SystemClock {
        return SystemClock {};
    }
}