use std::time::{SystemTime, UNIX_EPOCH};

pub struct SystemClock {}

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;

    fn now_seconds(&self) -> u64 {
        return self.now().duration_since(UNIX_EPOCH).unwrap().as_secs();
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