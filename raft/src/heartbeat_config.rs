use std::time::Duration;

use rand::{Rng, thread_rng};

pub struct HeartbeatConfig {
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
}

impl HeartbeatConfig {
    const MAXIMUM_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
    const HEARTBEAT_TIMEOUT_RANGE_MS: std::ops::RangeInclusive<u32> = 150..=300;

    pub fn default() -> Self {
        return HeartbeatConfig::new(Duration::from_millis(50));
    }

    pub fn new(heartbeat_interval: Duration) -> Self {
        if heartbeat_interval.gt(&Self::MAXIMUM_HEARTBEAT_INTERVAL) {
            panic!(
                "heartbeat interval can not be greater than the maximum interval defined {:?}",
                Self::MAXIMUM_HEARTBEAT_INTERVAL
            );
        }
        return HeartbeatConfig {
            heartbeat_interval,
            heartbeat_timeout: Self::heartbeat_timeout(),
        };
    }

    pub fn get_heartbeat_interval(&self) -> Duration {
        return self.heartbeat_interval;
    }

    pub fn get_heartbeat_timeout(&self) -> Duration {
        return self.heartbeat_timeout;
    }

    fn heartbeat_timeout() -> Duration {
        return Duration::from_millis(
            u64::from(thread_rng().gen_range(Self::HEARTBEAT_TIMEOUT_RANGE_MS))
        );
    }
}

#[cfg(all(test, feature="test_type_unit"))]
mod tests {
    use std::time::Duration;

    use crate::heartbeat_config::HeartbeatConfig;

    #[test]
    #[should_panic]
    fn heartbeat_config_with_heartbeat_interval_greater_than_maximum() {
        HeartbeatConfig::new(Duration::from_millis(130));
    }

    #[test]
    fn heartbeat_config_heartbeat_timeout() {
        let heartbeat_config = HeartbeatConfig::new(Duration::from_millis(100));
        let duration = heartbeat_config.get_heartbeat_timeout();

        assert!(duration.ge(&Duration::from_millis(150)));
        assert!(duration.le(&Duration::from_millis(300)));
    }
}