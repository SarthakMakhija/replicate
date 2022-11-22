use std::thread;
use std::time::Duration;

use crate::heartbeat::heartbeat_sender::HeartbeatSenderType;

pub struct HeartbeatScheduler {
    sender: HeartbeatSenderType,
    interval_ms: u64,
}

impl HeartbeatScheduler {
    pub fn new(sender: HeartbeatSenderType, heartbeat_interval_ms: u64) -> HeartbeatScheduler {
        return HeartbeatScheduler {
            sender,
            interval_ms: heartbeat_interval_ms,
        };
    }

    pub fn start(&self) {
        let heartbeat_sender = self.sender.clone();
        let interval_ms = Duration::from_millis(self.interval_ms);

        thread::spawn(move || {
            loop {
                heartbeat_sender.send();
                thread::sleep(interval_ms);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    mod setup {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        use crate::heartbeat::heartbeat_sender::HeartbeatSender;

        pub struct HeartbeatCounter {
            pub overwriting_counter: Arc<Mutex<HashMap<String, u64>>>,
        }

        impl HeartbeatSender for HeartbeatCounter {
            fn send(&self) {
                self.overwriting_counter.clone().lock().unwrap().insert(String::from("heartbeat"), 1);
            }
        }
    }

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use crate::heartbeat::heartbeat_scheduler::HeartbeatScheduler;
    use crate::heartbeat::heartbeat_scheduler::tests::setup::HeartbeatCounter;

    #[test]
    fn start_heartbeat_scheduler() {
        let heartbeat_counter = HeartbeatCounter { overwriting_counter: Arc::new(Mutex::new(HashMap::new())) };
        let heartbeat_sender = Arc::new(heartbeat_counter);
        let heartbeat_scheduler = HeartbeatScheduler::new(heartbeat_sender.clone(), 2);

        heartbeat_scheduler.start();
        thread::sleep(Duration::from_millis(5));

        let heartbeat_sender_cloned = heartbeat_sender.clone();
        let overwriting_counter = heartbeat_sender_cloned.overwriting_counter.lock().unwrap();
        let count = overwriting_counter.get(&String::from("heartbeat")).unwrap();

        assert_eq!(&1, count);
    }
}