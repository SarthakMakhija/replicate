use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::time;

use crate::heartbeat::heartbeat_sender::HeartbeatSenderType;

pub struct HeartbeatScheduler {
    sender: HeartbeatSenderType,
    interval: Duration,
    keep_running: Arc<AtomicBool>,
}

impl HeartbeatScheduler {
    pub fn new(sender: HeartbeatSenderType, heartbeat_interval: Duration) -> HeartbeatScheduler {
        return HeartbeatScheduler {
            sender,
            interval: heartbeat_interval,
            keep_running: Arc::new(AtomicBool::new(true)),
        };
    }

    pub fn start(&self) {
        let heartbeat_sender = self.sender.clone();
        let keep_running = self.keep_running.clone();
        let mut interval = time::interval(self.interval);

        tokio::spawn(async move {
            loop {
                if !keep_running.load(Ordering::SeqCst) {
                    return;
                }
                let _ = heartbeat_sender.send_heartbeat().await;
                interval.tick().await;
            }
        });
    }

    pub fn stop(&mut self) {
        self.keep_running.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::thread;
    use std::time::Duration;

    use crate::heartbeat::heartbeat_scheduler::HeartbeatScheduler;
    use crate::heartbeat::heartbeat_scheduler::tests::setup::HeartbeatCounter;

    mod setup {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU16, Ordering};

        use async_trait::async_trait;

        use crate::heartbeat::heartbeat_sender::HeartbeatSender;
        use crate::net::connect::service_client::ServiceResponseError;

        pub struct HeartbeatCounter {
            pub counter: Arc<AtomicU16>,
        }

        #[async_trait]
        impl HeartbeatSender for HeartbeatCounter {
            async fn send_heartbeat(&self) -> Result<(), ServiceResponseError> {
                self.counter.clone().fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn start_heartbeat_scheduler() {
        let heartbeat_counter = HeartbeatCounter { counter: Arc::new(AtomicU16::new(0)) };
        let heartbeat_sender = Arc::new(heartbeat_counter);

        let mut heartbeat_scheduler = HeartbeatScheduler::new(
            heartbeat_sender.clone(),
            Duration::from_millis(2),
        );

        heartbeat_scheduler.start();
        thread::sleep(Duration::from_millis(5));

        heartbeat_scheduler.stop();

        let heartbeat_sender_cloned = heartbeat_sender.clone();
        let count = heartbeat_sender_cloned.counter.clone().load(Ordering::SeqCst);

        assert!(count >= 2);
    }
}