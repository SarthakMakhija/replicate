use std::sync::Arc;

pub trait HeartbeatSender: Send + Sync {
    fn send(&self);
}

pub type HeartbeatSenderType = Arc<dyn HeartbeatSender>;