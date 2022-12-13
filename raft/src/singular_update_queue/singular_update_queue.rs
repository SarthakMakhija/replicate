use executors::Executor;
use executors::threadpool_executor::ThreadPoolExecutor;

pub struct SingularUpdateQueue {
    thread_pool_executor: ThreadPoolExecutor,
}

impl SingularUpdateQueue {
    fn new() -> SingularUpdateQueue {
        return SingularUpdateQueue { thread_pool_executor: ThreadPoolExecutor::new(1) };
    }

    fn submit<F>(&self, handler: F)
        where F: FnOnce() + Send + 'static {
        self.thread_pool_executor.execute(handler);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, mpsc, RwLock};
    use crate::singular_update_queue::singular_update_queue::SingularUpdateQueue;

    #[test]
    fn get_with_insert_by_a_single_task() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let readable_storage = storage.clone();
        let singular_update_queue = SingularUpdateQueue::new();

        let (sender, receiver) = mpsc::channel();
        singular_update_queue.submit(move || {
            storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
            sender.clone().send(()).unwrap();
        });

        let _ = receiver.recv().unwrap();
        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());
    }

    #[test]
    fn get_with_insert_by_multiple_tasks() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let writable_storage = storage.clone();
        let readable_storage = storage.clone();
        let singular_update_queue = SingularUpdateQueue::new();

        let (sender_one, receiver_one) = mpsc::channel();
        let (sender_other, receiver_other) = mpsc::channel();

        singular_update_queue.submit(move || {
            writable_storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
            sender_one.clone().send(()).unwrap();
        });

        singular_update_queue.submit(move || {
            storage.write().unwrap().insert("RAFT".to_string(), "consensus".to_string());
            sender_other.clone().send(()).unwrap();
        });

        let _ = receiver_one.recv().unwrap();
        let _ = receiver_other.recv().unwrap();

        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());
        assert_eq!("consensus", read_storage.get("RAFT").unwrap());
    }
}
