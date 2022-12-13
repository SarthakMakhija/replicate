use std::future::Future;
use tokio::runtime::{Builder, Runtime};

pub(crate) struct SingularUpdateQueue {
    thread_pool: Runtime,
}

impl SingularUpdateQueue {
    pub(crate) fn new() -> SingularUpdateQueue {
        let thread_pool = Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();

        return SingularUpdateQueue { thread_pool };
    }

    pub(crate) fn submit<F>(&self, handler: F)
        where
            F: Future + Send + 'static,
            F::Output: Send + 'static { self.thread_pool.spawn(handler); }

    pub(crate) fn shutdown(self) {
        let _ = &self.thread_pool.shutdown_background();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use tokio::sync::mpsc;

    use crate::singular_update_queue::singular_update_queue::SingularUpdateQueue;

    #[tokio::test]
    async fn get_with_insert_by_a_single_task() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let readable_storage = storage.clone();
        let singular_update_queue = SingularUpdateQueue::new();

        let (sender, mut receiver) = mpsc::channel(1);
        singular_update_queue.submit(async move {
            storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
            sender.send(()).await.unwrap();
        });

        let _ = receiver.recv().await.unwrap();
        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());

        singular_update_queue.shutdown();
    }


    #[tokio::test]
    async fn get_with_insert_by_multiple_tasks() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let writable_storage = storage.clone();
        let readable_storage = storage.clone();
        let singular_update_queue = SingularUpdateQueue::new();

        let (sender_one, mut receiver_one) = mpsc::channel(1);
        let (sender_other, mut receiver_other) = mpsc::channel(1);

        singular_update_queue.submit(async move {
            writable_storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
            sender_one.clone().send(()).await.unwrap();
        });

        singular_update_queue.submit(async move {
            storage.write().unwrap().insert("RAFT".to_string(), "consensus".to_string());
            sender_other.clone().send(()).await.unwrap();
        });

        let _ = receiver_one.recv().await.unwrap();
        let _ = receiver_other.recv().await.unwrap();

        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());
        assert_eq!("consensus", read_storage.get("RAFT").unwrap());

        singular_update_queue.shutdown();
    }

}
