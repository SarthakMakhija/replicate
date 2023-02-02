use std::future::Future;
use std::pin::Pin;

use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;

pub type AsyncBlock = Pin<Box<dyn Future<Output=()> + Send + 'static>>;

pub(crate) struct Task {
    block: AsyncBlock,
}

pub trait ToAsyncBlock {
    fn async_block(self) -> AsyncBlock;
}

impl<T: Future<Output=()> + Send + 'static> ToAsyncBlock for T {
    fn async_block(self) -> AsyncBlock {
        return Box::pin(self);
    }
}

pub(crate) struct SingularUpdateQueue {
    sender: Sender<Task>,
    single_thread_pool: Runtime,
}

impl SingularUpdateQueue {
    pub(crate) fn new() -> SingularUpdateQueue {
        let single_thread_pool = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        //TODO: make 100 configurable
        let (sender, receiver) = mpsc::channel::<Task>(100);
        let singular_update_queue = SingularUpdateQueue {
            sender,
            single_thread_pool,
        };
        singular_update_queue.start(receiver);
        return singular_update_queue;
    }

    pub(crate) async fn add<F>(&self, handler: F) -> Result<(), SendError<Task>>
        where
            F: Future<Output=()> + Send + 'static {
        let block = handler.async_block();
        return self.sender.clone().send(Task { block }).await;
    }

    pub(crate) async fn submit(&self, handler: AsyncBlock) -> Result<(), SendError<Task>> {
        return self.sender.clone().send(Task { block: handler }).await;
    }

    pub(crate) fn shutdown(self) {
        let _ = self.single_thread_pool.shutdown_background();
    }

    fn start(&self, mut receiver: Receiver<Task>) {
        self.single_thread_pool.spawn(async move {
            while let Some(task) = receiver.recv().await {
                task.block.await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    use tokio::sync::mpsc;

    use crate::singular_update_queue::singular_update_queue::{SingularUpdateQueue, ToAsyncBlock};

    #[tokio::test]
    async fn get_with_insert_by_a_single_task() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let readable_storage = storage.clone();
        let singular_update_queue = SingularUpdateQueue::new();

        let (sender, mut receiver) = mpsc::channel(1);
        let _ = singular_update_queue.add(async move {
            storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
            sender.send(()).await.unwrap();
        }).await;

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

        let _ = singular_update_queue.add(async move {
            writable_storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
            sender_one.clone().send(()).await.unwrap();
        }).await;

        let _ = singular_update_queue.add(async move {
            storage.write().unwrap().insert("RAFT".to_string(), "consensus".to_string());
            sender_other.clone().send(()).await.unwrap();
        }).await;

        let _ = receiver_one.recv().await.unwrap();
        let _ = receiver_other.recv().await.unwrap();

        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());
        assert_eq!("consensus", read_storage.get("RAFT").unwrap());

        singular_update_queue.shutdown();
    }

    #[tokio::test]
    async fn submit_multiple_task_and_confirm_their_execution_in_order() {
        let storage = Arc::new(RwLock::new(Vec::new()));
        let writable_storage_one = storage.clone();
        let writable_storage_two = storage.clone();
        let readable_storage = storage.clone();
        let singular_update_queue = SingularUpdateQueue::new();

        let (sender_one, mut receiver_one) = mpsc::channel(1);
        let (sender_other, mut receiver_other) = mpsc::channel(1);

        let _ = singular_update_queue.add(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            writable_storage_one.write().unwrap().push("WAL".to_string());
            sender_one.clone().send(()).await.unwrap();
        }).await;

        let _ = singular_update_queue.add(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            writable_storage_two.write().unwrap().push("consensus".to_string());
            sender_other.clone().send(()).await.unwrap();
        }).await;

        let _ = receiver_one.recv().await.unwrap();
        let _ = receiver_other.recv().await.unwrap();

        let read_storage = readable_storage.read().unwrap();
        assert_eq!(vec!["WAL".to_string(), "consensus".to_string()], *read_storage);

        singular_update_queue.shutdown();
    }

    #[tokio::test]
    async fn submit_a_pinned_handler() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let readable_storage = storage.clone();
        let singular_update_queue = SingularUpdateQueue::new();

        let (sender, mut receiver) = mpsc::channel(1);
        let _ = singular_update_queue.submit(async move {
            storage.write().unwrap().insert("WAL".to_string(), "write-ahead log".to_string());
            sender.send(()).await.unwrap();
        }.async_block()).await;

        let _ = receiver.recv().await.unwrap();
        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());

        singular_update_queue.shutdown();
    }
}
