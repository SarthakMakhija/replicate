use std::sync::{Arc, mpsc};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use crate::singular_update_queue::request::{ChanneledRequest, RequestHandler};

#[derive(Clone)]
pub struct SingularUpdateQueue<Request, Response>
    where Request: Send + 'static, Response: Send + 'static {
    sender: Sender<ChanneledRequest<Request, Response>>,
}

impl<Request, Response> SingularUpdateQueue<Request, Response>
    where Request: Send, Response: Send {
    pub fn init(handler: Arc<dyn RequestHandler<Request, Response>>) -> SingularUpdateQueue<Request, Response> {
        return SingularUpdateQueue::spin_receiver(handler);
    }

    pub fn submit(&self, request: Request) -> Receiver<Response> {
        let (sender, receiver) = mpsc::channel();
        let _ = self.sender.clone().send(ChanneledRequest::new(request, sender)).unwrap();
        return receiver;
    }

    fn spin_receiver(handler: Arc<dyn RequestHandler<Request, Response>>) -> SingularUpdateQueue<Request, Response> {
        let (sender, receiver): (Sender<ChanneledRequest<Request, Response>>, Receiver<ChanneledRequest<Request, Response>>) = mpsc::channel();
        let singular_update_queue = SingularUpdateQueue { sender };

        thread::spawn(move || {
            let handler_clone = handler.clone();
            for channeled_request in &receiver {
                Self::work_on(&handler_clone, channeled_request);
            }
        });
        return singular_update_queue;
    }

    fn work_on(handler: &Arc<dyn RequestHandler<Request, Response>>, channeled_request: ChanneledRequest<Request, Response>) {
        let request = channeled_request.request;
        channeled_request.respond_back.clone().send(handler.handle(request)).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use std::thread;

    use crate::singular_update_queue::singular_update_queue::SingularUpdateQueue;
    use crate::singular_update_queue::singular_update_queue::tests::setup::{InMemoryStorageHandler, PutKeyValueRequest, Status};

    mod setup {
        use std::collections::HashMap;
        use std::sync::{Arc, RwLock};

        use crate::singular_update_queue::singular_update_queue::RequestHandler;

        #[derive(Clone)]
        pub struct PutKeyValueRequest {
            pub key: String,
            pub value: String,
        }

        #[derive(Eq, PartialEq, Debug, Clone)]
        pub enum Status {
            Ok
        }

        pub type Storage = Arc<RwLock<HashMap<String, String>>>;

        pub struct InMemoryStorageHandler {
            pub storage: Storage,
        }

        impl RequestHandler<PutKeyValueRequest, Status> for InMemoryStorageHandler {
            fn handle(&self, request: PutKeyValueRequest) -> Status {
                self.storage.write().unwrap().insert(request.key, request.value);
                return Status::Ok;
            }
        }
    }

    #[test]
    fn get_with_insert_by_a_single_task() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let readable_storage = storage.clone();
        let handler = Arc::new(InMemoryStorageHandler { storage });
        let singular_update_queue = SingularUpdateQueue::<PutKeyValueRequest, Status>::init(handler.clone());

        let handle = thread::spawn(move || {
            let receiver = singular_update_queue.submit(PutKeyValueRequest {
                key: String::from("WAL"),
                value: String::from("write-ahead log"),
            });
            assert_eq!(Status::Ok, receiver.recv().unwrap());
        });

        let _ = handle.join();
        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());
    }

    #[test]
    fn get_with_insert_by_multiple_tasks() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let readable_storage = storage.clone();
        let handler = Arc::new(InMemoryStorageHandler { storage });
        let singular_update_queue = SingularUpdateQueue::<PutKeyValueRequest, Status>::init(handler.clone());

        let cloned_queue_one = singular_update_queue.clone();
        let cloned_queue_two = singular_update_queue.clone();

        let handle_one = thread::spawn(move || {
            let receiver = cloned_queue_one.submit(PutKeyValueRequest {
                key: String::from("WAL"),
                value: String::from("write-ahead log"),
            });
            assert_eq!(Status::Ok, receiver.recv().unwrap());
        });

        let handle_two = thread::spawn(move || {
            let receiver = cloned_queue_two.submit(PutKeyValueRequest {
                key: String::from("RAFT"),
                value: String::from("consensus"),
            });
            assert_eq!(Status::Ok, receiver.recv().unwrap());
        });

        let _ = handle_one.join();
        let _ = handle_two.join();

        let read_storage = readable_storage.read().unwrap();

        assert_eq!("write-ahead log", read_storage.get("WAL").unwrap());
        assert_eq!("consensus", read_storage.get("RAFT").unwrap());
    }
}
