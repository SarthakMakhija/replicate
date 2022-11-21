use std::collections::HashMap;
use std::sync::{Arc, mpsc, RwLock};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::Duration;

type Storage = Arc<RwLock<HashMap<String, String>>>;

#[derive(Clone)]
struct SingularUpdateQueue {
    sender: Sender<Command>,
}

#[derive(Debug)]
enum Command {
    Put {
        key: String,
        value: String,
        respond_back: Sender<Status>,
    },
    Delete {
        key: String,
        respond_back: Sender<Status>,
    },
}

#[derive(Debug, Eq, PartialEq)]
enum Status {
    Ok
}

impl SingularUpdateQueue {
    pub fn init(storage: Storage) -> SingularUpdateQueue {
        return SingularUpdateQueue::spin_receiver(storage);
    }

    pub fn execute(&self, command: Command) {
        return self.sender.clone().send(command).unwrap();
    }

    fn spin_receiver(storage: Storage) -> SingularUpdateQueue {
        let (sender, receiver): (Sender<Command>, Receiver<Command>) = mpsc::channel();
        let singular_update_queue = SingularUpdateQueue { sender };

        thread::spawn(move || {
            for (_, command) in receiver.into_iter().enumerate() {
                Self::work_on_command(&storage, command);
            }
        });
        return singular_update_queue;
    }

    fn work_on_command(storage: &Storage, command: Command) {
        match command {
            Command::Put { key, value, respond_back } => {
                storage.write().unwrap().insert(key, value);
                respond_back.send(Status::Ok).unwrap();
            }
            Command::Delete { key, respond_back } => {
                storage.write().unwrap().remove(&key);
                respond_back.send(Status::Ok).unwrap();
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_with_insert_by_a_single_task() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let cloned_storage = storage.clone();

        let singular_update_queue = SingularUpdateQueue::init(storage);
        let (sender, receiver) = mpsc::channel();
        let respond_back = sender.clone();

        let handle = thread::spawn(move || {
            singular_update_queue.execute(Command::Put {
                key: String::from("key1"),
                value: String::from("value1"),
                respond_back,
            });
            assert_eq!(Status::Ok, receiver.recv().unwrap());
        });

        let _ = handle.join();
        let read_storage = cloned_storage.read().unwrap();
        assert_eq!("value1", read_storage.get("key1").unwrap());
    }

    #[test]
    fn get_with_insert_by_multiple_tasks() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let cloned_storage = storage.clone();

        let singular_update_queue = SingularUpdateQueue::init(storage);
        let cloned_queue_one = singular_update_queue.clone();
        let cloned_queue_two = singular_update_queue.clone();

        let (sender, receiver) = mpsc::channel();
        let respond_back = sender.clone();

        let handle_one = thread::spawn(move || {
            cloned_queue_one.execute(Command::Put {
                key: String::from("key1"),
                value: String::from("value1"),
                respond_back,
            });
            assert_eq!(Status::Ok, receiver.recv().unwrap());
        });

        let (sender, receiver) = mpsc::channel();
        let respond_back = sender.clone();

        let handle_two = thread::spawn(move || {
            cloned_queue_two.execute(Command::Put {
                key: String::from("key2"),
                value: String::from("value2"),
                respond_back,
            });
            assert_eq!(Status::Ok, receiver.recv().unwrap());
        });

        let _ = handle_one.join();
        let _ = handle_two.join();

        let read_storage = cloned_storage.read().unwrap();
        assert_eq!("value1", read_storage.get("key1").unwrap());
        assert_eq!("value2", read_storage.get("key2").unwrap());
    }

    #[test]
    fn get_with_insert_and_delete_by_multiple_tasks() {
        let storage = Arc::new(RwLock::new(HashMap::new()));
        let cloned_storage = storage.clone();

        let singular_update_queue = SingularUpdateQueue::init(storage);
        let cloned_queue_one = singular_update_queue.clone();
        let cloned_queue_two = singular_update_queue.clone();

        let (sender, receiver) = mpsc::channel();
        let respond_back = sender.clone();

        let handle_one = thread::spawn(move || {
            cloned_queue_one.execute(Command::Put {
                key: String::from("key1"),
                value: String::from("value1"),
                respond_back,
            });
            assert_eq!(Status::Ok, receiver.recv().unwrap());
        });

        thread::sleep(Duration::from_millis(5));

        let (sender, receiver) = mpsc::channel();
        let respond_back = sender.clone();

        let handle_two = thread::spawn(move || {
            cloned_queue_two.execute(Command::Delete {
                key: String::from("key1"),
                respond_back,
            });
            assert_eq!(Status::Ok, receiver.recv().unwrap());
        });

        let _ = handle_one.join();
        let _ = handle_two.join();

        let read_storage = cloned_storage.read().unwrap();
        assert_eq!(None, read_storage.get("key1"));
    }
}