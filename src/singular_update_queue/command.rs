use std::sync::mpsc::{Receiver, Sender};

use crate::singular_update_queue::status::Status;

#[derive(Debug)]
pub enum Command {
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