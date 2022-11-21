use std::sync::mpsc::Sender;

use crate::singular_update_queue::status::Status;

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