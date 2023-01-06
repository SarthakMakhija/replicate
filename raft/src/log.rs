use bytes::Bytes;

use crate::net::rpc::grpc::Command;

pub(crate) struct LogEntry {
    term: u64,
    index: u64,
    command: LogCommand,
}

pub(crate) struct LogCommand {
    bytes: Bytes,
}

impl LogEntry {
    pub(crate) fn new(term: u64,
           index: u64,
           command: &Command) -> Self {
        return LogEntry {
            term,
            index,
            command: LogCommand::from(command),
        };
    }

    pub(crate) fn matches_term(&self, term: u64) -> bool {
        return self.term == term;
    }

    pub(crate) fn matches_index(&self, index: u64) -> bool {
        return self.index == index;
    }

    pub(crate) fn matches_command(&self, command: &Command) -> bool {
        return command.command == self.command.bytes.to_vec();
    }

    pub(crate) fn get_term(&self) -> u64 {
        return self.term;
    }
}

impl LogCommand {
    pub(crate) fn from(command: &Command) -> Self {
        return LogCommand {
            bytes: Bytes::copy_from_slice(command.command.as_ref())
        };
    }
}