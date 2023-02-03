use bytes::Bytes;

use crate::net::rpc::grpc::Command;

#[derive(PartialEq, Debug)]
pub struct LogEntry {
    term: u64,
    index: u64,
    acknowledgements: u64,
    command: LogCommand,
}

#[derive(PartialEq, Debug)]
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
            acknowledgements: 1
        };
    }

    pub(crate) fn from(entry: &LogEntry) -> Self {
        return LogEntry {
            term: entry.term,
            index: entry.index,
            command: LogCommand { bytes: entry.command.bytes.clone() },
            acknowledgements: entry.acknowledgements
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

    pub(crate) fn acknowledge(&mut self) {
        self.acknowledgements = self.acknowledgements + 1;
    }

    pub(crate) fn get_index(&self) -> u64 {
        return self.index;
    }

    pub(crate) fn is_replicated(&self, quorum: usize) -> bool {
        return self.acknowledgements >= quorum as u64;
    }

    pub fn get_term(&self) -> u64 {
        return self.term;
    }

    pub fn get_bytes_as_vec(&self) -> Vec<u8> {
        return self.command.bytes.to_vec();
    }

    pub fn get_acknowledgements(&self) -> u64 {
        return self.acknowledgements;
    }
}

impl LogCommand {
    pub(crate) fn from(command: &Command) -> Self {
        return LogCommand {
            bytes: Bytes::copy_from_slice(command.command.as_ref())
        };
    }
}

#[cfg(all(test, feature="test_type_unit"))]
mod tests {
    use crate::log_entry::LogEntry;
    use crate::net::rpc::grpc::Command;

    #[test]
    fn matches_term() {
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        let log_entry = LogEntry::new(1, 0, &command);
        assert!(log_entry.matches_term(1));
    }

    #[test]
    fn does_not_match_term() {
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        let log_entry = LogEntry::new(10, 0, &command);
        assert_eq!(false, log_entry.matches_term(1));
    }

    #[test]
    fn matches_index() {
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        let log_entry = LogEntry::new(1, 0, &command);
        assert!(log_entry.matches_index(0));
    }

    #[test]
    fn does_not_match_index() {
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        let log_entry = LogEntry::new(10, 0, &command);
        assert_eq!(false, log_entry.matches_index(10));
    }

    #[test]
    fn matches_command() {
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        let log_entry = LogEntry::new(1, 0, &command);
        assert!(log_entry.matches_command(&command));
    }

    #[test]
    fn does_not_match_command() {
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        let log_entry = LogEntry::new(10, 0, &command);

        let another_command = Command { command: "fail".as_bytes().to_vec() };
        assert_eq!(false, log_entry.matches_command(&another_command));
    }
}