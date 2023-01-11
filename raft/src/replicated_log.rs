use std::sync::RwLock;

use crate::log_entry::LogEntry;
use crate::net::rpc::grpc::Command;

pub struct ReplicatedLog {
    majority_quorum: usize,
    replicated_log_state: RwLock<ReplicatedLogState>,
}

struct ReplicatedLogState {
    log_entries: Vec<LogEntry>,
}

impl ReplicatedLog {
    pub(crate) fn new(majority_quorum: usize) -> Self {
        return ReplicatedLog {
            majority_quorum,
            replicated_log_state: RwLock::new(ReplicatedLogState {
                log_entries: Vec::new(),
            }),
        };
    }

    pub(crate) fn matches_log_entry_term_at(&self, index: usize, term: u64) -> bool {
        let guard = self.replicated_log_state.read().unwrap();
        return match (*guard).log_entries.get(index) {
            None => false,
            Some(log_entry) => log_entry.matches_term(term)
        };
    }

    pub(crate) fn get_log_term_at(&self, index: usize) -> Option<u64> {
        let guard = self.replicated_log_state.read().unwrap();
        return match (*guard).log_entries.get(index) {
            None => None,
            Some(log_entry) => Some(log_entry.get_term())
        };
    }

    pub(crate) fn acknowledge_log_entry_at(&self, index: usize) {
        let mut write_guard = self.replicated_log_state.write().unwrap();
        let replicated_log_state = &mut *write_guard;

        let log_entry = &mut replicated_log_state.log_entries[index];
        log_entry.acknowledge();
    }

    pub(crate) fn is_entry_replicated(&self, index: usize) -> bool {
        let guard = self.replicated_log_state.read().unwrap();
        let entry = &(*guard).log_entries[index];
        return entry.is_replicated(self.majority_quorum);
    }

    pub fn append_command(&self, command: &Command, term: u64) {
        let mut write_guard = self.replicated_log_state.write().unwrap();
        let replicated_log_state = &mut *write_guard;
        let log_entries_size = replicated_log_state.log_entries.len();

        let log_entry = LogEntry::new(term, log_entries_size as u64, command);
        replicated_log_state.log_entries.push(log_entry);
    }

    pub fn total_log_entries(&self) -> usize {
        let guard = self.replicated_log_state.read().unwrap();
        return (*guard).log_entries.len();
    }

    pub fn get_log_entry_at(&self, index: usize) -> Option<LogEntry> {
        let guard = self.replicated_log_state.read().unwrap();
        return match (*guard).log_entries.get(index) {
            None => None,
            Some(entry) => Some(LogEntry::from(entry))
        };
    }
}


#[cfg(test)]
mod tests {
    use crate::log_entry::LogEntry;
    use crate::net::rpc::grpc::Command;
    use crate::replicated_log::ReplicatedLog;

    #[test]
    fn append_command() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        replicated_log.append_command(&command, 1);

        let log_entry = replicated_log.get_log_entry_at(0).unwrap();
        assert_eq!(1, log_entry.get_term());
        assert_eq!(0, log_entry.get_index());
        assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn get_log_term_at_non_existing_index() {
        let replicated_log = ReplicatedLog::new(2);
        assert_eq!(None, replicated_log.get_log_term_at(99));
    }

    #[test]
    fn get_log_term_at_an_existing_index() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        replicated_log.append_command(&command, 1);
        assert_eq!(Some(1), replicated_log.get_log_term_at(0));
    }

    #[test]
    fn get_log_entry_at_non_existing_index() {
        let replicated_log = ReplicatedLog::new(2);
        assert_eq!(None, replicated_log.get_log_entry_at(99));
    }

    #[test]
    fn get_log_entry_at_an_existing_index() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append_command(&command, 1);

        assert_eq!(
            Some(LogEntry::new(1, 0, &command)),
            replicated_log.get_log_entry_at(0)
        );
    }

    #[test]
    fn acknowledge_log_entry() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append_command(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);
        assert_eq!(1, replicated_log.get_log_entry_at(0).unwrap().get_acknowledgements());
        assert_eq!(1, replicated_log.total_log_entries());
    }

    #[test]
    fn multiple_acknowledge_log_entry() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append_command(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(0);

        assert_eq!(2, replicated_log.get_log_entry_at(0).unwrap().get_acknowledgements());
        assert_eq!(1, replicated_log.total_log_entries());
    }

    #[test]
    fn is_entry_replicated() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append_command(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(0);

        assert!(replicated_log.is_entry_replicated(0));
    }

    #[test]
    fn is_entry_not_replicated() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append_command(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);

        assert_eq!(false, replicated_log.is_entry_replicated(0));
    }
}