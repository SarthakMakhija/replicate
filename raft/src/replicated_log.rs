use std::sync::RwLock;

use crate::log::LogEntry;
use crate::net::rpc::grpc::Command;

pub(crate) struct ReplicatedLog {
    majority_quorum: usize,
    replicated_log_state: RwLock<ReplicatedLogState>,
}

struct ReplicatedLogState {
    log_entries: Vec<LogEntry>,
    next_index: u64,
}

impl ReplicatedLog {
    pub(crate) fn new(majority_quorum: usize) -> Self {
        return ReplicatedLog {
            majority_quorum,
            replicated_log_state: RwLock::new(ReplicatedLogState {
                log_entries: Vec::new(),
                next_index: 1,
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

    pub(crate) fn get_previous_log_index(&self) -> Option<u64> {
        let guard = self.replicated_log_state.read().unwrap();
        let next_index = (*guard).next_index;
        if next_index >= 1 {
            return Some(next_index - 1);
        }
        return None;
    }

    pub(crate) fn reduce_next_index(&self) {
        let mut write_guard = self.replicated_log_state.write().unwrap();
        let mut replicated_log_state = &mut *write_guard;
        replicated_log_state.next_index = replicated_log_state.next_index - 1;
    }

    pub(crate) fn get_next_log_index(&self) -> u64 {
        let guard = self.replicated_log_state.read().unwrap();
        return (*guard).next_index;
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
    use crate::log::LogEntry;
    use crate::net::rpc::grpc::Command;
    use crate::replicated_log::ReplicatedLog;

    #[tokio::test]
    async fn append_command() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        replicated_log.append_command(&command, 1);

        let log_entry = replicated_log.get_log_entry_at(0).unwrap();
        assert_eq!(1, log_entry.get_term());
        assert_eq!(0, log_entry.get_index());
        assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[tokio::test]
    async fn get_non_existing_previous_log_index() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *guard;
            replicated_log_state.next_index = replicated_log_state.next_index - 1;
        }

        assert_eq!(None, replicated_log.get_previous_log_index());
    }

    #[tokio::test]
    async fn get_previous_log_index() {
        let replicated_log = ReplicatedLog::new(2);
        assert_eq!(Some(0), replicated_log.get_previous_log_index());
    }

    #[tokio::test]
    async fn get_log_term_at_non_existing_index() {
        let replicated_log = ReplicatedLog::new(2);
        assert_eq!(None, replicated_log.get_log_term_at(99));
    }

    #[tokio::test]
    async fn get_log_term_at_an_existing_index() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        replicated_log.append_command(&command, 1);
        assert_eq!(Some(1), replicated_log.get_log_term_at(0));
    }

    #[tokio::test]
    async fn get_log_entry_at_non_existing_index() {
        let replicated_log = ReplicatedLog::new(2);
        assert_eq!(None, replicated_log.get_log_entry_at(99));
    }

    #[tokio::test]
    async fn get_log_entry_at_an_existing_index() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append_command(&command, 1);

        assert_eq!(
            Some(LogEntry::new(1, 0, &command)),
            replicated_log.get_log_entry_at(0)
        );
    }

    #[tokio::test]
    async fn acknowledge_log_entry() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append_command(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);
        assert_eq!(1, replicated_log.get_log_entry_at(0).unwrap().get_acknowledgements());
        assert_eq!(1, replicated_log.total_log_entries());
    }

    #[tokio::test]
    async fn multiple_acknowledge_log_entry() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append_command(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(0);

        assert_eq!(2, replicated_log.get_log_entry_at(0).unwrap().get_acknowledgements());
        assert_eq!(1, replicated_log.total_log_entries());
    }
}