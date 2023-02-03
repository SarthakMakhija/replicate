use std::cmp::min;
use std::sync::RwLock;

use crate::log_entry::LogEntry;
use crate::net::rpc::grpc::{AppendEntries, Command, Entry};

pub struct ReplicatedLog {
    majority_quorum: usize,
    replicated_log_state: RwLock<ReplicatedLogState>,
}

struct ReplicatedLogState {
    log_entries: Vec<LogEntry>,
    commit_index: Option<u64>,
}

enum AppendAction {
    Append,
    Truncate,
    Ignore,
}

impl ReplicatedLog {
    pub(crate) fn new(majority_quorum: usize) -> Self {
        return ReplicatedLog {
            majority_quorum,
            replicated_log_state: RwLock::new(ReplicatedLogState {
                log_entries: Vec::new(),
                commit_index: None,
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

    pub(crate) fn commit<F>(&self, commit_execution_block: F) //apply
        where F: Fn(u64) -> () {
        let commit_indices = {
            let starting_commit_index: usize = match self.get_commit_index() {
                None => 0,
                Some(commit_index) => (commit_index + 1) as usize
            };
            let guard = self.replicated_log_state.read().unwrap();
            let replicated_log_state = &*guard;

            let mut commit_indices = Vec::new();
            for commit_index in starting_commit_index..replicated_log_state.log_entries.len() {
                if self._is_entry_replicated(commit_index, replicated_log_state) {
                    commit_indices.push(commit_index);
                } else {
                    break;
                }
            }
            commit_indices
        };
        for commit_index in commit_indices {
            let index = commit_index as u64;
            {
                let mut write_guard = self.replicated_log_state.write().unwrap();
                let replicated_log_state = &mut *write_guard;
                replicated_log_state.commit_index = Some(index);
            }
            commit_execution_block(index);
        }
    }

    pub(crate) fn maybe_advance_commit_index_to(&self, requested_commit_index: Option<u64>) {
        if let Some(commit_index) = requested_commit_index {
            let (last_log_index, _) = self.get_last_log_index_and_term();
            let self_commit_index = {
                match self.get_commit_index() {
                    None => 0,
                    Some(commit_index) => commit_index
                }
            };

            if commit_index >= self_commit_index {
                if let Some(last_log_index) = last_log_index {
                    let mut write_guard = self.replicated_log_state.write().unwrap();
                    let replicated_log_state = &mut *write_guard;
                    replicated_log_state.commit_index = Some(min(commit_index, last_log_index));
                }
            }
        }
    }

    pub(crate) fn should_accept(&self, append_entries: &AppendEntries, term: u64) -> bool {
        return if term > append_entries.term {
            false
        } else if append_entries.previous_log_index.is_none() {
            true
        } else if append_entries.previous_log_index.unwrap() >= self.total_log_entries() as u64 {
            false
        } else if !self.matches_log_entry_term_at(append_entries.previous_log_index.unwrap() as usize, append_entries.previous_log_term.unwrap()) {
            false
        } else {
            true
        };
    }

    pub fn get_commit_index(&self) -> Option<u64> {
        let guard = self.replicated_log_state.read().unwrap();
        return (*guard).commit_index;
    }

    pub fn append(&self, command: &Command, term: u64) -> u64 {
        let mut write_guard = self.replicated_log_state.write().unwrap();
        let replicated_log_state = &mut *write_guard;
        let log_entries_size = replicated_log_state.log_entries.len();

        let log_entry = LogEntry::new(term, log_entries_size as u64, command);
        replicated_log_state.log_entries.push(log_entry);

        return log_entries_size as u64;
    }

    pub fn maybe_append(&self, entry: &Entry) {
        let entry_index = entry.index;
        match self.append_action(entry) {
            AppendAction::Append => {
                self.append_new(entry);
            }
            AppendAction::Truncate => {
                self.truncate(entry_index);
                self.append_new(entry);
            }
            _ => {}
        }
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

    pub(crate) fn get_last_log_index_and_term(&self) -> (Option<u64>, Option<u64>) {
        let total_log_entries = self.total_log_entries();
        if total_log_entries == 0 {
            return (None, None);
        }
        let last_log_index = total_log_entries - 1;
        return (Some(last_log_index as u64), self.get_log_term_at(last_log_index));
    }

    pub(crate) fn is_request_log_up_to_date(&self,
                                            request_last_log_index: Option<u64>,
                                            request_last_log_term: Option<u64>) -> bool {
        let (self_last_log_index, self_last_log_term): (Option<u64>, Option<u64>) =
            self.get_last_log_index_and_term();

        return match self_last_log_index {
            None => true,
            Some(self_last_log_index) => {
                return if let Some(last_log_index) = request_last_log_index {
                    if self_last_log_index > last_log_index {
                        return false;
                    }
                    if self_last_log_term.unwrap() > request_last_log_term.unwrap() {
                        return false;
                    }
                    true
                } else { false };
            }
        };
    }

    fn _is_entry_replicated(&self, index: usize, replicated_log_state: &ReplicatedLogState) -> bool {
        let entry = &replicated_log_state.log_entries[index];
        return entry.is_replicated(self.majority_quorum);
    }

    fn append_action(&self, entry: &Entry) -> AppendAction {
        let guard = self.replicated_log_state.read().unwrap();
        let replicated_log_state = &*guard;
        let total_log_entries = replicated_log_state.log_entries.len();

        let (entry_index, entry_term) = (entry.index, entry.term);
        return if entry_index < (total_log_entries as u64) &&
            replicated_log_state.log_entries[entry_index as usize].matches_index(entry_index) &&
            replicated_log_state.log_entries[entry_index as usize].matches_term(entry_term) {
            AppendAction::Ignore
        } else if entry_index < (total_log_entries as u64) &&
            replicated_log_state.log_entries[entry_index as usize].matches_index(entry_index) &&
            !replicated_log_state.log_entries[entry_index as usize].matches_term(entry_term) {
            AppendAction::Truncate
        } else {
            AppendAction::Append
        };
    }

    fn truncate(&self, index: u64) {
        let mut write_guard = self.replicated_log_state.write().unwrap();
        let replicated_log_state = &mut *write_guard;
        replicated_log_state.log_entries.truncate(index as usize);
    }

    fn append_new(&self, entry: &Entry) {
        let mut write_guard = self.replicated_log_state.write().unwrap();
        let replicated_log_state = &mut *write_guard;

        let (entry_index, entry_term) = (entry.index, entry.term);

        let command = entry.command.as_ref().unwrap();
        replicated_log_state.log_entries.push(LogEntry::new(entry_term, entry_index, &command));
    }
}


#[cfg(all(test, feature="test_type_unit"))]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::log_entry::LogEntry;
    use crate::net::builder::log::ReplicateLogRequestBuilder;
    use crate::net::rpc::grpc::Command;
    use crate::net::rpc::grpc::Entry;
    use crate::replicated_log::ReplicatedLog;

    #[test]
    fn append_command() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        let index = replicated_log.append(&command, 1);

        let log_entry = replicated_log.get_log_entry_at(0).unwrap();
        assert_eq!(0, index);
        assert_eq!(1, log_entry.get_term());
        assert_eq!(0, log_entry.get_index());
        assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn append_multiple_commands() {
        let replicated_log = ReplicatedLog::new(2);
        for count in 1..=3 {
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };

            let index = replicated_log.append(&command, 1);
            let log_entry = replicated_log.get_log_entry_at(0).unwrap();

            assert_eq!(count - 1, index);
            assert_eq!(1, log_entry.get_term());
            assert_eq!(0, log_entry.get_index());
            assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
        }
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

        replicated_log.append(&command, 1);
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
        replicated_log.append(&command, 1);

        assert_eq!(
            Some(LogEntry::new(1, 0, &command)),
            replicated_log.get_log_entry_at(0)
        );
    }

    #[test]
    fn acknowledge_log_entry_as_soon_as_it_is_created() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append(&command, 1);

        assert_eq!(1, replicated_log.get_log_entry_at(0).unwrap().get_acknowledgements());
        assert_eq!(1, replicated_log.total_log_entries());
    }

    #[test]
    fn multiple_acknowledge_log_entry() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(0);

        assert_eq!(3, replicated_log.get_log_entry_at(0).unwrap().get_acknowledgements());
        assert_eq!(1, replicated_log.total_log_entries());
    }

    #[test]
    fn is_entry_replicated() {
        let replicated_log = ReplicatedLog::new(3);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(0);

        assert!(replicated_log.is_entry_replicated(0));
    }

    #[test]
    fn is_entry_not_replicated() {
        let replicated_log = ReplicatedLog::new(3);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);

        assert_eq!(false, replicated_log.is_entry_replicated(0));
    }

    #[test]
    fn initial_commit_index() {
        let replicated_log = ReplicatedLog::new(2);

        assert_eq!(None, replicated_log.get_commit_index())
    }

    #[test]
    fn commit_index_for_first_entry() {
        let replicated_log = ReplicatedLog::new(1);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };
        replicated_log.append(&command, 1);

        replicated_log.acknowledge_log_entry_at(0);

        replicated_log.commit(|_| {});
        assert_eq!(Some(0), replicated_log.get_commit_index())
    }

    #[test]
    fn commit_index_for_few_entries() {
        let replicated_log = ReplicatedLog::new(1);

        for _count in 1..=3 {
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            replicated_log.append(&command, 1);
        }

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(1);
        replicated_log.acknowledge_log_entry_at(2);

        replicated_log.commit(|_| {});
        assert_eq!(Some(2), replicated_log.get_commit_index())
    }

    #[test]
    fn commit_index_for_few_entries_with_execution_block() {
        let replicated_log = ReplicatedLog::new(1);

        for _count in 1..=3 {
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            replicated_log.append(&command, 1);
        }

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(1);
        replicated_log.acknowledge_log_entry_at(2);

        let commit_count = Arc::new(Mutex::new(0));
        replicated_log.commit(|_commit_index| {
            let mut guard = commit_count.lock().unwrap();
            *guard = *guard + 1;
        });

        let count = commit_count.lock().unwrap();
        assert_eq!(3, *count);
        assert_eq!(Some(2), replicated_log.get_commit_index());
    }

    #[test]
    fn commit_index_with_a_non_replicated_entry() {
        let replicated_log = ReplicatedLog::new(2);

        for _count in 1..=3 {
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            replicated_log.append(&command, 1);
        }

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(1);

        replicated_log.commit(|_| {});
        assert_eq!(Some(1), replicated_log.get_commit_index())
    }

    #[test]
    fn commit_index_with_a_non_replicated_entry_in_between() {
        let replicated_log = ReplicatedLog::new(2);

        for _count in 1..=3 {
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            replicated_log.append(&command, 1);
        }

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(2);

        replicated_log.commit(|_| {});
        assert_eq!(Some(0), replicated_log.get_commit_index())
    }

    #[test]
    fn do_not_advance_commit_index() {
        let replicated_log = ReplicatedLog::new(1);
        let content = String::from("Content");
        let command = Command { command: content.as_bytes().to_vec() };

        replicated_log.append(&command, 1);
        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.commit(|_| {});

        replicated_log.maybe_advance_commit_index_to(None);
        assert_eq!(Some(0), replicated_log.get_commit_index())
    }

    #[test]
    fn do_not_advance_commit_index_as_there_are_no_log_entries() {
        let replicated_log = ReplicatedLog::new(1);

        replicated_log.maybe_advance_commit_index_to(Some(2));
        assert_eq!(None, replicated_log.get_commit_index())
    }

    #[test]
    fn do_not_advance_commit_index_as_the_requested_commit_index_is_smaller() {
        let replicated_log = ReplicatedLog::new(1);
        for _count in 1..=2 {
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            replicated_log.append(&command, 1);
        }

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(1);
        replicated_log.commit(|_| {});

        replicated_log.maybe_advance_commit_index_to(Some(0));
        assert_eq!(Some(1), replicated_log.get_commit_index())
    }

    #[test]
    fn advance_commit_index_as_requested_commit_index_is_the_first() {
        let replicated_log = ReplicatedLog::new(1);
        replicated_log.append(&Command { command: String::from("Content").as_bytes().to_vec() }, 1);

        replicated_log.maybe_advance_commit_index_to(Some(0));
        assert_eq!(Some(0), replicated_log.get_commit_index())
    }

    #[test]
    fn advance_commit_index_to_minimum_of_the_commit_index_and_the_latest_entry_index() {
        let replicated_log = ReplicatedLog::new(1);
        for _count in 1..=2 {
            let content = String::from("Content");
            let command = Command { command: content.as_bytes().to_vec() };
            replicated_log.append(&command, 1);
        }

        replicated_log.acknowledge_log_entry_at(0);
        replicated_log.acknowledge_log_entry_at(1);
        replicated_log.commit(|_| {});

        replicated_log.maybe_advance_commit_index_to(Some(2));
        assert_eq!(Some(1), replicated_log.get_commit_index())
    }

    #[test]
    fn accepts_an_entry_with_higher_term() {
        let replicated_log = ReplicatedLog::new(1);
        let append_entries = ReplicateLogRequestBuilder::replicate_log_request_with_no_log_reference(
            10, 30, 100,
        );
        assert!(replicated_log.should_accept(&append_entries, 4));
    }

    #[test]
    fn does_not_accept_an_entry_with_lower_term() {
        let replicated_log = ReplicatedLog::new(1);
        let append_entries = ReplicateLogRequestBuilder::replicate_log_request_with_no_log_reference(
            4, 30, 100,
        );
        assert_eq!(false, replicated_log.should_accept(&append_entries, 10));
    }

    #[test]
    fn accepts_an_entry_with_no_previous_log_index() {
        let replicated_log = ReplicatedLog::new(1);
        let append_entries = ReplicateLogRequestBuilder::replicate_log_request_with_no_log_reference(
            1, 30, 100,
        );
        assert!(replicated_log.should_accept(&append_entries, 1));
    }

    #[test]
    fn does_not_accept_an_entry_with_non_matching_previous_log_term_at_previous_log_index() {
        let replicated_log = ReplicatedLog::new(1);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("Content").as_bytes().to_vec() },
            ));
        }
        let append_entries = ReplicateLogRequestBuilder::replicate_log_request(
            1, 30, 100, Some(0), Some(0), None, None,
        );
        assert_eq!(false, replicated_log.should_accept(&append_entries, 1));
    }

    #[test]
    fn accepts_an_entry_with_matching_previous_log_term_at_previous_log_index() {
        let replicated_log = ReplicatedLog::new(1);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("Content").as_bytes().to_vec() },
            ));
        }
        let append_entries = ReplicateLogRequestBuilder::replicate_log_request(
            1, 30, 100, Some(0), Some(1), None, None,
        );
        assert!(replicated_log.should_accept(&append_entries, 1));
    }

    #[test]
    fn does_not_accept_an_entry_with_previous_log_index_greater_than_total_entries() {
        let replicated_log = ReplicatedLog::new(1);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("Content").as_bytes().to_vec() },
            ));
        }

        let append_entries = ReplicateLogRequestBuilder::replicate_log_request(
            1, 30, 100, Some(5), Some(1), None, None,
        );
        assert_eq!(false, replicated_log.should_accept(&append_entries, 1));
    }

    #[test]
    fn does_not_accept_an_entry_with_previous_log_index_equal_to_total_entries() {
        let replicated_log = ReplicatedLog::new(1);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("Content").as_bytes().to_vec() },
            ));
        }

        let append_entries = ReplicateLogRequestBuilder::replicate_log_request(
            1, 30, 100, Some(1), Some(1), None, None,
        );
        assert_eq!(false, replicated_log.should_accept(&append_entries, 1));
    }

    #[test]
    fn maybe_append_an_entry_as_it_is_the_first_entry() {
        let replicated_log = ReplicatedLog::new(2);
        let content = String::from("Content");
        let entry = Entry {
            command: Some(Command { command: content.as_bytes().to_vec() }),
            term: 1,
            index: 0,
        };

        replicated_log.maybe_append(&entry);

        let log_entry = replicated_log.get_log_entry_at(0).unwrap();
        assert_eq!(1, log_entry.get_term());
        assert_eq!(0, log_entry.get_index());
        assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn maybe_append_an_entry_as_it_is_not_present() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
        }

        let content = String::from("second");
        let entry = Entry {
            command: Some(Command { command: content.as_bytes().to_vec() }),
            term: 1,
            index: 1,
        };

        replicated_log.maybe_append(&entry);

        let log_entry = replicated_log.get_log_entry_at(1).unwrap();
        assert_eq!(1, log_entry.get_term());
        assert_eq!(1, log_entry.get_index());
        assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn maybe_do_not_append_an_entry_as_it_is_already_present() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
        }

        let content = String::from("first");
        let entry = Entry {
            command: Some(Command { command: content.as_bytes().to_vec() }),
            term: 1,
            index: 0,
        };

        replicated_log.maybe_append(&entry);

        assert_eq!(1, replicated_log.total_log_entries());

        let log_entry = replicated_log.get_log_entry_at(0).unwrap();
        assert_eq!(1, log_entry.get_term());
        assert_eq!(0, log_entry.get_index());
        assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn maybe_truncate_an_entry_and_then_append() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
        }
        let content = String::from("new content");
        let entry = Entry {
            command: Some(Command { command: content.as_bytes().to_vec() }),
            term: 2,
            index: 0,
        };

        replicated_log.maybe_append(&entry);

        let log_entry = replicated_log.get_log_entry_at(0).unwrap();
        assert_eq!(1, replicated_log.total_log_entries());
        assert_eq!(2, log_entry.get_term());
        assert_eq!(0, log_entry.get_index());
        assert_eq!(content.as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn maybe_truncate_an_entry_at_index1_and_then_append() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
            replicated_log_state.log_entries.push(LogEntry::new(
                2,
                1,
                &Command { command: String::from("second").as_bytes().to_vec() },
            ));
        }
        let content = String::from("new content");
        let entry = Entry {
            command: Some(Command { command: content.as_bytes().to_vec() }),
            term: 3,
            index: 1,
        };

        replicated_log.maybe_append(&entry);

        assert_eq!(2, replicated_log.total_log_entries());

        let log_entry = replicated_log.get_log_entry_at(0).unwrap();
        assert_eq!(1, log_entry.get_term());
        assert_eq!(0, log_entry.get_index());
        assert_eq!(String::from("first").as_bytes().to_vec(), log_entry.get_bytes_as_vec());

        let log_entry = replicated_log.get_log_entry_at(1).unwrap();
        assert_eq!(3, log_entry.get_term());
        assert_eq!(1, log_entry.get_index());
        assert_eq!(String::from("new content").as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn maybe_truncate_few_entries_and_then_append() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            for count in 1..=6 {
                replicated_log_state.log_entries.push(LogEntry::new(
                    1,
                    count - 1,
                    &Command { command: (count - 1).to_string().as_bytes().to_vec() },
                ));
            }
        }
        let content = String::from("new content");
        let entry = Entry {
            command: Some(Command { command: content.as_bytes().to_vec() }),
            term: 3,
            index: 3,
        };

        replicated_log.maybe_append(&entry);

        assert_eq!(4, replicated_log.total_log_entries());
        for index in 0..=2 {
            let log_entry = replicated_log.get_log_entry_at(index).unwrap();

            assert_eq!(1, log_entry.get_term());
            assert_eq!(index as u64, log_entry.get_index());
            assert_eq!(index.to_string().as_bytes().to_vec(), log_entry.get_bytes_as_vec());
        }

        let log_entry = replicated_log.get_log_entry_at(3).unwrap();
        assert_eq!(3, log_entry.get_term());
        assert_eq!(3, log_entry.get_index());
        assert_eq!(String::from("new content").as_bytes().to_vec(), log_entry.get_bytes_as_vec());
    }

    #[test]
    fn get_last_log_index_and_term_for_no_log_entries() {
        let replicated_log = ReplicatedLog::new(2);
        let (last_log_index, term) = replicated_log.get_last_log_index_and_term();

        assert_eq!(None, last_log_index);
        assert_eq!(None, term);
    }

    #[test]
    fn get_last_log_index_and_term_for_with_log_entries() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
        }

        let (last_log_index, term) = replicated_log.get_last_log_index_and_term();

        assert_eq!(Some(0), last_log_index);
        assert_eq!(Some(1), term);
    }

    #[test]
    fn request_log_is_up_to_date_given_no_entries() {
        let replicated_log = ReplicatedLog::new(2);
        assert!(replicated_log.is_request_log_up_to_date(None, None));
    }

    #[test]
    fn request_log_is_up_to_date_given_incoming_log_index_is_greater_with_no_entries_on_other_side() {
        let replicated_log = ReplicatedLog::new(2);
        assert!(replicated_log.is_request_log_up_to_date(Some(2), Some(1)));
    }

    #[test]
    fn request_log_is_not_up_to_date_given_incoming_log_index_is_none() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
        }
        assert_eq!(false, replicated_log.is_request_log_up_to_date(None, None));
    }

    #[test]
    fn request_log_is_not_up_to_date_given_incoming_log_index_is_smaller() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                1,
                &Command { command: String::from("second").as_bytes().to_vec() },
            ));
        }
        assert_eq!(false, replicated_log.is_request_log_up_to_date(Some(0), Some(0)));
    }

    #[test]
    fn request_log_is_not_up_to_date_given_incoming_log_term_is_smaller() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                1,
                &Command { command: String::from("second").as_bytes().to_vec() },
            ));
        }
        assert_eq!(false, replicated_log.is_request_log_up_to_date(Some(1), Some(0)));
    }

    #[test]
    fn request_log_is_up_to_date_given_incoming_log_index_is_greater() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                1,
                &Command { command: String::from("second").as_bytes().to_vec() },
            ));
        }
        assert!(replicated_log.is_request_log_up_to_date(Some(2), Some(1)));
    }

    #[test]
    fn request_log_is_up_to_date_given_incoming_log_term_is_greater() {
        let replicated_log = ReplicatedLog::new(2);
        {
            let mut write_guard = replicated_log.replicated_log_state.write().unwrap();
            let replicated_log_state = &mut *write_guard;
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                0,
                &Command { command: String::from("first").as_bytes().to_vec() },
            ));
            replicated_log_state.log_entries.push(LogEntry::new(
                1,
                1,
                &Command { command: String::from("second").as_bytes().to_vec() },
            ));
        }
        assert!(replicated_log.is_request_log_up_to_date(Some(1), Some(2)));
    }
}