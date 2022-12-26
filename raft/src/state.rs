use std::sync::RwLock;

pub struct State {
    term: RwLock<u64>,
}

impl State {
    pub fn new() -> State {
        return State {
            term: RwLock::new(0),
        };
    }

    pub(crate) fn increment_term(&self) -> u64 {
        let mut guard = self.term.write().unwrap();
        *guard = *guard + 1;
        return *guard;
    }

    pub fn get_term(&self) -> u64 {
        let guard = self.term.read().unwrap();
        return *guard;
    }
}