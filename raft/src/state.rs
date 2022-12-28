use std::sync::RwLock;
use replicate::net::replica::ReplicaId;

pub struct State {
    consensus_state: RwLock<ConsensusState>,
}

struct ConsensusState {
    term: u64,
    role: ReplicaRole,
    voted_for: Option<u64>,
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ReplicaRole {
    Leader,
    Follower,
    Candidate
}

impl State {
    pub fn new() -> State {
        return State {
            consensus_state: RwLock::new(ConsensusState {
                term: 0,
                role: ReplicaRole::Follower,
                voted_for: None,
            }),
        };
    }

    pub(crate) fn change_to_candidate(&self, self_replica_id: ReplicaId) -> u64 {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.term = consensus_state.term + 1;
        consensus_state.role = ReplicaRole::Candidate;
        consensus_state.voted_for = Some(self_replica_id);

        return consensus_state.term;
    }

    pub(crate) fn change_to_follower(&self, term: u64) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.role = ReplicaRole::Follower;
        consensus_state.term = term;
        consensus_state.voted_for = None;
    }

    pub(crate) fn change_to_leader(&self) {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.role = ReplicaRole::Leader;
    }

    pub fn get_term(&self) -> u64 {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).term;
    }

    pub fn get_role(&self) -> ReplicaRole {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).role;
    }

    fn get_voted_for(&self) -> Option<ReplicaId> {
        let guard = self.consensus_state.read().unwrap();
        return (*guard).voted_for;
    }
}

#[cfg(test)]
mod tests {
    use crate::state::{ReplicaRole, State};

    #[test]
    fn become_candidate() {
        let state = State::new();
        state.change_to_candidate(10);

        assert_eq!(1, state.get_term());
        assert_eq!(ReplicaRole::Candidate, state.get_role());
        assert_eq!(Some(10), state.get_voted_for());
    }

    #[test]
    fn become_leader() {
        let state = State::new();
        state.change_to_candidate(10);
        state.change_to_leader();

        assert_eq!(1, state.get_term());
        assert_eq!(ReplicaRole::Leader, state.get_role());
        assert_eq!(Some(10), state.get_voted_for());
    }

    #[test]
    fn become_follower() {
        let state = State::new();
        state.change_to_candidate(10);
        state.change_to_follower(2);

        assert_eq!(2, state.get_term());
        assert_eq!(ReplicaRole::Follower, state.get_role());
        assert_eq!(None, state.get_voted_for());
    }
}