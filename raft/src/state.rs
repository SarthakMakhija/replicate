use std::sync::RwLock;

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

    pub(crate) fn change_to_candidate(&self, replica_id: u64) -> u64 {
        let mut write_guard = self.consensus_state.write().unwrap();
        let mut consensus_state = &mut *write_guard;
        consensus_state.term = consensus_state.term + 1;
        consensus_state.role = ReplicaRole::Candidate;
        consensus_state.voted_for = Some(replica_id);

        return consensus_state.term;
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
}