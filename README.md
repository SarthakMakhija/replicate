[![Build](https://github.com/SarthakMakhija/replicate/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/SarthakMakhija/replicate/actions/workflows/build.yml)

## Concepts to build

- [X] Singular update queue
- [X] Request waiting list
- [X] Quorum callback
- [X] Async network calls (grpc)
- [X] Quorum (as example using the building blocks)
- [ ] Raft
  - [ ] Election 
  - [ ] Election timer 
  - [ ] State transition (leader/follower/candidate) 
  - [ ] Log
  - [ ] Log replication 
  - [ ] Retries
  - [ ] Heartbeat
- [ ] Viewstamped replication

## Libraries that might come in
1. [tokio](https://tokio.rs/)
   - asynchronous tasks 
2. [tonic](https://github.com/hyperium/tonic)
   - grpc 
3. [Dashmap](https://crates.io/crates/dashmap)
   - concurrent hashmap

