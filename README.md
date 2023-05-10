[![Build](https://github.com/SarthakMakhija/replicate/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/SarthakMakhija/replicate/actions/workflows/build.yml)

`replicate` is the rust version of Unmesh Joshi's [replicate](https://github.com/unmeshjoshi/replicate). It is a basic framework to quickly build and test replication algorithms.
`replicate` implements various distributed systems patterns and provides a foundation to implement any consensus algorithm.

## Features and Distributed Systems patterns

- [X] [Singular update queue](https://martinfowler.com/articles/patterns-of-distributed-systems/singular-update-queue.html)
- [X] [Request waiting list](https://martinfowler.com/articles/patterns-of-distributed-systems/request-waiting-list.html)
- [X] [Heartbeat](https://martinfowler.com/articles/patterns-of-distributed-systems/heartbeat.html)
- [X] [Quorum](https://martinfowler.com/articles/patterns-of-distributed-systems/quorum.html) (as an example using the building blocks)
- [ ] [Raf](https://martinfowler.com/articles/patterns-of-distributed-systems/replicated-log.html) (as an example using the building blocks)
  - [X] Election 
  - [X] Election timer 
  - [X] State transition (leader/follower/candidate) 
  - [X] Log
  - [X] Log replication 
  - [ ] WAL 
  - [X] Heartbeat sender
- [X] Async Network calls
- [X] Support for simulation
- [X] Benchmarks for replication using Criterion

Example implementations of `quorum` and `raft` are provided, both of which are implemented using `replicate` as the foundation.

## Libraries that replicate depends on

1. [tokio](https://tokio.rs/)
2. [tonic](https://github.com/hyperium/tonic)
3. [Dashmap](https://crates.io/crates/dashmap)

