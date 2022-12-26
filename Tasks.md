1. Singular update queue
   1. cargo test complaining about unused functions
   2. confirm if loop{ .. } is the right way or should we switch to tokio tasks
      - NA
   3. should submit in singular_update_queue return something
      - done
   4. Make singular update queue work with any request and response
      - done
2. Request waiting list
   1. Incorporate expiry of keys
      - done
   2. Decide if the callback should be given a mutable reference or not
   3. Provide support for adding `key` in `RequestTimeoutError`
      - done
3. BuiltinHeartbeatSender
   1. Has a hard-coded node-id (at this stage)
   2. Uses eprintln!, replace with log
4. HeartbeatScheduler
   1. It ignores the error from HeartbeatSender `let _ = heartbeat_sender.send().await;`. Decide on the approach.
5. Renaming the module in Cargo.toml will impact the imports in integration tests
6. Code walk through
7. AsyncQuorumCallback
   1. Improve all_success_responses and all_error_responses
      - done
8. Handle typecast error in `QuorumCompletionHandle`
9. Relook at generic types across objects
10. Change the correlation id generator
11. Support Correlation id generator as a trait
      - done
12. Feedback on `Quorum` implementation
    1. Support `put` in Quorum
       - done
    2. Support `read repair` in Quorum
    3. Refactor `quorum integration test`
       - done
    4. Instantiate multiple replicas corresponding to each node in the integration test
       - done
    5. Build configuration to handle `request waiting list expiry`
    6. Decide on the data type of port in `HostAndPort` because `proto` files do not support `u16`
       - done 
    7. Decide on `threadpool` implementation in `SingularUpdateQueue`
    8. Support vector of services in ServiceRegistration
13. Change `package raft.election` in raft.proto