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
