1. Singular update queue
   1. cargo test complaining about unused functions
   2. confirm if loop{ .. } is the right way or should we switch to tokio tasks
      - NA
   3. should submit in singular_update_queue return something?
2. Request waiting list
   1. Incorporate expiry of keys
   2. Decide if the callback should be given a mutable reference or not
