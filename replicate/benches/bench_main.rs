use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::request_waiting_list::benches
}