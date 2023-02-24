use std::sync::{Arc, RwLock};

use criterion::{BatchSize, Criterion, criterion_group};
use tokio::runtime::Builder;

use replicate::singular_update_queue::singular_update_queue::SingularUpdateQueue;

struct State {
    value: RwLock<Value>,
}

struct Value {
    count: u64,
}

const SIZE: usize = 1024 * 1024;

fn add(criterion: &mut Criterion) {
    let singular_update_queue = SingularUpdateQueue::new();
    let state = Arc::new(State { value: RwLock::new(Value { count: 0 }) });

    criterion.bench_function("singular update queue add", |bencher| {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        bencher
            .to_async(runtime)
            .iter_batched(
                || (0..SIZE).collect::<Vec<_>>(),
                |ids| {
                    async {
                        for _id in ids {
                            let inner_state = state.clone();

                            let result = singular_update_queue.add(async move {
                                let mut value = inner_state.value.write().unwrap();
                                value.count = value.count + 1;
                            }).await;

                            if let Err(err) = result {
                                panic!("error while submitting a task to singular_update_queue: {}", err.to_string());
                            }
                        }
                    }
                }, BatchSize::SmallInput,
            );
    });
}

criterion_group!(benches, add);