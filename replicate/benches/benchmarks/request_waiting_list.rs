use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group};

use replicate::clock::clock::SystemClock;
use replicate::net::connect::correlation_id::CorrelationId;
use replicate::net::connect::host_and_port::HostAndPort;
use replicate::net::request_waiting_list::request_waiting_list::RequestWaitingList;
use replicate::net::request_waiting_list::request_waiting_list_config::RequestWaitingListConfig;
use replicate::net::request_waiting_list::response_callback::{AnyResponse, ResponseCallback, ResponseErrorType};

const SIZE: usize = 1024 * 1024;

struct SuccessResponseCallback {}

impl ResponseCallback for SuccessResponseCallback {
    fn on_response(&self, _: HostAndPort, _: Result<AnyResponse, ResponseErrorType>) {}
}

fn add(criterion: &mut Criterion) {
    let success_response_callback = Arc::new(SuccessResponseCallback {});
    let from = HostAndPort::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);

    let mut group = criterion.benchmark_group("request waiting list add");

    group.bench_function("add without capacity", |bencher| {
        let waiting_list = RequestWaitingList::new_with_disabled_expired_callbacks_removal(
            Box::new(SystemClock::new()),
        );

        bencher.iter_batched(
            || (0..SIZE).map(|index| index as CorrelationId).collect::<Vec<_>>(),
            |correlation_ids| {
                for correlation_id in correlation_ids {
                    waiting_list.add(correlation_id, from, success_response_callback.clone());
                }
            },
            BatchSize::SmallInput
        );
    });
    group.bench_function("add with capacity", |bencher| {
        let waiting_list = RequestWaitingList::new_with_capacity_and_disabled_expired_callbacks_removal(
            SIZE,
            Box::new(SystemClock::new()),
        );

        bencher.iter_batched(
            || (0..SIZE).map(|index| index as CorrelationId).collect::<Vec<_>>(),
            |correlation_ids| {
                for correlation_id in correlation_ids {
                    waiting_list.add(correlation_id, from, success_response_callback.clone());
                }
            },
            BatchSize::SmallInput
        );
    });
    group.bench_function("add with capacity expired callbacks removal thread", |bencher| {
        let waiting_list = RequestWaitingList::new_with_capacity(
            SIZE,
            Box::new(SystemClock::new()),
            RequestWaitingListConfig::new(
                Duration::from_secs(1),
                Duration::from_millis(500)
            )
        );

        bencher.iter_batched(
            || (0..SIZE).map(|index| index as CorrelationId).collect::<Vec<_>>(),
            |correlation_ids| {
                for correlation_id in correlation_ids {
                    waiting_list.add(correlation_id, from, success_response_callback.clone());
                }
            },
            BatchSize::SmallInput
        );
    });
    group.finish();
}

criterion_group!(benches, add);