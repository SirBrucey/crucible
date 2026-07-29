//! Regression benchmark for the observer's packet-count lookup. `EventIndex`
//! keeps per-service, per-direction counters, so `packet_count` is O(1): the
//! timings stay flat as the event log grows from 100 to 10k events. It guards
//! against a future change reintroducing a per-lookup scan, which matters
//! because the anchor waiter polls this every few milliseconds while a scenario
//! accumulates traffic.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use crucible_core::observer::EventIndex;
use crucible_protocol::{ConnEvent, Direction};

/// An `EventIndex` populated with `n` `Wrote` events for one service,
/// alternating direction.
fn populated(n: usize) -> EventIndex {
    let mut index = EventIndex::default();
    for i in 0..n {
        let direction = if i % 2 == 0 {
            Direction::ClientToUpstream
        } else {
            Direction::UpstreamToClient
        };
        index.record(
            "db".to_string(),
            ConnEvent::wrote_at(0, i as u128, direction, 1),
        );
    }
    index
}

fn packet_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("observer_packet_count");
    for n in [100usize, 1_000, 10_000] {
        let index = populated(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &index, |b, index| {
            b.iter(|| index.packet_count(black_box("db"), black_box(Direction::ClientToUpstream)));
        });
    }
    group.finish();
}

criterion_group!(benches, packet_count);
criterion_main!(benches);
