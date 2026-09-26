# 4. Reconnect

## What `3_inbox` told us

Idempotency was gone, and 64 durability faults were left. They sort by what the
fault took away:

| broken | faults |
| --- | --- |
| `broker` killed | 25 |
| `inventory -> broker` cut | 17 |
| `db` killed | 9 |
| `inventory -> db` cut | 6 |
| `api -> broker` cut | 5 |
| `api -> db` cut | 2 |

The first two are 42 of the 64, and they are one defect. The consumer read its
deliveries in `main`:

```rust
while let Some(delivery) = consumer.next().await {
    let delivery = delivery.context("delivery error")?;
```

so the moment its broker connection broke, that `?` returned from `main` and the
process was gone. Nothing brought it back. `orders.applied.count` settled at 0,
1, 2, 3 and 4 across those runs, in step with when the fault landed: the
consumer stopped where it was struck and never caught up.

## What changed

The broker work moved out of `main` into a function the process rebuilds rather
than dies with:

```rust
loop {
    if let Err(e) = consume(&db, &broker_url).await {
        tracing::warn!(?e, "consumer stopped");
    }
    tokio::time::sleep(RETRY_DELAY).await;
}
```

`consume` opens the connection, declares the queue and reads deliveries until
the broker goes away. The queue holds what it was sent meanwhile.

`diff -r ../3_inbox .` is the whole change, apart from the image names.

## What the campaign finds

| invariant | `3_inbox` | `4_reconnect` |
| --- | --- | --- |
| durability | 64 | 28 |
| idempotency | 0 | 0 |
| recovery | 3 | 2 |
| convergence | 1 | 1 |
| passed | 153 | 189 |
| inconclusive | 0 | 1 |

Thirty-six durability faults go, and they are exactly the ones that took the
consumer's broker away:

| broken | `3_inbox` | `4_reconnect` |
| --- | --- | --- |
| `inventory -> broker` cut | 17 | **0** |
| `broker` killed | 25 | **6** |
| everything else | 22 | 22 |

The six killed-broker failures that remain are not the consumer. They are the
relay publishing into a broker that is dying, which is the next section.

## What it did not fix

**The consumer still acknowledges work it did not do.** Fifteen faults, all of
them the database going away:

> `db` was killed during step 4, on 33 reads into what this edge carried. The
> fleet took 4 steps which left `orders.applied.count` at `3`, expected value
> `4`. It settled where fewer steps would have left it, so work was lost, which
> is durability.

The ack sits after the match, outside it, so it fires whether the work committed
or not. Surviving the broker does not help when the message is thrown away on
the way past. [`5_ack_on_success`](../5_ack_on_success) is that fix.

**The relay still has no publisher confirm.** Eleven faults, five cutting
`api -> broker` and six killing the broker under the relay's publish. `2_outbox`
documents this one: the channel is never put into confirm mode, so the await
that reads like a confirm resolves without waiting for one, and the row is
deleted for a publish the broker never took.

## What it cost

One schedule came back inconclusive:

```
schedule_id=215 reason=worker exceeded its 74.088577803s budget
```

A fleet that keeps trying takes longer than one that gives up, and the whole-run
faults are where that shows. The run is not wrong, it simply did not finish
inside the budget the campaign priced for it.

## Build and run

```
./examples/orders/4_reconnect/build.sh
cargo run -p crucible -- run examples/orders/4_reconnect/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
