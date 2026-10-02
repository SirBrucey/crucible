# 4. Reconnect

Fixes the consumer that dies with its broker connection, by rebuilding the
connection instead of returning from `main`.

## What `3_inbox` told us

42 of its 65 durability faults were one defect: 25 killing the broker and 17
cutting `inventory -> broker`. The consumer read its deliveries in `main`:

```rust
while let Some(delivery) = consumer.next().await {
    let delivery = delivery.context("delivery error")?;
```

so the moment its connection broke, that `?` returned from `main` and the
process was gone. The stock levels settled wherever it had got to and never
moved again.

## What changed

The broker work moved into a function the process rebuilds rather than dies
with:

```rust
loop {
    if let Err(e) = consume(&db, &broker_url).await {
        tracing::warn!(?e, "consumer stopped");
    }
    tokio::time::sleep(RETRY_DELAY).await;
}
```

The queue holds what it was sent meanwhile. `diff -r ../3_inbox .` is the whole
change, apart from the image names.

## What the campaign finds

| invariant | `3_inbox` | `4_reconnect` |
| --- | --- | --- |
| durability | 65 | 28 |
| idempotency | 0 | 0 |
| recovery | 3 | 1 |
| convergence | 1 | 1 |
| passed | 138 | 176 |
| inconclusive | 0 | 1 |

Thirty-seven durability faults go, every one that took the consumer's broker
away. `inventory -> broker` cut goes 17 to 0, `broker` killed 25 to 5, and
everything else stays at 23.

## What is left

**The consumer still acknowledges work it did not do.** Fifteen faults, every
one the database going away under it:

> `db` was killed during step 2, on a COM_QUERY COMMIT the server has not seen.
> The fleet took steps 1, 2, 4 and 5 which left `orders.stock.select level where
> item = "pen"` at `500`, expected value `490`. It settled where fewer steps
> would have left it, so work was lost, which is durability.

The ack sits outside the match, so it fires whether the work committed or not.
[`5_ack_on_success`](../5_ack_on_success) is that fix.

**Nine faults are the relay with no publisher confirm.**
[`6_confirm`](../6_confirm) is that fix.

One schedule came back `fault did not fire: ScenarioEndedBeforeAnchor`. The
moment it was aimed at never arrived, so the experiment was never run.

## Build and run

```
./examples/orders/4_reconnect/build.sh
cargo run -p crucible -- run examples/orders/4_reconnect/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
