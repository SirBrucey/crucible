# 5. Acknowledge on success

Fixes the consumer that acknowledges work it failed to do, by handing the
message back to the queue instead.

## What `4_reconnect` told us

Fifteen of its 28 durability faults were the database going away while the
consumer was working. The transaction rolled back and the message was
acknowledged anyway:

```rust
match delivery.routing_key.as_str() {
    CREATED_KEY => {
        if let Err(e) = apply_order(db, &event, event_id).await {
            tracing::warn!(?e, "failed to apply order");   // logged, and that is all
        }
    }
    ...
}
delivery.ack(BasicAckOptions::default()).await?;           // regardless
```

The broker drops its copy and the only record of the work is a log line.

## What changed

The ack is conditional on the work having committed, and a message whose work
failed goes back to the queue:

```rust
if done {
    delivery.ack(BasicAckOptions::default()).await?;
} else {
    delivery.nack(BasicNackOptions { requeue: true, ..Default::default() }).await?;
}
```

A message that failed to *parse* is still acknowledged, because it will never
parse. Safe only because `3_inbox` exists: requeuing means redelivery, and the
claim rolls back with the work, so the retry applies it once.

`diff -r ../4_reconnect .` is the whole change, apart from the image names.

## What the campaign finds

| invariant | `4_reconnect` | `5_ack_on_success` |
| --- | --- | --- |
| durability | 28 | 14 |
| idempotency | 0 | 0 |
| recovery | 1 | **0** |
| convergence | 1 | 1 |
| passed | 176 | 192 |
| inconclusive | 1 | 0 |

`db` killed goes 12 to 0 and `inventory -> db` cut 3 to 0. Recovery reaches
zero, which is what the fix is for: a message handed back is one the fleet gets
another go at once the store returns.

## What is left

**Nine faults are the relay with no publisher confirm**, five killing the broker
under its publish and four cutting `api -> broker` in flight.
[`6_confirm`](../6_confirm) is that fix.

**Five are the API committing a write and then failing the request.** Cut
`api -> db` after the server has run the statement and before the OK packet
comes back, and the row is on the books while the caller holds a `500`. No rung
of this staircase fixes it.

**One convergence fault**, unchanged since `1_base`: the events carry no
per-order sequence. [`7_sequence`](../7_sequence) is that fix.

The campaign takes 40 minutes against `4_reconnect`'s 37, because the fleet
keeps working at a problem a giving-up fleet abandoned.

## Build and run

```
./examples/orders/5_ack_on_success/build.sh
cargo run -p crucible -- run examples/orders/5_ack_on_success/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
