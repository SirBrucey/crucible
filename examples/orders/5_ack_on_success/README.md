# 5. Acknowledge on success

## What `4_reconnect` told us

Fifteen of the remaining 28 durability faults were the database going away while
the consumer was working:

> `db` was killed during step 4, on 33 reads into what this edge carried. The
> fleet took 4 steps which left `orders.applied.count` at `3`, expected value
> `4`. It settled where fewer steps would have left it, so work was lost, which
> is durability.

The transaction rolled back, so nothing was half done. The message was then
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

The ack sits outside the match. The broker drops its copy, and the only record
of the work is a warning in a log.

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
parse and requeuing it forever helps nobody. Only work that could succeed later
is handed back.

This rung is safe only because `3_inbox` exists. Requeuing means redelivery, and
a redelivery without the inbox is a duplicate. The claim rolls back with the
work it was part of, so the retry finds the event unclaimed and applies it once.

`diff -r ../4_reconnect .` is the whole change, apart from the image names.

## What the campaign finds

| invariant | `4_reconnect` | `5_ack_on_success` |
| --- | --- | --- |
| durability | 28 | 11 |
| idempotency | 0 | 0 |
| recovery | 2 | 1 |
| convergence | 1 | 1 |
| passed | 189 | 206 |
| inconclusive | 1 | 2 |

Seventeen durability faults go: every one where the database was the thing taken
away.

| broken | `4_reconnect` | `5_ack_on_success` |
| --- | --- | --- |
| `db` killed | 9 | **0** |
| `inventory -> db` cut | 6 | **0** |
| `api -> db` cut | 2 | **0** |
| `broker` killed | 6 | 6 |
| `api -> broker` cut | 5 | 5 |

## What is left

**Eleven durability faults, and they are one defect.** All eleven are the relay
publishing into a broker that is not going to take it, either because the edge
is cut or because the broker is being killed underneath it. `basic_publish`
returns once the frame is on the socket, and the channel is never put into
confirm mode, so the relay deletes the outbox row for a publish the broker never
saw. `channel.confirm_select()` is the fix, and no rung here has made it.

**One convergence fault**, unchanged since `1_base`:

> `inventory -> broker` was reordered around during step 5, on a message held
> back until after the one the broker sent next. The fleet took 5 steps which
> left `orders.stock.select level where item = "book"` at `94`, expected value
> `98`.

An amendment reads the order to work out the stock difference, so two amendments
applied the other way round settle somewhere neither asked for. Nothing in the
staircase addresses ordering; the events carry no per-order sequence for the
consumer to check.

**One recovery fault**, `api -> broker` cut for the whole run. The outbox holds
every event and the run ends before the edge comes back.

## What it cost

Two schedules came back inconclusive, against one in `4_reconnect`:

```
schedule_id=215 reason=worker exceeded its 73.785828527s budget
schedule_id=221 reason=worker exceeded its 73.785828527s budget
```

Requeuing on top of reconnecting means the fleet keeps working at a problem that
a giving-up fleet abandoned, and two whole-run schedules now run past the budget
the campaign priced for them. Nineteen fewer faults and two fewer verdicts is
the trade this rung makes.

## Build and run

```
./examples/orders/5_ack_on_success/build.sh
cargo run -p crucible -- run examples/orders/5_ack_on_success/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
