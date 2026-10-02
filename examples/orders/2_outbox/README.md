# 2. The outbox

Fixes the event `1_base` loses between writing the order and announcing it, by
making the two one transaction.

## What `1_base` told us

Cut the API off from the broker at the publish and the order it had just written
was never acted on.

## What changed

The API no longer talks to the broker on the request path:

```rust
let mut tx = state.db.begin().await?;
sqlx::query("INSERT INTO orders ...").execute(&mut *tx).await?;
queue(&mut *tx, ROUTING_KEY, &payload).await?;
tx.commit().await?;
```

A relay announces what the outbox holds and deletes each row once it has gone.
`diff -r ../1_base .` is the whole change, apart from the image names.

## What the campaign finds

207 schedules, 117 passed, 90 faults, 0 inconclusive, 0 errored.

| invariant | `1_base` | `2_outbox` |
| --- | --- | --- |
| durability | 65 | 66 |
| idempotency | 11 | 20 |
| recovery | 2 | 3 |
| convergence | 1 | 1 |

Passes go from 86 to 117, and durability does not fall. A step the fleet refused
is one it does not owe, and the API used to refuse whenever it could not reach
the broker. Same fault, same moment, one rung apart:

> `broker` was killed during step 1, on a publish the broker has taken but not
> confirmed. The fleet took **1 step** which left `orders.orders.count` at `3`,
> expected value `1`. It holds more than the steps it took responsibility for
> owed, so it kept work it turned away, which is durability.

> `broker` was killed during step 1, on a publish the broker has taken but not
> confirmed. The fleet took **5 steps** which left `orders.stock.select level
> where item = "pen"` at `500`, expected value `490`. It settled where fewer
> steps would have left it, so work was lost, which is durability.

## What is left

**The announcement is still not durable.** `basic_publish` returns once the
frame is on the socket, so a connection severed in flight leaves the relay
deleting the row for an event the broker never saw.
[`6_confirm`](../6_confirm) is that fix.

**Twenty schedules end in a duplicate**, against eleven in `1_base`. The relay
announces a row, is killed before deleting it, and announces it again on
restart. The consumer is unchanged byte for byte.
[`3_inbox`](../3_inbox) is that fix.

## Build and run

```
./examples/orders/2_outbox/build.sh
cargo run -p crucible -- run examples/orders/2_outbox/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
