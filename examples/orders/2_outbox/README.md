# 2. The outbox

## What `1_base` told us

The API wrote an order down, then announced it, then answered. Three steps, and
nothing joined them. Cut it off from the broker at the announcement and one
request was accepted with nothing done about it; kill the broker and three order
rows sat on the books, two of which the fleet had told its callers it refused.
This is the failure a transactional outbox exists to prevent.

## What changed

The API no longer talks to the broker on the request path. It writes the order
and the event announcing it in one transaction:

```rust
let mut tx = state.db.begin().await?;
sqlx::query("INSERT INTO orders ...").execute(&mut *tx).await?;
queue(&mut *tx, ROUTING_KEY, &payload).await?;
tx.commit().await?;
```

A relay announces what the outbox holds and deletes each row once it has gone,
rebuilding its broker connection whenever the broker goes away.

`diff -r ../1_base .` is the whole change, apart from the image names and the
one check added for the outbox.

## What the campaign finds

221 schedules, 134 passed, 87 faults, 0 inconclusive, 0 errored.

| invariant | `1_base` | `2_outbox` |
| --- | --- | --- |
| durability | 78 | 64 |
| idempotency | 5 | 19 |
| recovery | 2 | 3 |
| convergence | 1 | 1 |

Durability falls by fourteen and idempotency rises by fourteen. That trade is
the whole of what this change does.

### The caller is no longer turned away

Cutting the API off from the broker at the publish, in both fleets:

| fleet | steps accepted | events applied |
| --- | --- | --- |
| `1_base` | 1 of 5 | 0 of 1 |
| `2_outbox` | 5 of 5 | 4 of 5 |

The API used to fail the caller whenever it could not reach the broker, because
announcing was part of answering. Now it is not.

### It did not make the announcement durable

> `api -> broker` was cut off during step 1, on a publish the sender has
> committed to and the broker has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `4`, expected value `5`. It settled where fewer
> steps would have left it, so work was lost, which is durability.

Four of five, where `1_base` managed none of one. `basic_publish` returns once
the frame is written to the socket, and the channel is never put into confirm
mode, so a connection severed in flight leaves the relay believing it announced
the event and deleting the row.

Kill the broker and the same mistake takes all five:

> `broker` was killed during step 1, on a publish the sender has committed to
> and the broker has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `0`, expected value `5`.

That run settles with `orders.outbox.count` at `0`. The relay published every
row and deleted it; what it sent while the broker was dying the broker never
saw, and what it sent once the broker was back had nothing listening. An empty
outbox has nothing to retry.

Durability changed shape rather than going away: from work the fleet refused and
kept anyway, to work it accepted and lost.

## What it introduced

Kill the API in the relay's publish-then-delete window and the number moves the
other way:

> `api` was killed during step 1, on a publish the sender has committed to and
> the broker has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `6`, expected value `5`. It settled where the steps
> it took would have left it had one of them been taken twice, so work was done
> twice, which is idempotency.

The relay announced the event, was killed before it could delete the row, and
announced it again on restart.

Nineteen schedules end in a duplicate here, against five in `1_base`. The
consumer is unchanged, byte for byte: `diff -r ../1_base/inventory inventory` is
empty apart from the package name. At-least-once delivery is an obligation on
the consumer, and this one has not met it. [`3_inbox`](../3_inbox) is that fix.

## Build and run

```
./examples/orders/2_outbox/build.sh
cargo run -p crucible -- run examples/orders/2_outbox/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
