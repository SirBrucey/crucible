# 2. The outbox

## What `1_base` told us

The API wrote an order down, then announced it, then answered. Those are three
steps and nothing joins them. Cut it off from the broker at the moment it
announces:

> `api -> broker` was cut off during step 1, on a publish the sender has
> committed to and the broker has not seen. The fleet took 1 step which left
> `orders.applied.count` at `0`, expected value `1`. It settled where fewer
> steps would have left it, so work was lost, which is durability.

And what the same fleet does when the broker dies with a delivery in flight:

> `broker` was killed during step 1 ... The fleet took 1 step which left
> `orders.orders.count` at `3`, expected value `1`. It held more than it owed on
> any reading, one of which is a point the fault-free run passed through rather
> than where it stopped.

One request accepted and nothing done about it, while three order rows sit on
the books, two of which the fleet told its callers it had refused. This is the
failure a transactional outbox exists to prevent.

## What changed

The API no longer talks to the broker on the request path. It writes the order
and the event announcing it in one transaction, so the fleet cannot hold one
without the other:

```rust
let mut tx = state.db.begin().await?;
sqlx::query("INSERT INTO orders ...").execute(&mut *tx).await?;
queue(&mut *tx, ROUTING_KEY, &payload).await?;
tx.commit().await?;
```

A relay task announces what the outbox holds and deletes each row once it has
gone. It keeps its own broker connection and rebuilds it whenever the broker
goes away. An outbox that gives up on its first failure keeps the event and
never sends it, which is not what the pattern is for.

`diff -r ../1_base .` is the whole change, apart from the image names and the
one check added for the outbox.

## What the campaign finds now

This campaign's failures are the same shapes as `1_base`'s, durability among
them still.
What moved is underneath that. Cutting the API off from the broker at the
publish, in both fleets:

| fleet | steps accepted | events applied |
| --- | --- | --- |
| `1_base` | 1 of 5 | 0 of 1 |
| `2_outbox` | 5 of 5 | 4 of 5 |

The API used to fail the caller whenever it could not reach the broker, because
announcing was part of answering. Now it is not, so the fleet accepts all five
steps under a fault that previously made it refuse four. That is the outbox
working: no caller is turned away for a broker it never needed to touch.

What it did not do is make the announcement durable:

> `api -> broker` was cut off during step 1, on a publish the sender has
> committed to and the broker has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `4`, expected value `5`. It settled where fewer
> steps would have left it, so work was lost, which is durability.

Four of five, where `1_base` managed none of one. The one that got away is worth
understanding: `basic_publish` returns as soon as the frame is written to the
socket, so when the connection is severed in flight the relay believes it
announced the event and deletes the row. The channel is never put into confirm
mode, so the await that reads like a confirm resolves without waiting for one.
The outbox moved the durability boundary from the API's process surviving to the
relay's belief that the broker took it, and nothing establishes that belief.

Kill the broker instead and the same mistake takes all five at once:

> `broker` was killed during step 1 ... The fleet took 5 steps which left
> `orders.applied.count` at `0`, expected value `5`.

That run settles with `orders.outbox.count` at `0` and `orders.applied.count` at
`0`. The relay published every row and deleted it, believing each had gone: what
it sent while the broker was dying, the broker never saw, and what it sent once
the broker was back had nothing listening, the consumer having exited at the
kill. An empty outbox has nothing to retry either way.

So durability did not go away. It changed shape: from work the fleet refused and
kept anyway, to work the fleet accepted and lost. The second is the better
failure to have, because the caller is no longer told a lie, but it is still a
failure.

## What it introduced

Kill the API at the same moment and the number moves the other way:

> `api` was killed during step 1, on a publish the sender has committed to and
> the broker has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `6`, expected value `5`. It held more than it owed
> on any reading. It settled where losing a step, taking one twice and taking
> one out of order would all have left it somewhere else, so which of
> durability, idempotency or convergence broke cannot be read from where it
> settled.

The campaign does not name it. Six applications for five steps is exactly where
one step taken twice leaves the count. What does not fit is the stock: doubling any one of the
five leaves `stock.book` at `98`, and the fleet holds `94`. The scenario
declares the stock level as something a step sets, so a repeat sets it to the
same thing again, and here the repeated create took four off it a second time.
The count says a step was doubled and the stock says something the scenario told
the campaign not to expect, so it names neither.

What happened is not in doubt. The relay announced the event, was killed before
it could delete the row, and announced it again when it came back. The consumer
adjusts stock on every delivery, so the order was counted twice.

The fault did not go away, it turned over. In `1_base` a fault in this window
lost an event; here the same fault duplicates one. That is what the outbox
actually buys: it converts a durability problem into an idempotency obligation,
and this fleet has not met it.

## What it did not fix

The consumer is unchanged, byte for byte:
`diff -r ../1_base/inventory inventory` is empty apart from the package name. So
the redelivery case is exactly as it was, because it was never about the API:

> `inventory -> broker` was redelivered to during step 1 ... left
> `orders.applied.count` at `6`, expected value `5`. Breaking the fleet this way
> can show nothing but idempotency, so that is what broke.

## Build and run

```
./examples/orders/2_outbox/build.sh
cargo run -p crucible -- run examples/orders/2_outbox/orders.cru
```
