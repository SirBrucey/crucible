# 1. The naive fleet

An event-driven fleet written without accounting for the problems a distributed
system has. The API writes what it was told and announces it. A consumer acts on
the announcement.

- **api**: HTTP. `POST /orders {id, item, quantity}` inserts an order row and
  publishes `order.created`. `PUT /orders/{id} {quantity}` publishes
  `order.modified`.
- **broker**: RabbitMQ, topic exchange `orders`.
- **db**: MariaDB.
- **inventory**: consumes `order.*`, adjusts `stock.level`, and appends a row to
  `applied` in the same transaction.

## The two write paths

The API, per request:

1. `INSERT` the order (db)
2. publish the event (broker)
3. `201` to the caller

The consumer, per message:

1. apply it: adjust stock and append to `applied`, in one transaction (db)
2. `ack` (broker)

Neither pair is atomic.

## Why `applied` is there

`stock.level` cannot tell an event that never arrived from one applied twice.
Both leave a number that is wrong. `applied` is append-only, one row per event
the consumer acted on, and nothing reads it back to decide anything. Five steps
owe five rows; a lost event leaves four, a duplicate leaves six.

## What the campaign finds

215 schedules, 129 passed, 86 faults, 0 inconclusive, 0 errored.

| invariant | failures |
| --- | --- |
| durability | 78 |
| idempotency | 5 |
| recovery | 2 |
| convergence | 1 |

All four are reachable on this fleet. Durability dominates because most
schedules are a kill or a cut, so more moments can reach it than can reach the
other three. The count is of failed schedules, not of defects.

### Applied twice

The broker redelivers a message the consumer had finished with, which is what
happens when an ack is lost.

> `inventory -> broker` was redelivered to during step 1, on a message the
> consumer finished with, delivered to it again. The fleet took 5 steps which
> left `orders.applied.count` at `6`, expected value `5`. Breaking the fleet
> this way can show nothing but idempotency, and where it settled says the
> same, so that is what broke.

Five steps, six applications, every request `2xx`. The consumer adjusts stock on
each delivery without asking whether it has seen the message before.

### Lost between the write and the announcement

The API inserts the order, then publishes, then answers. Cutting it off from the
broker at the publish:

> `api -> broker` was cut off during step 1, on a publish the sender has
> committed to and the broker has not seen. The fleet took 1 step which left
> `orders.applied.count` at `0`, expected value `1`. It settled where fewer
> steps would have left it, so work was lost, which is durability.

One request accepted, nothing acted on. Killing the broker at the same point
reads the same way.

### Lost after the announcement

Cut the consumer off from the broker instead and the messages survive:

> `inventory -> broker` was cut off during step 1, on a delivery the broker has
> released and the consumer has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `0`, expected value `5`. It settled where fewer
> steps would have left it, so work was lost, which is durability.

The broker kept all five. The consumer opens one connection and reads one loop,
and when the cut ends that stream it returns from `main` with nothing to bring
it back.

### The same break, held two ways

Cutting that edge for the whole run instead of for a moment leaves the fleet in
the same place:

> `inventory -> broker` was cut off for the whole run. The fleet took 5 steps
> which left `orders.applied.count` at `0`, expected value `5`. It took work on
> while it was down and does not hold it now it is back, so it never caught up,
> which is recovery.

Same edge, same reading, same number, different invariant. A fault held for a
moment heals, so the fleet has the rest of the run to catch up; one held from
start to finish leaves a fleet that was down throughout. How a fault is held
decides which invariant it shows, as much as what the fault does.

### Kept what it refused

A fleet can break durability by holding too much as well as too little:

> `broker` was killed during step 1, on a delivery the broker has released and
> the consumer has not seen. The fleet took 1 step which left
> `orders.orders.count` at `3`, expected value `1`. It holds more than the
> steps it took responsibility for owed, and no step taken twice puts it there,
> so it kept work it turned away, which is durability.

The order row is written before the announcement is attempted, so a publish that
fails leaves the row behind and the caller still gets a `500`. Three orders on
the books, one acknowledged and two refused.

### Applied out of order

An amendment reads what the order was to work out the stock difference, so two
amendments applied the other way round leave both somewhere the fleet was never
told to put them:

> `inventory -> broker` was reordered around during step 5, on a message held
> back until after the one the broker sent next. The fleet took 5 steps which
> left `orders.stock.select level where item = "book"` at `94`, expected value
> `98`. Breaking the fleet this way can show nothing but convergence, so that
> is what broke, though where it settled does not say so.

The order ends at the quantity it was told first. `applied` holds five rows, so
nothing was lost or doubled; only the stock is wrong.

## Build and run

```
./examples/orders/1_base/build.sh
cargo run -p crucible -- run examples/orders/1_base/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
