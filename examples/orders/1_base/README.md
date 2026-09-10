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
owe five rows; a lost event leaves four, a duplicate leaves six. Without it a
campaign reports that something broke. With it, it names which invariant.

## Build and run

```
./examples/orders/1_base/build.sh
cargo run -p crucible -- run examples/orders/1_base/orders.cru
```

## What the campaign finds

A campaign against this fleet finds four things worth showing. Each is quoted
below from the run that produced it.

### Applied twice

The broker redelivers a message the consumer had finished with, which is what
happens when an ack is lost.

> `inventory -> broker` was redelivered to during step 1, on a message the
> consumer finished with, delivered to it again. The fleet took 5 steps which
> left `orders.applied.count` at `6`, expected value `5`. Breaking the fleet
> this way can show nothing but idempotency, so that is what broke.

Five steps, six applications. Every request returned `2xx`. The consumer adjusts
stock on each delivery without asking whether it has seen the message before.

### Lost between the write and the announcement

The API inserts the order, then publishes, then answers. A fault in between
leaves an order nobody will hear about, and the caller is told the request
failed. Cutting the API off from the broker at the publish:

> `api -> broker` was cut off during step 1, on a publish the sender has
> committed to and the broker has not seen. The fleet took 1 step which left
> `orders.applied.count` at `0`, expected value `1`. It settled where fewer
> steps would have left it, so work was lost, which is durability.

One request accepted, nothing acted on. Killing the broker outright at the same
point reads the same way.

### Lost after the announcement

Cut the consumer off from the broker instead, and the messages survive:

> `inventory -> broker` was cut off during step 1, on a delivery the broker has
> released and the consumer has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `0`, expected value `5`. It settled where fewer
> steps would have left it, so work was lost, which is durability.

Five requests acknowledged to the caller, none of them acted on. The broker
never died here, so it had all five and, with nothing left to consume them,
nothing took them away again. What is gone is the consumer. It opens one
connection and reads one loop, and when the cut ends that stream it returns from
`main` with nothing configured to bring it back. Work the fleet accepted is lost
because the only thing that would act on it is no longer running.

### The same break, held two ways

Cutting that edge for the whole run instead of for a moment leaves the fleet in
exactly the same place:

> `inventory -> broker` was cut off for the whole run. The fleet took 5 steps
> which left `orders.applied.count` at `0`, expected value `5`. Breaking the
> fleet this way can show nothing but recovery, so that is what broke.

Same edge, same reading, same number, and a different invariant. Nothing about
where the fleet settled decided that: a fault held from start to finish leaves a
fleet that was degraded for the whole run, and the only thing such a run can ask
is whether it comes back, so recovery is the only thing that verdict can name.
A fault held for a moment heals, so the fleet has the rest of the run to catch
up, and what it still owes when it stops is read off where it settled.

Which invariant a campaign names is decided as much by how a fault is held as by
what the fault does.

### Kept what it refused

One run names nothing:

> `broker` was killed during step 1, on a delivery the broker has released and
> the consumer has not seen. The fleet took 1 step which left
> `orders.orders.count` at `3`, expected value `1`. It held more than it owed on
> any reading, one of which is a point the fault-free run passed through rather
> than where it stopped. It settled where losing a step, taking one twice and
> taking one out of order would all have left it somewhere else, so which of
> durability, idempotency or convergence broke cannot be read from where it
> settled.

The order row is written before the announcement is attempted, so a publish that
fails leaves the row behind and the caller still gets a `500`. Three orders on
the books, one of them acknowledged and two of them refused.

The campaign put every question it could and none of them describes this. Losing
a step leaves less than was owed and this fleet has more. Taking its one
accepted step twice would leave two orders, and the fleet holds three; with one
step accepted there is nothing to take out of order. Two of the three rows were
written for callers the fleet then told it had failed, and nothing it does to
work it took on will account for work it said it refused. Holding what you
refused is none of the three, so the campaign says so rather than picking the
nearest.

## What this budget does not reach

Amending an order reads what it was to work out the stock difference, so two
amendments applied the other way round leave the order and the stock somewhere
the fleet was never told to put them. Reordering costs more moments than five
minutes affords here. Raise `budget` in `orders.cru` to reach it.
