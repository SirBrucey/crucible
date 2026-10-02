# 1. The naive fleet

The baseline. An event-driven fleet written without accounting for anything a
distributed system does. Every defect the staircase fixes is here.

- **api**: HTTP. `POST /orders {id, item, quantity}` inserts an order row and
  publishes `order.created`. `PUT /orders/{id} {quantity}` publishes
  `order.modified`.
- **broker**: RabbitMQ, topic exchange `orders`.
- **db**: MariaDB.
- **inventory**: consumes `order.*` and adjusts `stock.level`.

The API inserts the order, publishes, then answers `201`. The consumer adjusts
the stock level, then acks. Neither pair is atomic.

The scenario places three orders for three different items, then amends the
first twice.

## What the campaign finds

165 schedules, 86 passed, 79 faults, 0 inconclusive, 0 errored.

| invariant | failures | reached by |
| --- | --- | --- |
| durability | 65 | a kill or cut anywhere on either write path |
| idempotency | 11 | a redelivery, or the API killed mid-publish |
| recovery | 2 | an edge cut for the whole run |
| convergence | 1 | the two amendments delivered the other way round |

Durability dominates because most schedules are a kill or a cut. The count is of
failed schedules, not of defects.

One verdict, as the reporter prints it:

> `broker` was killed for the whole run. The fleet took 0 steps which left
> `orders.orders.count` at `3`, expected value `0`. It holds more than the
> steps it took responsibility for owed, and no step taken twice puts it there,
> so it kept work it turned away, which is durability.

The order row is written before the publish is attempted, so a publish that
fails leaves the row and the caller gets a `500`.

## Build and run

```
./examples/orders/1_base/build.sh
cargo run -p crucible -- run examples/orders/1_base/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
