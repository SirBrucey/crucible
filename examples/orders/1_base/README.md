# 1. The naive fleet

The baseline. An event-driven fleet written without accounting for anything a
distributed system does, so every defect the staircase fixes is here. The API
inserts the order, publishes, then answers `201`; the consumer adjusts the
stock level, then acks. Neither pair is atomic.

- **api**: HTTP. `POST /orders {id, item, quantity}` inserts an order row and
  publishes `order.created`. `PUT /orders/{id} {quantity}` publishes
  `order.modified`.
- **broker**: RabbitMQ, topic exchange `orders`.
- **db**: MariaDB.
- **inventory**: consumes `order.*` and adjusts `stock.level`.

The scenario places three orders for three different items, then amends the
first twice.

## Build and run

```
./examples/orders/1_base/build.sh
cargo run -p crucible -- run examples/orders/1_base/orders.cru
```

Appendix A of the project report works the whole sequence through, with what
each rung's campaign returns. The figures live there rather than here, so a
sweep does not leave nine files stale.
