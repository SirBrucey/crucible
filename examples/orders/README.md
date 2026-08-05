# orders

An example fleet demonstrating a real event-driven flow:

- **api**: HTTP service. `POST /orders {item, quantity}` inserts an order row and publishes `order.created` to the `orders` topic exchange.
- **broker**: rabbitmq with a topic exchange `orders`.
- **db**: mariadb.
- **inventory**: consumer bound to `orders/order.*`. Decrements `stock.level` per order.

## Build

```
./examples/orders/build.sh
```

Produces two local images:
- `crucible-example/orders-api:0.1`
- `crucible-example/orders-inventory:0.1`

## Run

`orders.cru` describes the fleet and the scenario to drive against it. Running it brings the whole fleet up per worker, executes schedules, and tears it down:

```
cargo run -p crucible -- run examples/orders/orders.cru
```
