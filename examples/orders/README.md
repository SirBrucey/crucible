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

The example fleet is what `crucible-core::fleet::EXAMPLE` points to. Running the framework binary brings the whole fleet up per worker, executes schedules, and tears it down:

```
cargo run --bin crucible
```
