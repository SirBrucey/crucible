# 2. The outbox

Fixes the event `1_base` loses between writing the order and announcing it, by
making the two one transaction. The API writes the order and an outbox row
together, and a relay publishes what the outbox holds.

## Build and run

```
./examples/orders/2_outbox/build.sh
cargo run -p crucible -- run examples/orders/2_outbox/orders.cru
```

Appendix A of the project report works the whole sequence through, with what
each rung's campaign returns. The figures live there rather than here, so a
sweep does not leave nine files stale.
