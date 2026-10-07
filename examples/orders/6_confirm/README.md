# 6. Publisher confirms

Fixes the relay that forgets an event the broker never took, by putting the
channel into confirm mode and waiting for the broker to answer before deleting
the outbox row.

## Build and run

```
./examples/orders/6_confirm/build.sh
cargo run -p crucible -- run examples/orders/6_confirm/orders.cru
```

Appendix A of the project report works the whole sequence through, with what
each rung's campaign returns. The figures live there rather than here, so a
sweep does not leave nine files stale.
