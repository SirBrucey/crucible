# 7. Per-order sequence

Fixes amendments applied in the wrong order, by giving each order a counter the
consumer checks before applying anything. It needs the inbox claim too, to
record a superseded event as handled without applying it.

## Build and run

```
./examples/orders/7_sequence/build.sh
cargo run -p crucible -- run examples/orders/7_sequence/orders.cru
```

Appendix A of the project report works the whole sequence through, with what
each rung's campaign returns. The figures live there rather than here, so a
sweep does not leave nine files stale.
