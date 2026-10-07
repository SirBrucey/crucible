# 4. Reconnect

Fixes the consumer that dies with its broker connection, by rebuilding the
connection instead of returning from `main`.

## Build and run

```
./examples/orders/4_reconnect/build.sh
cargo run -p crucible -- run examples/orders/4_reconnect/orders.cru
```

Appendix A of the project report works the whole sequence through, with what
each rung's campaign returns. The figures live there rather than here, so a
sweep does not leave nine files stale.
