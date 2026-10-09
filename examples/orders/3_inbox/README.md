# 3. The inbox

Fixes the duplicates `2_outbox` admits, by claiming each event's id in the
transaction that does its work. An event already claimed is acknowledged and
dropped.

## Build and run

```
./examples/orders/3_inbox/build.sh
cargo run -p crucible -- run examples/orders/3_inbox/orders.cru
```

Appendix A of the project report works the whole sequence through, with what
each rung's campaign returns. The figures live there rather than here, so a
sweep does not leave nine files stale.
