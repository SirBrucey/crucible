# 5. Acknowledge on success

Fixes the consumer that acknowledges work it failed to do, by handing the
message back to the queue instead. The inbox claim of `3_inbox` is what makes
that safe, since the message will be delivered again.

## Build and run

```
./examples/orders/5_ack_on_success/build.sh
cargo run -p crucible -- run examples/orders/5_ack_on_success/orders.cru
```

Appendix A of the project report works the whole sequence through, with what
each rung's campaign returns. The figures live there rather than here, so a
sweep does not leave nine files stale.
