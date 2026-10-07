# Local first

Not a step in the staircase. It moves the API's store onto local disk for
availability, and gives up `2_outbox`'s transactional guarantee on the way,
since the order and the event announcing it can no longer share a transaction.
Kept because it is the only fleet here whose defect nothing on the wire can
see: both writes are local, so no edge lies between them.

## Build and run

```
./examples/orders/local_first/build.sh
cargo run -p crucible -- run examples/orders/local_first/orders.cru
```

Appendix A of the project report works the whole sequence through, with what
each rung's campaign returns. The figures live there rather than here, so a
sweep does not leave nine files stale.
