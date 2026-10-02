# 3. The inbox

Fixes the duplicates `2_outbox` admits, by claiming each event's id in the
transaction that does its work.

## What `2_outbox` told us

Twenty schedules ended in a duplicate, from a redelivery or from the relay
announcing a row it was killed before deleting:

> `inventory -> broker` was redelivered to during step 1, on a message the
> consumer finished with, delivered to it again. The fleet took 5 steps which
> left `orders.stock.select level where item = "book"` at `94`, expected value
> `98`. Breaking the fleet this way can show nothing but idempotency, and where
> it settled says the same, so that is what broke.

## What changed

The relay names each event with the outbox row's sequence, which the write
transaction already assigned. The consumer records that id in an `inbox` table,
in the same transaction as the work it causes:

```rust
if !claim(&mut tx, event_id).await? {
    tx.rollback().await.context("rollback")?;
    return Ok(());
}
```

`claim` is an `INSERT IGNORE`, so the second delivery finds its id present and
does nothing. `diff -r ../2_outbox .` is the whole change, apart from the image
names.

## What the campaign finds

Both fleets fit 207 schedules, so these are the same faults in the same places.

| invariant | `2_outbox` | `3_inbox` |
| --- | --- | --- |
| durability | 66 | 65 |
| idempotency | 20 | **0** |
| recovery | 3 | 3 |
| convergence | 1 | 1 |
| passed | 117 | 138 |

Every schedule that showed idempotency now passes. Nothing else moves by more
than one.

## What is left

Recovery still fails three times, every one a fault held for the whole run. The
inbox says nothing about an event the fleet was never delivered.
[`4_reconnect`](../4_reconnect) is the next fix.

## Build and run

```
./examples/orders/3_inbox/build.sh
cargo run -p crucible -- run examples/orders/3_inbox/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
