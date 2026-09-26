# 3. The inbox

## What `2_outbox` told us

The outbox stopped the API losing events and started it sending some twice. Two
faults reach the same end. The broker redelivers a message the consumer has
already finished with:

> `inventory -> broker` was redelivered to during step 1, on a message the
> consumer finished with, delivered to it again. The fleet took 5 steps which
> left `orders.applied.count` at `6`, expected value `5`. Breaking the fleet
> this way can show nothing but idempotency, and where it settled says the same,
> so that is what broke.

And the relay announces a row it was killed before deleting:

> `api` was killed during step 1, on a publish the sender has committed to and
> the broker has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `6`, expected value `5`. It settled where the steps
> it took would have left it had one of them been taken twice, so work was done
> twice, which is idempotency.

Nineteen schedules ended that way. The consumer had gone unchanged since
`1_base`, and this is the obligation at-least-once delivery hands it.

## What changed

The relay names each event with the outbox row's own sequence, which the write
transaction already assigned:

```rust
BasicProperties::default().with_message_id(seq.to_string().into()),
```

The consumer records that id in an `inbox` table, in the same transaction as the
work it causes:

```rust
if !claim(&mut tx, event_id).await? {
    tx.rollback().await.context("rollback")?;
    return Ok(());
}
```

`claim` is an `INSERT IGNORE`, so the second delivery of an event finds its id
present and does nothing. `diff -r ../2_outbox .` is the whole change, apart
from the image names.

## What the campaign finds now

Both fleets fit 221 schedules, so these are the same faults in the same places.

| invariant | `2_outbox` | `3_inbox` |
| --- | --- | --- |
| durability | 64 | 64 |
| idempotency | 19 | **0** |
| recovery | 3 | 3 |
| convergence | 1 | 1 |
| passed | 134 | 153 |

The nineteen schedules that showed idempotency in `2_outbox` all pass here, and
no other verdict moves. The nineteen faults the campaign loses are exactly the
nineteen it stops finding.

## What it did not fix

Durability is unchanged at 64, and it is the largest number here for a reason
that is not about this fleet: most schedules are a kill or a cut, so far more
moments can reach durability than can reach any of the other three. A count of
failed schedules is not a count of defects.

Recovery still fails three times, all of them a fault held for the whole run:

> `broker` was killed for the whole run. The fleet took 5 steps which left
> `orders.applied.count` at `0`, expected value `5`. It took work on while it
> was down and does not hold it now it is back, so it never caught up, which is
> recovery.

The inbox says nothing about a fleet that was never delivered the event at all.

## Build and run

```
./examples/orders/3_inbox/build.sh
cargo run -p crucible -- run examples/orders/3_inbox/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
