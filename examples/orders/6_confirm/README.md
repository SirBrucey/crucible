# 6. Publisher confirms

Fixes the relay that forgets an event the broker never took, by waiting for the
broker to answer before deleting the outbox row.

## What `5_ack_on_success` told us

Nine of the fifteen faults left were one defect, each losing exactly one event:

> `api -> broker` was cut off during step 1, on a publish the sender has
> committed to and the broker has not seen. The fleet took 5 steps which left
> `orders.stock.select level where item = "book"` at `102`, expected value `98`.
> It settled where fewer steps would have left it, so work was lost, which is
> durability.

`create_channel` was never followed by `confirm_select`, so lapin's publish
future resolved as soon as the frame was on the socket. The `.await` in
`announce` read like it waited for the broker and did not.

## What changed

The relay's channel answers for what it publishes, and the row goes only once
the broker has acknowledged the message:

```rust
channel.confirm_select(ConfirmSelectOptions::default()).await?;
...
let confirmation = tokio::time::timeout(CONFIRM_TIMEOUT, sent).await??;
if !matches!(confirmation, Confirmation::Ack(_)) {
    anyhow::bail!("the broker did not take the message");
}
```

The wait is bounded, because a broker killed mid-publish never answers and a
relay waiting on it would stop draining the outbox for good.

`diff -r ../5_ack_on_success .` is the whole change, apart from the image names.

## What the campaign finds

| invariant | `5_ack_on_success` | `6_confirm` |
| --- | --- | --- |
| durability | 14 | **5** |
| idempotency | 0 | 1 |
| recovery | 0 | 1 |
| convergence | 1 | 1 |
| unattributed | 0 | 0 |
| passed | 192 | 234 |
| inconclusive | 0 | 0 |

This fleet fits 242 schedules where the last fit 207, because confirm mode puts
acknowledgement frames on `api -> broker` and there is more traffic to burst.
More places to break the fleet, fewer ways it breaks. At 46 minutes it is the
longest campaign of the staircase, because a relay waiting out its confirm
timeout does nothing for five seconds.

## What is left

**One fault the fix cannot reach.** The acknowledgement was dropped in flight,
so the relay kept the row to offer again and waits out its five-second timeout
in silence. The framework reads a fleet after a second of silence, so the fleet
is right and the reading is early.

**Four are the API committing a write and then failing the request**, two read
as durability and two as idempotency depending on which step the cut landed in.
[`7_sequence`](../7_sequence) has the verdicts.

**One recovery fault** and **one convergence fault**, the amendment ordering,
unchanged since `1_base`. [`7_sequence`](../7_sequence) is that fix.

## Build and run

```
./examples/orders/6_confirm/build.sh
cargo run -p crucible -- run examples/orders/6_confirm/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
