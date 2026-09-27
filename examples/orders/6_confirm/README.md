# 6. Publisher confirms

## What `5_ack_on_success` told us

Eleven of the thirteen faults left were one defect. Every one landed on the
relay's publish and lost exactly one event:

> `api -> broker` was cut off during step 3, on a publish the sender has
> committed to and the broker has not seen. The fleet took 5 steps which left
> `orders.applied.count` at `4`, expected value `5`.

The relay deleted the outbox row for a message the broker never took. The
twelfth was the same defect at full width: `api -> broker` cut for the whole run
left `applied.count` at `0`, because the relay spent the run publishing into a
severed socket and deleting rows as it went.

`create_channel` was never followed by `confirm_select`, so lapin's publish
future resolved as soon as the frame was on the socket. The `.await` in
`announce` read like it waited for the broker and did not.

## What changed

The relay's channel answers for what it publishes:

```rust
channel.confirm_select(ConfirmSelectOptions::default()).await?;
```

and the row is deleted only once the broker has acknowledged the message:

```rust
let confirmation = tokio::time::timeout(CONFIRM_TIMEOUT, sent).await??;
if !matches!(confirmation, Confirmation::Ack(_)) {
    anyhow::bail!("the broker did not take the message");
}
```

The wait is bounded on purpose. A broker killed mid-publish never answers, and a
relay waiting on it would stop draining the outbox for good.

`diff -r ../5_ack_on_success .` is the whole change, apart from the image names.

## What the campaign finds

| invariant | `5_ack_on_success` | `6_confirm` |
| --- | --- | --- |
| durability | 11 | **1** |
| recovery | 1 | **0** |
| convergence | 1 | 1 |
| unattributed | 0 | 1 |
| passed | 206 | 250 |
| inconclusive | 2 | 3 |

Read the totals with care. This fleet offers 100 points where the last offered
85, and fits 256 schedules where the last fit 221, because confirm mode puts
acknowledgement frames on the `api -> broker` edge and there is more traffic to
burst. More places to break the fleet, and fewer ways it breaks.

## What is left

**One unattributed fault**, and it is the fix working rather than failing:

> `api -> broker` had a message dropped on it during step 5, on a publish the
> broker took and the publisher was never told about. The fleet took 5 steps
> which left `orders.outbox.count` at `1`, expected value `0`. It held more
> than it owed on any reading.

The broker took the message and the acknowledgement was dropped, so the relay
kept the row to offer again. That is what keeping the row is for. The run was
read before the retry completed, so the scenario's `outbox.count == 0` is a
statement about a fleet that has finished settling, and this one had not.

**One durability fault**, the API keeping work it told the caller it refused:

> `api -> db` was cut off during step 4, on 45 reads into what this edge
> carried. The fleet took steps 1, 2, 3 and 5 which left `orders.applied.count`
> at `5`, expected value `4`. It holds more than the steps it took
> responsibility for owed.

The write committed and the response did not reach the caller. Nothing in the
fleet is wrong with the order; the caller was told something untrue about it.

**One convergence fault**, the amendment ordering, unchanged since `1_base`.
[`7_sequence`](../7_sequence) is that fix.

## Build and run

```
./examples/orders/6_confirm/build.sh
cargo run -p crucible -- run examples/orders/6_confirm/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
