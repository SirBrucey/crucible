# 7. Per-order sequence

## What `6_confirm` told us

One fault had survived every rung since `1_base`:

> `inventory -> broker` was reordered around during step 5, on a message held
> back until after the one the broker sent next. The fleet took 5 steps which
> left `orders.stock.select level where item = "book"` at `94`, expected value
> `98`. Breaking the fleet this way can show nothing but convergence, so that is
> what broke, though where it settled does not say so.

An amendment works out the stock difference from what the order was, so two
amendments applied the other way round settle where neither asked. Nothing in
the events said which came first.

## What changed

Each order carries a counter, bumped in the same transaction as the event that
reports it, so two amendments cannot claim the same place in its history:

```rust
let (was,): (i32,) = sqlx::query_as("SELECT seq FROM orders WHERE id = ? FOR UPDATE")
```

The consumer records how far through each order it has got, and an event at or
below that point is recorded as handled without being applied. A later amendment
has already decided what the order is for.

`diff -r ../6_confirm .` is the whole change, apart from the image names.

### The version that did not work

The first attempt asked the database whether it had moved anything:

```rust
"INSERT INTO order_seq (order_id, seq) VALUES (?, ?)
 ON DUPLICATE KEY UPDATE seq = IF(VALUES(seq) > seq, VALUES(seq), seq)"
// ... rows_affected() != 0
```

`sqlx` connects with `FOUND_ROWS`, so a write reports the rows it *matched*
rather than the rows it *changed*. An update that deliberately left the sequence
alone still reported a row, every stale event looked like progress, and nothing
was ever superseded.

The campaign caught it and the fault-free run could not have. In delivery order
the sequence only ever moves forward, so all eight checks passed; the defect
existed only when something arrived out of order. `advance` now reads the held
sequence and compares it, which is what the code meant to say.

## What the campaign finds

| invariant | `6_confirm` | `7_sequence` |
| --- | --- | --- |
| durability | 1 | 2 |
| convergence | **1** | **0** |
| unattributed | 1 | 1 |
| passed | 250 | 246 |
| inconclusive | 3 | 7 |

Convergence is gone, and with it the last invariant that had failed on every
fleet in the staircase. Three faults remain out of 256 schedules, against 86 out
of 215 in `1_base`.

## What it cost

**A heavier write path.** An amendment used to be one insert into the outbox.
It is now a locking read, an update and an insert, in a transaction. The extra
durability fault is that window:

> `api -> db` was cut off during step 4 ... The fleet took steps 1, 2, 3 and 5
> which left `orders.applied.count` at `5`, expected value `4`. It holds more
> than the steps it took responsibility for owed.

**Faults that no longer land.** Five schedules came back
`fault did not fire: ScenarioEndedBeforeAnchor`, against none before:

```
5 reason=fault did not fire: ScenarioEndedBeforeAnchor
2 reason=worker exceeded its 71.407921673s budget
```

Those faults are aimed at a packet count taken from the learn run, and the
busier, lock-taking write path this rung introduces makes that count drift
between runs. The fault was placed at a moment the run never reached. Nothing
about the fleet is wrong; the campaign simply could not say anything about those
five. A moment a protocol plugin names does not drift this way, which is the
argument for anchoring on what crossed rather than on how much.

## Build and run

```
./examples/orders/7_sequence/build.sh
cargo run -p crucible -- run examples/orders/7_sequence/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
