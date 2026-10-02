# 7. Per-order sequence

Fixes amendments applied in the wrong order, by giving each order a counter the
consumer checks before applying anything.

## What `6_confirm` told us

One fault had survived every rung since `1_base`:

> `inventory -> broker` was reordered around during step 5, on a message held
> back until after the one the broker sent next. The fleet took 5 steps which
> left `orders.stock.select level where item = "book"` at `94`, expected value
> `98`. Breaking the fleet this way can show nothing but convergence, so that is
> what broke, though where it settled does not say so.

An amendment works out the stock difference from what the order was, so two
applied the other way round settle where neither asked.

## What changed

Each order carries a counter, incremented in the same transaction as the event
that reports it:

```rust
let (was,): (i32,) = sqlx::query_as("SELECT seq FROM orders WHERE id = ? FOR UPDATE")
```

The consumer records how far through each order it has got, and an event at or
below that point is recorded as handled without being applied.

`diff -r ../6_confirm .` is the whole change, apart from the image names.

### The version that did not work

The first attempt asked the database whether it had moved anything:

```rust
"INSERT INTO order_seq (order_id, seq) VALUES (?, ?)
 ON DUPLICATE KEY UPDATE seq = IF(VALUES(seq) > seq, VALUES(seq), seq)"
// ... rows_affected() != 0
```

`sqlx` connects with `FOUND_ROWS`, so a write reports the rows it *matched*
rather than the rows it *changed*. Every stale event looked like progress. In
delivery order the sequence only moves forward, so the fault-free run passed
every check and only a reordering showed it.

## What the campaign finds

| invariant | `6_confirm` | `7_sequence` |
| --- | --- | --- |
| durability | 5 | 3 |
| idempotency | 1 | 2 |
| recovery | 1 | 0 |
| convergence | **1** | **0** |
| unattributed | 0 | 0 |
| passed | 234 | 235 |
| inconclusive | 0 | 2 |

Convergence is gone, and with it the last invariant that had failed on every
fleet in the staircase. Five faults out of 242 schedules, against 79 out of 165
in `1_base`.

## What is left

Four are the API committing a write and then failing the request:

> `api -> db` was cut off during step 3, on a COM_QUERY COMMIT with no OK packet
> back yet. The fleet took steps 1, 2, 4 and 5 which left `orders.orders.count`
> at `3`, expected value `2`. It holds more than the steps it took
> responsibility for owed, and no step taken twice puts it there, so it kept
> work it turned away, which is durability.

One cut, two names: on a create it leaves a row the fleet said it had not taken,
and on an amendment it gives stock back once more than owed. The remedy is an
identifier on the write that makes the caller's retry safe.

The fifth is the dropped publisher confirm `6_confirm` describes. Two further
runs could not be judged, because the fleet may have accepted steps 2, 3, 4 and
5 and no reference run has driven that set.

The locking read was expected to make the counted anchors drift and leave faults
unplaced. All 242 placed, because both database edges are anchored on commits
the MariaDB plugin names.

## Build and run

```
./examples/orders/7_sequence/build.sh
cargo run -p crucible -- run examples/orders/7_sequence/orders.cru
```

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
