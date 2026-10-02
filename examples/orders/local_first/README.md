# Local first

Not a step in the staircase. It moves the API's store onto local disk for
availability, and undoes `2_outbox`'s transactional guarantee on the way. Kept
because it is the only fleet here whose defect nothing on the wire can see.

## What prompted it

`api -> db` cut for the whole run is the one whole-run fault `2_outbox` passes.
Every request was refused, so the fleet took responsibility for nothing and held
nothing, which is all these invariants ask of it. Refusing everything is not
available, and availability is not one of the four.

## What changed

The API's orders live in a SQLite file on its own disk, and its outbox in a
second one:

```rust
sqlx::query("INSERT INTO orders ...").execute(&state.orders).await?;  // one file
queue(&state, ROUTING_KEY, &payload).await?;                          // another
```

Two SQLite files cannot share a transaction, so the API is back to two writes
with a gap between them, and a crash in that gap loses the event for good.

Discovery finds four edges and `api -> db` is not among them, so the campaign
builds seven whole-run faults where `2_outbox` builds eight.

## Why the wire cannot see it

Both writes are local. No edge lies between them, and the fleet's own edge list
is the evidence: the nearest packets are the inbound request, before both, and
the response, after both.

Only a moment the service reports reaches it. The API names its own span
boundaries, and the campaign places a fault between them:

> `api` was killed during step 1, on `record:1:end` holds api part way through
> its own work. The fleet took 5 steps which left `orders.orders.count` at `2`,
> expected value `3`. It settled where fewer steps would have left it, so work
> was lost, which is durability.

The API recorded the order and died before queueing its announcement. Its own
store holds all three orders and the consumer heard about two.

Reaching that verdict needs the reference run. The API's reply to step 1 never
arrived, so the fleet may have accepted that step or refused it, and the
campaign drove steps 2 to 5 on a clean fleet to find where landing only those
leaves it.

## What the campaign finds

142 schedules, 63 passed, 79 faults, 0 inconclusive, 0 errored.

| invariant | `2_outbox` | `local_first` |
| --- | --- | --- |
| durability | 66 | 65 |
| idempotency | 20 | 9 |
| recovery | 3 | 4 |
| convergence | 1 | 1 |

Durability is level with the fleet this was branched from, on a campaign two
thirds the size. There are fewer places to break it, because `api -> db` is no
longer an edge, and idempotency falls for the same reason.

Six faults are anchored inside the API, at a moment it reports itself. That site
has the highest yield of any in the sweep, six faults from sixteen schedules,
and nothing on the wire could have placed them.

## Build and run

```
./examples/orders/local_first/build.sh
cargo run -p crucible -- run examples/orders/local_first/orders.cru
```

Built from the repository root rather than the examples workspace, because the
API carries crucible's span adapter.

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
