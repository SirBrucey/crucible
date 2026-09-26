# Local first

Not a step in the staircase. The fleets numbered 1 to 3 each fix what the
campaign before them found; this one changes the API's storage for availability,
which no campaign asked for, and gives up a guarantee on the way. It is kept
because it is the only fleet here whose defect nothing on the wire can see.

## What prompted it

Among `2_outbox`'s failures under a fault held for the whole run:

```
`broker` was killed for the whole run.
`inventory -> broker` was cut off for the whole run.
`inventory -> db` was cut off for the whole run.
```

The API's own database is missing from that list, and not because the fleet came
through it well. `api -> db` was cut for the whole run and the campaign passed
it: every request was refused, so the fleet took responsibility for nothing and
held nothing, which is all these invariants ask of it. Refusing everything is
not incorrect. It is not available either, and availability is not one of the
four.

## What changed

The API's orders live in a SQLite file on its own disk, and its outbox in a
second one. It reaches the network only to announce what it has already written
down.

```rust
sqlx::query("INSERT INTO orders ...").execute(&state.orders).await?;  // one file
queue(&state, ROUTING_KEY, &payload).await?;                          // another
```

The consumer keeps its own copy of the orders, built from the events it is told
about, since the API's are no longer in the shared database.

Discovery finds four edges and no others:

```
Edge { client: None,              upstream: "api" }       the inbound request
Edge { client: Some("api"),       upstream: "broker" }    the relay's publish
Edge { client: Some("inventory"), upstream: "broker" }
Edge { client: Some("inventory"), upstream: "db" }
```

`api -> db` is gone, so the campaign builds seven whole-run faults where
`2_outbox` builds eight.

## What it undid

The transactional outbox was correct for one reason: the order and the event
announcing it shared a transaction. Two SQLite files cannot. Nobody removed that
guarantee on purpose; it left with the shared database.

So the API is back to two writes with a gap between them, which is the defect
`1_base` had and `2_outbox` fixed. A crash in that gap loses the event
permanently, there being nothing in the outbox to retry.

## Why the wire cannot see it

Both of those writes are local. No edge lies between them: the nearest packets
are the inbound request, before both, and the response, after both. This is not
an argument that the wire tier finds the window hard. The fleet's own edge list
is the framework's evidence that there is nothing there to anchor to.

Only a moment the service reports reaches it. The API names the boundaries of
its own spans:

```rust
.instrument(tracing::info_span!("record"))   // the order
.instrument(tracing::info_span!("queue"))    // the announcement
```

and the campaign places a fault between them:

> `api` was killed during step 1, on `record:1:end` holds api part way through
> its own work. The fleet took 5 steps which left `orders.orders.count` at `2`,
> expected value `3`. It settled where fewer steps would have left it, so work
> was lost, which is durability.

The API recorded the order and died before queueing its announcement. It came
back and served the rest of the run, so its own store holds all three orders;
the consumer only ever heard about two. The run reads both stores and the outbox
between them:

| check | reads |
| --- | --- |
| `api.get "/stats" at: "$.orders"` | 3, so the order was written down |
| `api.get "/stats" at: "$.outbox"` | 0, so nothing is waiting to be announced |
| `db.orders.orders.count` | 2, so the consumer was told about two of them |

An outbox holding nothing is the whole problem. There is no retry to wait for,
because the write that would have queued one never happened.

Reaching that verdict needs the reference run. The API's reply to step 1 never
arrived, so the fleet may have accepted that step or refused it, and the
campaign drove steps 2 to 5 on a clean fleet to find where landing only those
leaves it. No run of these steps leaves the API holding an order the consumer
never heard about.

## What the campaign finds

160 schedules, 77 passed, 83 faults, 0 inconclusive, 0 errored.

| invariant | `2_outbox` | `local_first` |
| --- | --- | --- |
| durability | 64 | 73 |
| idempotency | 19 | 5 |
| recovery | 3 | 4 |
| convergence | 1 | 1 |

Durability rises by nine against the fleet this was branched from, which is the
undone transaction. Idempotency falls to five only because there are fewer
places left to duplicate from, not because the consumer learned anything:
[`3_inbox`](../3_inbox) is the fleet that fixed that, and this one does not
carry the fix.

## Build and run

```
./examples/orders/local_first/build.sh
cargo run -p crucible -- run examples/orders/local_first/orders.cru
```

Built from the repository root rather than the examples workspace, because the
API carries crucible's span adapter.

Figures above are from an unbounded run, with the scenario's `budget` line
dropped so every schedule is run.
