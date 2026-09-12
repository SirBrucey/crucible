# 3. Local first

## What `2_outbox` told us

Among its failures under a fault held for the whole run:

```
`broker` was killed for the whole run.
`inventory -> broker` was cut off for the whole run.
`inventory -> db` was cut off for the whole run.
```

The API's own database is missing from that list, and not because the fleet came
through it well. `api -> db` was cut for the whole run and the campaign passed
it: every request was refused and the API kept nothing back, so the fleet took
responsibility for nothing and held nothing, which is all these invariants ask
of it. Refusing everything is not incorrect. A fleet doing it is not available
either, and availability is not what the four ask about.

So give the API a store of its own. It can then accept orders whatever the
network is doing, and the relay catches up when the broker comes back.

## What changed

The API's orders live in a SQLite file on its own disk, and its outbox lives in
a second one. It reaches the network for one thing only: announcing what it has
already written down.

```rust
sqlx::query("INSERT INTO orders ...").execute(&state.orders).await?;  // one file
queue(&state, ROUTING_KEY, &payload).await?;                          // another
```

The consumer keeps its own copy of the orders now, built from the events it is
told about, since the API's are no longer in the shared database. That is the
only change to it.

The campaign's discovery finds these edges and no others:

```
Edge { client: None,              upstream: "api" }       the inbound request
Edge { client: Some("api"),       upstream: "broker" }    the relay's publish
Edge { client: Some("inventory"), upstream: "broker" }
Edge { client: Some("inventory"), upstream: "db" }
```

`api -> db` is gone. The API no longer depends on a database it has to reach,
and the recovery schedule that used to cut that edge does not exist.

## What it undid

The transactional outbox in `2_outbox` was correct for one reason: the order and
the event announcing it were written in the same transaction, so the fleet could
not hold one without the other. Two SQLite files cannot share a transaction.
Nobody removed that guarantee on purpose. It left with the shared database.

So the API is back to two separate writes with a gap between them, which is the
defect `1_base` had and `2_outbox` fixed. A crash in that gap leaves an order on
the books that will never be announced, permanently, because there is nothing in
the outbox to retry.

## Why the wire cannot see it

Look at the edge list again. `INSERT INTO orders` is a write to a local file.
`INSERT INTO outbox` is a write to another local file. No edge lies between
them. The nearest packets are the inbound request, which arrives before both,
and the response, which leaves after both. A proxy watching every byte the fleet
sends has nothing to anchor a fault to in that window, because nothing crosses
it.

This is what the application-aware tier is for. The API reports the boundaries
of its own spans, so a schedule can name the moment directly:

```rust
.instrument(tracing::info_span!("record"))   // the order
.instrument(tracing::info_span!("queue"))    // the announcement
```

## What the campaign finds

Moving the store onto local disk changed the shape of the fleet before any fault
ran. The campaign builds one whole-run fault per service, and one per edge
between two of them, and this fleet has one edge fewer: seven where `2_outbox`
builds eight, because `api -> db` is gone. That is the change working.

What it did not do is make the fleet more reliable, and the rest of this section
says where it went instead.

The gap the shared transaction used to close:

> `api` was killed during step 1, on `record:1:end` holds api part way through
> its own work. The fleet took 5 steps which left `orders.orders.count` at `2`,
> expected value `3`. It settled where losing a step, taking one twice and
> taking one out of order would all have left it somewhere else, so which of
> durability, idempotency or convergence broke cannot be read from where it
> settled.

The API recorded the order and died before queueing its announcement. It came
back and served the rest of the run, so its own store holds all three orders.
The consumer only ever heard about two, and it never will.

The run reads both stores and the outbox between them:

| check | reads |
| --- | --- |
| `api.get "/stats" at: "$.orders"` | 3, so the order was written down |
| `api.get "/stats" at: "$.outbox"` | 0, so nothing is waiting to be announced |
| `db.orders.orders.count` | 2, so the consumer was told about two of them |

An outbox holding nothing is the whole problem. There is no retry to wait for,
because the write that would have queued one never happened.

The verdict names no invariant, and it says why rather than shrugging. The API's
reply to step 1 never arrived, so the fleet may have accepted that step or
refused it, and the campaign ran a reference run of steps 2 to 5 on a clean
fleet to find out where landing only those leaves it. That is enough to be
certain this fleet is somewhere neither answer allows.

It is not enough to name the invariant, and the reference run is what lets the
campaign say so with a reason. It drove steps 2 to 5 on a clean fleet and
recorded where the fleet stood after each, so all three questions could be put:
stopping short of those steps, taking one of them twice, taking one of them
last. None of the three leaves the API holding more than the consumer. Its API
holds an order its consumer will never hear about, and that is not work lost,
done twice, or done out of order. It is one fleet holding two answers.

That verdict is only reachable because the API says where it is. Every other
fault placed at a moment in this campaign is anchored to a packet. This one is
anchored to `record:1:end`, a boundary the API reported from inside itself, in a
window where the fleet sends nothing.

## What it did not fix

The consumer still applies whatever it is handed:

> `inventory -> broker` was redelivered to during step 1 ... left
> `orders.applied.count` at `6`, expected value `5`. Breaking the fleet this way
> can show nothing but idempotency, so that is what broke.

Three examples in, and the one defect that has survived every change is the one
nobody has addressed.

## Build and run

```
./examples/orders/3_local_first/build.sh
cargo run -p crucible -- run examples/orders/3_local_first/orders.cru
```

Built from the repository root rather than the examples workspace, because the
API carries crucible's span adapter.
