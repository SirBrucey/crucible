# orders

One fleet, written several times. Each directory is a full copy of the fleet,
changed in response to what the campaign before it found.

## The staircase

Read these in order. Each fixes the fault the last one left, and every fix is
the textbook remedy for it.

1. [`1_base`](1_base): an event-driven fleet written without accounting for the
   problems a distributed system has.
2. [`2_outbox`](2_outbox): the textbook fix for what `1_base` loses. It closes
   that gap and opens another.
3. [`3_inbox`](3_inbox): the fix for the duplicates `2_outbox` admits.
4. [`4_reconnect`](4_reconnect): the fix for a consumer that dies with its
   broker connection.
5. [`5_ack_on_success`](5_ack_on_success): the fix for a consumer that
   acknowledges work it failed to do.

`diff -r 1_base 2_outbox` is the change itself. Each README says what changed,
quotes the campaign output that motivated it, and reports what the change fixed,
what it did not, and what it cost.

What each rung does to the campaign, every figure from an unbounded run:

| | durability | idempotency | recovery | convergence | passed | inconclusive |
| --- | --- | --- | --- | --- | --- | --- |
| `1_base` | 78 | 5 | 2 | 1 | 129 | 0 |
| `2_outbox` | 64 | 19 | 3 | 1 | 134 | 0 |
| `3_inbox` | 64 | 0 | 3 | 1 | 153 | 0 |
| `4_reconnect` | 28 | 0 | 2 | 1 | 189 | 1 |
| `5_ack_on_success` | 11 | 0 | 1 | 1 | 206 | 2 |

`1_base` fits 215 schedules and the rest 221, so every row after the first is
the same faults in the same places. The rungs are not independent: `3_inbox` is
what makes `5_ack_on_success` safe, because requeuing a failed message means
redelivering it.

What survives all five is one defect and one gap. The relay never puts its
channel in confirm mode, which is every remaining durability fault; and nothing
gives the consumer a per-order sequence to check, which is the convergence one.

## On its own

[`local_first`](local_first) is not a step in that sequence. It moves the API's
store onto local disk, which is an availability change rather than a fix for
anything the campaign named, and it silently undoes `2_outbox`'s transactional
guarantee. It is kept because it is the one fleet here whose defect no proxy can
see: both writes are local, no edge lies between them, and only a moment the
service reports itself reaches the window. It is built from the repository root
rather than the examples workspace, because its API carries crucible's span
adapter.

## Figures

Campaign figures in these READMEs come from unbounded runs, with the scenario's
`budget` line dropped so every schedule the scheduler produces is run. A
budgeted run fits however many schedules the machine had time for that day, so
its totals move between runs while the verdicts do not.
