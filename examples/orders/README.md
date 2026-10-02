# orders

One fleet, written several times. Each directory is a full copy, changed in
response to what the campaign before it found.

## The staircase

Read these in order. Each fixes the fault the last one left, and every fix is
the textbook remedy for it.

1. [`1_base`](1_base): an event-driven fleet written without accounting for the
   problems a distributed system has.
2. [`2_outbox`](2_outbox): the fix for the event `1_base` loses between writing
   the order and announcing it. It closes that gap and opens another.
3. [`3_inbox`](3_inbox): the fix for the duplicates `2_outbox` admits.
4. [`4_reconnect`](4_reconnect): the fix for a consumer that dies with its
   broker connection.
5. [`5_ack_on_success`](5_ack_on_success): the fix for a consumer that
   acknowledges work it failed to do.
6. [`6_confirm`](6_confirm): the fix for a relay that forgets an event the
   broker never took.
7. [`7_sequence`](7_sequence): the fix for amendments applied in the wrong
   order.

`diff -r 1_base 2_outbox` is the change itself. Each README says what changed,
quotes the verdict that motivated it, and reports what it fixed and what it did
not.

| | schedules | durability | idempotency | recovery | convergence | unattributed | passed | inconclusive |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `1_base` | 165 | 65 | 11 | 2 | 1 | 0 | 86 | 0 |
| `2_outbox` | 207 | 66 | 20 | 3 | 1 | 0 | 117 | 0 |
| `3_inbox` | 207 | 65 | 0 | 3 | 1 | 0 | 138 | 0 |
| `4_reconnect` | 207 | 28 | 0 | 1 | 1 | 0 | 176 | 1 |
| `5_ack_on_success` | 207 | 14 | 0 | 0 | 1 | 0 | 192 | 0 |
| `6_confirm` | 242 | 5 | 1 | 1 | 1 | 0 | 234 | 0 |
| `7_sequence` | 242 | 3 | 2 | 0 | **0** | 0 | 235 | 2 |

79 faults become 5, and every invariant reaches zero somewhere along the way.

Three things to read before the fault counts. Rungs 2 to 5 fit the same 207
schedules, so those rows are the same faults in the same places; rungs 6 and 7
fit 242, because confirm mode puts acknowledgement frames on an edge. Durability
rises at `2_outbox`, the rung that exists to fix it, because the API now answers
`201` to steps it used to refuse and a step the fleet accepted is one it owes.
And the rungs are not independent: `3_inbox` is what makes `5_ack_on_success`
safe, and `7_sequence` needs the same claim to record a superseded event as
handled.

## On its own

[`local_first`](local_first) is not a step in that sequence. It moves the API's
store onto local disk, an availability change rather than a fix for anything the
campaign named, and undoes `2_outbox`'s transactional guarantee on the way. It
is kept because it is the one fleet here whose defect no proxy can see: both
writes are local and no edge lies between them.

## Figures

Campaign figures in these READMEs come from unbounded runs, with the scenario's
`budget` line dropped so every schedule the scheduler produces is run. A
budgeted run fits however many schedules the machine had time for that day, so
its totals move between runs while the verdicts do not.
