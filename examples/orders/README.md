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
6. [`6_confirm`](6_confirm): the fix for a relay that forgets an event the
   broker never took.
7. [`7_sequence`](7_sequence): the fix for amendments applied in the wrong
   order.

`diff -r 1_base 2_outbox` is the change itself. Each README says what changed,
quotes the campaign output that motivated it, and reports what the change fixed,
what it did not, and what it cost.

What each rung does to the campaign, every figure from an unbounded run:

| | schedules | durability | idempotency | recovery | convergence | unattributed | passed | inconclusive |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `1_base` | 215 | 78 | 5 | 2 | 1 | 0 | 129 | 0 |
| `2_outbox` | 221 | 64 | 19 | 3 | 1 | 0 | 134 | 0 |
| `3_inbox` | 221 | 64 | 0 | 3 | 1 | 0 | 153 | 0 |
| `4_reconnect` | 221 | 28 | 0 | 2 | 1 | 0 | 189 | 1 |
| `5_ack_on_success` | 221 | 11 | 0 | 1 | 1 | 0 | 206 | 2 |
| `6_confirm` | 256 | 1 | 0 | 0 | 1 | 1 | 250 | 3 |
| `7_sequence` | 256 | 2 | 0 | 0 | **0** | 1 | 246 | 7 |

86 faults become 3, and every invariant reaches zero at some point along the
way. Read the schedule counts before the fault counts: rungs 2 to 5 fit the same
221, so those rows are the same faults in the same places, and rungs 6 and 7 fit
256 because confirm mode puts acknowledgement frames on an edge and there is
more traffic to burst.

The rungs are not independent. `3_inbox` is what makes `5_ack_on_success` safe,
because requeuing a failed message means redelivering it, and `7_sequence` needs
the same claim to record a superseded event as handled.

Each fix costs something, and the READMEs say what. Reconnecting and requeuing
make the fleet work at a problem a giving-up fleet abandoned, so runs take
longer and some exceed their budget. `7_sequence` puts a locking read on the
write path, which makes the packet counts the campaign anchors on drift, so five
of its faults never landed at all.

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
