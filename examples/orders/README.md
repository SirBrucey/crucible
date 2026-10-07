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

`diff -r 1_base 2_outbox` is the change itself.

The rungs are not independent. `3_inbox` is what makes `5_ack_on_success` safe,
because handing a failed message back to the queue means it will be delivered
again, and `7_sequence` needs the same claim to record a superseded event as
handled without applying it.

## On its own

[`local_first`](local_first) is not a step in that sequence. It moves the API's
store onto local disk, an availability change rather than a fix for anything the
campaign named, and undoes `2_outbox`'s transactional guarantee on the way. It
is kept because it is the one fleet here whose defect no proxy can see: both
writes are local and no edge lies between them.

## What the campaigns find

Appendix A of the project report carries the figures, rung by rung, with what
each fix was predicted to do and what it did. They are not repeated here,
because a sweep would leave nine files stale and the report is the copy that
has to be right.

Two things to know before reading them there. The figures come from unbounded
runs, with the scenario's `budget` line dropped so every schedule the scheduler
produces is run. And the schedule count moves between rungs, because the fit is
derived from a fresh learn run each time and a fleet carrying more traffic
offers more places to break it.
