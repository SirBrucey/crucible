# orders

One fleet, written three times. Each directory is the one before it, changed in
response to what its campaign found. Read them in order.

1. [`1_base`](1_base): an event-driven fleet written without accounting for the
   problems a distributed system has.
2. [`2_outbox`](2_outbox): the textbook fix for what `1_base` loses. It closes
   that gap and opens another.
3. [`3_local_first`](3_local_first): the fix for what made `2_outbox` fragile
   under a partition, which undoes the guarantee `2_outbox` depended on.

Each README says what changed, quotes the campaign output that motivated the
change, and reports what the change fixed, what it did not, and what it
introduced. `diff -r 1_base 2_outbox` is the change itself.

They quote verdicts, not tallies. How many schedules a five minute budget fits
depends on how fast the machine brought containers up that day, so the totals
move between runs while the verdicts do not.
