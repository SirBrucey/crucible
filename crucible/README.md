# crucible

The two binaries a campaign runs as.

`crucible` owns the campaign: it learns the fleet's traffic, derives fault
schedules from it, drives a pool of workers over them, and tallies the verdicts.
`crucible check <file.cru>` validates a scenario without running anything.

`crucible-worker` owns exactly one isolated fleet replica: it brings the replica
up, runs the schedule it is dispatched, and reports the verdict back. Workers are
separate processes, so a replica that dies cannot take the campaign with it.

They share a crate because they share a protocol. A change to a message
recompiles both or neither, so neither can be built against a shape the other no
longer speaks.
