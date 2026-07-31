# crucible

The runner binary. Owns a campaign: it learns the fleet's traffic, derives fault
schedules from it, drives a pool of workers over them, and tallies the verdicts.
`crucible check <file.cru>` validates a scenario without running anything.
