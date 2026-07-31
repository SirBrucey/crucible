# crucible-worker

The worker binary. Owns exactly one isolated fleet replica: it brings the
replica up, runs the scenario it is dispatched, and reports the verdict back to
the runner. Workers are separate processes, so a replica that dies cannot take
the campaign with it.
