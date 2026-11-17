# Duplicate completion and terminal originals

`wt close FOLLOWER --duplicate-of PRIMARY --no-code` links the same incident;
it does not bypass either ticket's acceptance gates. A closed primary with
current passing gate evidence completes the follower immediately and supplies
its commit/result. An open or reviewing primary keeps its followers waiting.

A terminal primary that was force-closed without passing its required gates
cannot satisfy a duplicate wait. New links are rejected with an explanation;
the follower and its claim remain unchanged. Reopen and verify the primary to
follow it, or keep the report independent.

Local save-time synchronization and the stage-tick sweep repair old stranded
links. The GitHub duplicate sweep performs the same repair when this host can
read both pipelines and the link's generation is current. It releases the
follower for independent work, retains the union of required gates, and records
`duplicate_release` history plus a `duplicate_result` snapshot with
`released: true, complete: false`. This is not a successful resolution. Local
feedback retains the primary's closing summary; GitHub history still links the
original result. The primary remains closed, with its caveats unchanged.

Missing/foreign GitHub pipelines and stale generations still require their
existing reconciliation; no missing evidence is treated as proof. A released
follower no longer automatically follows a reopened original: explicitly relink
it if that is the right incident relationship.

WT-96 reproduced CCC-229/284/287/292 waiting on closed CCC-142. The primary had
three command gates and no passing results, with its close explicitly noting
failed performance acceptance. Releasing these waits preserves all three gates
rather than falsely marking the reports fixed.
