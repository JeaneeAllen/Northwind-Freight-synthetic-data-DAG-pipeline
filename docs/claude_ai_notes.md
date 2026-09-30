# AI Collaboration Notes

## Main Claude Prompts

- Generate 90 days of deterministic synthetic shipment, lane, and
  fuel-surcharge data with duplicate IDs and six late-arriving bills.
- Build a dependency-aware Python DAG runner with modular DuckDB SQL.
- Add idempotent date partitions, checkpoints, failure injection,
  a run log, and bounded backfills.
- Review the implementation, test its behavior, and document usage.

## AI Mistake I Challenged

The initial March 31 cutoff produced 878 available unique shipments
instead of the expected 894.

I asked Claude to reconcile the source records, staging counts,
and run log before changing the pipeline.

Claude identified that the generator used ordinary lane transit times
to set bill arrival dates. This placed 16 additional shipments beyond
the cutoff, alongside the six deliberately late bills.

The pipeline's arrival-date filter was working correctly.
The generator did not match the intended demonstration assumptions.

## Corrective Prompt and Change

I instructed Claude to treat arrival_date as bill availability rather
than physical delivery.

Ordinary bills should be available on shipment_date. Exactly two bills
per day for March 29–31 should arrive seven days later.

The correction targeted the generator and preserved the pipeline's
arrival-date filter. Related tests and documentation also needed to
reflect this assumption.

## Verification Evidence

### Initial Run: March 31 Cutoff

The verification script confirmed that the corrected 90-day run
contained 894 unique available shipments at the March 31 cutoff.
The six deliberately late bills were excluded.

### After Backfill: April 7 Cutoff

The observed results after backfilling March 29–31 were:
- 909 raw rows, including nine intentional duplicate rows.
- 900 staging rows and 900 distinct shipment IDs.
- Zero duplicate shipment IDs in staging.
- March 28–31: 10 shipments per day.

### Automated Verification

All 9 of 9 checks passed:

1. The initial March 31 cutoff produced 894 unique shipments.
2. An intermediate failure blocked mart and export.
3. Failure after DELETE rolled back without leaving partial rows.
4. Recovery reused successful upstream tasks and retried failed
   and blocked tasks.
5. A forced rerun produced a byte-identical CSV.
6. The April 7 backfill increased the total to 900 without duplicates.
7. All 87 dates outside the backfill remained byte-identical.
8. Repeating the corrected backfill produced identical output.
9. Reverting to an earlier cutoff triggered recomputation instead
   of serving stale results.

### Manual CSV Hash Check

I also forced the March 31 partition to recompute using the April 7
cutoff. Every task reported success, and the exported CSV retained
the same SHA-256 hash:

7b495b56f1216a4eaa93acf760ba22f038c46870079cb323d1bb517a3ef704b4

This confirms byte-identical CSV output for that partition after
recomputation with unchanged inputs and cutoff.

## What I Learned

Generated code needs review against explicit requirements.
Reconciling the unexpected counts helped identify a data-generation
mistake without changing correct pipeline logic.