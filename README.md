# Northwind Freight — synthetic data + DAG pipeline

Two parts, built separately:

1. **`generate_data.py`** — makes up 90 days of fake shipment data (Jan 1 – Mar 31, 2026)
   and writes it to `data/shipments.csv`, `data/lanes.csv`, `data/fuel.csv`.
2. **`pipeline.py`** — a small DAG runner, backed by DuckDB, that turns that raw
   data into a daily revenue/cost/margin report per shipping lane.

If you've never touched this project before, read this file top to bottom once —
each section builds on the last.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt      # duckdb, pinned to 1.5.5
python3 generate_data.py             # writes data/shipments.csv, data/lanes.csv, data/fuel.csv
```

`generate_data.py` prints a PASS/FAIL verification block after writing the files.
All PASS means the data matches its own spec (90 dates, 900 unique shipment IDs,
909 total rows, 6 deliberately-late shipments). Nothing downstream should be run
until that block is all green.

## Data assumptions

These are choices baked into the synthetic data and the pipeline's math — not
things you can infer from the code alone, so they're written down here:

- **Money is always integer cents**, never a float. `116565` means $1,165.65.
- **3 static lanes**, each with a fixed distance, a fixed rate per mile, and a
  fixed "normal" transit time (1, 2, or 3 days). Lanes never change once
  generated.
- **1 fuel surcharge rate per calendar date**, cents per mile, cycling 45–54¢/mi.
- **9 exact duplicate shipment rows** are deliberately injected (same
  `shipment_id`, same everything) so the pipeline has something real to
  deduplicate. `staging_shipments` collapses them with `GROUP BY shipment_id`
  using `MIN()` on every column — safe here because duplicates are byte-identical,
  so `MIN()` can't accidentally pick a "wrong" version.
- **6 shipments arrive 7 days late** (2 per day, on March 29, 30, and 31) instead
  of following their lane's normal transit time. These exist to exercise the
  `--as-of` cutoff logic (see below).
- **Cost has no real source in the data**, so `intermediate.sql` assumes a cost
  model explicitly: the fuel surcharge is a pure pass-through cost (no markup),
  and the freight charge carries an 80% cost ratio — i.e. a flat 20% margin on
  the freight portion of revenue. This is a modeling choice, not something
  derived from the generator.

## `shipment_date` vs. `arrival_date` — read this before running anything

This is the single most important concept in the pipeline, and it's easy to
get backwards:

- **`shipment_date` decides which partition a shipment belongs to.** A
  shipment that shipped on March 29 is *always* March 29's shipment, in every
  table, forever — the `mart`/`export` row for March 29 is about shipments
  that shipped that day, not shipments that arrived that day.
- **`arrival_date` decides whether that shipment is visible yet, given a
  cutoff.** `raw.sql` only loads a shipment if `arrival_date <= --as-of`. A
  shipment can ship on March 29 and still be invisible to a March 31 cutoff,
  if it hasn't arrived by then.

Concretely, with this generated data:

| Cutoff (`--as-of`) | shipments visible for March 29 | March 30 | March 31 |
|---|---|---|---|
| `2026-03-31` | 5 of 10 | 3 of 10 | 0 of 10 |
| `2026-04-07` | 10 of 10 | 10 of 10 | 10 of 10 |

At a March 31 cutoff, March 31's own shipments show up as **0** — nothing
shipped that day has arrived yet by the cutoff, even shipments with a normal
1-day transit time. That's not a bug; it's what "as of March 31" means. Verify
it yourself:

```bash
python3 pipeline.py backfill --start 2026-01-01 --end 2026-03-31 --as-of 2026-03-31
python3 inspect_results.py    # shows the 5 / 3 / 0 breakdown above
```

## Determinism: what must match, and what's allowed to change

- **Same inputs, same `--as-of` → identical CSV bytes, every time.** Nothing in
  the pipeline uses randomness or wall-clock time as an input to a
  calculation. Run the same command twice and every exported CSV is
  byte-for-byte identical (checked in `verify.py`).
- **A newer `--as-of` legitimately changes results — this is not
  nondeterminism.** A wider cutoff makes more shipments visible (never fewer),
  so revenue/cost/margin for the affected dates can legitimately go up. The
  table above is the same command with two different cutoffs producing two
  different, both-correct answers.
- The one thing that must never happen: asking for an **old** `--as-of` again,
  after a newer one has run, and getting the *newer* cutoff's numbers back
  instead. `pipeline.py` tracks which cutoff is currently loaded into each
  partition specifically to prevent this — see "Known limitations" for the
  one place this still needs care.

## Pipeline architecture (the DAG)

```
raw ──┬──> staging_shipments ──┐
      ├──> staging_lanes ──────┼──> intermediate ──> mart ──> export
      └──> staging_fuel ───────┘
```

| Task | Reads | Writes | Scope |
|---|---|---|---|
| `raw` | `data/*.csv` | `raw_shipments`, `raw_lanes`, `raw_fuel` | global — filtered by `--as-of`, not by date |
| `staging_shipments` | `raw_shipments` | `staging_shipments` | one date partition — deduplicated |
| `staging_lanes` | `raw_lanes` | `staging_lanes` | global — static reference data |
| `staging_fuel` | `raw_fuel` | `staging_fuel` | one date partition |
| `intermediate` | all three staging tables | `intermediate` | one date partition — computes revenue/cost/margin |
| `mart` | `intermediate` | `mart` | one date partition — aggregates by date + lane |
| `export` | `mart` | `work/exports/shipments_<date>.csv` | one date partition, written atomically |

The dependency graph is declared once, as data, in `pipeline.py`'s
`DEPENDENCIES` dict — nothing about execution order is hard-coded elsewhere.
Before running anything, `pipeline.py` checks that dict for an unknown parent
or a cycle and refuses to run if it finds one.

**If an upstream task fails or is blocked, every task downstream of it is
blocked too** — it's never executed, and no partial data from it exists.
`raw`/`staging_lanes` are the two "global" tasks: they aren't split by date,
so they're computed once per `--as-of` cutoff and then skipped for every
other date sharing that same cutoff (a real efficiency win during a
backfill) — everything else is computed and checkpointed once per date.

## Commands

Every command below was run against this exact codebase to confirm it works
as documented.

**Process one date:**
```bash
python3 pipeline.py run --date 2026-01-05 --as-of 2026-01-31
```
```
[2026-01-05] raw=success, staging_shipments=success, staging_lanes=success, staging_fuel=success, intermediate=success, mart=success, export=success
```

**Backfill a range of dates** (use a cutoff on or after the latest date's
possible arrival — the late shipments on Mar 29–31 need `--as-of 2026-04-07`
or later to be included):
```bash
python3 pipeline.py backfill --start 2026-01-01 --end 2026-03-31 --as-of 2026-04-07
```
```
...
[2026-03-31] raw=skipped, staging_shipments=success, staging_lanes=skipped, staging_fuel=success, intermediate=success, mart=success, export=success
```

**Inject a failure**, to see the rollback and blocking behavior (exits `1`):
```bash
python3 pipeline.py run --date 2026-02-10 --as-of 2026-02-17 --fail-task intermediate
```
```
[2026-02-10] raw=success, staging_shipments=success, staging_lanes=success, staging_fuel=success, intermediate=failed, mart=blocked, export=blocked
```
`intermediate` failed after its `DELETE` and before its `INSERT`, so the
transaction rolled back — zero partial rows, `mart` and `export` never ran.

**Recover** — just run the same command again, without `--fail-task`:
```bash
python3 pipeline.py run --date 2026-02-10 --as-of 2026-02-17
```
```
[2026-02-10] raw=skipped, staging_shipments=skipped, staging_lanes=skipped, staging_fuel=skipped, intermediate=success, mart=success, export=success
```
`raw`/`staging_*` were already checkpointed from the failed attempt (they
succeeded before `intermediate` failed), so recovery only redoes what
actually needs redoing.

**Force a full recompute**, ignoring every checkpoint:
```bash
python3 pipeline.py run --date 2026-01-05 --as-of 2026-01-31 --force
```

## Checking the results

```bash
python3 inspect_results.py
```
prints, straight from `work/warehouse.duckdb`:
- total shipment rows (raw vs. deduplicated)
- which `shipment_id`s were duplicated in the raw data, and how many rows each has
- daily shipment counts for March 28–31 specifically (the window where the
  `--as-of` cutoff matters most)

```bash
python3 verify.py
```
runs a full isolated test suite (rollback, blocking, recovery, determinism,
backfill extension) in a throwaway copy of the project — it never touches
this project's own `data/` or `work/`. See the code review notes at the
bottom of this file for what it checks and why.

**Run log**: every task attempt, across every run and backfill ever executed,
is appended to `work/run_log.csv` (never overwritten). Columns: `run_id`,
`command`, `task`, `shipment_date`, `as_of`, `scope_key`, `outcome`,
`checkpoint_hash`, `started_at`, `ended_at`, `duration_ms`, `message`.

## Known limitations

- **Single-writer, sequential.** Tasks run one at a time, in one process,
  against one DuckDB file. There's no parallelism and no support for two
  `pipeline.py` invocations running against the same `work/warehouse.duckdb`
  at once.
- **`--force` forces everything**, not one task. There's no "just recompute
  `mart`" — it's all seven tasks, for every date in the run/backfill.
- **`raw_shipments` and `raw_lanes`/`raw_fuel` are shared, not versioned.**
  They physically hold the data for whichever `--as-of` cutoff was loaded
  *most recently* — `inspect_results.py` prints a reminder of this. Never read
  those raw tables directly to answer a "how much revenue as of cutoff X"
  question; always go through `staging_shipments`/`mart`, which are correctly
  partitioned and checkpoint-guarded per `(date, as_of)`.
- **`pipeline_checkpoints` and `pipeline_partition_scope` grow forever.**
  Nothing prunes old rows. For a real production system this would need a
  retention policy; for this assignment's scale it's a non-issue.
- **No schema migration story.** `sql/_schema.sql` uses
  `CREATE TABLE IF NOT EXISTS`, so if you change a table's columns later, an
  existing `work/warehouse.duckdb` won't pick up the change — delete
  `work/` and re-run.
- **No input validation beyond DuckDB's own CSV type inference.** A malformed
  `data/*.csv` will load whatever DuckDB can parse from it, silently, rather
  than raising a clear error.

## What the code review found

A full review against append-only behavior, missing date filters, unstable
CSV ordering, late-arrival filtering, stale shared-table checkpoints, and
post-failure execution turned up one real bug (since fixed): a task's
checkpoint was keyed by `(task, date, as_of)`, but its physical storage was
keyed only by `(task, date)` — so revisiting a date under an older `--as-of`,
after a newer one had overwritten that same partition, would report
`skipped` and silently serve the newer cutoff's data. Fixed by adding
`pipeline_partition_scope`, which tracks which `as_of` is currently resident
in each physical partition; a checkpoint is only honored if that matches too.
`verify.py` includes a regression test for exactly this scenario. Full
details, evidence, and the one non-bug finding (the checklist's assumed
"894 unique shipments at cutoff March 31" doesn't match this data's transit
times — the correct figure is 878) are in that review's original write-up.
