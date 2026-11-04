# Interview guide

Hard questions an interviewer is likely to ask about this project, with answers that point at the
code. Then the main tradeoffs and a live demo script.

## Five hard questions

### 1. "Your process is killed halfway through a file. Walk me through why the rerun does not double count."

`store.load_file` opens one transaction and writes, in order: quarantine rows, staging rows and
the `UPSERT` into `orders`, then the `manifest` row, then commits. The manifest row is the only
thing `pipeline.process_file` looks at (via `store.find_by_hash`) to decide a file is done, and it
becomes visible at the same commit as the data. Killed before `COMMIT`: DuckDB discards the
uncommitted transaction, the file has no manifest row, and the rerun loads it from scratch with
the same `file_seq` (assigned by `store.next_file_seq` inside the transaction). Killed after
`COMMIT`: the file is complete and the rerun reports it as `already_loaded`.

Evidence: `tests/test_restart_safety.py::test_crash_then_rerun_is_byte_identical_to_clean_run`
runs the real CLI in a child process with `DATAPULSE_CRASH_AT=<point>:<file>` for all four points
in `crash.POINTS` on two files, so `os._exit` kills the child, then reruns and compares every
table export and the report byte for byte with a clean run.

Follow-up, "what if the manifest check passed but the upsert ran twice?": the upsert would still
be a no-op, because `store._newer` only replaces a row whose `(event_time, src_file_seq,
src_line)` is strictly greater.

### 2. "How do you decide which record wins, and is it deterministic?"

`ValidRow.precedence` in `schema.py` is `(event_time, file_seq, line)`; the greatest wins.
`dedup.latest_per_key` applies it within a file, and the `WHERE` of `store.UPSERT` applies the
same comparison against what is already stored. Event time first means a late, older event does
not overwrite newer state (counted as `rows_stale` in the manifest). File and line break exact
ties, which is how a restatement (same event time, corrected amount, later file) wins. The tuple
is a total order over distinct source positions, so the result does not depend on input order:
`tests/test_dedup.py::test_result_is_independent_of_input_order_and_sorted_by_key` checks all
permutations.

The honest caveat: `file_seq` is load order. Within one run that is name order
(`pipeline.discover` sorts by name); across runs a later delivery is always later. If a producer
names files so that name order differs from real order, ties break by name order.

### 3. "A partner renames a column. What happens, and why not just quarantine those rows?"

`ingest.read_file` calls `check_columns` on the CSV header, or on the union of keys across the
JSONL lines that parse. If a required column is missing the whole file is rejected: one manifest
row with `status = 'rejected'` and the reason, nothing in `orders` or `quarantine`, and the
`rejected_files` check counts it. Quarantining every row would hide a contract change inside the
quarantine rate and fill the table with identical errors. Extra columns are not a break, so the
file loads and `manifest.extra_columns` records them.

Because the rejected file has a manifest row, rerunning does not retry it; the producer sends a
fixed file, which has a new hash. Tests: `tests/test_ingest.py::test_schema_drifted_csv_is_rejected_as_a_whole`
and `test_extra_column_is_recorded_but_file_loads`.

### 4. "How do you know the pipeline loads the right data, not just some data?"

The generator knows what it emitted. `generator._expected` computes the final state per order
from its own records, skipping the rows it corrupted and the files it made unloadable, and
`tests/test_pipeline.py::test_full_run_matches_generator_ground_truth` compares the whole `orders`
table to it, plus the exact count of quarantined lines, the rejected file and the duplicate drop.
That covers parsing, time zone normalisation (about 15% of generated timestamps carry `+07:00`), decimal
handling, `file_seq` assignment and the SQL upsert together. Unit tests in
`tests/test_validation.py` check each field rule and the stored reason.

### 5. "What breaks first at a hundred times the volume, and what would you change?"

In order:

1. `ingest.read_file` reads each file whole with `Path.read_bytes`, and `load_file` inserts staging
   rows with `executemany`. Memory and insert speed are the first limits. I would stream lines
   and bulk insert through Arrow, or let DuckDB's `read_csv` / `read_json` load staging and keep
   Python for validation errors only. I have not measured throughput, so I would write a
   benchmark script first and quote only its numbers.
2. One transaction per file: a huge file becomes a huge transaction. Splitting it needs a
   per-chunk manifest (file hash plus chunk index) so the same restart argument still holds.
3. One writer: DuckDB's file lock serialises runs. That is a feature at this size; for parallel
   loads I would move to PostgreSQL with a run lock and keep the same transaction design.

## Tradeoffs worth defending

| Choice | Gain | Cost |
| --- | --- | --- |
| Manifest in the same transaction (`store.load_file`) | Crash anywhere is safe | One transaction per file |
| Hash-based manifest (`ingest.sha256_of`) | Re-drops detected regardless of name | Identical bytes can never load twice |
| Line-oriented parsing (`ingest._physical_lines`) | Exact line numbers; one bad byte affects one line | No embedded newlines in CSV fields |
| Reject on missing column, tolerate extra (`ingest.check_columns`) | Contract breaks are loud | A fixed file must be re-sent |
| Report from state only (`quality.run_checks`) | Byte-comparable reports | No per-run history in the report |
| Naive UTC `TIMESTAMP` | No session time zone effects | Offsets are not preserved (normalised at validation) |
| DuckDB embedded | No server, real transactions | Single writer, single machine |

## Live demo (about five minutes)

```sh
uv sync --locked
uv run datapulse demo
```

Points to make while it runs:

1. Step 1 lists two trap files: the `_partner.csv` with a renamed column and the `_resend.csv`
   that is byte-identical to an earlier file.
2. Step 2: each file's valid, quarantined and stale counts. The partner file is `rejected`, the
   resend is a `duplicate_drop`, the day-6 JSONL loads with `extra columns: channel`.
3. Step 3: the child process exits with 86 while loading `orders_2026-01-05.csv`, after its
   writes but before `COMMIT`; the manifest has 4 files.
4. Step 5: every export and the report have the same SHA-256 for the clean run and the
   crash-plus-rerun.
5. The final lines: the report passes with the demo thresholds and fails `rejected_files` with
   `max_rejected_files = 0`, which is what makes `run` exit 1.

Then show a quarantine row and the code path:

```sh
head -3 .datapulse-demo/clean-export/quarantine.csv
uv run pytest -q tests/test_restart_safety.py
```

and open `src/datapulse/store.py` at `load_file` and `_newer`.

To show the crash by hand:

```sh
uv run datapulse generate --out landing
DATAPULSE_CRASH_AT=after_upsert:orders_2026-01-04.jsonl \
  uv run datapulse run --landing landing --db w.duckdb --config datapulse.toml --as-of 2026-01-09T00:00:00Z
echo $?   # 86
uv run datapulse run --landing landing --db w.duckdb --config datapulse.toml --as-of 2026-01-09T00:00:00Z
```

(In PowerShell set the variable with `$env:DATAPULSE_CRASH_AT = "..."` and remove it with
`Remove-Item Env:DATAPULSE_CRASH_AT` before the rerun.)
