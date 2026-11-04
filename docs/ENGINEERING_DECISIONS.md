# Engineering decisions

Each entry states the decision, why, what was rejected, and what it costs.

## 1. The manifest row commits in the same transaction as the data

`store.load_file` writes quarantine rows, upserts `orders` and inserts the `manifest` row, then
commits once. "File is done" and "file's effects are visible" are therefore the same event.

- Rejected: write data, then mark the file done in a separate step (or a separate state file).
  A crash between the two loads the file twice on rerun; that is the bug this project exists to
  avoid.
- Rejected: a temp table per file swapped in at the end. Same guarantee, more moving parts,
  and still needs the manifest update to be atomic with the swap.
- Cost: one transaction per file, so a very large file is one large transaction. Fine for the
  file sizes here; see README Limitations.

The crash points in `crash.py` (`after_quarantine`, `after_upsert`, `before_commit`,
`after_commit`) sit between these writes, and `tests/test_restart_safety.py` kills a real child
process at each of them with `os._exit`, which skips `finally` blocks and the commit, as
`kill -9` would.

## 2. Idempotency comes from the upsert predicate, not from "load once"

The manifest already prevents reloading a file, but the upsert would also be safe if it did
not: `UPSERT` in `store.py` only replaces a row when the incoming
`(event_time, src_file_seq, src_line)` is strictly greater (`_newer`). Reapplying the same rows
changes nothing; an older, late event changes nothing. This is the same ordering as
`ValidRow.precedence`, used by `dedup.latest_per_key` inside a file, so there is one rule, applied
in two places.

- Rejected: last write wins by load order. Late data would overwrite newer state.
- Rejected: event time only. Two events with the same timestamp (a restatement) would leave the
  result dependent on upsert order. Adding file and line makes the order total.
- Cost: lineage columns `src_file_seq` and `src_line` are stored in `orders`.

## 3. `file_seq` is derived from committed state

`next_file_seq` reads `max(file_seq) + 1` from the manifest just before the file is parsed, and
the number becomes permanent only when that file's manifest row commits. A crashed file's number is not consumed, so the rerun assigns the same
numbers a clean run would, which is required for byte-identical lineage columns. This is safe
only because DuckDB allows one writing process per database file (see 7).

## 4. Content hash, not file name, identifies a drop

`ingest.sha256_of` hashes the bytes; `manifest.sha256` is `UNIQUE`. A re-sent file under a new
name is recorded in `duplicate_drops` and skipped; a changed file under an old name is a new
drop. Name-based tracking fails both cases. Cost: two byte-identical files that are really two
different deliveries cannot both load; for event feeds that is the desired behaviour.

## 5. Line-oriented parsing

Both parsers split on `\n` first (`ingest._physical_lines`), strip `\r` and a UTF-8 BOM, then
decode and parse each line on its own. Every quarantine entry gets the exact physical line, a bad
byte only affects its own line, and CRLF files from Windows producers parse the same as LF files.
The cost is no support for CSV fields with embedded newlines; such rows are quarantined, not
misread (`test_csv_field_with_embedded_newline_is_quarantined_not_misread`).

JSON is parsed with `parse_float=Decimal`, so `19.99` stays exactly `19.99` all the way to
`DECIMAL(12, 2)`.

## 6. Schema drift: reject missing, tolerate extra

`ingest.check_columns` compares the file's columns (CSV header, or the union of keys across
parsable JSONL lines) with `schema.REQUIRED_COLUMNS`. A missing required column rejects the whole
file with a reason in `manifest.reject_reason`, because quarantining every row of a renamed
column hides a contract change behind a high quarantine rate. An extra column is recorded in
`manifest.extra_columns` and ignored, since additive changes are common and harmless. A JSONL row
that lacks a key the rest of the file has is a row problem and goes to quarantine.

## 7. DuckDB, embedded

A real SQL engine with transactions and `ON CONFLICT ... DO UPDATE ... WHERE`, no server, one file.
Its file lock gives the single-writer guarantee decisions 3 relies on; a second process fails
with `IOException`, which the CLI turns into exit code 3
(`test_second_writer_is_refused_while_the_database_is_open`). The shared portfolio convention is
PostgreSQL; this project's brief asked for an embedded database so it runs with `uv sync` alone.
Moving to PostgreSQL would need an advisory lock or a `SELECT ... FOR UPDATE` on a run-lock row
in place of the file lock.

`connect` always closes its handle (context manager). On Windows an open DuckDB file cannot be
deleted or opened by another process, which matters for tests and for the demo's child processes.

Timestamps are stored as naive UTC `TIMESTAMP`, not `TIMESTAMPTZ`, so results never depend on
the session time zone and no time zone database is needed in Python.

## 8. The report depends only on state and `as_of`

`quality.run_checks` reads the tables and the given `as_of`; it does not include wall-clock time,
run ids or per-run counters, and it is serialised with sorted keys and LF. The same warehouse
state therefore gives the same report bytes, which lets the restart test compare reports.
Per-run information (what this run loaded or skipped) is printed by the CLI instead. Checks are
cumulative over the warehouse rather than per run; a per-run history would be a separate table.

The thresholds are a pydantic model with `extra="forbid"` (`config.Thresholds`), so a typo in the
TOML fails the run rather than silently using a default. Column names for null-rate checks come
from a fixed allow-list (`config.NULLABLE_COLUMNS`) because they are interpolated into SQL.

## 9. Byte-identity is checked on exports, not on the database file

DuckDB's on-disk layout depends on write history (for example whether changes were checkpointed
or replayed from the write-ahead log), and nothing guarantees it is byte-stable, so comparing
`.duckdb` files would test DuckDB internals rather than this pipeline.
`export.export_tables` writes every table with a total `ORDER BY` on its key, fixed formatting
and LF endings; together with the report these are the observable outputs.

## 10. Generator computes its own ground truth

`generator.generate` records what it emitted and computes the expected final state in
`generator._expected`, without going through the pipeline. `test_full_run_matches_generator_ground_truth`
compares the warehouse against it. The rule is the same one, but implemented independently over
the generator's own records, so parsing, normalisation, `file_seq` assignment and the SQL upsert
are all tested end to end. A pinned digest of the seed-42 output
(`test_golden_digest_for_default_demo_data`) checks that generation is the same on Windows and
Linux.

## 11. Standard library CLI

`argparse` covers five subcommands; a CLI framework would be a dependency for no new behaviour.
Exit codes are documented in the README and tested in `tests/test_quality.py`.
