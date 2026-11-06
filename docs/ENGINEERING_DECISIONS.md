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

`pipeline.snapshot` reads the file once; `ingest.sha256_hex` hashes those bytes and
`ingest.read_file` parses the same bytes, so a file changing on disk cannot pair one version's
hash with another version's rows. `manifest.sha256` is `UNIQUE`. A re-sent file under a new
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

## 11. Producer contract: atomic rename preferred, settle interval as a defence

A content hash cannot tell a half-written file from a finished one: without a contract, the
partial file loads, its truncated last line is quarantined, and the finished file later loads
again as a new drop. The contract (README, "Producer contract") prefers write-then-rename, the
only option that is safe in all cases. For producers that write in place, `pipeline.snapshot`
requires an mtime older than `settle_seconds` and the same size and mtime before and after the
read.

- Rejected: a completion marker per file (`a.csv.done`). Reliable, but it adds a second file to
  every feed and its own ordering problem.
- Rejected: requiring a trailing newline. A producer that omits it would wait forever.
- Choice: stop at the first `not_ready` file rather than skip it. Later files wait, so load order
  stays name order and tie-breaks do not depend on which producer finished first. Cost: a file
  that never settles blocks those behind it, visibly, as `not_ready` and `waiting`.

## 12. Strict parsing, not pydantic's lax mode

Lax mode reads the number `20260101` as Unix time (1970-08-23), `1_0` as 10 and `5.0` as an
integer. `schema.py` puts `mode="before"` validators in front of those fields, which accept only
the forms in `ISO_8601`, `INTEGER` and `DECIMAL`. A value that needs guessing is quarantined.
Timestamps with more than 6 fractional digits are quarantined rather than truncated, because
truncation could create ties. Repeated JSON keys fail through `object_pairs_hook`
(`ingest._no_duplicate_keys`), since `json.loads` would silently keep the last.

## 13. Upper bound on event time

"Latest wins" trusts event time, so one event dated 2099 would pin an order forever and make
freshness look perfect. `ingest.validate_record` quarantines rows after `as_of` plus
`[ingest] max_future_skew_hours`, and `quality.run_checks` fails freshness when the newest stored
event is further ahead than that (for example after loading with a later `as_of`). The reason
text is fixed and does not contain the limit, so a rerun at another time does not change the
quarantine bytes for rows that are future in both runs. Cost: the boundary depends on `as_of`, so
byte-identical reruns need the same `--as-of`, and a quarantined row is not reconsidered once its
time has passed.

## 14. A duplicate check that can fail

An earlier `duplicate_keys` check counted repeated `order_id` in `orders`, which the primary key
makes impossible. It was replaced by `duplicate_rate`: valid rows repeating an `order_id` and
event time already seen in the same file (`manifest.rows_duplicate`), over valid rows. A producer
that starts double-sending shows up here. Repeats across files are not counted.

## 15. Standard library CLI

`argparse` covers five subcommands; a CLI framework would be a dependency for no new behaviour.
Exit codes are documented in the README and tested in `tests/test_quality.py`.
