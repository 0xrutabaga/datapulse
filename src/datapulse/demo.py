"""`datapulse demo`: generate data, crash a load on purpose, rerun, prove the outputs match."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import tomllib
from datetime import datetime
from pathlib import Path

from datapulse import pipeline, store
from datapulse.cli import EXIT_CHECKS_FAILED, print_report, print_summary
from datapulse.config import Config, Thresholds
from datapulse.crash import CRASH_EXIT_CODE, ENV_VAR
from datapulse.export import export_tables
from datapulse.generator import generate
from datapulse.quality import Report, run_checks, write_report

MARKER = ".datapulse-demo"
CRASH_FILE = "orders_2026-01-05.csv"
DEMO_CONFIG_TOML = """# Settings used by `datapulse demo`, written next to the demo data.
[ingest]
max_future_skew_hours = 1
settle_seconds = 5  # generated files carry an old mtime, so they are already settled

[checks]
min_orders = 100
max_quarantine_rate = 0.05
max_rejected_files = 1  # the generator's schema-drifted partner file
max_freshness_lag_hours = 24
max_duplicate_rate = 0.1
amount_min = 0
amount_max = 100000

[checks.max_null_rate]
discount_code = 0.9
"""
DEMO_CONFIG = Config.model_validate(tomllib.loads(DEMO_CONFIG_TOML))


def _reset(workdir: Path) -> None:
    """Delete a previous demo directory, but never a directory the demo did not create."""
    if workdir.exists():
        if not (workdir / MARKER).is_file():
            if any(workdir.iterdir()):
                raise FileExistsError(f"{workdir} exists, is not empty and was not made by demo")
        else:
            shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / MARKER).write_text("created by `datapulse demo`; safe to delete\n", encoding="utf-8")


def _outputs(db: Path, out: Path, report_path: Path, report: Report) -> dict[str, str]:
    """Export tables and the report, return {file name: sha256}."""
    with store.connect(db, read_only=True) as con:
        files = export_tables(con, out)
    write_report(report, report_path)
    files.append(report_path)
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}


def _checks(db: Path, as_of: datetime) -> Report:
    with store.connect(db, read_only=True) as con:
        return run_checks(con, DEMO_CONFIG.checks, as_of, DEMO_CONFIG.ingest.max_future_skew_hours)


def _options(as_of: datetime) -> pipeline.RunOptions:
    return pipeline.RunOptions(as_of=as_of, ingest=DEMO_CONFIG.ingest)


def _cli_run(
    landing: Path, db: Path, config: Path, crash: str | None
) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if k != ENV_VAR}
    if crash:
        env[ENV_VAR] = crash
    cmd = [sys.executable, "-m", "datapulse", "run", "--landing", str(landing), "--db", str(db)]
    cmd += ["--config", str(config), "--as-of", "2026-01-09T00:00:00Z"]
    cmd += ["--report", str(db.with_suffix(".report.json"))]
    # Fixed argument list built from our own paths; no shell.
    return subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)  # noqa: S603


def _echo(text: str, indent: str = "") -> None:
    for line in text.splitlines():
        print(f"{indent}{line}")


def run_demo(workdir: Path, *, seed: int = 42) -> int:
    _reset(workdir)
    landing = workdir / "landing"
    config_path = workdir / "datapulse.toml"
    config_path.write_text(DEMO_CONFIG_TOML, encoding="utf-8", newline="\n")
    w = workdir.as_posix()

    print(f"[1/6] Generate seeded landing data (seed {seed}) into {w}/landing")
    gen = generate(landing, seed=seed)
    for path in gen.files:
        note = ""
        if path.name in gen.rejected_files:
            note = "  <- schema drift: `amount` renamed, `channel` added"
        elif path.name in gen.duplicate_files:
            note = "  <- byte-identical re-drop of an earlier file"
        print(f"  {path.name:<32} {path.stat().st_size:>6} bytes{note}")

    print("\n[2/6] Reference run with no crash -> clean.duckdb")
    clean_db = workdir / "clean.duckdb"
    print_summary(pipeline.run(landing, clean_db, _options(gen.as_of)))

    crash = f"before_commit:{CRASH_FILE}"
    print(f"\n[3/6] Same run in a child process with {ENV_VAR}={crash} -> crashed.duckdb")
    crashed_db = workdir / "crashed.duckdb"
    proc = _cli_run(landing, crashed_db, config_path, crash)
    _echo(proc.stdout)
    _echo(proc.stderr, indent="  ")
    print(f"  child exit code: {proc.returncode} (injected crash exits {CRASH_EXIT_CODE})")
    if proc.returncode != CRASH_EXIT_CODE:
        print("  unexpected: the injected crash did not happen")
        return 1
    with store.connect(crashed_db, read_only=True) as con:
        committed = con.execute("SELECT file_name FROM manifest ORDER BY file_seq").fetchall()
    names = [str(r[0]) for r in committed]
    where = "is not" if CRASH_FILE not in names else "IS (unexpected)"
    print(f"  manifest after crash: {len(names)} files committed; {CRASH_FILE} {where} among them")

    print("\n[4/6] Rerun the same command without the crash")
    proc = _cli_run(landing, crashed_db, config_path, None)
    _echo(proc.stdout)
    print(f"  child exit code: {proc.returncode}")

    print("\n[5/6] Compare outputs: clean run vs crash + rerun (table exports and DQ report)")
    clean_hashes = _outputs(
        clean_db, workdir / "clean-export", workdir / "clean-export" / "report.json",
        _checks(clean_db, gen.as_of),
    )  # fmt: skip
    rerun_hashes = _outputs(
        crashed_db, workdir / "rerun-export", workdir / "rerun-export" / "report.json",
        _checks(crashed_db, gen.as_of),
    )  # fmt: skip
    identical = clean_hashes == rerun_hashes
    for name, digest in clean_hashes.items():
        same = "identical" if rerun_hashes.get(name) == digest else "DIFFERENT"
        print(f"  {name:<20} {digest[:16]}  {same}")

    print("\n[6/6] Run the clean pipeline again: every file is already known")
    print_summary(pipeline.run(landing, clean_db, _options(gen.as_of)))
    after = _outputs(
        clean_db, workdir / "again-export", workdir / "again-export" / "report.json",
        _checks(clean_db, gen.as_of),
    )  # fmt: skip
    print(f"  outputs unchanged by the second run: {after == clean_hashes}")

    report = _checks(clean_db, gen.as_of)
    report_path = workdir / "report.json"
    write_report(report, report_path)
    print(f"\nData-quality report (as of {report.as_of})")
    print(f"  rows: {report.rows}")
    print_report(report, report_path)

    strict = Thresholds.model_validate({**DEMO_CONFIG.checks.model_dump(), "max_rejected_files": 0})
    with store.connect(clean_db, read_only=True) as con:
        strict_report = run_checks(con, strict, gen.as_of, DEMO_CONFIG.ingest.max_future_skew_hours)
    failing = [c.name for c in strict_report.checks if not c.passed]
    code = EXIT_CHECKS_FAILED if not strict_report.passed else 0
    print(
        f"\nWith max_rejected_files = 0 the same state fails {failing}; `run`/`check` exit {code}"
    )

    ok = identical and after == clean_hashes and report.passed
    print(f"\ndemo {'succeeded' if ok else 'FAILED'}: outputs of {w} are safe to delete")
    return 0 if ok else 1
