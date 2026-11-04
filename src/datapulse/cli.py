"""Command-line interface (argparse, standard library)."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from pydantic import ValidationError

from datapulse import __version__, pipeline, store
from datapulse.config import load_config
from datapulse.export import export_tables
from datapulse.generator import generate
from datapulse.quality import Report, run_checks, write_report

EXIT_OK = 0
EXIT_CHECKS_FAILED = 1
EXIT_USAGE = 2  # argparse's own code for bad arguments
EXIT_ERROR = 3


def _as_of(text: str) -> datetime:
    try:
        value = datetime.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an ISO 8601 timestamp: {text!r}") from exc
    if value.tzinfo is None:
        raise argparse.ArgumentTypeError("--as-of needs a UTC offset, e.g. 2026-01-09T00:00:00Z")
    return value


def _add_check_options(p: argparse.ArgumentParser) -> None:
    p.add_argument("--config", type=Path, help="TOML file with [checks] thresholds")
    p.add_argument(
        "--report", type=Path, help="JSON report path (default: report.json next to the db)"
    )
    p.add_argument(
        "--as-of", type=_as_of, default=None,
        help="reference time for the freshness check (default: now, UTC)",
    )  # fmt: skip


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="datapulse",
        description=("Restart-safe incremental pipeline: landing CSV/JSONL files into DuckDB."),
    )
    parser.add_argument("--version", action="version", version=f"datapulse {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="write seeded synthetic landing files")
    g.add_argument("--out", type=Path, required=True)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--orders", type=int, default=400)
    g.add_argument("--days", type=int, default=8)

    r = sub.add_parser("run", help="load new landing files, then run data-quality checks")
    r.add_argument("--landing", type=Path, required=True)
    r.add_argument("--db", type=Path, required=True)
    _add_check_options(r)

    c = sub.add_parser("check", help="run data-quality checks only")
    c.add_argument("--db", type=Path, required=True)
    _add_check_options(c)

    e = sub.add_parser("export", help="write every table as deterministic CSV")
    e.add_argument("--db", type=Path, required=True)
    e.add_argument("--out", type=Path, required=True)

    d = sub.add_parser("demo", help="generate, crash on purpose, rerun, compare, report")
    d.add_argument("--workdir", type=Path, default=Path(".datapulse-demo"))
    d.add_argument("--seed", type=int, default=42)
    return parser


def _show(value: object) -> str:
    return json.dumps(value, default=str, sort_keys=True)


def print_report(report: Report, path: Path) -> None:
    for check in report.checks:
        mark = "PASS" if check.passed else "FAIL"
        observed, threshold = _show(check.observed), _show(check.threshold)
        print(f"  {mark}  {check.name:<24} observed={observed}  threshold={threshold}")
    print(f"  report: {path.as_posix()}  ({'passed' if report.passed else 'FAILED'})")


def checks_and_report(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    as_of = args.as_of or datetime.now(UTC)
    report_path = args.report or args.db.parent / "report.json"
    with store.connect(args.db, read_only=True) as con:
        report = run_checks(con, config.checks, as_of)
    write_report(report, report_path)
    print_report(report, report_path)
    return EXIT_OK if report.passed else EXIT_CHECKS_FAILED


def print_summary(summary: pipeline.RunSummary) -> None:
    for o in summary.outcomes:
        print(f"  {o.action:<15} {o.file_name}" + (f"  ({o.detail})" if o.detail else ""))


def _dispatch(args: argparse.Namespace) -> int:
    match args.command:
        case "generate":
            gen = generate(args.out, seed=args.seed, n_orders=args.orders, n_days=args.days)
            print(f"wrote {len(gen.files)} files to {args.out.as_posix()} (seed {args.seed})")
            return EXIT_OK
        case "run":
            # Validate the config before loading anything, so a typo fails fast.
            load_config(args.config)
            print_summary(pipeline.run(args.landing, args.db))
            return checks_and_report(args)
        case "check":
            if not args.db.is_file():
                raise FileNotFoundError(f"database not found: {args.db}")
            return checks_and_report(args)
        case "export":
            with store.connect(args.db, read_only=True) as con:
                for path in export_tables(con, args.out):
                    print(f"  wrote {path.as_posix()}")
            return EXIT_OK
        case "demo":
            from datapulse.demo import run_demo

            return run_demo(args.workdir, seed=args.seed)
    raise AssertionError(args.command)  # pragma: no cover - argparse restricts choices


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return _dispatch(args)
    except (
        OSError,
        duckdb.Error,
        ValidationError,
        tomllib.TOMLDecodeError,
        store.SchemaVersionError,
        ValueError,
    ) as exc:
        print(f"datapulse: error: {exc}", file=sys.stderr)
        return EXIT_ERROR
