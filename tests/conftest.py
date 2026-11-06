from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from datapulse import store
from datapulse.config import IngestSettings, Thresholds
from datapulse.crash import ENV_VAR
from datapulse.export import export_tables
from datapulse.generator import Generation, generate
from datapulse.pipeline import RunOptions
from datapulse.quality import run_checks, write_report

AS_OF = "2026-01-09T00:00:00Z"
# Fixed as_of (matches the generator) and no settle wait: test files are written just now.
OPTS = RunOptions(as_of=datetime.fromisoformat(AS_OF), ingest=IngestSettings(settle_seconds=0))


@pytest.fixture
def generated(tmp_path: Path) -> Generation:
    return generate(tmp_path / "landing", seed=42, n_orders=120, n_days=8)


def run_cli(*args: str, crash: str | None = None) -> subprocess.CompletedProcess[str]:
    """Run the real CLI in a child process, so an injected crash kills only the child."""
    env = {k: v for k, v in os.environ.items() if k != ENV_VAR}
    if crash:
        env[ENV_VAR] = crash
    return subprocess.run(
        [sys.executable, "-m", "datapulse", *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def outputs(db: Path, out: Path, as_of: str = AS_OF) -> dict[str, bytes]:
    """Every observable output of a warehouse: table exports plus the DQ report, as bytes."""
    from datetime import datetime

    with store.connect(db, read_only=True) as con:
        files = export_tables(con, out)
        report = run_checks(con, Thresholds(max_rejected_files=1), datetime.fromisoformat(as_of))
    write_report(report, out / "report.json")
    files.append(out / "report.json")
    return {p.name: p.read_bytes() for p in files}


def digest(files: dict[str, bytes]) -> dict[str, str]:
    return {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}
