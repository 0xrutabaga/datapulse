"""Crash injection for restart-safety tests and the demo.

Set ``DATAPULSE_CRASH_AT`` to ``<point>`` or ``<point>:<file name>``. When the pipeline reaches
that point the process ends immediately with ``os._exit`` (no ``finally`` blocks, no commit, no
flush), which is the closest portable stand-in for ``kill -9`` or a power cut.
"""

from __future__ import annotations

import os
import sys
from typing import Final

ENV_VAR: Final = "DATAPULSE_CRASH_AT"
CRASH_EXIT_CODE: Final = 86
POINTS: Final = ("after_quarantine", "after_upsert", "before_commit", "after_commit")


def parse_spec(spec: str) -> tuple[str, str | None]:
    point, _, file_name = spec.partition(":")
    if point not in POINTS:
        raise ValueError(f"{ENV_VAR}: unknown crash point {point!r}; expected one of {POINTS}")
    return point, file_name or None


def maybe_crash(point: str, file_name: str) -> None:
    spec = os.environ.get(ENV_VAR)
    if not spec:
        return
    wanted_point, wanted_file = parse_spec(spec)
    if wanted_point == point and wanted_file in (None, file_name):
        sys.stderr.write(f"datapulse: injected crash at {point} while loading {file_name}\n")
        sys.stderr.flush()
        os._exit(CRASH_EXIT_CODE)
