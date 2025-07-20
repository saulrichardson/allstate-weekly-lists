"""Library entry-point for generating weekly assignment lists.

This module holds **no CLI logic** so it can be imported from notebooks,
scheduled jobs, or unit-tests without depending on argparse or environment
variables.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import yaml

from .employee import Employee
from .loaders import load_all_sources
from .rules import build_predicate
from .assigner import assign_tasks
from .postprocess import clean_tasks
from .writers import export_assignments


def run_weekly(
    *,
    base: Path,
    output_dir: Path,
    generate_pdf: bool = False,
    log: logging.Logger | None = None,
) -> None:
    """Run the full weekly-list pipeline.

    Parameters
    ----------
    base
        Directory that contains the *config/* folder and *data/* Excel files.
        In the repo layout this is usually ``Path('weekly-lists')`` but callers
        can point it to any location (e.g. an S3 mount).
    output_dir
        Where the per-employee Excel (and optional PDF) files will be written.
    generate_pdf
        If True, writers.export_assignments will attempt to create PDFs in
        addition to Excel workbooks.
    log
        Optional logger instance.  If omitted, a module-level logger is used.
    """

    log = log or logging.getLogger(__name__)

    cfg_dir = base / "config"
    employees_cfg_path = cfg_dir / "employees.yml"
    employees_raw = yaml.safe_load(employees_cfg_path.read_text()).get("employees", [])

    profiles: List[Employee] = []
    for p in employees_raw:
        profiles.append(
            Employee(
                name=p["name"],
                priority_level=p.get("priority_level", 100),
                predicate=build_predicate(p.get("predicate_cfg")),
                capacity_per_source=p.get("capacity_per_source", {}) or {},
            )
        )

    log.info("Loaded %d employee profiles", len(profiles))

    # ------------------------------------------------------------------
    # Load sources, assign, clean, export
    # ------------------------------------------------------------------

    dfs = load_all_sources(base)
    log.info("Loaded %d data sources", len(dfs))

    assignments = assign_tasks(dfs, profiles)
    assignments = clean_tasks(assignments)

    export_assignments(assignments, output_dir, generate_pdf=generate_pdf)

    log.info("Export completed – outputs written to %s", output_dir)
