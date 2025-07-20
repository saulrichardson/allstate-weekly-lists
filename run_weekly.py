#!/usr/bin/env python3
"""Command-line wrapper for the weekly-lists pipeline.

All heavy lifting is delegated to ``src.runner.run_weekly`` so that the core
logic can also be imported and executed from other Python code.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.runner import run_weekly


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate weekly assignment lists")
    # Default base directory = current working directory so the script can be
    # executed from cron or any folder without forcing *cd* into the repo.
    # Users can still override with --base if they keep config/data elsewhere.

    default_base = Path.cwd()

    parser.add_argument(
        "--base",
        default=str(default_base),
        help=(
            "Root directory that contains the config/ and data/ folders. "
            "Defaults to the current working directory (where the command is executed)"
        ),
    )
    parser.add_argument(
        "--out",
        default=str(default_base / "output"),
        help=(
            "Directory where output workbooks will be written. "
            "Defaults to <base>/output, i.e. " + str(default_base / "output")
        ),
    )
    parser.add_argument(
        "--pdf",
        action="store_true",
        help="Also generate PDF files in addition to Excel workbooks",
    )

    args = parser.parse_args()

    import os

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(levelname)s: %(message)s")
    log = logging.getLogger("weekly")

    run_weekly(
        base=Path(args.base).resolve(),
        output_dir=Path(args.out).resolve(),
        generate_pdf=args.pdf,
        log=log,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
