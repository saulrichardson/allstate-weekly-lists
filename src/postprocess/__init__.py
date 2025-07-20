"""Dispatch to per-source cleaners while providing a generic fallback."""

# ---------------------------------------------------------------------------
# Post-processing dispatcher with graceful fallback + logging
# ---------------------------------------------------------------------------

import logging
from importlib import import_module
from typing import Dict, List, Tuple, Any


logger = logging.getLogger(__name__)


def clean_tasks(assignments: List[Tuple[Dict[str, Any], str]]):
    """Return a new list of (task, employee) after per-source clean-up."""

    out: List[Tuple[Dict[str, Any], str]] = []

    for task, emp in assignments:
        src = task.get("__source") or "generic"
        try:
            mod = import_module(f"src.postprocess.{src}")
            cleaner = getattr(mod, "clean")
        except ModuleNotFoundError:
            logger.warning("No post-processor found for source '%s'; using generic cleaner", src)
            from .generic import clean as cleaner  # type: ignore

        row = cleaner(dict(task))  # copy to avoid mutating original
        out.append((row, emp))
    return out
