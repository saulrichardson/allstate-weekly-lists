"""Task assignment engine with fully ordered priority levels.

Rules implemented
------------------
1. Each employee has an integer `priority_level` (lower number ⇒ higher
   precedence).  If omitted, it defaults to 100.

2. Each employee can optionally define `capacity_per_source` – a mapping
   from audit-source name (pending_cancel, cancellation, renewal, …) to the
   maximum number of rows they should receive for that source.  Missing or
   falsy value is treated as unlimited.

3. For every source the algorithm:
      a. Sorts remaining rows by premium (high → low).
      b. Iterates priority levels in ascending order.
      c. Within a level, distributes rows round-robin among employees in the
         order they appear in employees.yml (fair sharing).

4. The helper `_assign_round_robin` respects each employee’s predicate and
   per-source remaining capacity.

Rows that cannot be assigned (because no employee predicate matches or all
capacities are exhausted) are returned with employee None so the exporter can
place them in an “Unassigned” workbook.
"""

from __future__ import annotations

from collections import defaultdict
from itertools import cycle
from numbers import Number as _Number
from typing import Any, Dict, List, Tuple, Iterable

import pandas as pd

from .employee import Employee, SOURCES_ORDER

PREMIUM_KEYS = ("amount",)


def _get_premium(task: Dict[str, Any]) -> float:
    """Return numeric premium for sorting (fallback 0)."""

# canonical amount field
    val = task.get("amount")
    # Treat None or NaN as zero to avoid NaN sorting to the top
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        # Handle float('nan')
        from math import isnan
        return 0.0 if (isinstance(val, float) and isnan(val)) else float(val)
    if isinstance(val, _Number) and not isinstance(val, bool):
        return float(val)
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0

# helper for future date-based sorting

def _get_event_date(task: Dict[str, Any]):
    """Return event_date or minimal date if missing."""

    from datetime import date

    val = task.get("event_date")
    if val is None:
        return date.min
    return val


def _interleave_extremes(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Interleave earliest/latest tasks to balance leftovers across priorities."""

    if len(tasks) <= 2:
        return tasks
    left, right = 0, len(tasks) - 1
    woven: List[Dict[str, Any]] = []
    while left < right:
        woven.append(tasks[left])
        woven.append(tasks[right])
        left += 1
        right -= 1
    if left == right:
        woven.append(tasks[left])
    return woven


def _assign_round_robin(
    tasks: List[Dict[str, Any]],
    employees: List[Employee],
    src_name: str,
) -> List[Tuple[Dict[str, Any], str]]:
    """Round-robin allocation for one source and one priority level."""

    if not tasks or not employees:
        return []

    emp_cycle = cycle(employees)
    assigned: List[Tuple[Dict[str, Any], str]] = []

    for task in tasks:
        # Handle exclusive assignments first for this level. If the target
        # employee is not in this level's cohort, defer to their level by
        # skipping any attempt to allocate this task here.
        exclusive_to = task.get("exclusive_assignee")
        # Normalize NaN/empty to None so only real names are treated as exclusive
        try:
            from math import isnan as _isnan
            if isinstance(exclusive_to, float) and _isnan(exclusive_to):
                exclusive_to = None
        except Exception:
            pass
        if isinstance(exclusive_to, str) and exclusive_to.strip() == "":
            exclusive_to = None
        if exclusive_to is not None:
            target = next((e for e in employees if e.name == exclusive_to), None)
            if target is not None and target.accept_task(task, src_name):
                assigned.append((task, target.name))
                # Do not decrement capacity for exclusive tasks
                continue
            else:
                # Defer to later levels – do not assign to others
                continue

        for _ in range(len(employees)):
            emp = next(emp_cycle)
            if not emp.has_capacity(src_name):
                continue
            if emp.accept_task(task, src_name):
                assigned.append((task, emp.name))
                # Only decrement capacity for non-exclusive tasks
                if task.get("exclusive_assignee") != emp.name:
                    emp.decrement_capacity(src_name)
                break

    return assigned


def _assign_single_source(
    tasks: List[Dict[str, Any]],
    employee_profiles: List[Employee],
) -> List[Tuple[Dict[str, Any], str | None]]:
    """Assign tasks for a single source list (expects __source set)."""

    # Make a fresh copy of Employee objects and reset capacities for this run
    employees = [emp for emp in employee_profiles]
    for emp in employees:
        emp.reset_capacity(SOURCES_ORDER)

    # Global assignments list and remaining pool
    assignments: List[Tuple[Dict[str, Any], str]] = []
    remaining_tasks = list(tasks)

    # Process each source independently
    for src in SOURCES_ORDER:
        src_tasks = [t for t in remaining_tasks if t["__source"] == src]
        # Sort by soonest event_date first, then by highest premium
        # Secondary: high premium; Primary: earliest date
        # Sort for allocation: earliest event date first, and for same date highest premium
        src_tasks.sort(key=_get_premium, reverse=True)
        src_tasks.sort(key=_get_event_date)
        src_tasks = _interleave_extremes(src_tasks)

        # Iterate levels ascending
        levels = sorted({e.priority_level for e in employees})
        for level in levels:
            level_emps = [e for e in employees if e.priority_level == level]
            # Preserve original order inside level (config order)
            level_emps = sorted(level_emps, key=lambda e: employee_profiles.index(e))

            rr = _assign_round_robin(src_tasks, level_emps, src)
            if not rr:
                continue
            assignments.extend(rr)
            # Remove assigned tasks from src_tasks and global pool
            assigned_ids = {t["_row_id"] for t, _ in rr}
            src_tasks = [t for t in src_tasks if t["_row_id"] not in assigned_ids]
            remaining_tasks = [t for t in remaining_tasks if t["_row_id"] not in assigned_ids]

    # Anything left unassigned
    unassigned = [(t, None) for t in remaining_tasks]
    assignments.extend(unassigned)


    return assignments


# ---------------------------------------------------------------------------
# Multi-source wrapper
# ---------------------------------------------------------------------------


def assign_tasks(
    dfs: Dict[str, pd.DataFrame],
    employee_profiles: List[Employee],
) -> List[Tuple[Dict[str, Any], str | None]]:
    """Assign tasks for every source.

    Args:
        dfs: dict mapping source_name -> pandas DataFrame as returned by
             loaders.load_all_sources.
        employee_profiles: list of employee config dicts.

    Returns:
        Flat list of (task_dict, employee_name|None) for all sources.
    """

    all_assignments: List[Tuple[Dict[str, Any], str | None]] = []
    for src_name, df in dfs.items():
        tasks = df.to_dict(orient="records")
        for t in tasks:
            t["__source"] = src_name
        all_assignments.extend(_assign_single_source(tasks, employee_profiles))

    return all_assignments
