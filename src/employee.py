"""Employee data model used for task assignment."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Any, List


SOURCES_ORDER = ["pending_cancel", "cancellation", "renewal"]


Predicate = Callable[[Dict[str, Any]], bool]


@dataclass
class Employee:
    """In-memory representation of an employee, including capacity tracking."""

    name: str
    priority_level: int = 100
    predicate: Predicate = lambda _row: True  # noqa: E731
    capacity_per_source: Dict[str, int | None] = field(default_factory=dict)

    # Internal mutable state – remaining capacity during assignment run
    _cap_src_rem: Dict[str, float] = field(init=False, repr=False)

    def __post_init__(self):
        self.reset_capacity()

    # ------------------------------------------------------------------
    # Capacity helpers
    # ------------------------------------------------------------------

    def reset_capacity(self, sources_order: List[str] | None = None):
        """Initialise or reset remaining-capacity counters for a new run."""

        if sources_order is None:
            sources_order = SOURCES_ORDER

        self._cap_src_rem = {}
        for src in sources_order:
            limit = (self.capacity_per_source or {}).get(src)
            self._cap_src_rem[src] = float("inf") if limit is None else limit

    # ------------------------------------------------------------------
    # Public helpers used by assigner
    # ------------------------------------------------------------------

    def has_capacity(self, src_name: str) -> bool:
        return self._cap_src_rem.get(src_name, float("inf")) > 0

    def accept_task(self, task: Dict[str, Any], src_name: str) -> bool:
        """Return True if this employee should take *task* for *src_name*.

        This checks remaining capacity and evaluates the predicate.
        """

        if not self.has_capacity(src_name):
            return False
        return self.predicate(task)

    def decrement_capacity(self, src_name: str):
        if self._cap_src_rem.get(src_name, float("inf")) != float("inf"):
            self._cap_src_rem[src_name] -= 1

    # For backwards-compatibility with code that may expect mapping access
    # (name, priority_level) we expose __getitem__ but encourage attribute use.
    def __getitem__(self, item):  # type: ignore[override]
        return getattr(self, item)

    # Sorting helpers
    def __lt__(self, other: "Employee") -> bool:  # type: ignore[override]
        return self.priority_level < other.priority_level
