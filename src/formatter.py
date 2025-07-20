"""DataFrame formatting utilities used by all output writers (Excel, PDF, …)."""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Dict, Any

import pandas as _pd


def format_subset(
    subset: _pd.DataFrame,
    *,
    src_name: str,
    global_order: list[str],
    sheet_cfg: Dict[str, Any],
    drop_cols_global: set[str] | None = None,
) -> _pd.DataFrame:
    """Return a *new* DataFrame with presentation-ready formatting.

    This logic used to be embedded directly in exporter.export_assignments but is
    now shared so additional writer backends (PDF, CSV, …) can reuse the same
    prettification rules.
    """

    drop_cols_global = drop_cols_global or set()

    df = subset.copy()

    # ------------------------------------------------------------------
    # Global drop list (identical for all sheets)
    # ------------------------------------------------------------------
    df = df.drop(columns=[c for c in drop_cols_global if c in df.columns], errors="ignore")

    # ------------------------------------------------------------------
    # Sheet-specific drops / renames
    # ------------------------------------------------------------------
    rename_map: Dict[str, str] = sheet_cfg.get("rename", {})

    # 1. Explicit per-sheet drop before renames
    for col in sheet_cfg.get("drop", []):
        df = df.drop(columns=[c for c in df.columns if c == col], errors="ignore")

    # 2. Apply rename map (canonical → presentation)
    df = df.rename(columns=rename_map)

    # 3. Remove duplicate columns (can happen if original column collides with rename)
    df = df.loc[:, ~df.columns.duplicated()]

    # 4. Drop columns not whitelisted in global_order + rename targets
    allowed = set(global_order) | set(rename_map.values())
    df = df[[c for c in df.columns if c in allowed]]

    # 5. Drop columns fully empty in this subset
    df = df.dropna(axis=1, how="all")

    # 6. Re-order according to global order list (mapped through rename)
    ordering_keys = [rename_map.get(c, c) for c in global_order]
    ordered = [c for c in ordering_keys if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    df = df[ordered + remaining]

    # 7. Format date columns as MM/DD
    for col in list(df.columns):
        if _pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%m/%d")
        elif df[col].dtype == object and df[col].apply(lambda x: isinstance(x, (_dt.date, _dt.datetime))).any():
            df[col] = df[col].apply(lambda d: d.strftime("%m/%d") if isinstance(d, (_dt.date, _dt.datetime)) else d)

    # 8. Pretty headers: snake_case → Title Case
    df = df.rename(columns=lambda c: c.replace("_", " ").title())

    # 9. Drop duplicate headers created by title-casing
    df = df.loc[:, ~df.columns.duplicated()]

    # 10. Shorten First / Last once pretty-printed
    df = df.rename(columns={"First Name": "First", "Last Name": "Last"})

    return df.reset_index(drop=True)
