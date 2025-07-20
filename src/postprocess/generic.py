"""Generic, source-agnostic row clean-up helpers."""


from typing import Dict, Any

# ---------------------------------------------------------------------------
# Load look-up tables (office codes, product names, etc.) once at import time
# ---------------------------------------------------------------------------

import yaml
from pathlib import Path


_LOOKUPS: Dict[str, Dict[str, str]] = {}

# locate repo root: <repo>/weekly-lists/src/postprocess/generic.py → parents[2]
_cfg_path = Path(__file__).resolve().parent.parent.parent / "config" / "lookups.yml"

if _cfg_path.exists():
    try:
        _LOOKUPS = yaml.safe_load(_cfg_path.read_text()) or {}
    except Exception:  # pragma: no cover – fallback on malformed file
        _LOOKUPS = {}


def _get_lookup(name: str) -> Dict[str, str]:
    """Return mapping dict for *name*; empty dict if missing."""

    return _LOOKUPS.get(name, {})


def _strip_strings(row: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in row.items():
        if isinstance(v, str):
            row[k] = v.strip()
    return row


def _upper_state(row: Dict[str, Any]) -> Dict[str, Any]:
    state = row.get("state")
    if isinstance(state, str):
        row["state"] = state.upper()
    return row


def _format_phone(row: Dict[str, Any]) -> Dict[str, Any]:
    phone_key = None
    for key in ("insured_phone", "phone"):
        if key in row:
            phone_key = key
            break
    if not phone_key:
        return row

    val = row[phone_key]
    if not isinstance(val, str):
        return row

    digits = "".join(filter(str.isdigit, val))
    if len(digits) == 10:
        row[phone_key] = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return row


def _combine_address(row: Dict[str, Any]) -> Dict[str, Any]:
    parts = [
        row.get("street_address"),
        row.get("city"),
        row.get("state"),
        row.get("zip_code"),
    ]
    if any(parts):
        row["full_address"] = ", ".join(str(p).strip() for p in parts if p)
    return row


# ---------------------------------------------------------------------------
# Business-specific normalisation helpers
# ---------------------------------------------------------------------------


def _normalize_product(row: Dict[str, Any]) -> Dict[str, Any]:
    """Collapse verbose product descriptors to short, user-friendly labels."""

    mapping = _get_lookup("product_map")
    if not mapping:
        return row

    for key in ("product", "product_name"):
        if key in row and isinstance(row[key], str):
            row[key] = mapping.get(row[key], row[key])
    return row


def clean(row: Dict[str, Any]) -> Dict[str, Any]:
    """Apply generic clean-up helpers that do not alter business data."""

    # 0. rename insured_* columns to shorter names (first_name, phone, etc.)
    rename_map = {}
    for key in list(row.keys()):
        if key.startswith("insured_"):
            new_key = key.replace("insured_", "")
            rename_map[key] = new_key
    for old, new in rename_map.items():
        row[new] = row.pop(old)

    # 1. rename agent_number -> office and map codes to office names
    if "agent_number" in row:
        code = str(row.pop("agent_number"))
        office_map = _get_lookup("office_map")
        row["office"] = office_map.get(code.upper(), code)

    # 2. run generic cleanup helpers
    for fn in (
        _strip_strings,
        _upper_state,
        _format_phone,
        _combine_address,
        _normalize_product,
    ):
        row = fn(row)
    return row
