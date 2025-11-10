"""
Load Excel data sources into pandas DataFrames.
"""

import yaml
import pandas as pd
import importlib
import logging
import shutil
import os
from datetime import datetime
from pathlib import Path


logger = logging.getLogger(__name__)



def load_all_sources(base_dir: Path) -> pd.DataFrame:
    """
    Load and normalize all configured data sources.

    Reads `config/sources.yml`, finds each source's Excel files,
    applies the named normalizer plugin, tags rows with __source,
    and concatenates everything into one DataFrame.
    """
    cfg_path = base_dir / "config" / "sources.yml"
    cfg = yaml.safe_load(cfg_path.read_text())
    dfs_dict = {}
    # Optional override sheet: route policies by policy number per-employee
    policy_to_assignee: dict[str, str] = {}
    try:
        # Prefer the generic router list; fall back to legacy name
        override_candidates = [
            base_dir / "data" / "router-list.xlsx",
            base_dir / "data" / "jill-formatted.xlsx",
        ]
        overrides_path = next((p for p in override_candidates if p.exists()), None)
        if overrides_path is not None:
            # Load known employees for validation and nicer warnings
            try:
                emp_cfg = yaml.safe_load((base_dir / "config" / "employees.yml").read_text())
                known_emps = {e.get("name", "").strip() for e in emp_cfg.get("employees", [])}
            except Exception:
                known_emps = set()
            # Archive a timestamped copy for audit trail (use actual filename stem)
            try:
                archive_dir = base_dir / "data" / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                shutil.copy2(overrides_path, archive_dir / f"{overrides_path.stem}_{ts}.xlsx")
            except Exception as exc:
                logger.warning("Could not archive %s: %s", overrides_path.name, exc)

            # Read all sheets; headers are employee names; cells are policy numbers
            xls = pd.ExcelFile(overrides_path)
            conflicts: list[tuple[str, str, str]] = []
            unknown_assignees: set[str] = set()

            def _norm_policy(val) -> str:
                s = str(val).strip()
                if s.endswith('.0') and s.replace('.', '', 1).isdigit():
                    s = s[:-2]
                return s

            for sheet in xls.sheet_names:
                try:
                    odf = xls.parse(sheet_name=sheet, header=0)
                except Exception as exc:
                    logger.warning("Could not read sheet %s from %s: %s", sheet, overrides_path.name, exc)
                    continue
                for col in odf.columns:
                    assignee = str(col).strip()
                    if not assignee or assignee.lower().startswith('unnamed'):
                        continue
                    # Validate against known employee names if available
                    if known_emps and assignee not in known_emps:
                        unknown_assignees.add(assignee)
                    for raw in odf[col].dropna().tolist():
                        policy = _norm_policy(raw)
                        if not policy or policy.lower() == 'nan':
                            continue
                        prev = policy_to_assignee.get(policy)
                        if prev is not None and prev != assignee:
                            conflicts.append((policy, prev, assignee))
                        policy_to_assignee[policy] = assignee
            logger.info(
                "Loaded %d routing overrides across %d employees from %s",
                len(policy_to_assignee),
                len({v for v in policy_to_assignee.values()}),
                overrides_path.name,
            )
            if conflicts:
                logger.warning(
                    "Found %d policy routing conflicts; last assignment wins. Examples: %s",
                    len(conflicts),
                    ", ".join(f"{p}:{a1}->{a2}" for p, a1, a2 in conflicts[:5]),
                )
            if unknown_assignees:
                logger.warning(
                    "Override file has columns for unknown employees: %s",
                    ", ".join(sorted(unknown_assignees)),
                )
    except Exception as exc:
        logger.error("Failed reading jill-formatted.xlsx: %s – continuing without overrides", exc)
    next_id = 0  # stable row_id counter across all sources
    for src in cfg.get("sources", []):
        # ------------------------------------------------------------------
        # Resolve the per-source *normalize* function.  If the normalizer
        # module is missing we log an error and **skip** the entire source
        # rather than aborting the weekly run.
        # ------------------------------------------------------------------
        try:
            mod = importlib.import_module(f"src.normalizers.{src['normalizer']}")
            normalize = getattr(mod, "normalize")
        except (ModuleNotFoundError, AttributeError) as exc:
            logger.error(
                "Cannot import normalizer '%s' for source '%s': %s – skipping this source",
                src.get("normalizer"),
                src.get("name"),
                exc,
            )
            continue
        matched_paths = list(base_dir.glob(src["path_glob"]))
        if not matched_paths:
            logger.warning(
                "No files matched pattern '%s' for source '%s'", src["path_glob"], src.get("name")
            )
            continue

        for path in matched_paths:
            logger.info("Loading %s file %s", src["name"], path.name)
            try:
                xls = pd.ExcelFile(path)
                sheet = xls.sheet_names[0]
                # Allow per-source header row override (default 4 for BOB audits)
                header_row = src.get("header_row", 4)
                raw = xls.parse(sheet_name=sheet, header=header_row)
            except Exception as exc:  # broad but we want to keep pipeline alive
                logger.error("Failed reading %s: %s – skipping file", path.name, exc)
                continue
            df = normalize(raw)

            # Business rule: for Renewal audits allocate only policies whose
            # renewal has **not** been taken ("Renewal Status" == "Renewal Not Taken").
            if src["name"] == "renewal" and "renewal_status" in df.columns:
                before = len(df)
                df = df[df["renewal_status"].str.contains("Not Taken", na=False)]
                logger.info(
                    "Filtered renewal rows: %d → %d (Renewal Not Taken)", before, len(df)
                )

            # ------------------------------------------------------------------
            # Generic rule: only allocate rows whose event_date is *today or later*
            # ------------------------------------------------------------------
            if "event_date" in df.columns:
                from datetime import date as _date, timedelta as _timedelta
                today = _date.today()
                before = len(df)
                df = df[df["event_date"].notna() & (df["event_date"] >= today)]
                # Apply an upper bound: default 15 days ahead; override with MAX_DAYS_AHEAD
                try:
                    env_val = os.getenv("MAX_DAYS_AHEAD")
                    max_days_ahead = int(env_val.strip()) if env_val else 15
                except Exception:
                    max_days_ahead = 15
                if max_days_ahead >= 0:
                    upper = today + _timedelta(days=max_days_ahead)
                    df = df[df["event_date"] <= upper]
                if before != len(df):
                    logger.info(
                        "Filtered %s rows: %d → %d (event_date window)",
                        src["name"],
                        before,
                        len(df),
                    )

            # Business rule: for Cancellation audits we allocate **only** rows
            # that have not already been cancelled *and* whose cancel date is
            # today or in the future.  This prevents agents from receiving
            # tasks they can no longer act on.
            if src["name"] == "cancellation":
                if "status" in df.columns or "event_date" in df.columns:
                    from datetime import date as _date

                    today = _date.today()
                    before = len(df)

                    mask_status = (~df["status"].str.contains("cancelled", case=False, na=False)) if "status" in df.columns else True
                    df = df[mask_status]

                    logger.info(
                        "Filtered cancellation rows: %d → %d (status != Cancelled)",
                        before,
                        len(df),
                    )
            # Assign stable row_id to each row for downstream tracking
            df["_row_id"] = range(next_id, next_id + len(df))
            next_id += len(df)
            # No generic premium_amount synthesis here – we keep original column names
            # Rename Agent# to agent_number and tag row source
            df = df.rename(columns={"Agent#": "agent_number"}) if "Agent#" in df.columns else df
            df["__source"] = src["name"]

            # Apply policy-number routing overrides (exclusive assignment)
            if policy_to_assignee and "policy_number" in df.columns:
                try:
                    pol = df["policy_number"].astype(str).str.strip()
                    assignees = pol.map(policy_to_assignee).astype(object)
                    mask = assignees.notna()
                    if mask.any():
                        df.loc[mask, "exclusive_assignee"] = assignees[mask].values
                        logger.info(
                            "Tagged %d rows in %s with exclusive assignee from overrides",
                            int(mask.sum()),
                            src["name"],
                        )
                except Exception as exc:
                    logger.warning("Failed applying policy routing overrides to %s: %s", src["name"], exc)

            # Apply stricter horizon for exclusive tasks: max 10 days ahead
            if "event_date" in df.columns and "exclusive_assignee" in df.columns:
                try:
                    from datetime import date as _date, timedelta as _timedelta
                    today = _date.today()
                    upper_excl = today + _timedelta(days=10)
                    before = len(df)
                    excl_mask = df["exclusive_assignee"].notna()
                    df = df[~(excl_mask & (df["event_date"] > upper_excl))]
                    dropped = before - len(df)
                    if dropped:
                        logger.info(
                            "Filtered %s rows: -%d exclusive beyond 10 days",
                            src["name"],
                            dropped,
                        )
                except Exception as exc:
                    logger.warning("Failed applying 10-day cap for exclusive tasks in %s: %s", src["name"], exc)
            if src["name"] in dfs_dict:
                dfs_dict[src["name"]] = pd.concat([dfs_dict[src["name"]], df], ignore_index=True)
            else:
                dfs_dict[src["name"]] = df

    logger.debug("Finished loading sources: %s", ", ".join(dfs_dict))

    return dfs_dict
