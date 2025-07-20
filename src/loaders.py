"""
Load Excel data sources into pandas DataFrames.
"""

import yaml
import pandas as pd
import importlib
import logging
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
                raw = xls.parse(sheet_name=sheet, header=4)
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
                from datetime import date as _date
                today = _date.today()
                before = len(df)
                df = df[df["event_date"].notna() & (df["event_date"] >= today)]
                if before != len(df):
                    logger.info(
                        "Filtered %s rows: %d → %d (event_date >= today)",
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
            if src["name"] in dfs_dict:
                dfs_dict[src["name"]] = pd.concat([dfs_dict[src["name"]], df], ignore_index=True)
            else:
                dfs_dict[src["name"]] = df

    logger.debug("Finished loading sources: %s", ", ".join(dfs_dict))

    return dfs_dict
