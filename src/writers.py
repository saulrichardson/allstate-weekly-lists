"""Output writers for assignment results (Excel, PDF, …)."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple, Any

import yaml
import pandas as pd

from .formatter import format_subset

# ---------------------------------------------------------------------------
# Excel writer (openpyxl backend)
# ---------------------------------------------------------------------------


def _write_excel(
    tasks: List[Tuple[Dict[str, Any], str]],
    *,
    output_dir: Path,
    cfg_base_dir: Path,
):
    """Export assignments to per-employee Excel workbooks."""

    # Ensure the entire path exists (parents=True handles nested paths)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group tasks by employee
    rows_by_emp: Dict[str, List[Dict[str, Any]]] = {}
    for task, emp in tasks:
        rows_by_emp.setdefault(emp or "Unassigned", []).append(task)

    # Load configs
    src_cfg = yaml.safe_load((cfg_base_dir / "sources.yml").read_text())
    all_sources = [s["name"] for s in src_cfg.get("sources", [])]

    exp_cfg = yaml.safe_load((cfg_base_dir / "export.yml").read_text())
    global_order = exp_cfg.get("order", [])
    sheet_cfgs = exp_cfg.get("columns", {})

    drop_cols_global = {"account_type", "company_code", "product_code"}

    for emp, tasks in rows_by_emp.items():
        df_all = pd.DataFrame(tasks)
        out_path = output_dir / f"{emp}.xlsx"

        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            workbook_frames: Dict[str, pd.DataFrame] = {}

            written_any_sheet = False

            for src_name in all_sources:
                subset = pd.DataFrame()
                if "__source" in df_all.columns:
                    subset = df_all[df_all["__source"] == src_name].drop(columns="__source", errors="ignore")

                if subset.empty:
                    # Skip creating a worksheet if there are no rows for this source
                    workbook_frames[src_name] = subset
                    continue

                # Sort rows so each sheet shows the actionable items first (soonest event).
                sort_cols: list[str] = []
                ascending: list[bool] = []
                if "event_date" in subset.columns:
                    sort_cols.append("event_date")
                    ascending.append(True)
                if "amount" in subset.columns:
                    sort_cols.append("amount")
                    ascending.append(False)
                if sort_cols:
                    subset = subset.sort_values(by=sort_cols, ascending=ascending, kind="mergesort")

                fmt_subset = format_subset(
                    subset,
                    src_name=src_name,
                    global_order=global_order,
                    sheet_cfg=sheet_cfgs.get(src_name, {}),
                    drop_cols_global=drop_cols_global,
                )

                # add a blank 'Result' column for manual outcome/notes
                fmt_subset["Result"] = ""
                workbook_frames[src_name] = fmt_subset

                # Write sheet
                sheet_name = src_name[:31]
                fmt_subset.to_excel(writer, sheet_name=sheet_name, index=False)
                written_any_sheet = True

                # Auto-fit columns
                worksheet = writer.sheets[sheet_name]
                # Keep most columns compact, but give the free-text
                # "Result" column extra room so users can type longer notes
                # directly in the exported workbook.  A wider default makes
                # the column usable out-of-the-box without manual resizing.

                MIN_W, MAX_W = 8, 60  # allow larger upper bound overall
                for column_cells in worksheet.columns:
                    header = column_cells[0].value or ""
                    length = len(str(header))
                    for cell in column_cells[1:500]:  # up to 500 rows
                        if cell.value is not None:
                            length = max(length, len(str(cell.value)))
                    # Base width on the longest encountered value plus some
                    # padding, but ensure sensible min/max bounds.
                    width = max(length + 2, MIN_W)

                    # Explicitly widen the dedicated notes column.
                    if header.strip().lower() == "result":
                        width = max(width, 40)  # enough space for free text

                    width = min(width, MAX_W)
                    worksheet.column_dimensions[column_cells[0].column_letter].width = width

            # Ensure workbook has at least one sheet; openpyxl requires it.
            if not written_any_sheet:
                pd.DataFrame({"Info": ["No tasks assigned"]}).to_excel(
                    writer, sheet_name="No Tasks", index=False
                )

        yield emp, workbook_frames  # for PDF writer


# ---------------------------------------------------------------------------
# PDF writer (matplotlib.backends.backend_pdf) – simple table per sheet
# ---------------------------------------------------------------------------


def _write_pdf(
    emp: str,
    sheets: Dict[str, pd.DataFrame],
    *,
    output_dir: Path,
):
    """Create a single PDF file per employee with one page per sheet."""

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    pdf_path = output_dir / f"{emp}.pdf"

    with PdfPages(pdf_path) as pdf:
        for sheet_name, df in sheets.items():
            # If empty, create a blank page with header
            if df.empty:
                fig = plt.figure(figsize=(8.5, 11))
                fig.suptitle(f"{sheet_name.title()} – No entries")
                pdf.savefig(fig)
                plt.close(fig)
                continue

            n_rows, n_cols = df.shape
            # heuristic sizing
            height = min(11, max(2, 0.35 * (n_rows + 1)))
            width = min(17, max(8.5, 0.9 * n_cols))  # allow landscape for many cols

            fig, ax = plt.subplots(figsize=(width, height))
            ax.axis('off')

            col_width = 1.0 / n_cols
            tbl = ax.table(
                cellText=df.values,
                colLabels=df.columns,
                loc='center',
                colWidths=[col_width] * n_cols,
            )
            tbl.auto_set_font_size(False)
            tbl.set_fontsize(8)
            tbl.scale(1, 1.2)

            ax.set_title(sheet_name.title())
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)


def export_assignments(
    tasks: List[Tuple[Dict[str, Any], str]],
    output_dir: Path,
    *,
    generate_pdf: bool = False,
) -> None:
    """Write per-employee Excel workbooks.

    If *generate_pdf* is True, also attempt to produce matching PDFs (via
    LibreOffice or matplotlib fallback).
    """

    cfg_base_dir = Path(__file__).resolve().parent.parent / "config"

    # Always write Excel files and gather metadata for optional PDF phase
    excel_info: Dict[str, Dict[str, Any]] = {}
    for emp, sheets in _write_excel(tasks, output_dir=output_dir, cfg_base_dir=cfg_base_dir):
        excel_info[emp] = {
            "sheets": sheets,
            "xlsx_path": output_dir / f"{emp}.xlsx",
        }

    if not generate_pdf:
        return

    # Optionally: high-fidelity PDF conversion or fallback renderer
    for emp, meta in excel_info.items():
        xlsx_path = meta["xlsx_path"]
        if _convert_excel_to_pdf_via_libreoffice(xlsx_path, output_dir):
            continue
        _write_pdf(emp, meta["sheets"], output_dir=output_dir)


def _convert_excel_to_pdf_via_libreoffice(xlsx_path: Path, output_dir: Path) -> bool:
    """Attempt to convert *xlsx_path* to PDF using LibreOffice.

    Returns True on success, False if LibreOffice is not available or conversion
    fails.  This gives high-quality pagination identical to manual 'Save As PDF'.
    """

    import shutil
    import subprocess

    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        return False

    try:
        subprocess.run(
            [
                soffice,
                "--headless",
                "--convert-to",
                "pdf",
                "--outdir",
                str(output_dir),
                str(xlsx_path),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return (output_dir / f"{xlsx_path.stem}.pdf").exists()
    except Exception:
        return False
