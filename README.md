# Weekly Lists Pipeline

This directory contains a framework to generate weekly assignment lists
for employees based on configurable business rules.

## Project Structure

```
weekly-lists/
├─ data/                          # Raw Excel downloads (e.g. cancellation, renewal, pending cancel)
├─ config/                        # Configuration for sources and employee profiles
│   ├─ sources.yml                # Data-source registry + normalizer mapping
│   └─ employees.yml              # Employee profiles (capacity, preference, predicate_cfg)
├─ src/                           # Library modules
│   ├─ __init__.py
│   ├─ loaders.py                 # Load & normalize all sources into a unified DataFrame
│   ├─ normalizers/               # Per-source normalization plugins (pending_cancel, renewal, cancellation)
│   ├─ rules.py                   # Build predicates from config
│   ├─ assigner.py                # Pure assignment logic
│   └─ exporter.py                # Export assigned tasks to Excel workbooks
├─ run_weekly.py                  # CLI entry point
├─ requirements.txt               # Python dependencies
└─ .gitignore                     # Ignore patterns for this project
```

## Quickstart

1. Requires **Python 3.9+** (tested with 3.9–3.11).

2. Install the package (editable install while developing):
   ```bash
   pip install -e .        # or: pip install weekly-lists once published
   ```
3. Edit `config/sources.yml` to register each data source and its normalizer.
4. Edit `config/employees.yml` to set each employee’s:
   - `capacity` (max tasks)
   - `prefer` (`high` or `low` premium)
   - `predicate_cfg` (eligibility filter; for now use `agent_number` to split by agent)
4. Drop the raw Excel files into `data/`.
5. Run the pipeline (from any directory):
   ```bash
   weekly-lists --base /path/to/workdir --out ./output
   ```

## Data Sources & Normalization

- **config/sources.yml**: register each raw Excel audit via its glob and named `normalizer`.
- **src/normalizers/**: one module per source, each exporting `normalize(df)` to map the raw sheet
  to a canonical set of key fields (e.g. `policy_number`, `premium_amount`, etc.) and rename them,
  while preserving all other original columns so no data is lost at ingestion.
The loader (`src/loaders.py`) reads each sheet (with `header=4`), applies the normalizer,
  tags rows with `__source`, normalizes the raw `Agent#` to `agent_number` for filtering,
  and concatenates all sources into one DataFrame. All other raw columns remain unchanged,
  so you explicitly enumerate every incoming column in the normalizers and drop anything you
  haven’t explicitly mapped.

## Assignment logic

- Each employee in `employees.yml` specifies a `prefer` key (`high` or `low`).
- In phase 1, tasks are sorted by descending premium and assigned (round-robin) to all "high" employees up to their capacities.
- In phase 2, remaining tasks are sorted by ascending premium and assigned to all "low" employees up to capacity.
- All tasks end up assigned subject to each employee's `capacity`.

## Configuration files

```
config/
├─ sources.yml     # input audit registry
└─ employees.yml   # employee capacity / filters
```

Example `sources.yml`:

```yaml
sources:
  - name: cancellation
    path_glob: data/*Cancellation*.xlsx  # relative to --base
    normalizer: cancellation             # module in src/normalizers/
```

Example `employees.yml`:

```yaml
employees:
  - name: Jill
    priority_level: 10            # lower = assign earlier
    capacity_per_source:
      cancellation: 40
      renewal: 25
    predicate_cfg:                # eligibility filter
      agent_number: ["1234", "5678"]
```

Field reference:

• `priority_level` – order in which employees receive tasks (default 100).
• `capacity_per_source` – per-audit quota; if omitted falls back to infinity.
• `predicate_cfg` – passed to `src.rules.build_predicate()` to exclude rows.

## Output

- After running, each employee gets an Excel workbook in `output/`:
- `<Employee Name>.xlsx` contains one sheet per data source (as named in `config/sources.yml`)
- showing only that subset of tasks (the `__source` column is dropped in these sheets).
