# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Polars-based Python implementation of the **62 firm characteristics from Freyberger, Neuhierl, & Weber (2020)**, with normalization per Barroso, Saxena, & Wang (2025). Uses US data from CRSP + Compustat NA. Requires a WRDS subscription for data access.

## Commands

```bash
# Environment (uses Pixi package manager)
pixi install                   # Install all dependencies
pixi shell                     # Activate environment

# Data pipeline
pixi run download              # Download data from WRDS (needs WRDS_USER in .env)
pixi run us                    # Build characteristics for US
pixi run validate              # Validate data paths without processing

# Code quality
pixi run lint                  # ruff check .
pixi run format                # ruff format .
pixi run test                  # pytest tests/ -v

# Direct CLI usage
python main.py --no-normalize --quiet
python download_data.py --start-date 1960-01-01 --end-date 2024-12-31 --force
```

## Architecture

### Pipeline (Monthly-First Normalization)

The pipeline in `main.py` runs 6 steps:

1. **Build yearly characteristics** (IDs 6-48, 61) from Compustat fundamentals → `yearly_raw_characteristics_region.parquet`
2. **Build monthly price characteristics** (IDs 1-5, 49-60, 62) from CRSP security data → `monthly_prices_raw_price_charact_region.parquet`
3. **Merge** raw yearly into monthly via Fama-French timing forward-fill (chunked for memory) → `final_output_unnormalized_region.parquet`
4. **Compute LME_adj** (industry-adjusted size) using FF48 industry mapping
5. **Normalize** all 62 characteristics monthly cross-sectionally (rank to [-0.5, 0.5]) → `final_output_normalized_region.parquet`
6. **Clean** outputs by dropping intermediate calculation columns

### Key Design Decisions

- **All data loaded as `pl.LazyFrame`** for memory efficiency; `.collect()` only at save points
- **Type optimization**: Int32/Float32 instead of 64-bit where possible
- **Normalization is monthly**: Even though accounting values are constant within a fiscal year, cross-sectional ranks change each month as stocks enter/exit the universe

### Package Layout

- **`data_collection/config.py`** — `PathConfig`, `VariableMapping` with US column mappings, all 62 `CharacteristicDef` definitions, filter configs
- **`data_collection/data_loader.py`** — `DataLoader` class; all loaders return `pl.LazyFrame`
- **`data_collection/cleaners.py`** — US filters (exchange codes 1-3, share codes 10-11), winsorization
- **`data_collection/characteristics.py`** — `CharacteristicBuilder` orchestrator class; caches intermediate computations
- **`data_collection/normalization.py`** — `normalize_barroso()`: impute with cross-sectional median → rank → scale to [-0.5, 0.5]
- **`data_collection/construction/`** — Characteristic computation split into:
  - `prices.py` — momentum (r2_1 through r36_13), beta, volatility, trading characteristics
  - `fundamentals.py` — intermediate variables (BE, NOA, OpAcc, GP), investment, profitability, intangibles, value characteristics
  - `merge.py` — Fama-French timing merge, chunked merge for memory efficiency
- **`download_data.py`** — Parallel WRDS download (8 workers); downloads CRSP, Compustat, FF factors

### ID Columns

- US monthly: `permno`, `gvkey`, `date`
- Yearly: `gvkey`, `datadate`, `fyear`, `fyr`

## Code Style

- **No print statements** — use `click.echo()` instead (enforced by pre-commit hook)
- **Absolute imports only** (enforced by absolufy-imports hook)
- **No commits to main** — enforced by pre-commit
- Ruff handles linting and formatting (runs as pre-commit hook)
- Python 3.11+

## Environment Variables

Set in `.env` (copy from `.env.example`):
- `WRDS_USER` — WRDS username for data download
- `DATA_DIR` — Input data directory (default: `./data/inputs`)
- `OUTPUT_DIR` — Output directory (default: `./data/outputs`)
