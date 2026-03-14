"""
Main CLI driver for Freyberger 62 characteristics construction (US only).

Architecture: Monthly-First Normalization
==========================================
1. Build yearly characteristics (fundamentals + value using fiscal year-end ME)
2. Save yearly_raw_characteristics.parquet
3. Build price characteristics (momentum, trading) at monthly frequency
4. Save raw price characteristics: monthly_prices_raw_price_charact.parquet
5. Merge raw yearly with raw monthly (FF timing forward-fill) -> final_output_unnormalized.parquet
6. Normalize ALL 62 characteristics monthly cross-sectionally -> final_output_normalized.parquet

Per Freyberger et al. (2020) and Barroso et al. (2025):
- Annual accounting characteristics are forward-filled to monthly via Fama-French timing
- All 62 characteristics are rank-normalized cross-sectionally EACH MONTH
- Even though raw accounting values are constant for 12 months, their cross-sectional
  ranks change each month as the stock universe changes (entries/exits/delistings)

Output Files:
- yearly_raw_characteristics.parquet: Raw yearly characteristics (intermediate)
- monthly_prices_raw_price_charact.parquet: Raw monthly price characteristics (intermediate)
- final_output_unnormalized.parquet: All 62 raw characteristics at monthly frequency
- final_output_normalized.parquet: All 62 normalized characteristics at monthly frequency

Usage:
    pixi run us        # Process US data

Or directly:
    python main.py
"""

import click
from pathlib import Path
from dotenv import load_dotenv
import os
import sys
import polars as pl

from data_collection import (
    PathConfig,
    CharacteristicBuilder,
    normalize_barroso,
    add_suffix_to_columns,
)
from data_collection.construction import (
    merge_normalized_yearly_with_monthly_chunked,
)


# Load environment variables
load_dotenv()


# Yearly characteristic names (IDs 6-48: Investment, Profitability, Intangibles, Value, Size)
# These must match the actual variable names computed in characteristics.py
YEARLY_CHAR_NAMES = [
    # Investment (6-11)
    "Investment",  # ID 6: Asset growth (Δat / at_{t-1})
    "ACEQ",  # ID 7: % change in book equity
    "DPI2A",  # ID 8: (Δppent + Δinvt) / at_{t-1}
    "AShrout",  # ID 9: % change in shares outstanding
    "IVC",  # ID 10: Δinvt / avg(at)
    "NOA_ch",  # ID 11: NOA / at_{t-1}
    # Size (48) - from Compustat fundamentals
    "AT_raw",  # ID 48: Total Assets (renamed from 'at')
    # Profitability (12-28)
    "ATO",  # ID 12: Sales / NOA_{t-1}
    "CTO",  # ID 13: Sales / at_{t-1}
    "dGM_dSales",  # ID 14: Δ Gross Margin - Δ Sales
    "EPS",  # ID 15: Earnings per share
    "IPM",  # ID 16: Pretax income / sales
    "PCM",  # ID 17: Gross profit margin (GP / sale)
    "PM",  # ID 18: Operating profit margin (oiadp / sale)
    "PM_adj",  # ID 19: Industry-adjusted PM
    "Prof",  # ID 20: Gross profitability (GP / BE)
    "RNA",  # ID 21: Return on net operating assets
    "ROA",  # ID 22: Return on assets
    "ROC",  # ID 23: Return on cash
    "ROE",  # ID 24: Return on equity
    "ROIC",  # ID 25: Return on invested capital
    "S2C",  # ID 26: Sales to cash
    "SAT",  # ID 27: Sales to assets
    "SAT_adj",  # ID 28: Industry-adjusted SAT
    # Intangibles (29-32)
    "AOA",  # ID 29: Absolute operating accruals
    "OL",  # ID 30: Operating leverage
    "Tan",  # ID 31: Tangibility
    "OA",  # ID 32: Operating accruals
    # Value (33-47)
    "A2ME",  # ID 33: Assets to market equity
    "BEME",  # ID 34: Book to market
    "BEME_adj",  # ID 35: Industry-adjusted B/M
    "C",  # ID 36: Cash to assets
    "C2D",  # ID 37: Cash flow to debt
    "ASO",  # ID 38: Log change in split-adjusted shares
    "Debt2P",  # ID 39: Debt to price
    "E2P",  # ID 40: Earnings to price
    "Free_CF",  # ID 41: Free cash flow to BE
    "LDP",  # ID 42: Dividend yield
    "NOP",  # ID 43: Net payouts to price
    "O2P",  # ID 44: Operating payouts to price
    "Q",  # ID 45: Tobin's Q
    "S2P",  # ID 46: Sales to price
    "Sales_g",  # ID 47: Sales growth
    # Trading (61) - computed from fiscal year daily data
    "SUV",  # ID 61: Standard unexplained volume (fiscal year)
]

# Price-based characteristic names to normalize (IDs 1-5, 49-60, 62)
# Note: SUV (ID 61) is computed at fiscal year level and is in YEARLY_CHAR_NAMES
PRICE_CHAR_NAMES = [
    # Momentum (1-5)
    "r2_1",  # ID 1: Return 1 month before
    "r6_2",  # ID 2: Return 6 to 2 months before
    "r12_2",  # ID 3: Return 12 to 2 months before
    "r12_7",  # ID 4: Return 12 to 7 months before
    "r36_13",  # ID 5: Return 36 to 13 months before
    # Trading/Risk (49-62) - Note: AT_raw (ID 48) is in YEARLY_CHAR_NAMES
    "Beta_Cor",  # ID 49: Correlation component of beta
    "Beta",  # ID 50: CAPM beta
    "DTO",  # ID 51: Detrended turnover
    "Idio_vol",  # ID 52: Idiosyncratic volatility
    "LME",  # ID 53: Log market equity
    "LME_adj",  # ID 54: Industry-adjusted LME
    "LTurnover",  # ID 55: Log turnover
    "Rel2High",  # ID 56: Price relative to 52-week high
    "Ret_max",  # ID 57: Maximum daily return
    "Spread",  # ID 58: Bid-ask spread
    "Std_Turn",  # ID 59: Std dev of turnover
    "Std_Vol",  # ID 60: Std dev of volume
    "Total_vol",  # ID 62: Total volatility (std of returns)
]

# Intermediate variables to drop from output (used for calculations but not final chars)
INTERMEDIATE_COLS = [
    "BE",  # Book Equity (used for BEME, ROE, etc.)
    "NOA",  # Net Operating Assets (used for RNA, NOA_char, etc.)
    "GP",  # Gross Profit (used for GP2A, GP2AT, etc.)
    "OpAcc",  # Operating Accruals (used for AOA, OA)
    "wcapch_calc",  # Balance sheet wcapch approximation (fallback for Free_CF)
    "ME",  # Market Equity (used for value ratios) - keep in final merged
    "act_lag",
    "che_lag",
    "lct_lag",
    "txp_lag",  # Lagged values for OpAcc
    "at_lag",
    "ceq_lag",
    "BE_lag",
    "ppent_lag",
    "invt_lag",
    "NOA_lag",  # Investment lags
    "gross_margin",
    "gross_margin_lag",
    "sale_lag",  # Profitability intermediates
    "turnover_trend",  # DTO intermediate
    "industry",  # FF48 industry code (used for _adj vars)
    "sic",  # SIC code (standardized)
    "sich",  # Historical SIC code (Compustat original)
    "n_days",  # Data quality indicator
]

# Columns to rename (raw Compustat names -> characteristic names)
RENAME_COLS = {
    "at": "AT_raw",  # Total assets (raw value, ID 48 is ambiguous)
    "NOA_char": "NOA_ch",  # NOA characteristic (renamed from NOA_ratio)
}

# Identifier columns to keep
ID_COLS_YEARLY = ["gvkey", "datadate", "fyear", "fyr"]
ID_COLS_MONTHLY_US = ["permno", "gvkey", "date"]


def clean_output_dataframe(
    df: pl.LazyFrame | pl.DataFrame,
    char_names: list[str],
    id_cols: list[str],
    rename_cols: dict[str, str] | None = None,
    drop_intermediate: bool = True,
) -> pl.LazyFrame | pl.DataFrame:
    """
    Clean output dataframe by removing intermediate variables and renaming columns.

    Args:
        df: Input dataframe (lazy or eager)
        char_names: List of characteristic column names to keep
        id_cols: List of identifier columns to keep
        rename_cols: Dictionary of columns to rename {old_name: new_name}
        drop_intermediate: Whether to drop intermediate calculation columns

    Returns:
        Cleaned dataframe with only final characteristics and identifiers
    """
    existing_cols = (
        df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
    )

    # Apply renaming first
    if rename_cols:
        rename_existing = {k: v for k, v in rename_cols.items() if k in existing_cols}
        if rename_existing:
            df = df.rename(rename_existing)
            # Update existing_cols after rename
            existing_cols = (
                df.collect_schema().names()
                if isinstance(df, pl.LazyFrame)
                else df.columns
            )

    # Determine columns to keep
    cols_to_keep = []

    # Add identifier columns that exist
    for col in id_cols:
        if col in existing_cols:
            cols_to_keep.append(col)

    # Add characteristic columns that exist (including renamed ones)
    char_names_with_renamed = list(char_names)
    if rename_cols:
        char_names_with_renamed.extend(rename_cols.values())

    for col in char_names_with_renamed:
        if col in existing_cols and col not in cols_to_keep:
            cols_to_keep.append(col)

    # If not dropping intermediate, keep all non-intermediate columns
    if not drop_intermediate:
        for col in existing_cols:
            if col not in cols_to_keep and col not in INTERMEDIATE_COLS:
                cols_to_keep.append(col)

    return df.select(cols_to_keep)


def get_default_config() -> PathConfig:
    """
    Get PathConfig from environment variables or defaults.

    Environment variables:
    - DATA_DIR: Base directory for data files (default: ./data/inputs)
    - OUTPUT_DIR: Output directory for results (default: ./data/outputs)

    Expected file structure under DATA_DIR:
    - us/crsp_monthly.parquet
    - us/crsp_fiscal_chars.parquet (fiscal year characteristics from daily data)
    - us/compustat_funda.parquet
    - us/ccm_link.parquet
    - factors/ff_factors_daily.parquet
    - factors/ff48_industries.parquet
    """
    data_dir = Path(os.getenv("DATA_DIR", "./data/inputs"))
    output_dir = Path(os.getenv("OUTPUT_DIR", "./data/outputs"))

    return PathConfig(
        # US paths
        us_crsp_monthly=data_dir / "us" / "crsp_monthly.parquet",
        us_crsp_daily=data_dir
        / "us"
        / "crsp_daily.parquet",  # Required for beta computation
        us_compustat=data_dir / "us" / "compustat_funda.parquet",
        us_ccm_link=data_dir / "us" / "ccm_link.parquet",
        # Fiscal year characteristics (computed from daily data during download)
        us_fiscal_chars=data_dir / "us" / "crsp_fiscal_chars.parquet",
        # Factor paths
        ff_factors=data_dir / "factors" / "ff_factors_daily.parquet",
        ff48_industries=data_dir / "factors" / "ff48_industries.parquet",
        # Output
        output_dir=output_dir,
    )


def validate_paths(config: PathConfig) -> list[str]:
    """
    Validate that required US data files exist.

    Returns list of missing files.
    """
    missing = []

    if config.us_crsp_monthly and not config.us_crsp_monthly.exists():
        missing.append(str(config.us_crsp_monthly))
    if config.us_crsp_daily and not config.us_crsp_daily.exists():
        missing.append(str(config.us_crsp_daily))
    if config.us_fiscal_chars and not config.us_fiscal_chars.exists():
        missing.append(str(config.us_fiscal_chars))
    if config.us_compustat and not config.us_compustat.exists():
        missing.append(str(config.us_compustat))
    if config.us_ccm_link and not config.us_ccm_link.exists():
        missing.append(str(config.us_ccm_link))

    # Common files
    if config.ff_factors and not config.ff_factors.exists():
        missing.append(str(config.ff_factors))

    return missing


def process_region(
    config: PathConfig,
    normalize: bool = True,
    verbose: bool = True,
) -> None:
    """
    Process US characteristics using monthly-first normalization.

    Pipeline:
    1. Build yearly raw characteristics (fundamentals + value with fiscal year-end ME)
    2. Build monthly raw price characteristics (momentum, trading)
    3. Merge raw yearly -> monthly via FF timing + raw monthly prices
    4. Compute LME_adj on raw merged data
    5. Normalize ALL 62 characteristics monthly cross-sectionally
    6. Clean final outputs

    Per Freyberger et al. (2020) and Barroso et al. (2025), even though raw accounting
    values don't change within a fiscal year, their cross-sectional ranks change every
    month as stocks enter/exit the universe. Normalization must be monthly.

    Args:
        config: Path configuration
        normalize: Whether to apply normalization
        verbose: Print progress messages
    """
    import shutil

    click.echo(f"\n{'=' * 60}")
    click.echo("Processing US characteristics")
    click.echo("(Using monthly-first normalization architecture)")
    click.echo(f"{'=' * 60}")

    builder = CharacteristicBuilder(config, verbose=verbose)

    # =========================================================================
    # Step 1: Build yearly raw characteristics (IDs 6-47, 48, 61)
    # =========================================================================
    click.echo("\n[Step 1/6] Building yearly characteristics...")
    try:
        yearly_chars = builder.build_yearly_characteristics()
    except FileNotFoundError as e:
        click.echo(f"Error: Data file not found - {e}", err=True)
        return
    except Exception as e:
        click.echo(f"Error building yearly characteristics: {e}", err=True)
        raise

    # Save raw yearly characteristics
    yearly_raw_output = config.output_yearly_raw
    click.echo(f"  Saving raw yearly characteristics to {yearly_raw_output}...")
    yearly_raw_output.parent.mkdir(parents=True, exist_ok=True)
    yearly_chars.collect().write_parquet(yearly_raw_output)

    # =========================================================================
    # Step 2: Build monthly raw price characteristics (IDs 1-5, 49-62)
    # =========================================================================
    click.echo("\n[Step 2/6] Building monthly price characteristics...")

    # Load data for price characteristics
    builder.load_data()

    # Compute price characteristics (momentum + trading)
    price_chars = builder.compute_price_characteristics()

    # Apply column renames early (e.g., 'at' -> 'AT_raw')
    existing_cols_before_rename = price_chars.collect_schema().names()
    rename_existing = {
        k: v for k, v in RENAME_COLS.items() if k in existing_cols_before_rename
    }
    if rename_existing:
        price_chars = price_chars.rename(rename_existing)

    # Save raw monthly price characteristics
    monthly_raw_output = config.output_monthly_raw
    click.echo(f"  Saving raw price characteristics to {monthly_raw_output}...")
    monthly_raw_output.parent.mkdir(parents=True, exist_ok=True)
    price_chars.collect().write_parquet(monthly_raw_output)

    # =========================================================================
    # Step 3: Merge raw yearly + raw monthly (FF timing forward-fill)
    # =========================================================================
    click.echo("\n[Step 3/6] Merging raw yearly with raw monthly characteristics...")

    # Load link table for US
    link_table = builder.loader.load_link_table()

    # Load raw yearly and raw monthly characteristics
    yearly_raw_lf = pl.scan_parquet(yearly_raw_output)
    monthly_raw_lf = pl.scan_parquet(monthly_raw_output)

    # Merge using chunked approach for memory efficiency
    click.echo("  Using chunked merge for memory efficiency...")
    temp_dir_raw = config.output_dir / ".temp_merge_chunks_raw"
    merged_raw = merge_normalized_yearly_with_monthly_chunked(
        yearly_raw_lf,
        monthly_raw_lf,
        chunk_years=5,
        temp_dir=temp_dir_raw,
        link_table=link_table,
    )

    # Save final unnormalized output
    final_unnorm_output = config.output_final_unnormalized
    click.echo(f"  Saving unnormalized characteristics to {final_unnorm_output}...")
    final_unnorm_output.parent.mkdir(parents=True, exist_ok=True)
    merged_raw.sink_parquet(final_unnorm_output)

    # Clean up temp chunks
    if temp_dir_raw.exists():
        click.echo("  Cleaning up temporary merge chunks...")
        shutil.rmtree(temp_dir_raw)

    # =========================================================================
    # Step 4: Compute LME_adj (ID 54) - Industry-adjusted Size
    # =========================================================================
    click.echo("\n[Step 4/6] Computing LME_adj (industry-adjusted size)...")

    # Load FF48 industry mapping
    if config.ff48_industries and config.ff48_industries.exists():
        industry_lf = builder.loader.load_industries()

        # Compute LME_adj for unnormalized output
        # Note: Use read_parquet (eager) to avoid Windows file locking issues
        final_unnorm_df = pl.read_parquet(final_unnorm_output)
        existing_cols = final_unnorm_df.columns

        # Check if we have the required columns
        has_sic = "sich" in existing_cols or "sic" in existing_cols
        has_lme = "LME" in existing_cols

        if has_sic and has_lme:
            click.echo("  Computing LME_adj from LME and FF48 industries...")
            sic_col = "sich" if "sich" in existing_cols else "sic"

            # Rename sich to sic if needed for join
            if sic_col == "sich":
                final_unnorm_df = final_unnorm_df.rename({"sich": "sic"})

            # Join industry and compute adjusted LME
            final_with_ind = final_unnorm_df.join(
                industry_lf.collect(), on="sic", how="left"
            )
            final_with_lme_adj = final_with_ind.with_columns(
                (pl.col("LME") - pl.col("LME").mean().over(["date", "industry"])).alias(
                    "LME_adj"
                )
            )

            # Save back
            final_with_lme_adj.write_parquet(final_unnorm_output)
            click.echo("  LME_adj added to unnormalized output.")
        else:
            if not has_sic:
                click.echo("  Skipping LME_adj (SIC code not available in merged data)")
            if not has_lme:
                click.echo("  Skipping LME_adj (LME not available)")
    else:
        click.echo("  Skipping LME_adj (FF48 industries file not available)")

    # =========================================================================
    # Step 5: Normalize ALL 62 characteristics monthly cross-sectionally
    # =========================================================================
    if normalize:
        click.echo(
            "\n[Step 5/6] Normalizing all 62 characteristics (monthly cross-sectional)..."
        )

        all_char_names = YEARLY_CHAR_NAMES + PRICE_CHAR_NAMES

        norm_date_col = "date"
        click.echo("  Normalizing per month (grouped by date)...")

        # Load merged raw data
        final_unnorm_lf = pl.scan_parquet(final_unnorm_output)

        # Apply column renames to match characteristic names
        existing_cols_before_rename = final_unnorm_lf.collect_schema().names()
        rename_existing_norm = {
            k: v for k, v in RENAME_COLS.items() if k in existing_cols_before_rename
        }
        if rename_existing_norm:
            final_unnorm_lf = final_unnorm_lf.rename(rename_existing_norm)

        # Get characteristic columns that exist in the merged data
        existing_cols = final_unnorm_lf.collect_schema().names()
        all_chars_present = [c for c in all_char_names if c in existing_cols]

        click.echo(f"  Normalizing {len(all_chars_present)} characteristics...")

        # Apply Barroso normalization on the full merged monthly data
        normalized = normalize_barroso(
            final_unnorm_lf, all_chars_present, date_col=norm_date_col
        )

        # Add _norm suffix to characteristic columns
        normalized_with_suffix = add_suffix_to_columns(
            normalized, all_chars_present, suffix="_norm"
        )

        # Save normalized output
        final_norm_output = config.output_final_normalized
        click.echo(f"  Saving normalized characteristics to {final_norm_output}...")
        final_norm_output.parent.mkdir(parents=True, exist_ok=True)
        normalized_with_suffix.collect().write_parquet(final_norm_output)
    else:
        click.echo("\n[Step 5/6] Skipping normalization (--no-normalize flag)")
        final_norm_output = None

    # =========================================================================
    # Step 6: Clean final outputs (remove intermediate variables)
    # =========================================================================
    click.echo("\n[Step 6/6] Cleaning final output files...")

    id_cols_final = ID_COLS_MONTHLY_US
    all_char_names = YEARLY_CHAR_NAMES + PRICE_CHAR_NAMES

    # Clean final unnormalized output
    click.echo(f"  Cleaning {final_unnorm_output}...")
    final_unnorm_df = pl.read_parquet(final_unnorm_output)
    final_unnorm_cleaned = clean_output_dataframe(
        final_unnorm_df,
        char_names=all_char_names,
        id_cols=id_cols_final,
        rename_cols=RENAME_COLS,
        drop_intermediate=True,
    )
    # Write to temp file and replace to avoid Windows file locking
    temp_unnorm = final_unnorm_output.with_suffix(".tmp.parquet")
    final_unnorm_cleaned.write_parquet(temp_unnorm)
    del final_unnorm_df, final_unnorm_cleaned  # Release memory and file handles
    final_unnorm_output.unlink()  # Delete original
    temp_unnorm.rename(final_unnorm_output)  # Rename temp to original

    # Clean final normalized output if it exists
    if final_norm_output is not None:
        click.echo(f"  Cleaning {final_norm_output}...")
        # Normalized output has _norm suffix on all characteristic columns
        norm_char_names = [f"{c}_norm" for c in all_char_names]
        # Also need to rename at_norm -> AT_raw_norm, NOA_char_norm -> NOA_ch_norm
        norm_rename_cols = {f"{k}_norm": f"{v}_norm" for k, v in RENAME_COLS.items()}
        final_norm_df = pl.read_parquet(final_norm_output)
        final_norm_cleaned = clean_output_dataframe(
            final_norm_df,
            char_names=norm_char_names,
            id_cols=id_cols_final,
            rename_cols=norm_rename_cols,
            drop_intermediate=True,
        )
        # Write to temp file and replace to avoid Windows file locking
        temp_norm = final_norm_output.with_suffix(".tmp.parquet")
        final_norm_cleaned.write_parquet(temp_norm)
        del final_norm_df, final_norm_cleaned  # Release memory and file handles
        final_norm_output.unlink()  # Delete original
        temp_norm.rename(final_norm_output)  # Rename temp to original

    click.echo("\nUS processing complete!")
    click.echo(f"  Yearly raw: {yearly_raw_output}")
    click.echo(f"  Monthly raw: {monthly_raw_output}")
    click.echo(f"  Final unnormalized: {final_unnorm_output}")
    if normalize:
        click.echo(f"  Final normalized: {final_norm_output}")


@click.command()
@click.option(
    "--no-normalize",
    is_flag=True,
    default=False,
    help="Skip normalization step",
)
@click.option(
    "--data-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Override data directory",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Override output directory",
)
@click.option(
    "--quiet",
    is_flag=True,
    default=False,
    help="Suppress progress messages",
)
@click.option(
    "--validate-only",
    is_flag=True,
    default=False,
    help="Only validate data paths, don't process",
)
def main(
    no_normalize: bool,
    data_dir: Path | None,
    output_dir: Path | None,
    quiet: bool,
    validate_only: bool,
) -> None:
    """
    Build Freyberger 62 firm characteristics (US).

    Constructs 62 characteristics from Freyberger, Neuhierl, & Weber (2020)
    using CRSP/Compustat data.

    Architecture: Monthly-First Normalization
    - Forward-fills raw yearly characteristics to monthly via Fama-French timing
    - Normalizes ALL 62 characteristics monthly cross-sectionally
    - Cross-sectional ranks change each month as the stock universe changes

    Examples:

        # Process US data
        python main.py

        # Process without normalization
        python main.py --no-normalize

        # Process with custom directories
        python main.py --data-dir ./mydata --output-dir ./results
    """
    click.echo("Freyberger 62 Characteristics Builder")
    click.echo("=====================================")
    click.echo("Architecture: Monthly-First Normalization")

    # Get configuration
    config = get_default_config()

    # Override paths if provided
    if data_dir:
        config = PathConfig(
            us_crsp_monthly=data_dir / "us" / "crsp_monthly.parquet",
            us_crsp_daily=data_dir / "us" / "crsp_daily.parquet",
            us_compustat=data_dir / "us" / "compustat_funda.parquet",
            us_ccm_link=data_dir / "us" / "ccm_link.parquet",
            us_fiscal_chars=data_dir / "us" / "crsp_fiscal_chars.parquet",
            ff_factors=data_dir / "factors" / "ff_factors_daily.parquet",
            ff48_industries=data_dir / "factors" / "ff48_industries.parquet",
            output_dir=config.output_dir,
        )

    if output_dir:
        config.output_dir = output_dir

    # Validate paths
    missing = validate_paths(config)

    if missing:
        click.echo("\nMissing data files:", err=True)
        for f in missing:
            click.echo(f"  - {f}", err=True)

        if validate_only:
            click.echo("\nValidation failed.")
            sys.exit(1)
        else:
            click.echo("\nContinuing anyway (may fail during processing)...")
    elif validate_only:
        click.echo("\nAll required files found. Validation passed.")
        sys.exit(0)

    # Ensure output directory exists
    config.output_dir.mkdir(parents=True, exist_ok=True)

    # Process US
    normalize = not no_normalize
    verbose = not quiet

    process_region(config, normalize, verbose)

    click.echo("\n" + "=" * 60)
    click.echo("All processing complete!")
    click.echo(f"Output directory: {config.output_dir}")
    click.echo("=" * 60)


if __name__ == "__main__":
    main()
