"""
Data download script for US WRDS data using the official wrds library.

This script downloads all required US data from WRDS (CRSP and Compustat NA)
and Fama-French factors from Kenneth French's website, saving everything as
parquet files. Run this ONCE before using the main characteristic builder.

Usage:
    1. Set your WRDS username below or in .env file
    2. Run: pixi shell
    3. Run: python download_data.py [OPTIONS]

Options:
    --start-date    Start date in YYYY-MM-DD format (default: 1987-01-01)
    --end-date      End date in YYYY-MM-DD format (default: Dec 31 of last
                    complete year - uses year-2 if before March, else year-1)
    --force         Force re-download even if files already exist
    --skip-factors  Skip downloading Fama-French factors from Ken French's website

Examples:
    python download_data.py
    python download_data.py --start-date 1960-01-01 --end-date 2024-12-31
    python download_data.py --force  # Re-download all files
    python download_data.py --skip-factors  # Skip FF factors if already have them

You will be prompted for your WRDS password on first use.
The wrds library caches credentials in ~/.pgpass after initial authentication.
"""

import argparse
import io
import os
import re
import urllib.request
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import wrds
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =============================================================================
# CONFIGURATION - Edit these settings
# =============================================================================

# Your WRDS username (or set WRDS_USER in .env file)
WRDS_USER = os.getenv("WRDS_USER", "your_wrds_username")

# Output directory for downloaded data (default: ./data/inputs inside project)
DATA_DIR = Path(os.getenv("DATA_DIR", "./data/inputs"))

# Global flag for forcing re-download (set via --force argument)
FORCE_DOWNLOAD = False

# Maximum parallel workers for daily data downloads (5-8 recommended to avoid WRDS blocking)
MAX_DOWNLOAD_WORKERS = 8


# =============================================================================
# WRDS Connection
# =============================================================================


def get_wrds_connection() -> wrds.Connection:
    """Create WRDS connection using official library."""
    if WRDS_USER == "your_wrds_username":
        print("ERROR: Please set your WRDS username!")
        print("Either edit WRDS_USER in this file or set WRDS_USER in .env")
        exit(1)

    print(f"Connecting to WRDS as {WRDS_USER}...")
    return wrds.Connection(wrds_username=WRDS_USER)


def run_query(conn: wrds.Connection, query: str) -> pl.DataFrame:
    """Run SQL query and return as Polars DataFrame."""
    pdf = conn.raw_sql(query)
    return pl.from_pandas(pdf)


def file_exists_skip(output_path: Path, description: str) -> bool:
    """Check if file exists and print skip message if so (unless --force is set)."""
    if FORCE_DOWNLOAD:
        return False
    if output_path.exists():
        print(f"Skipping {description} (file exists: {output_path})")
        return True
    return False


# =============================================================================
# Parallel Download Utilities
# =============================================================================


def _generate_yearly_chunks(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """
    Generate (start, end) date tuples for each year in the range.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of (chunk_start, chunk_end) tuples, one per year
    """
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    chunks = []

    for year in range(start_year, end_year + 1):
        # Use original start_date for first year, Jan 1 otherwise
        chunk_start = start_date if year == start_year else f"{year}-01-01"
        # Use original end_date for last year, Dec 31 otherwise
        chunk_end = end_date if year == end_year else f"{year}-12-31"
        chunks.append((chunk_start, chunk_end))

    return chunks


def _download_compustat_funda_chunk(
    start_date: str, end_date: str, wrds_user: str
) -> pd.DataFrame | None:
    """
    Worker function: Download one chunk of Compustat NA Fundamentals data.

    Creates its own WRDS connection (required for multiprocessing).
    Must be a top-level function for pickling.
    """
    try:
        conn = wrds.Connection(wrds_username=wrds_user, verbose=False)

        query = f"""
        SELECT gvkey, datadate, fyear, fyr,
               act, at, capx, ceq, che, cogs, dlc, dltt, dp, dvt,
               ebit, epspx, ib, invt, lct, lt, ni, oiadp, pi,
               ppent, prstkc, sale, txp, wcapch, xsga, pstk, ajex, prcc_f, csho, sich
        FROM comp.funda
        WHERE indfmt='INDL'
          AND datafmt='STD'
          AND popsrc='D'
          AND consol='C'
          AND datadate >= '{start_date}'
          AND datadate <= '{end_date}'
        """

        df = conn.raw_sql(query)
        conn.close()
        return df

    except Exception as e:
        print(
            f"  [!] Error downloading Compustat Funda {start_date} to {end_date}: {e}"
        )
        return None


def download_compustat_funda_parallel(
    output_path: Path, start_date: str, end_date: str, max_workers: int
) -> None:
    """Download Compustat NA Fundamentals using parallel yearly chunks."""
    if file_exists_skip(output_path, "Compustat NA Fundamentals"):
        return

    print("Downloading Compustat NA Fundamentals (parallel by year)...")

    chunks = _generate_yearly_chunks(start_date, end_date)
    print(f"  Splitting into {len(chunks)} yearly chunks with {max_workers} workers")

    all_results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_dates = {
            executor.submit(_download_compustat_funda_chunk, s, e, WRDS_USER): (s, e)
            for s, e in chunks
        }

        for future in as_completed(future_to_dates):
            s, e = future_to_dates[future]
            try:
                df_chunk = future.result()
                if df_chunk is not None and not df_chunk.empty:
                    print(f"  [+] Finished {s} to {e}: {len(df_chunk):,} rows")
                    all_results.append(df_chunk)
                else:
                    print(f"  [-] No data for {s} to {e}")
            except Exception as exc:
                print(f"  [!] Exception for {s} to {e}: {exc}")

    if all_results:
        print("  Merging chunks...")
        merged_pdf = pd.concat(all_results, ignore_index=True)
        df = pl.from_pandas(merged_pdf)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path)
        print(f"  Saved {len(df):,} rows to {output_path}")
    else:
        print("  [!] No data was downloaded")


# =============================================================================
# Fiscal Year Characteristics Computation
# =============================================================================
# These functions compute annual characteristics from daily data using the
# Fama-French fiscal year timing convention: June year t → May year t+1


# -----------------------------------------------------------------------------
# Checkpoint Helper Functions
# -----------------------------------------------------------------------------
# These functions enable saving intermediate fiscal year results to disk,
# allowing resumption from the last successful year if the process is interrupted.


def _get_checkpoint_folder(output_path: Path, prefix: str) -> Path:
    """
    Get the checkpoint folder path for a given output file.

    Args:
        output_path: Path to the final output file
        prefix: Prefix for the checkpoint folder name (e.g., 'crsp_fiscal')

    Returns:
        Path to the checkpoint folder (hidden folder with dot prefix)
    """
    return output_path.parent / f".{prefix}_checkpoints"


def _get_completed_fiscal_years(checkpoint_folder: Path) -> set[int]:
    """
    Scan checkpoint folder and return set of completed fiscal years.

    Args:
        checkpoint_folder: Path to the checkpoint folder

    Returns:
        Set of fiscal years that have already been computed and saved
    """
    if not checkpoint_folder.exists():
        return set()
    completed = set()
    for f in checkpoint_folder.glob("fiscal_year_*.parquet"):
        match = re.match(r"fiscal_year_(\d+)\.parquet", f.name)
        if match:
            completed.add(int(match.group(1)))
    return completed


def _save_fiscal_year_checkpoint(
    checkpoint_folder: Path,
    fiscal_year: int,
    df: pd.DataFrame,
) -> None:
    """
    Save a single fiscal year's characteristics to checkpoint.

    Args:
        checkpoint_folder: Path to the checkpoint folder
        fiscal_year: The fiscal year being saved
        df: DataFrame containing the fiscal year characteristics
    """
    checkpoint_folder.mkdir(parents=True, exist_ok=True)
    filepath = checkpoint_folder / f"fiscal_year_{fiscal_year}.parquet"
    pl.from_pandas(df).write_parquet(filepath)
    print(f"    [checkpoint] Saved fiscal year {fiscal_year}")


def _combine_checkpoints(checkpoint_folder: Path, output_path: Path) -> None:
    """
    Combine all checkpoint files into final output.

    Args:
        checkpoint_folder: Path to the checkpoint folder
        output_path: Path to the final combined output file
    """
    all_files = sorted(checkpoint_folder.glob("fiscal_year_*.parquet"))
    if not all_files:
        print("  [!] No checkpoint files found to combine")
        return

    print(f"  Combining {len(all_files)} checkpoint files...")
    dfs = [pl.read_parquet(f) for f in all_files]
    combined = pl.concat(dfs)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(output_path)
    print(f"  Saved {len(combined):,} total rows to {output_path}")


def _cleanup_checkpoints(checkpoint_folder: Path) -> None:
    """
    Remove checkpoint folder and all its contents.

    Args:
        checkpoint_folder: Path to the checkpoint folder to remove
    """
    import shutil

    if checkpoint_folder.exists():
        shutil.rmtree(checkpoint_folder)
        print(f"  [cleanup] Removed checkpoint folder: {checkpoint_folder}")


# -----------------------------------------------------------------------------
# Fiscal Year Characteristics Computation Functions
# -----------------------------------------------------------------------------


def compute_fiscal_year_characteristics_crsp(
    daily_df: pd.DataFrame,
    fiscal_year: int,
    factors_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Compute fiscal year characteristics for US CRSP data.

    Characteristics are computed from daily data spanning June fiscal_year
    to May fiscal_year+1, following the Fama-French timing convention.

    Note: SUV (ID 61) is computed from monthly aggregated data (12 observations)
    rather than daily data, significantly improving computational efficiency.

    Args:
        daily_df: Daily CRSP data with columns: permno, date, prc, ret, vol,
                  shrout, askhi (high), bidlo (low), bid, ask
        fiscal_year: The fiscal year (e.g., 2020 means Jun 2020 - May 2021)
        factors_df: Fama-French factors for beta calculation (optional)

    Returns:
        DataFrame with one row per permno containing fiscal year characteristics:
        - permno, fiscal_year
        - Total_vol, Ret_max, Std_Vol, Std_Turn
        - LTurnover, Spread, Rel2High
        - SUV (computed from monthly aggregated data)

    Note: Beta characteristics (Beta, Beta_Cor, Idio_vol) are no longer computed here.
    They are computed monthly using a 252-day rolling window on daily returns in the
    main characteristic building pipeline (compute_beta_characteristics in prices.py).
    """
    import numpy as np

    # Filter to fiscal year window: June fiscal_year to May fiscal_year+1
    start_date = pd.Timestamp(f"{fiscal_year}-06-01")
    end_date = pd.Timestamp(f"{fiscal_year + 1}-05-31")

    # Ensure date column is datetime
    if not pd.api.types.is_datetime64_any_dtype(daily_df["date"]):
        daily_df = daily_df.copy()
        daily_df["date"] = pd.to_datetime(daily_df["date"])

    fy_data = daily_df[
        (daily_df["date"] >= start_date) & (daily_df["date"] <= end_date)
    ].copy()

    if fy_data.empty:
        return pd.DataFrame()

    # Compute turnover
    fy_data["turnover"] = fy_data["vol"] / fy_data["shrout"]

    # Compute quoted spread: (ask - bid) / midpoint
    midpoint = (fy_data["ask"] + fy_data["bid"]) / 2
    fy_data["daily_spread"] = (fy_data["ask"] - fy_data["bid"]) / midpoint.replace(
        0, np.nan
    )

    # Aggregate by permno to get fiscal year characteristics
    chars = (
        fy_data.groupby("permno")
        .agg(
            # ID 62: Total_vol - Std of daily returns
            Total_vol=("ret", "std"),
            # ID 57: Ret_max - Maximum daily return
            Ret_max=("ret", "max"),
            # ID 60: Std_Vol - Std of daily volume
            Std_Vol=("vol", "std"),
            # ID 59: Std_Turn - Std of daily turnover
            Std_Turn=("turnover", "std"),
            # ID 55: LTurnover - Mean daily turnover
            LTurnover=("turnover", "mean"),
            # ID 58: Spread - Mean daily spread
            Spread=("daily_spread", "mean"),
            # Count for data quality
            n_days=("ret", "count"),
            # For Rel2High calculation
            last_prc=("prc", "last"),
            high_52w=("askhi", "max"),
        )
        .reset_index()
    )

    # ID 56: Rel2High - Price relative to 52-week (fiscal year) high
    chars["Rel2High"] = chars["last_prc"] / chars["high_52w"].replace(0, np.nan)

    # Filter to firms with sufficient data (at least 120 trading days)
    chars = chars[chars["n_days"] >= 120].copy()

    # Compute SUV (ID 61) - Standard Unexplained Volume
    suv_chars = _compute_suv_for_fiscal_year(fy_data, id_col="permno")
    if not suv_chars.empty:
        chars = chars.merge(suv_chars, on="permno", how="left")

    # Add fiscal year column
    chars["fiscal_year"] = fiscal_year

    # Select final columns
    output_cols = [
        "permno",
        "fiscal_year",
        "Total_vol",
        "Ret_max",
        "Std_Vol",
        "Std_Turn",
        "LTurnover",
        "Spread",
        "Rel2High",
        "SUV",
        "n_days",
    ]

    return chars[[c for c in output_cols if c in chars.columns]]


def _aggregate_daily_to_monthly(
    daily_df: pd.DataFrame,
    id_col: str | list[str] = "permno",
) -> pd.DataFrame:
    """
    Aggregate daily data to monthly for SUV computation.

    Converts daily trading data to monthly frequency by:
    - Summing daily volume to get total monthly volume
    - Compounding daily returns to get monthly returns
    - Only keeping months with at least 10 trading days

    Args:
        daily_df: Daily data with date, ret, vol columns
        id_col: Identifier column(s) - 'permno' for US

    Returns:
        Monthly aggregated DataFrame with columns: id_col(s), year_month, vol, ret, n_days
    """
    df = daily_df.copy()

    date_col = "datadate" if "datadate" in df.columns else "date"

    df["year_month"] = df[date_col].dt.to_period("M")

    group_cols = [id_col] if isinstance(id_col, str) else id_col
    group_cols = group_cols + ["year_month"]

    monthly = (
        df.groupby(group_cols)
        .agg(
            {
                "vol": "sum",  # Total monthly volume
                "ret": lambda x: (1 + x.dropna()).prod() - 1,  # Compound return
                date_col: "count",  # Number of days with data
            }
        )
        .reset_index()
    )

    monthly.rename(columns={date_col: "n_days"}, inplace=True)

    # Only keep months with at least 10 trading days
    monthly = monthly[monthly["n_days"] >= 10].copy()

    return monthly[group_cols + ["vol", "ret", "n_days"]]


def _compute_suv_for_fiscal_year(
    daily_df: pd.DataFrame,
    id_col: str | list[str] = "permno",
) -> pd.DataFrame:
    """
    Compute SUV (Standard Unexplained Volume) for fiscal year using monthly data.

    ID 61: SUV = standardized residuals from volume prediction model
    Model: Monthly_Volume_m = α + β1 * |Monthly_Ret_m+| + β2 * |Monthly_Ret_m-| + ε
    SUV = mean(residuals / std(residuals)) over 12 months

    The function aggregates daily data to monthly frequency (12 observations per fiscal year)
    before running the OLS regression, significantly reducing computational load while
    maintaining the predictive signal.

    Args:
        daily_df: Daily data with id_col, date, vol, ret columns
        id_col: Identifier column (permno for US)

    Returns:
        DataFrame with id_col(s) and SUV
    """
    import numpy as np

    # Aggregate daily to monthly
    monthly_df = _aggregate_daily_to_monthly(daily_df, id_col)

    # Prepare data
    df = monthly_df.copy()
    # NaN returns will naturally propagate through np.where
    df["abs_ret_pos"] = np.where(df["ret"] > 0, np.abs(df["ret"]), 0)
    df["abs_ret_neg"] = np.where(df["ret"] < 0, np.abs(df["ret"]), 0)

    # Handle grouping for single or multiple id columns
    if isinstance(id_col, list):
        group_cols = id_col
    else:
        group_cols = [id_col]

    def compute_suv_group(group):
        """Compute SUV for a single security."""
        # Need sufficient data (at least 10 months out of 12)
        valid = group.dropna(subset=["vol", "ret"])
        if len(valid) < 10:
            return np.nan

        y = valid["vol"].values
        X = np.column_stack(
            [
                np.ones(len(valid)),  # constant
                valid["abs_ret_pos"].values,
                valid["abs_ret_neg"].values,
            ]
        )

        try:
            # OLS regression
            beta, residuals, rank, s = np.linalg.lstsq(X, y, rcond=None)
            y_pred = X @ beta
            resid = y - y_pred

            # Standardize residuals
            std_resid = np.std(resid, ddof=3)  # 3 parameters estimated
            if std_resid == 0:
                return np.nan

            # SUV is the mean of standardized residuals (summary statistic)
            suv = np.mean(resid / std_resid)
            return suv
        except Exception:
            return np.nan

    # Apply to each group (use df which has the abs_ret columns)
    suv_results = (
        df.groupby(group_cols)
        .apply(
            lambda g: pd.Series({"SUV": compute_suv_group(g)}),
            include_groups=False,
        )
        .reset_index()
    )

    return suv_results


# =============================================================================
# Sequential Fiscal Year Download Functions
# =============================================================================


def _download_single_calendar_year_crsp(
    year: int, wrds_user: str
) -> pd.DataFrame | None:
    """
    Download one calendar year of CRSP daily data.

    Args:
        year: Calendar year to download
        wrds_user: WRDS username

    Returns:
        pandas DataFrame with daily data, or None if error
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    try:
        conn = wrds.Connection(wrds_username=wrds_user, verbose=False)
        query = f"""
        SELECT permno, date, prc, ret, vol, shrout, askhi, bidlo, bid, ask
        FROM crsp.dsf
        WHERE date >= '{start_date}' AND date <= '{end_date}'
        """
        df = conn.raw_sql(query)
        conn.close()
        return df
    except Exception as e:
        print(f"  [!] Error downloading CRSP year {year}: {e}")
        return None



def download_crsp_daily_compute_fiscal_chars(
    output_path: Path,
    start_date: str,
    end_date: str,
    factors_path: Path | None = None,
    clean_checkpoints: bool = False,
) -> None:
    """
    Download CRSP daily data year-by-year and compute fiscal year characteristics.

    Memory efficient: only keeps 2 calendar years in memory at a time.
    Saves each fiscal year's characteristics to a checkpoint file immediately,
    allowing resumption from the last successful year if interrupted.

    Fiscal year timing: June year t to May year t+1 (Fama-French convention)

    Args:
        output_path: Path to save the fiscal year characteristics parquet file
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        factors_path: Optional path to Fama-French factors parquet file
        clean_checkpoints: If True, remove checkpoint folder after successful completion
    """
    if file_exists_skip(output_path, "CRSP Fiscal Year Characteristics"):
        return

    print("Downloading CRSP Daily and computing Fiscal Year Characteristics...")

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    # Setup checkpoint folder and determine which fiscal years are already done
    checkpoint_folder = _get_checkpoint_folder(output_path, "crsp_fiscal")
    completed_fiscal_years = _get_completed_fiscal_years(checkpoint_folder)

    if completed_fiscal_years:
        print(
            f"  Found {len(completed_fiscal_years)} existing checkpoints: "
            f"{min(completed_fiscal_years)}-{max(completed_fiscal_years)}"
        )

    # Load factors if provided
    factors_df = None
    if factors_path and factors_path.exists():
        print(f"  Loading Fama-French factors from {factors_path}...")
        factors_df = pl.read_parquet(factors_path).to_pandas()

    prev_year_data = None
    prev_year = None
    new_fiscal_years_computed = 0

    # Need to download from start_year to end_year+1 to compute all fiscal years
    # Fiscal year t needs data from June t to May t+1
    for cal_year in range(start_year, end_year + 2):
        # Determine if we need this calendar year's data
        # We need it if: (1) prev_year fiscal is not checkpointed, OR
        #                (2) cal_year fiscal is not checkpointed (need this year's data for next iteration)
        prev_year_needed = (
            prev_year is not None and prev_year not in completed_fiscal_years
        )
        curr_year_needed = (
            cal_year not in completed_fiscal_years and cal_year <= end_year
        )

        if not prev_year_needed and not curr_year_needed and prev_year_data is None:
            # Skip downloading - this year's data not needed
            print(
                f"  [skip] Calendar year {cal_year} - fiscal years already checkpointed"
            )
            prev_year = cal_year
            continue

        print(f"  Downloading calendar year {cal_year}...")
        curr_year_data = _download_single_calendar_year_crsp(cal_year, WRDS_USER)

        if curr_year_data is None or curr_year_data.empty:
            print(f"  [-] No data for year {cal_year}")
            prev_year_data = None
            prev_year = cal_year
            continue

        print(f"    Downloaded {len(curr_year_data):,} rows")

        if prev_year_data is not None and prev_year is not None:
            # Check if this fiscal year is already checkpointed
            if prev_year in completed_fiscal_years:
                print(f"  [skip] Fiscal year {prev_year} already checkpointed")
            else:
                # Concatenate two years and compute
                print(
                    f"  Computing fiscal year {prev_year} characteristics (Jun {prev_year} - May {cal_year})..."
                )
                combined = pd.concat(
                    [prev_year_data, curr_year_data], ignore_index=True
                )

                # Compute fiscal year characteristics
                fy_chars = compute_fiscal_year_characteristics_crsp(
                    combined, prev_year, factors_df
                )

                if not fy_chars.empty:
                    print(f"    Computed characteristics for {len(fy_chars):,} firms")
                    # Save checkpoint immediately
                    _save_fiscal_year_checkpoint(checkpoint_folder, prev_year, fy_chars)
                    new_fiscal_years_computed += 1
                else:
                    print("    No characteristics computed (insufficient data)")

            # Discard older year to free memory
            del prev_year_data

        # Keep current year for next iteration
        prev_year_data = curr_year_data
        prev_year = cal_year

    # Combine all checkpoints into final output
    updated_completed = _get_completed_fiscal_years(checkpoint_folder)
    if updated_completed:
        print(
            f"  Total fiscal years available: {len(updated_completed)} "
            f"({new_fiscal_years_computed} newly computed)"
        )
        _combine_checkpoints(checkpoint_folder, output_path)

        # Optionally clean up checkpoints
        if clean_checkpoints:
            _cleanup_checkpoints(checkpoint_folder)
    else:
        print("  [!] No fiscal year characteristics were computed")


# =============================================================================
# US Data Queries
# =============================================================================


def download_crsp_monthly(
    conn: wrds.Connection,
    output_path: Path,
    start_date: str,
    end_date: str,
) -> None:
    """Download CRSP Monthly Stock File (raw, without names)."""
    if file_exists_skip(output_path, "CRSP Monthly Stock File"):
        return

    print("Downloading CRSP Monthly Stock File...")

    query = f"""
    SELECT permno, date, prc, ret, vol, shrout, cfacpr
    FROM crsp.msf
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    """

    df = run_query(conn, query)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"  Saved {len(df):,} rows to {output_path}")


def download_crsp_daily(
    conn: wrds.Connection,
    output_path: Path,
    start_date: str,
    end_date: str,
) -> None:
    """Download CRSP Daily Stock File (raw, without names). Downloads year-by-year sequentially."""
    if file_exists_skip(output_path, "CRSP Daily Stock File"):
        return

    print("Downloading CRSP Daily Stock File...")

    chunks = _generate_yearly_chunks(start_date, end_date)
    all_dfs = []
    for chunk_start, chunk_end in chunks:
        year = chunk_start[:4]
        print(f"  Downloading year {year}...", flush=True)
        query = f"""
        SELECT permno, date, prc, ret, vol, shrout, askhi, bidlo, bid, ask
        FROM crsp.dsf
        WHERE date >= '{chunk_start}' AND date <= '{chunk_end}'
        """
        df_chunk = run_query(conn, query)
        print(f"  Year {year}: {len(df_chunk):,} rows", flush=True)
        all_dfs.append(df_chunk)

    df = pl.concat(all_dfs)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"  Saved {len(df):,} rows to {output_path}")


def download_compustat_funda(
    conn: wrds.Connection,
    output_path: Path,
    start_date: str,
    end_date: str,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
    use_parallel: bool = True,
) -> None:
    """
    Download Compustat NA Annual Fundamentals.

    For date ranges spanning multiple years, uses parallel downloading.
    """
    if file_exists_skip(output_path, "Compustat NA Fundamentals"):
        return

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    multi_year = end_year > start_year

    if use_parallel and multi_year:
        download_compustat_funda_parallel(
            output_path, start_date, end_date, max_workers
        )
    else:
        print("Downloading Compustat NA Fundamentals (sequential)...")

        query = f"""
        SELECT gvkey, datadate, fyear, fyr,
               act, at, capx, ceq, che, cogs, dlc, dltt, dp, dvt,
               ebit, epspx, ib, invt, lct, lt, ni, oiadp, pi,
               ppent, prstkc, sale, txp, wcapch, xsga, pstk, ajex, prcc_f, csho, sich
        FROM comp.funda
        WHERE indfmt='INDL'
          AND datafmt='STD'
          AND popsrc='D'
          AND consol='C'
          AND datadate >= '{start_date}'
          AND datadate <= '{end_date}'
        """

        df = run_query(conn, query)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path)
        print(f"  Saved {len(df):,} rows to {output_path}")


def download_crsp_msenames(conn: wrds.Connection, output_path: Path) -> None:
    """Download CRSP Names/Header table."""
    if file_exists_skip(output_path, "CRSP Names table"):
        return

    print("Downloading CRSP Names table...")

    query = """
    SELECT permno, namedt, nameendt, exchcd, shrcd
    FROM crsp.msenames
    """

    df = run_query(conn, query)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"  Saved {len(df):,} rows to {output_path}")


def download_ccm_link(conn: wrds.Connection, output_path: Path) -> None:
    """Download CRSP-Compustat Link Table."""
    if file_exists_skip(output_path, "CCM Link Table"):
        return

    print("Downloading CCM Link Table...")

    query = """
    SELECT gvkey, lpermno, linkdt, linkenddt, linktype, linkprim
    FROM crsp.ccmxpf_lnkhist
    WHERE linktype IN ('LU', 'LC')
      AND linkprim IN ('P', 'C')
    """

    df = run_query(conn, query)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"  Saved {len(df):,} rows to {output_path}")


# =============================================================================
# Local Join Functions
# =============================================================================


def join_crsp_with_names(msf_path: Path, names_path: Path, output_path: Path) -> None:
    """Join CRSP monthly with names table locally using ASOF join."""
    if file_exists_skip(output_path, "CRSP monthly joined"):
        return

    print("Joining CRSP monthly with names table locally...")

    msf = pl.read_parquet(msf_path)
    names = pl.read_parquet(names_path)

    # Sort for ASOF join
    msf = msf.sort(["permno", "date"])
    names = names.sort(["permno", "namedt"])

    # ASOF join: for each msf row, find the most recent names row
    # where namedt <= date
    result = msf.join_asof(
        names.select(["permno", "namedt", "nameendt", "exchcd", "shrcd"]),
        left_on="date",
        right_on="namedt",
        by="permno",
        strategy="backward",
    )

    # Filter: keep only rows where date <= nameendt
    result = result.filter(pl.col("date") <= pl.col("nameendt"))

    # Drop helper columns
    result = result.drop(["namedt", "nameendt"])

    result.write_parquet(output_path)
    print(f"  Saved {len(result):,} rows to {output_path}")


# =============================================================================
# Fama-French Factor Downloads (from Kenneth French's website)
# =============================================================================

# URLs for Fama-French data
FF3_DAILY_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"
FF48_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes48.zip"


def download_ff_factors(output_path: Path) -> None:
    """
    Download Fama-French 3 Factors (Daily) from Kenneth French's website.

    Downloads ZIP, extracts CSV, parses and converts to parquet.
    Output columns: date, mktrf, smb, hml, rf
    """
    if file_exists_skip(output_path, "Fama-French 3 Factors (Daily)"):
        return

    print("Downloading Fama-French 3 Factors (Daily)...")

    try:
        # Download ZIP file
        with urllib.request.urlopen(FF3_DAILY_URL) as response:
            zip_data = response.read()

        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            # Find the CSV file (usually named F-F_Research_Data_Factors_daily.CSV)
            csv_name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
            csv_content = zf.read(csv_name).decode("utf-8")

        # Parse CSV - skip header comment lines
        lines = csv_content.strip().split("\n")

        # Find the header line (contains "Mkt-RF")
        header_idx = None
        for i, line in enumerate(lines):
            if "Mkt-RF" in line or "Mkt_RF" in line or "mkt" in line.lower():
                header_idx = i
                break

        if header_idx is None:
            raise ValueError("Could not find header line in FF factors CSV")

        # Parse data lines (skip header comments and find where data ends)
        data_rows = []
        for line in lines[header_idx + 1 :]:
            line = line.strip()
            if not line:
                continue
            # Data lines start with a date (8 digits)
            parts = line.split(",")
            if len(parts) >= 5 and parts[0].strip().isdigit():
                date_str = parts[0].strip()
                if len(date_str) == 8:  # YYYYMMDD format
                    try:
                        mktrf = float(parts[1].strip()) / 100  # Convert from %
                        smb = float(parts[2].strip()) / 100
                        hml = float(parts[3].strip()) / 100
                        rf = float(parts[4].strip()) / 100
                        data_rows.append(
                            {
                                "date": datetime.strptime(date_str, "%Y%m%d").date(),
                                "mktrf": mktrf,
                                "smb": smb,
                                "hml": hml,
                                "rf": rf,
                            }
                        )
                    except (ValueError, IndexError):
                        continue  # Skip malformed lines

        if not data_rows:
            raise ValueError("No valid data rows found in FF factors CSV")

        # Create Polars DataFrame and save
        df = pl.DataFrame(data_rows)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path)
        print(f"  Saved {len(df):,} rows to {output_path}")

    except Exception as e:
        print(f"  ERROR downloading FF factors: {e}")
        raise


def download_ff48_industries(output_path: Path) -> None:
    """
    Download Fama-French 48 Industry Classifications from Kenneth French's website.

    Parses SIC code ranges and expands them to individual SIC codes.
    Output columns: sic, ff48
    """
    if file_exists_skip(output_path, "Fama-French 48 Industries"):
        return

    print("Downloading Fama-French 48 Industry Classifications...")

    try:
        # Download ZIP file
        with urllib.request.urlopen(FF48_URL) as response:
            zip_data = response.read()

        # Extract the SIC codes file from ZIP
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            # Find the siccodes file
            sic_name = [n for n in zf.namelist() if "sic" in n.lower()][0]
            sic_content = zf.read(sic_name).decode("utf-8")

        # Parse the SIC ranges file
        # Format:
        #  1 Agric  Agriculture
        #        0100-0199
        #        0200-0299
        #  2 Food   Food Products
        #        2000-2009
        lines = sic_content.strip().split("\n")

        sic_to_ff48 = []
        current_industry = None

        for line in lines:
            line = line.rstrip()
            if not line:
                continue

            # Check if this is an industry header line (starts with industry number 1-48)
            # Pattern: " 1 Agric" or "1 Agric"
            header_match = re.match(r"^\s*(\d{1,2})\s+[A-Za-z]", line)
            if header_match:
                ind_num = int(header_match.group(1))
                if 1 <= ind_num <= 48:
                    current_industry = ind_num
                continue

            # Check if this is a SIC range line
            # Pattern: "       0100-0199" or "0100-0199" or "100-199"
            # Allow trailing text/comments
            range_match = re.match(r"^\s*(\d+)\s*-\s*(\d+)", line)
            if range_match and current_industry is not None:
                start_sic = int(range_match.group(1))
                end_sic = int(range_match.group(2))
                # Expand range to individual SIC codes
                for sic in range(start_sic, end_sic + 1):
                    sic_to_ff48.append({"sic": sic, "ff48": current_industry})

        if not sic_to_ff48:
            raise ValueError("No SIC codes found in FF48 file")

        # Create Polars DataFrame and save
        df = pl.DataFrame(sic_to_ff48)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path)
        print(f"  Saved {len(df):,} SIC-to-FF48 mappings to {output_path}")

    except Exception as e:
        print(f"  ERROR downloading FF48 industries: {e}")
        raise


# =============================================================================
# Main Download Function
# =============================================================================


def main():
    """Main download function for US data."""
    # Calculate dynamic defaults
    # If before March, use year - 2 (data not yet published)
    # If March or later, use year - 1
    now = datetime.now()
    years_back = 2 if now.month < 3 else 1
    default_end = f"{now.year - years_back}-12-31"
    default_start = "1987-01-01"

    parser = argparse.ArgumentParser(
        description="Download US WRDS data for Freyberger 62 Characteristics"
    )
    parser.add_argument(
        "--start-date",
        default=default_start,
        help=f"Start date in YYYY-MM-DD format (default: {default_start})",
    )
    parser.add_argument(
        "--end-date",
        default=default_end,
        help=f"End date in YYYY-MM-DD format (default: {default_end})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if files already exist",
    )
    parser.add_argument(
        "--skip-factors",
        action="store_true",
        help="Skip downloading Fama-French factors from Ken French's website",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=MAX_DOWNLOAD_WORKERS,
        help=f"Maximum parallel download workers for daily data (default: {MAX_DOWNLOAD_WORKERS}, max recommended: 8)",
    )
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Disable parallel downloading (use sequential download instead)",
    )
    parser.add_argument(
        "--clean-checkpoints",
        action="store_true",
        help="Remove checkpoint folders after successful completion of fiscal year characteristics",
    )
    args = parser.parse_args()

    # Set global force flag
    global FORCE_DOWNLOAD
    FORCE_DOWNLOAD = args.force

    start_date = args.start_date
    end_date = args.end_date
    max_workers = args.max_workers
    use_parallel = not args.no_parallel
    clean_checkpoints = args.clean_checkpoints

    print("=" * 60)
    print("US WRDS Data Downloader for Freyberger 62 Characteristics")
    print("=" * 60)
    print(f"\nData will be saved to: {DATA_DIR.absolute()}")
    print(f"Date range: {start_date} to {end_date}")
    if use_parallel:
        print(f"Parallel download: enabled ({max_workers} workers)")
    else:
        print("Parallel download: disabled (sequential mode)")
    print(
        f"Checkpoint cleanup: {'enabled' if clean_checkpoints else 'disabled (checkpoints will be kept)'}"
    )
    print()

    # Get connection (wrds library handles authentication)
    conn = get_wrds_connection()
    print("  Connection successful!\n")

    # Download Fama-French factors FIRST (needed for fiscal year characteristics)
    if not args.skip_factors:
        print("\n" + "=" * 40)
        print("Downloading Fama-French Factors (needed for Beta calculation)")
        print("=" * 40)

        download_ff_factors(DATA_DIR / "factors" / "ff_factors_daily.parquet")
        download_ff48_industries(DATA_DIR / "factors" / "ff48_industries.parquet")

    # Download US data
    print("\n" + "=" * 40)
    print("Downloading US Data")
    print("=" * 40)

    # Download CRSP monthly and names separately, then join locally
    download_crsp_monthly(
        conn,
        DATA_DIR / "us" / "crsp_msf_raw.parquet",
        start_date,
        end_date,
    )
    download_crsp_msenames(conn, DATA_DIR / "us" / "crsp_msenames.parquet")

    # Perform local ASOF join (much faster than SQL join)
    join_crsp_with_names(
        DATA_DIR / "us" / "crsp_msf_raw.parquet",
        DATA_DIR / "us" / "crsp_msenames.parquet",
        DATA_DIR / "us" / "crsp_monthly.parquet",
    )

    # Download CRSP daily data year-by-year and compute fiscal year characteristics
    # This is memory-efficient: only 2 years in memory at a time
    # Output: fiscal year characteristics (not raw daily data)
    # Uses checkpointing: saves each fiscal year to disk immediately, can resume if interrupted
    download_crsp_daily_compute_fiscal_chars(
        DATA_DIR / "us" / "crsp_fiscal_chars.parquet",
        start_date,
        end_date,
        factors_path=DATA_DIR / "factors" / "ff_factors_daily.parquet",
        clean_checkpoints=clean_checkpoints,
    )

    # Reconnect after long-running download (connection may have timed out)
    try:
        conn.close()
    except Exception:
        pass  # Connection might already be dead
    conn = get_wrds_connection()
    print("  Reconnected to WRDS.\n")

    # Download raw CRSP daily data (needed for beta computation in characteristics pipeline)
    download_crsp_daily(
        conn,
        DATA_DIR / "us" / "crsp_daily.parquet",
        start_date,
        end_date,
    )

    download_compustat_funda(
        conn,
        DATA_DIR / "us" / "compustat_funda.parquet",
        start_date,
        end_date,
        max_workers=max_workers,
        use_parallel=use_parallel,
    )
    download_ccm_link(conn, DATA_DIR / "us" / "ccm_link.parquet")

    # Close connection
    conn.close()

    print("\n" + "=" * 60)
    print("Download complete!")
    print("=" * 60)
    print(f"\nData saved to: {DATA_DIR.absolute()}")


if __name__ == "__main__":
    main()
