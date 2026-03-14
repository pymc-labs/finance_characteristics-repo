"""
Data merging module for Freyberger 62 characteristics (US data).

Handles merging of price/return data with annual fundamentals using
Fama-French timing conventions.

Key convention:
- Fiscal Year End in calendar year t-1 matched with returns from July year t to June year t+1
- 6-month publication lag assumption
"""

from pathlib import Path

import polars as pl


# =============================================================================
# Fama-French Timing Convention
# =============================================================================


def apply_fama_french_timing(
    funda_lf: pl.LazyFrame,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Apply Fama-French timing to fundamentals data.

    Convention:
    - Fiscal year ending in calendar year t-1 is matched to returns
      from July of year t through June of year t+1
    - This accounts for ~6 month publication lag

    Creates columns:
    - link_start: First month to use this fiscal year's data (July t)
    - link_end: Last month to use this fiscal year's data (June t+1)

    Args:
        funda_lf: Annual fundamentals with datadate and fyear
        id_col: Identifier column

    Returns:
        LazyFrame with link date columns
    """
    return funda_lf.with_columns(
        [
            # Calendar year of fiscal year end
            pl.col("datadate").dt.year().alias("cal_year"),
        ]
    ).with_columns(
        [
            # Link start: July of calendar year + 1
            pl.date(pl.col("cal_year") + 1, 7, 1).alias("link_start"),
            # Link end: June of calendar year + 2
            pl.date(pl.col("cal_year") + 2, 6, 30).alias("link_end"),
        ]
    )


def merge_price_fundamentals(
    price_lf: pl.LazyFrame,
    funda_lf: pl.LazyFrame,
    link_table: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Merge CRSP price/return data with Compustat fundamentals using FF timing.

    Uses CCM link table to map permno to gvkey.

    Args:
        price_lf: Monthly price/return data
        funda_lf: Annual fundamentals with FF timing applied
        link_table: CCM link table

    Returns:
        Merged LazyFrame with price and fundamental data
    """
    # Step 1: Add gvkey to price data via link table
    price_with_gvkey = (
        price_lf.join(link_table, on="permno", how="inner")
        # Filter to valid link period
        .filter(
            (pl.col("date") >= pl.col("linkdt"))
            & (pl.col("linkenddt").is_null() | (pl.col("date") <= pl.col("linkenddt")))
        )
        .drop(["linkdt", "linkenddt", "linktype", "linkprim"])
    )

    # Step 2: Apply FF timing to fundamentals
    funda_with_timing = apply_fama_french_timing(funda_lf)

    # Step 3: Merge using FF timing
    # Price date must fall within link_start and link_end
    merged = price_with_gvkey.join(funda_with_timing, on="gvkey", how="left").filter(
        (pl.col("date") >= pl.col("link_start"))
        & (pl.col("date") <= pl.col("link_end"))
    )

    return merged


# =============================================================================
# Alternative Timing Methods
# =============================================================================


def merge_with_lag(
    price_lf: pl.LazyFrame,
    funda_lf: pl.LazyFrame,
    id_col: str = "gvkey",
    lag_months: int = 6,
) -> pl.LazyFrame:
    """
    Alternative merging with fixed lag (not FF convention).

    Matches fundamentals to prices with a specified lag in months.

    Args:
        price_lf: Monthly price data
        funda_lf: Annual fundamentals
        id_col: Identifier column
        lag_months: Months to lag (default 6)

    Returns:
        Merged LazyFrame
    """
    # Add lagged availability date to fundamentals
    funda_with_lag = funda_lf.with_columns(
        [
            # Available lag_months after fiscal year end
            (pl.col("datadate") + pl.duration(days=lag_months * 30)).alias(
                "available_date"
            ),
        ]
    )

    # For each price date, find the most recent available fundamental
    # This requires an asof join
    merged = price_lf.sort("date").join_asof(
        funda_with_lag.sort("available_date"),
        left_on="date",
        right_on="available_date",
        by=id_col,
        strategy="backward",
    )

    return merged


# =============================================================================
# Point-in-Time Merge
# =============================================================================


def merge_point_in_time(
    price_lf: pl.LazyFrame,
    funda_lf: pl.LazyFrame,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Point-in-time merge ensuring no look-ahead bias.

    Uses the most recent fundamentals available at each price date,
    based on the fiscal year end date plus a standard publication lag.

    Args:
        price_lf: Price data
        funda_lf: Fundamentals data
        id_col: Identifier column

    Returns:
        Merged LazyFrame with point-in-time data
    """
    # Assume 4-month average publication lag
    funda_pit = funda_lf.with_columns(
        [
            # Estimated publication date
            (pl.col("datadate") + pl.duration(days=120)).alias("pub_date"),
        ]
    )

    # Asof join to get most recent published fundamentals
    merged = price_lf.sort("date").join_asof(
        funda_pit.sort("pub_date"),
        left_on="date",
        right_on="pub_date",
        by=id_col,
        strategy="backward",
    )

    return merged


# =============================================================================
# Merge Daily with Monthly Fundamentals
# =============================================================================


def merge_daily_with_monthly_funda(
    daily_lf: pl.LazyFrame,
    monthly_funda_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Merge daily price data with monthly-aligned fundamentals.

    Used when fundamentals have already been aligned to monthly frequency.

    Args:
        daily_lf: Daily price data
        monthly_funda_lf: Monthly-aligned fundamentals
        id_col: Identifier column

    Returns:
        Daily data with fundamentals
    """
    # Add month-end to daily data
    daily_with_month = daily_lf.with_columns(
        [
            pl.col("date").dt.month_end().alias("month_end"),
        ]
    )

    # Join on id and month
    merged = daily_with_month.join(
        monthly_funda_lf,
        left_on=[id_col, "month_end"],
        right_on=[id_col, "date"],
        how="left",
    )

    return merged


# =============================================================================
# Fiscal Year Characteristics Merge
# =============================================================================


def merge_fiscal_chars_to_monthly(
    monthly_lf: pl.LazyFrame,
    fiscal_chars_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Merge fiscal year characteristics to monthly price data.

    Follows Fama-French timing convention:
    - Fiscal year t characteristics (computed from June t to May t+1 daily data)
      are assigned to months June t through May t+1

    This means:
    - June 2020 through May 2021 get fiscal_year=2020 characteristics
    - June 2021 through May 2022 get fiscal_year=2021 characteristics

    Args:
        monthly_lf: Monthly price/return data with date column
        fiscal_chars_lf: Fiscal year characteristics with fiscal_year column
        id_col: Identifier column (e.g., 'permno')

    Returns:
        LazyFrame with monthly data and fiscal year characteristics merged
    """
    # Add fiscal_year column to monthly data
    # June onwards = current calendar year
    # Jan-May = previous calendar year
    monthly_with_fy = monthly_lf.with_columns(
        [
            pl.when(pl.col("date").dt.month() >= 6)
            .then(pl.col("date").dt.year())
            .otherwise(pl.col("date").dt.year() - 1)
            .alias("fiscal_year")
        ]
    )

    # Join on id + fiscal_year
    merged = monthly_with_fy.join(
        fiscal_chars_lf,
        on=[id_col, "fiscal_year"],
        how="left",
    )

    return merged


# =============================================================================
# Helper Functions
# =============================================================================


def get_june_me(
    price_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Get June market equity for annual sorting.

    Standard in Fama-French methodology: use June ME for portfolio sorts.

    Args:
        price_lf: Monthly price data with ME column
        id_col: Identifier column

    Returns:
        LazyFrame with June ME by year
    """
    return (
        price_lf.filter(pl.col("date").dt.month() == 6)
        .with_columns(
            [
                pl.col("date").dt.year().alias("year"),
            ]
        )
        .select([id_col, "year", "ME"])
        .rename({"ME": "ME_june"})
    )


def get_december_me(
    price_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Get December market equity for B/M ratio.

    Used in some specifications for book-to-market calculation.

    Args:
        price_lf: Monthly price data with ME column
        id_col: Identifier column

    Returns:
        LazyFrame with December ME by year
    """
    return (
        price_lf.filter(pl.col("date").dt.month() == 12)
        .with_columns(
            [
                pl.col("date").dt.year().alias("year"),
            ]
        )
        .select([id_col, "year", "ME"])
        .rename({"ME": "ME_dec"})
    )


# =============================================================================
# Normalized Yearly + Monthly Price Merge
# =============================================================================


def merge_normalized_yearly_with_monthly(
    normalized_yearly_lf: pl.LazyFrame,
    monthly_price_lf: pl.LazyFrame,
    link_table: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Merge normalized yearly characteristics with monthly price data.

    This function:
    1. Takes already-normalized yearly characteristics
    2. Takes monthly price characteristics (momentum, trading)
    3. Forward-fills yearly data to monthly frequency using FF timing
    4. Returns merged dataset at monthly frequency

    Fama-French timing convention:
    - Fiscal year ending in calendar year t is matched to returns
      from July of year t+1 through June of year t+2
    - This accounts for ~6 month publication lag

    Args:
        normalized_yearly_lf: Normalized yearly characteristics with columns:
            - gvkey, datadate, fyear, + characteristic columns (IDs 6-47)
        monthly_price_lf: Monthly price data with price characteristics:
            - permno, date, prc, ME, + momentum (IDs 1-5), trading (IDs 48-62)
        link_table: CCM link table (maps permno to gvkey)

    Returns:
        LazyFrame with monthly frequency containing all characteristics
    """
    # Apply FF timing to yearly characteristics
    yearly_with_timing = normalized_yearly_lf.with_columns(
        [
            pl.col("datadate").dt.year().alias("cal_year"),
        ]
    ).with_columns(
        [
            pl.date(pl.col("cal_year") + 1, 7, 1).alias("link_start"),
            pl.date(pl.col("cal_year") + 2, 6, 30).alias("link_end"),
        ]
    )

    # Get characteristic columns from yearly data (exclude metadata columns)
    yearly_schema = normalized_yearly_lf.collect_schema().names()
    metadata_cols = [
        "gvkey",
        "datadate",
        "fyear",
        "fyr",
        "cal_year",
        "link_start",
        "link_end",
        "ME",
        "BE",
        "NOA",
        "GP",
        "OpAcc",
    ]
    yearly_char_cols = [c for c in yearly_schema if c not in metadata_cols]

    select_cols = ["gvkey", "link_start", "link_end"]
    select_cols.extend(yearly_char_cols)

    yearly_for_merge = yearly_with_timing.select(select_cols)

    # Add gvkey to monthly data via CCM link table
    price_with_gvkey = (
        monthly_price_lf.join(link_table, on="permno", how="inner")
        .filter(
            (pl.col("date") >= pl.col("linkdt"))
            & (
                pl.col("linkenddt").is_null()
                | (pl.col("date") <= pl.col("linkenddt"))
            )
        )
        .drop(["linkdt", "linkenddt", "linktype", "linkprim"])
    )

    # Merge with yearly characteristics
    merged = (
        price_with_gvkey.join(yearly_for_merge, on="gvkey", how="left")
        .filter(
            (pl.col("date") >= pl.col("link_start"))
            & (pl.col("date") <= pl.col("link_end"))
        )
        .drop(["link_start", "link_end"])
    )

    return merged


def merge_normalized_yearly_with_monthly_chunked(
    normalized_yearly_lf: pl.LazyFrame,
    monthly_price_lf: pl.LazyFrame,
    chunk_years: int = 5,
    temp_dir: "Path | None" = None,
    link_table: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """
    Memory-efficient chunked merge of normalized yearly with monthly data.

    Processes data in year chunks to avoid memory issues.

    Args:
        normalized_yearly_lf: Normalized yearly characteristics
        monthly_price_lf: Monthly price data with price characteristics
        chunk_years: Number of years per chunk
        temp_dir: Directory for temporary chunk files
        link_table: CCM link table (maps permno to gvkey)

    Returns:
        LazyFrame scanning all merged chunks
    """
    import shutil

    if link_table is None:
        raise ValueError("link_table required for US data")

    # Get year range from monthly data
    year_range = monthly_price_lf.select(
        [
            pl.col("date").dt.year().min().alias("min_year"),
            pl.col("date").dt.year().max().alias("max_year"),
        ]
    ).collect()

    min_year = year_range["min_year"][0]
    max_year = year_range["max_year"][0]

    # Generate chunks
    chunks = []
    current_year = min_year
    while current_year <= max_year:
        end_year = min(current_year + chunk_years - 1, max_year)
        chunks.append((current_year, end_year))
        current_year = end_year + 1

    # Setup temp directory
    if temp_dir is None:
        temp_dir = Path("./data/outputs/.temp_merge_chunks")

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Process each chunk
    chunk_files = []
    for i, (start_year, end_year) in enumerate(chunks):
        # Filter monthly data for this chunk
        monthly_chunk = monthly_price_lf.filter(
            (pl.col("date").dt.year() >= start_year)
            & (pl.col("date").dt.year() <= end_year)
        )

        # Filter yearly data (need years before chunk for FF timing)
        yearly_chunk = normalized_yearly_lf.filter(
            (pl.col("datadate").dt.year() >= start_year - 2)
            & (pl.col("datadate").dt.year() <= end_year)
        )

        # Merge this chunk
        merged_chunk = merge_normalized_yearly_with_monthly(
            yearly_chunk, monthly_chunk, link_table
        )

        # Save chunk
        chunk_file = temp_dir / f"merged_chunk_{i:03d}_{start_year}_{end_year}.parquet"
        merged_chunk.collect().write_parquet(chunk_file)
        chunk_files.append(chunk_file)

    # Return lazy scan of all chunks
    return pl.scan_parquet(temp_dir / "merged_chunk_*.parquet")
