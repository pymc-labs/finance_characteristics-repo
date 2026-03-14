"""
Data cleaning and filtering module for Freyberger 62 characteristics (US data).

Implements US-specific filters:
- Exchange codes (NYSE, AMEX, NASDAQ)
- Share codes (common shares)
"""

import polars as pl

from data_collection.config import USFilters


# =============================================================================
# US Data Cleaners
# =============================================================================


def clean_us_crsp(
    lf: pl.LazyFrame,
    filters: USFilters | None = None,
) -> pl.LazyFrame:
    """
    Clean US CRSP data with standard filters.

    Filters applied:
    - Exchange codes 1-3 (NYSE, AMEX, NASDAQ)
    - Share codes 10, 11 (common shares)
    - Non-null price and return

    Args:
        lf: LazyFrame with CRSP data
        filters: USFilters configuration (default filters if None)

    Returns:
        Cleaned LazyFrame
    """
    if filters is None:
        filters = USFilters()

    # Check which columns exist
    cols = lf.collect_schema().names()

    result = lf.filter(pl.col("prc").is_not_null() & (pl.col("prc") > 0))

    # Apply exchange code filter if column exists
    if "exchcd" in cols:
        result = result.filter(pl.col("exchcd").is_in(filters.exchange_codes))

    # Apply share code filter if column exists
    if "shrcd" in cols:
        result = result.filter(pl.col("shrcd").is_in(filters.share_codes))

    return result


def clean_us_compustat(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Clean US Compustat annual data.

    Filters applied:
    - Valid fiscal year
    - Non-null total assets
    """
    return lf.filter(pl.col("at").is_not_null()).filter(pl.col("fyear").is_not_null())


# =============================================================================
# Return Cleaning
# =============================================================================


def clean_returns(
    lf: pl.LazyFrame,
    ret_col: str = "ret",
    min_ret: float = -0.99,
    max_ret: float = 10.0,
) -> pl.LazyFrame:
    """
    Clean return data by removing extreme values.

    Args:
        lf: LazyFrame with returns
        ret_col: Return column name
        min_ret: Minimum valid return (-99%)
        max_ret: Maximum valid return (1000%)

    Returns:
        LazyFrame with cleaned returns
    """
    return lf.filter(
        pl.col(ret_col).is_not_null()
        & (pl.col(ret_col) >= min_ret)
        & (pl.col(ret_col) <= max_ret)
    )


def winsorize(
    lf: pl.LazyFrame,
    cols: list[str],
    lower: float = 0.01,
    upper: float = 0.99,
    by_date: bool = True,
) -> pl.LazyFrame:
    """
    Winsorize columns at specified percentiles.

    Args:
        lf: LazyFrame with data
        cols: Columns to winsorize
        lower: Lower percentile (default 1%)
        upper: Upper percentile (default 99%)
        by_date: Whether to winsorize cross-sectionally by date

    Returns:
        LazyFrame with winsorized values
    """
    winsorize_exprs = []

    for col in cols:
        if by_date:
            lower_bound = pl.col(col).quantile(lower).over("date")
            upper_bound = pl.col(col).quantile(upper).over("date")
        else:
            lower_bound = pl.col(col).quantile(lower)
            upper_bound = pl.col(col).quantile(upper)

        winsorize_exprs.append(pl.col(col).clip(lower_bound, upper_bound).alias(col))

    return lf.with_columns(winsorize_exprs)


# =============================================================================
# Unified Cleaner
# =============================================================================


class DataCleaner:
    """
    Data cleaner for US CRSP/Compustat data.
    """

    def __init__(self):
        self.us_filters = USFilters()

    def clean_price_data(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Clean CRSP price/security data."""
        return clean_us_crsp(lf, self.us_filters)

    def clean_fundamentals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Clean Compustat fundamentals data."""
        return clean_us_compustat(lf)

    def clean_returns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Clean return data."""
        return clean_returns(lf)
