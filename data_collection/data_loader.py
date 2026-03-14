"""
Data loader module for Freyberger 62 characteristics (US data).

Provides functions to load US (CRSP/Compustat NA) data using Polars
LazyFrames for efficient memory management.
"""

import polars as pl
from pathlib import Path
from typing import Literal

from data_collection.config import PathConfig, FUNDAMENTAL_VARIABLES


# =============================================================================
# US Data Loaders (CRSP / Compustat NA)
# =============================================================================


def load_us_crsp_monthly(path: Path) -> pl.LazyFrame:
    """
    Load US CRSP monthly stock file.

    Variables loaded: permno, date, prc, ret, vol, shrout, cfacpr

    OPTIMIZED: Uses Int32/Float32 where possible to reduce memory footprint.
    """
    return (
        pl.scan_parquet(path)
        .select(
            [
                pl.col("permno").cast(pl.Int32),  # OPTIMIZED: Int32 is sufficient
                pl.col("date").cast(pl.Date),
                pl.col("prc").abs().cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("ret").cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("vol").cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("shrout").cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("cfacpr").cast(pl.Float32),  # OPTIMIZED: Float32
            ]
        )
        .filter(pl.col("prc").is_not_null())
    )


def load_us_crsp_daily(path: Path) -> pl.LazyFrame:
    """
    Load US CRSP daily stock file.

    Variables loaded: permno, date, prc, ret, vol, shrout, askhi, bidlo, bid, ask

    OPTIMIZED: Uses Int32/Float32 where possible to reduce memory footprint.
    """
    return (
        pl.scan_parquet(path)
        .select(
            [
                pl.col("permno").cast(pl.Int32),  # OPTIMIZED: Int32
                pl.col("date").cast(pl.Date),
                pl.col("prc").abs().cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("ret").cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("vol").cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("shrout").cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("askhi").cast(pl.Float32).alias("high"),  # OPTIMIZED: Float32
                pl.col("bidlo").cast(pl.Float32).alias("low"),  # OPTIMIZED: Float32
                pl.col("bid").cast(pl.Float32),  # OPTIMIZED: Float32
                pl.col("ask").cast(pl.Float32),  # OPTIMIZED: Float32
            ]
        )
        .filter(pl.col("prc").is_not_null())
    )


def load_us_compustat(path: Path) -> pl.LazyFrame:
    """
    Load US Compustat NA annual fundamentals.

    Loads all variables defined in FUNDAMENTAL_VARIABLES plus identifiers.
    """
    # Build column selection dynamically
    cols = ["gvkey", "datadate", "fyear", "fyr"] + FUNDAMENTAL_VARIABLES

    # Add EPS variable (US uses epspx)
    if "epspx" not in cols:
        cols.append("epspx")

    # Add fiscal year-end price
    if "prcc_f" not in cols:
        cols.append("prcc_f")

    return (
        pl.scan_parquet(path)
        .select([pl.col(c) for c in cols if c != "eps"])  # eps is epspx in US
        .rename({"epspx": "eps"})
        .with_columns(
            [
                pl.col("gvkey").cast(pl.Utf8),
                pl.col("datadate").cast(pl.Date),
            ]
        )
    )


def load_us_ccm_link(path: Path) -> pl.LazyFrame:
    """
    Load CRSP-Compustat Merged link table.

    Used to merge CRSP (permno) with Compustat (gvkey).
    """
    return (
        pl.scan_parquet(path)
        .select(
            [
                pl.col("gvkey").cast(pl.Utf8),
                pl.col("lpermno").cast(pl.Int64).alias("permno"),
                pl.col("linkdt").cast(pl.Date),
                pl.col("linkenddt").cast(pl.Date),
                pl.col("linktype"),
                pl.col("linkprim"),
            ]
        )
        # Keep only primary links
        .filter(
            pl.col("linktype").is_in(["LU", "LC"])
            & pl.col("linkprim").is_in(["P", "C"])
        )
    )


# =============================================================================
# Factor Data Loaders
# =============================================================================


def load_ff_factors(
    path: Path, frequency: Literal["daily", "monthly"] = "monthly"
) -> pl.LazyFrame:
    """
    Load Fama-French factor data.

    Used for:
    - Beta calculation (Market factor)
    - Idiosyncratic volatility (FF3 factors)

    OPTIMIZED: Uses Float32 to reduce memory footprint.
    """
    return pl.scan_parquet(path).select(
        [
            pl.col("date").cast(pl.Date),
            pl.col("mktrf").cast(pl.Float32),  # OPTIMIZED: Float32
            pl.col("smb").cast(pl.Float32),  # OPTIMIZED: Float32
            pl.col("hml").cast(pl.Float32),  # OPTIMIZED: Float32
            pl.col("rf").cast(pl.Float32),  # OPTIMIZED: Float32
        ]
    )


def load_ff48_industries(path: Path) -> pl.LazyFrame:
    """
    Load Fama-French 48 industry classifications.

    Used for industry-adjusted characteristics (PM_adj, SAT_adj, BEME_adj, LME_adj).
    """
    return pl.scan_parquet(path).select(
        [
            pl.col("sic").cast(pl.Int32),
            pl.col("ff48").cast(pl.Int32).alias("industry"),
        ]
    )


# =============================================================================
# Unified Data Loader
# =============================================================================


class DataLoader:
    """
    Data loader for US CRSP/Compustat data.

    Handles loading and initial preprocessing of all required datasets.
    """

    def __init__(self, config: PathConfig):
        self.config = config

    def load_price_data(
        self, frequency: Literal["daily", "monthly"] = "monthly"
    ) -> pl.LazyFrame:
        """Load CRSP price/returns data."""
        if frequency == "monthly":
            return load_us_crsp_monthly(self.config.us_crsp_monthly)
        else:
            return load_us_crsp_daily(self.config.us_crsp_daily)

    def load_fundamentals(self) -> pl.LazyFrame:
        """Load Compustat annual fundamentals data."""
        return load_us_compustat(self.config.us_compustat)

    def load_link_table(self) -> pl.LazyFrame:
        """Load CRSP-Compustat Merged link table."""
        return load_us_ccm_link(self.config.us_ccm_link)

    def load_factors(
        self, frequency: Literal["daily", "monthly"] = "monthly"
    ) -> pl.LazyFrame:
        """Load Fama-French factors."""
        return load_ff_factors(self.config.ff_factors, frequency)

    def load_industries(self) -> pl.LazyFrame:
        """Load FF48 industry classifications."""
        return load_ff48_industries(self.config.ff48_industries)
