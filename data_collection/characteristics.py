"""
Main characteristic builder for Freyberger 62 characteristics (US data).

This module orchestrates the computation of all 62 firm characteristics
from Freyberger, Neuhierl, & Weber (2020) using CRSP/Compustat NA data.

Categories:
- Past Returns (IDs 1-5): Momentum and reversal
- Investment (IDs 6-11): Asset growth, share issuance
- Profitability (IDs 12-28): ROA, ROE, margins
- Intangibles (IDs 29-32): Accruals, tangibility
- Value (IDs 33-47): Book-to-market, earnings yield
- Trading (IDs 48-62): Beta, volatility, liquidity
"""

import polars as pl
import shutil
from pathlib import Path

from data_collection.config import (
    PathConfig,
    get_characteristic_names,
)
from data_collection.data_loader import DataLoader
from data_collection.cleaners import DataCleaner
from data_collection.construction import (
    compute_momentum_characteristics,
    compute_volatility_characteristics,
    compute_beta_characteristics,
    compute_trading_characteristics,
    compute_intermediate_variables,
    compute_investment_characteristics,
    compute_profitability_characteristics,
    compute_intangibles_characteristics,
    compute_value_characteristics,
    merge_price_fundamentals,
    merge_fiscal_chars_to_monthly,
)
from data_collection.construction.prices import compute_market_equity, compute_spread
from data_collection.construction.fundamentals import (
    compute_aso,
    compute_industry_adjusted,
)


class CharacteristicBuilder:
    """
    Builder class for computing Freyberger 62 characteristics (US).

    Handles the full pipeline:
    1. Load and clean data
    2. Compute intermediate variables
    3. Compute characteristics by category
    4. Merge price and fundamental characteristics
    5. Output final dataset
    """

    def __init__(
        self,
        config: PathConfig,
        verbose: bool = True,
    ):
        self.config = config
        self.verbose = verbose

        self.loader = DataLoader(config)
        self.cleaner = DataCleaner()

        # Cached data
        self._monthly_price: pl.LazyFrame | None = None
        self._daily_price: pl.LazyFrame | None = None
        self._fiscal_chars: pl.LazyFrame | None = None
        self._fundamentals: pl.LazyFrame | None = None
        self._fundamentals_with_intermediates: pl.LazyFrame | None = None
        self._factors: pl.LazyFrame | None = None
        self._merged_data: pl.LazyFrame | None = None

    def _log(self, msg: str) -> None:
        """Print message if verbose."""
        if self.verbose:
            print(f"[CharacteristicBuilder] {msg}")

    # =========================================================================
    # Data Loading
    # =========================================================================

    def load_data(self) -> None:
        """
        Load and clean all required data.

        Uses pre-computed fiscal year characteristics instead of raw daily data
        when available. This is much more memory efficient.
        """
        self._log("Loading US data...")

        # Load monthly CRSP data
        self._log("Loading monthly CRSP data...")
        monthly_raw = self.loader.load_price_data(frequency="monthly")
        self._monthly_price = self.cleaner.clean_price_data(monthly_raw)

        # Load pre-computed fiscal year characteristics if available
        if self.config.us_fiscal_chars and self.config.us_fiscal_chars.exists():
            self._log("Loading pre-computed fiscal year characteristics...")
            self._fiscal_chars = pl.scan_parquet(self.config.us_fiscal_chars)

        # Load daily data — required for beta computation
        if not (self.config.us_crsp_daily and self.config.us_crsp_daily.exists()):
            raise FileNotFoundError(
                "Daily CRSP data not found (crsp_daily.parquet). "
                "Run download_data.py to fetch daily data before computing characteristics."
            )
        self._log("Loading daily CRSP data...")
        daily_raw = self.loader.load_price_data(frequency="daily")
        self._daily_price = self.cleaner.clean_price_data(daily_raw)

        # Load fundamentals
        self._log("Loading fundamentals...")
        funda_raw = self.loader.load_fundamentals()
        self._fundamentals = self.cleaner.clean_fundamentals(funda_raw)

        # Always load Fama-French factors — required for beta computation
        self._log("Loading Fama-French factors...")
        self._factors = self.loader.load_factors(frequency="daily")

        self._log("Data loading complete.")

    def load_fundamentals_only(self) -> None:
        """
        Load only fundamentals data (skip price data entirely).

        This is much faster as it skips daily price data and FF factors.
        """
        self._log("Loading US fundamentals only...")

        self._log("Loading fundamentals...")
        funda_raw = self.loader.load_fundamentals()
        self._fundamentals = self.cleaner.clean_fundamentals(funda_raw)

        self._log("Fundamentals loading complete.")

    def build_fundamentals_only(self) -> pl.LazyFrame:
        """
        Build only fundamental characteristics (yearly frequency).

        Characteristics included:
        - Investment (IDs 6-11)
        - Profitability (IDs 12-28)
        - Intangibles (IDs 29-32)

        NOT included (require price data):
        - Past Returns (IDs 1-5)
        - Value (IDs 33-47) - require Market Equity
        - Trading (IDs 48-62) - require daily prices
        """
        if self._fundamentals is None:
            self.load_fundamentals_only()

        funda_chars = self.compute_fundamental_characteristics()
        self._merged_data = funda_chars

        self._log("Fundamentals-only build complete.")
        return funda_chars

    def build_yearly_characteristics(self) -> pl.LazyFrame:
        """
        Build yearly characteristics including Value (using fiscal year-end prices).

        Characteristics included:
        - Investment (IDs 6-11)
        - Profitability (IDs 12-28)
        - Intangibles (IDs 29-32)
        - Value (IDs 33-47) using fiscal year-end ME

        NOT included (require monthly/daily data):
        - Past Returns (IDs 1-5)
        - Trading (IDs 48-62)
        """
        if self._fundamentals is None:
            self.load_fundamentals_only()

        # Load fiscal chars if needed (for SUV)
        if self._fiscal_chars is None:
            if self.config.us_fiscal_chars and self.config.us_fiscal_chars.exists():
                self._log(
                    "  Loading pre-computed fiscal year characteristics for SUV..."
                )
                self._fiscal_chars = pl.scan_parquet(self.config.us_fiscal_chars)

        self._log("Building yearly characteristics with fiscal year-end ME...")

        id_col = "gvkey"

        # Compute intermediate variables (BE, NOA, GP, OpAcc)
        if self._fundamentals_with_intermediates is None:
            self._log("  Computing intermediate variables...")
            self._fundamentals_with_intermediates = compute_intermediate_variables(
                self._fundamentals, id_col
            )

        funda_with_int = self._fundamentals_with_intermediates

        # Compute fiscal year-end Market Equity: prcc_f * csho
        self._log("  Computing fiscal year-end Market Equity...")
        funda_with_me = funda_with_int.with_columns(
            (pl.col("prcc_f") * pl.col("csho")).alias("ME")
        )

        # Investment characteristics (IDs 6-11)
        self._log("  Computing investment characteristics (IDs 6-11)...")
        inv_lf = compute_investment_characteristics(funda_with_me, id_col)

        # AShrout (ID 9) - % change in shares outstanding
        self._log("  Computing AShrout (ID 9)...")
        inv_lf = (
            inv_lf.with_columns(
                pl.col("csho").shift(1).over(id_col).alias("shares_lag")
            )
            .with_columns(
                (
                    (pl.col("csho") - pl.col("shares_lag")) / pl.col("shares_lag")
                ).alias("AShrout")
            )
            .drop("shares_lag")
        )

        # Profitability characteristics (IDs 12-28)
        self._log("  Computing profitability characteristics (IDs 12-28)...")
        prof_lf = compute_profitability_characteristics(inv_lf, id_col)

        # ROC (ID 23)
        self._log("  Computing ROC (ID 23)...")
        prof_lf = prof_lf.with_columns(
            ((pl.col("ME") + pl.col("dltt") - pl.col("at")) / pl.col("che")).alias(
                "ROC"
            )
        )

        # Intangibles characteristics (IDs 29-32)
        self._log("  Computing intangibles characteristics (IDs 29-32)...")
        intan_lf = compute_intangibles_characteristics(prof_lf, id_col)

        # ASO (ID 38)
        self._log("  Computing ASO (ID 38)...")
        aso_lf = compute_aso(intan_lf, id_col)

        # Value characteristics (IDs 33-47)
        self._log("  Computing value characteristics (IDs 33-47)...")
        value_lf = compute_value_characteristics(aso_lf, None, id_col)

        # Compute LME (Log Market Equity) - ID 53
        value_lf = value_lf.with_columns(pl.col("ME").log().alias("LME"))

        # Industry-adjusted characteristics
        existing_cols = value_lf.collect_schema().names()
        has_sic = "sic" in existing_cols or "sich" in existing_cols
        has_ff48 = (
            self.config.ff48_industries is not None
            and self.config.ff48_industries.exists()
        )

        if has_sic and has_ff48:
            self._log("  Computing industry-adjusted characteristics...")
            industry_lf = self.loader.load_industries()
            value_lf = compute_industry_adjusted(
                value_lf,
                industry_lf,
                char_cols=["PM", "SAT", "BEME", "LME"],
                date_col="datadate",
            )
        else:
            if not has_sic:
                self._log("  Skipping industry-adjusted (SIC code not in data)...")
            else:
                self._log(
                    "  Skipping industry-adjusted (FF48 industries file not available)..."
                )

        self._merged_data = value_lf

        self._log("Yearly characteristics build complete.")
        return value_lf

    # =========================================================================
    # Characteristic Computation
    # =========================================================================

    def compute_price_characteristics(self) -> pl.LazyFrame:
        """
        Compute characteristics from price/return data.

        Includes:
        - Past Returns (IDs 1-5): Computed from monthly returns
        - Trading (IDs 48-62 partial): Either from pre-computed fiscal chars
          or computed from daily data if not available
        """
        self._log("Computing price-based characteristics...")

        if self._monthly_price is None:
            raise RuntimeError("Data not loaded. Call load_data() first.")

        id_col = "permno"

        # Market Equity
        self._log("  Computing Market Equity (ME)...")
        me_lf = compute_market_equity(self._monthly_price, id_col)

        # Momentum characteristics (IDs 1-5)
        self._log("  Computing momentum characteristics (IDs 1-5)...")
        momentum_lf = compute_momentum_characteristics(me_lf, id_col)

        # Check if we have pre-computed fiscal year characteristics
        if self._fiscal_chars is not None:
            self._log("  Using pre-computed fiscal year characteristics...")

            price_chars = merge_fiscal_chars_to_monthly(
                momentum_lf, self._fiscal_chars, id_col
            )

            self._log("  Fiscal year characteristics merged with monthly data.")

            # Compute DTO (Detrended Turnover) from LTurnover
            self._log("  Computing DTO (Detrended Turnover) from LTurnover...")
            price_chars = (
                price_chars.sort([id_col, "date"])
                .with_columns(
                    pl.col("LTurnover")
                    .rolling_mean(window_size=12)
                    .over(id_col)
                    .alias("turnover_trend")
                )
                .with_columns(
                    (pl.col("LTurnover") - pl.col("turnover_trend")).alias("DTO")
                )
                .drop("turnover_trend")
            )

            # Beta from daily data
            self._log(
                "  Computing beta characteristics (IDs 49-50, 52) from daily data..."
            )
            beta_lf = compute_beta_characteristics(
                self._daily_price, self._factors, id_col
            )
            price_chars = price_chars.join(
                beta_lf.select([id_col, "date", "Beta", "Beta_Cor", "Idio_vol"]),
                on=[id_col, "date"],
                how="left",
            )
        else:
            # Fallback: compute from daily data
            self._log("  Computing characteristics from daily data (fallback)...")

            if self._daily_price is None:
                raise RuntimeError(
                    "Daily data not loaded and fiscal chars not available."
                )

            # Volatility characteristics from daily data
            self._log("  Computing volatility characteristics...")
            vol_lf = compute_volatility_characteristics(
                self._daily_price, momentum_lf, id_col
            )

            # Beta characteristics
            self._log("  Computing beta characteristics (IDs 49-50, 52)...")
            beta_lf = compute_beta_characteristics(
                self._daily_price, self._factors, id_col
            )

            # Trading characteristics
            self._log("  Computing trading characteristics...")
            trading_lf = compute_trading_characteristics(
                self._daily_price, momentum_lf, id_col
            )

            # Spread (ID 58) - uses quoted bid/ask
            self._log("  Computing spread (ID 58)...")
            spread_lf = compute_spread(self._daily_price, id_col)

            # Merge all price characteristics
            self._log("  Merging price characteristics...")
            price_chars = (
                momentum_lf.join(vol_lf, on=[id_col, "date"], how="left")
                .join(
                    beta_lf.select([id_col, "date", "Beta", "Beta_Cor", "Idio_vol"]),
                    on=[id_col, "date"],
                    how="left",
                )
                .join(
                    trading_lf.select([id_col, "date", "LTurnover", "DTO", "Rel2High"]),
                    on=[id_col, "date"],
                    how="left",
                )
                .join(spread_lf, on=[id_col, "date"], how="left")
            )

        return price_chars

    def compute_fundamental_characteristics(self) -> pl.LazyFrame:
        """
        Compute characteristics from fundamental data.

        Includes:
        - Investment (IDs 6-11)
        - Profitability (IDs 12-28)
        - Intangibles (IDs 29-32)
        - Value (IDs 33-47)
        """
        self._log("Computing fundamental-based characteristics...")

        if self._fundamentals is None:
            raise RuntimeError("Data not loaded. Call load_data() first.")

        id_col = "gvkey"

        if self._fundamentals_with_intermediates is None:
            self._log("  Computing intermediate variables...")
            self._fundamentals_with_intermediates = compute_intermediate_variables(
                self._fundamentals, id_col
            )

        funda_with_int = self._fundamentals_with_intermediates

        # Investment (IDs 6-11)
        self._log("  Computing investment characteristics (IDs 6-11)...")
        inv_lf = compute_investment_characteristics(funda_with_int, id_col)

        # Profitability (IDs 12-28)
        self._log("  Computing profitability characteristics (IDs 12-28)...")
        prof_lf = compute_profitability_characteristics(inv_lf, id_col)

        # Intangibles (IDs 29-32)
        self._log("  Computing intangibles characteristics (IDs 29-32)...")
        intan_lf = compute_intangibles_characteristics(prof_lf, id_col)

        # ASO (ID 38)
        self._log("  Computing ASO (ID 38)...")
        aso_lf = compute_aso(intan_lf, id_col)

        return aso_lf

    def compute_merged_characteristics(self) -> pl.LazyFrame:
        """
        Compute characteristics requiring both price and fundamental data.

        Value characteristics (IDs 33-47) need ME from price data.
        """
        self._log("Computing merged characteristics...")

        me_lf = compute_market_equity(self._monthly_price, "permno")

        if self._fundamentals_with_intermediates is None:
            self._fundamentals_with_intermediates = compute_intermediate_variables(
                self._fundamentals, "gvkey"
            )

        funda_with_int = self._fundamentals_with_intermediates

        self._log("  Computing value characteristics (IDs 33-47)...")
        value_lf = compute_value_characteristics(funda_with_int, me_lf, "gvkey")

        return value_lf

    # =========================================================================
    # Full Pipeline
    # =========================================================================

    def build(self) -> pl.LazyFrame:
        """Build all 62 characteristics."""
        if self._monthly_price is None:
            self.load_data()

        price_chars = self.compute_price_characteristics()
        funda_chars = self.compute_fundamental_characteristics()

        self._log("Merging price and fundamental characteristics...")

        link_table = self.loader.load_link_table()

        merged = merge_price_fundamentals(
            price_chars,
            funda_chars,
            link_table,
        )

        # Compute value characteristics
        value_chars = compute_value_characteristics(merged, None, "gvkey")

        self._log("Creating final characteristic dataset...")

        VALUE_CHAR_COLS = [
            "A2ME", "BEME", "C", "C2D", "Debt2P", "E2P",
            "Free_CF", "LDP", "NOP", "O2P", "Q", "S2P", "Sales_g",
        ]

        final = merged.join(
            value_chars.select(["gvkey", "datadate"] + VALUE_CHAR_COLS),
            on=["gvkey", "datadate"],
            how="left",
        )

        self._merged_data = final
        return final

    # =========================================================================
    # Chunked Processing (Memory-Efficient)
    # =========================================================================

    def _generate_year_chunks(
        self, start_year: int, end_year: int, chunk_size: int = 5
    ) -> list[tuple[int, int]]:
        """Generate (start, end) year tuples for chunked processing."""
        chunks = []
        current = start_year
        while current <= end_year:
            chunk_end = min(current + chunk_size - 1, end_year)
            chunks.append((current, chunk_end))
            current = chunk_end + 1
        return chunks

    def _get_year_range(
        self, lf: pl.LazyFrame, date_col: str = "date"
    ) -> tuple[int, int]:
        """Get the minimum and maximum years from a LazyFrame."""
        year_stats = lf.select(
            [
                pl.col(date_col).dt.year().min().alias("min_year"),
                pl.col(date_col).dt.year().max().alias("max_year"),
            ]
        ).collect()
        return int(year_stats["min_year"][0]), int(year_stats["max_year"][0])

    def _build_chunk(
        self,
        start_year: int,
        end_year: int,
        price_chars: pl.LazyFrame,
        funda_chars: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Build characteristics for a single year chunk."""
        self._log(f"  Processing chunk {start_year}-{end_year}...")

        price_chunk = price_chars.filter(
            (pl.col("date").dt.year() >= start_year)
            & (pl.col("date").dt.year() <= end_year)
        )

        funda_chunk = funda_chars.filter(
            (pl.col("datadate").dt.year() >= start_year - 1)
            & (pl.col("datadate").dt.year() <= end_year)
        )

        link_table = self.loader.load_link_table()

        merged = merge_price_fundamentals(
            price_chunk,
            funda_chunk,
            link_table,
        )

        value_chars = compute_value_characteristics(merged, None, "gvkey")

        VALUE_CHAR_COLS = [
            "A2ME", "BEME", "C", "C2D", "Debt2P", "E2P",
            "Free_CF", "LDP", "NOP", "O2P", "Q", "S2P", "Sales_g",
        ]

        chunk_final = merged.join(
            value_chars.select(["gvkey", "datadate"] + VALUE_CHAR_COLS),
            on=["gvkey", "datadate"],
            how="left",
        )

        return chunk_final

    def build_chunked(
        self,
        chunk_years: int = 5,
        temp_dir: Path | None = None,
        cleanup_temp: bool = True,
    ) -> pl.LazyFrame:
        """
        Build characteristics in year chunks to reduce memory usage.

        Args:
            chunk_years: Number of years per chunk (default 5)
            temp_dir: Directory for temporary chunk files
            cleanup_temp: Whether to mark temp files for cleanup after final save

        Returns:
            LazyFrame scanning all concatenated chunks
        """
        self._log(f"Building characteristics in {chunk_years}-year chunks...")

        if self._monthly_price is None:
            self.load_data()

        self._log("Computing price characteristics...")
        price_chars = self.compute_price_characteristics()

        self._log("Computing fundamental characteristics...")
        funda_chars = self.compute_fundamental_characteristics()

        min_year, max_year = self._get_year_range(self._monthly_price)
        self._log(
            f"Data spans {min_year} to {max_year} ({max_year - min_year + 1} years)"
        )

        chunks = self._generate_year_chunks(min_year, max_year, chunk_years)
        self._log(f"Processing {len(chunks)} chunks...")

        if temp_dir is None:
            temp_dir = self.config.output_dir / ".temp_chunks"

        if temp_dir.exists():
            self._log(f"Clearing existing temp directory: {temp_dir}")
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

        chunk_files = []
        for i, (start_year, end_year) in enumerate(chunks):
            chunk_lf = self._build_chunk(start_year, end_year, price_chars, funda_chars)

            chunk_file = temp_dir / f"chunk_{i:03d}_{start_year}_{end_year}.parquet"
            self._log(f"  Saving chunk to {chunk_file.name}...")

            chunk_lf.collect().write_parquet(chunk_file)
            chunk_files.append(chunk_file)

            self._log(f"  Chunk {start_year}-{end_year} complete.")

        self._log(
            f"All chunks saved. Creating lazy scan of {len(chunk_files)} files..."
        )
        combined = pl.scan_parquet(temp_dir / "chunk_*.parquet")

        self._temp_dir = temp_dir
        self._merged_data = combined

        return combined

    def cleanup_temp_files(self) -> None:
        """Clean up temporary chunk files after processing is complete."""
        if hasattr(self, "_temp_dir") and self._temp_dir is not None:
            if self._temp_dir.exists():
                self._log(f"Cleaning up temporary files: {self._temp_dir}")
                shutil.rmtree(self._temp_dir)
                self._temp_dir = None

    # =========================================================================
    # Output
    # =========================================================================

    def save(
        self,
        output_path: Path | None = None,
        collect: bool = True,
    ) -> None:
        """Save characteristics to parquet file."""
        if self._merged_data is None:
            raise RuntimeError("No data to save. Call build() first.")

        if output_path is None:
            output_path = self.config.output_raw

        output_path.parent.mkdir(parents=True, exist_ok=True)

        self._log(f"Saving to {output_path}...")

        if collect:
            self._merged_data.collect().write_parquet(output_path)
        else:
            self._merged_data.sink_parquet(output_path)

        self._log("Save complete.")

    def get_characteristic_list(self) -> list[str]:
        """Get list of all characteristic column names."""
        return get_characteristic_names()

    def get_summary(self) -> dict:
        """Get summary statistics of computed characteristics."""
        if self._merged_data is None:
            raise RuntimeError("No data available. Call build() first.")

        char_cols = self.get_characteristic_list()

        summary = {
            "n_characteristics": len(char_cols),
            "characteristics": char_cols,
        }

        df = self._merged_data.select(char_cols).collect()

        summary["coverage"] = {
            col: 1 - df[col].null_count() / len(df)
            for col in char_cols
            if col in df.columns
        }

        return summary


# =============================================================================
# Convenience Function
# =============================================================================


def build_us_characteristics(config: PathConfig) -> pl.LazyFrame:
    """Build US characteristics with default settings."""
    builder = CharacteristicBuilder(config)
    return builder.build()
