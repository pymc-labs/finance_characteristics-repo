"""
Normalization module for Freyberger 62 characteristics.

Implements the normalization procedure from Barroso, Saxena, & Wang (2025) Appendix A.1:

1. Impute missing values with cross-sectional median
2. Rank transform within each cross-section
3. Scale to [-0.5, 0.5] range

This produces characteristics that are:
- Comparable across time
- Robust to outliers
- Uniformly distributed
"""

import polars as pl
from typing import Literal


# =============================================================================
# Core Normalization Functions
# =============================================================================


def normalize_characteristics(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str] = "date",
    method: Literal["rank", "zscore", "minmax"] = "rank",
) -> pl.LazyFrame:
    """
    Normalize characteristics using Barroso et al. (2025) methodology.

    Steps:
    1. Impute missing values with cross-sectional median
    2. Rank transform within each cross-section (date)
    3. Scale to [-0.5, 0.5]

    Args:
        lf: LazyFrame with raw characteristics
        char_cols: List of characteristic column names to normalize
        date_col: Date column for cross-sectional grouping
        method: Normalization method ('rank', 'zscore', 'minmax')

    Returns:
        LazyFrame with normalized characteristics
    """
    if method == "rank":
        return _rank_normalize(lf, char_cols, date_col)
    elif method == "zscore":
        return _zscore_normalize(lf, char_cols, date_col)
    elif method == "minmax":
        return _minmax_normalize(lf, char_cols, date_col)
    else:
        raise ValueError(f"Unknown normalization method: {method}")


def _rank_normalize(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str],
) -> pl.LazyFrame:
    """
    Rank normalization per Barroso et al. (2025).

    For each characteristic c at each date t:
    1. Fill nulls with cross-sectional median
    2. Compute rank (average method for ties)
    3. Scale: rank / (N + 1) - 0.5

    Result is uniformly distributed on [-0.5, 0.5]
    """
    norm_exprs = []

    for col in char_cols:
        # Step 1: Impute with cross-sectional median
        imputed = (
            pl.when(pl.col(col).is_null())
            .then(pl.col(col).median().over(date_col))
            .otherwise(pl.col(col))
        )

        # Step 2 & 3: Rank and scale
        # rank / (count + 1) - 0.5
        normalized = (
            imputed.rank(method="average")
            .over(date_col)
            .truediv(pl.len().over(date_col) + 1)
            - 0.5
        )

        norm_exprs.append(normalized.alias(col))

    return lf.with_columns(norm_exprs)


def _zscore_normalize(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str],
) -> pl.LazyFrame:
    """
    Z-score normalization (cross-sectional).

    For each characteristic: (x - mean) / std
    """
    norm_exprs = []

    for col in char_cols:
        normalized = (pl.col(col) - pl.col(col).mean().over(date_col)) / pl.col(
            col
        ).std().over(date_col)
        norm_exprs.append(normalized.alias(col))

    return lf.with_columns(norm_exprs)


def _minmax_normalize(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str],
) -> pl.LazyFrame:
    """
    Min-max normalization to [0, 1] (cross-sectional).
    """
    norm_exprs = []

    for col in char_cols:
        min_val = pl.col(col).min().over(date_col)
        max_val = pl.col(col).max().over(date_col)

        normalized = (pl.col(col) - min_val) / (max_val - min_val)
        norm_exprs.append(normalized.alias(col))

    return lf.with_columns(norm_exprs)


# =============================================================================
# OPTIMIZED: Barroso Normalization with Single Group-By
# =============================================================================


def _compute_cross_sectional_stats(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str],
    lower_pct: float = 0.01,
    upper_pct: float = 0.99,
) -> pl.LazyFrame:
    """
    OPTIMIZED: Compute all cross-sectional statistics in a single group_by.

    Computes for each characteristic:
    - p01: 1st percentile (for winsorization lower bound)
    - p99: 99th percentile (for winsorization upper bound)
    - med: median (for imputation)
    - n: count of non-null values (for rank scaling)

    This replaces ~310 separate .over() calls with a single group_by operation.

    Args:
        lf: LazyFrame with characteristics
        char_cols: Characteristic columns
        date_col: Date column for cross-sectional grouping
        lower_pct: Lower percentile for winsorization
        upper_pct: Upper percentile for winsorization

    Returns:
        LazyFrame with cross-sectional statistics per date
    """
    stats_exprs = []

    for col in char_cols:
        stats_exprs.extend(
            [
                pl.col(col).quantile(lower_pct).alias(f"{col}__p01"),
                pl.col(col).quantile(upper_pct).alias(f"{col}__p99"),
                pl.col(col).median().alias(f"{col}__med"),
            ]
        )

    # Total rows per date group (correct N after imputation fills all nulls)
    stats_exprs.append(pl.len().alias("__n"))

    return lf.group_by(date_col).agg(stats_exprs)


def _apply_barroso_transform_fast(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str],
    lower_pct: float = 0.01,
    upper_pct: float = 0.99,
) -> pl.LazyFrame:
    """
    OPTIMIZED: Apply full Barroso et al. (2025) normalization efficiently.

    Methodology (preserved exactly):
    1. Winsorize at 1st and 99th percentiles (cross-sectional)
    2. Impute missing values with cross-sectional median
    3. Rank transform within each cross-section
    4. Scale to [-0.5, 0.5] range

    Optimization: Uses single group_by + join instead of multiple .over() calls.

    Args:
        lf: LazyFrame with raw characteristics
        char_cols: Characteristic columns to normalize
        date_col: Date column for cross-sectional grouping
        lower_pct: Lower percentile for winsorization (default 0.01)
        upper_pct: Upper percentile for winsorization (default 0.99)

    Returns:
        LazyFrame with normalized characteristics
    """
    # Step 1: Compute all cross-sectional statistics in one group_by
    stats_lf = _compute_cross_sectional_stats(
        lf, char_cols, date_col, lower_pct, upper_pct
    )

    # Step 2: Join statistics back to main data
    lf_with_stats = lf.join(stats_lf, on=date_col, how="left")

    # Step 3: Apply winsorization + imputation in one pass
    winsor_impute_exprs = []
    for col in char_cols:
        # Winsorize: clip to [p01, p99]
        winsorized = pl.col(col).clip(
            lower_bound=pl.col(f"{col}__p01"),
            upper_bound=pl.col(f"{col}__p99"),
        )
        # Impute: fill nulls with median
        imputed = winsorized.fill_null(pl.col(f"{col}__med"))
        winsor_impute_exprs.append(imputed.alias(col))

    lf_winsor_imputed = lf_with_stats.with_columns(winsor_impute_exprs)

    # Step 4: Apply rank transform and scale to [-0.5, 0.5]
    # rank / (N + 1) - 0.5
    rank_exprs = []
    for col in char_cols:
        ranked = (
            pl.col(col).rank(method="average").over(date_col).truediv(pl.col("__n") + 1)
            - 0.5
        )
        rank_exprs.append(ranked.alias(col))

    lf_normalized = lf_winsor_imputed.with_columns(rank_exprs)

    # Step 5: Drop the temporary stats columns
    stats_cols = ["__n"]
    for col in char_cols:
        stats_cols.extend([f"{col}__p01", f"{col}__p99", f"{col}__med"])

    return lf_normalized.drop(stats_cols)


# =============================================================================
# Winsorization
# =============================================================================


def winsorize_characteristics(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str] = "date",
    lower_pct: float = 0.01,
    upper_pct: float = 0.99,
) -> pl.LazyFrame:
    """
    Winsorize characteristics at specified percentiles.

    Cross-sectional winsorization to handle outliers.

    Args:
        lf: LazyFrame with characteristics
        char_cols: Columns to winsorize
        date_col: Date column for grouping
        lower_pct: Lower percentile (default 1%)
        upper_pct: Upper percentile (default 99%)

    Returns:
        LazyFrame with winsorized values
    """
    winsor_exprs = []

    for col in char_cols:
        lower_bound = pl.col(col).quantile(lower_pct).over(date_col)
        upper_bound = pl.col(col).quantile(upper_pct).over(date_col)

        winsorized = pl.col(col).clip(lower_bound, upper_bound)
        winsor_exprs.append(winsorized.alias(col))

    return lf.with_columns(winsor_exprs)


# =============================================================================
# Missing Value Handling
# =============================================================================


def impute_missing(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str] = "date",
    method: Literal["median", "mean", "zero"] = "median",
) -> pl.LazyFrame:
    """
    Impute missing values in characteristics.

    Args:
        lf: LazyFrame with characteristics
        char_cols: Columns to impute
        date_col: Date column for cross-sectional imputation
        method: Imputation method ('median', 'mean', 'zero')

    Returns:
        LazyFrame with imputed values
    """
    impute_exprs = []

    for col in char_cols:
        if method == "median":
            fill_value = pl.col(col).median().over(date_col)
        elif method == "mean":
            fill_value = pl.col(col).mean().over(date_col)
        elif method == "zero":
            fill_value = 0.0
        else:
            raise ValueError(f"Unknown imputation method: {method}")

        imputed = pl.col(col).fill_null(fill_value)
        impute_exprs.append(imputed.alias(col))

    return lf.with_columns(impute_exprs)


# =============================================================================
# Full Normalization Pipeline
# =============================================================================


class CharacteristicNormalizer:
    """
    Full normalization pipeline for characteristics.
    """

    def __init__(
        self,
        winsorize_pct: tuple[float, float] = (0.01, 0.99),
        normalize_method: Literal["rank", "zscore", "minmax"] = "rank",
        impute_method: Literal["median", "mean", "zero"] = "median",
        min_coverage: float = 0.2,
    ):
        """
        Initialize normalizer.

        Args:
            winsorize_pct: (lower, upper) percentiles for winsorization
            normalize_method: Normalization method
            impute_method: Missing value imputation method
            min_coverage: Minimum coverage to keep characteristic
        """
        self.winsorize_pct = winsorize_pct
        self.normalize_method = normalize_method
        self.impute_method = impute_method
        self.min_coverage = min_coverage

        self.kept_cols: list[str] = []

    def fit_transform(
        self,
        lf: pl.LazyFrame,
        char_cols: list[str],
        date_col: str | list[str] = "date",
    ) -> pl.LazyFrame:
        """
        Apply full normalization pipeline.

        Steps:
        1. Winsorize
        2. Impute missing values
        3. Normalize

        Args:
            lf: LazyFrame with raw characteristics
            char_cols: Characteristic columns
            date_col: Date column

        Returns:
            Normalized LazyFrame
        """
        # Keep all characteristics (no sparse dropping)
        self.kept_cols = char_cols

        # Step 1: Winsorize
        lf = winsorize_characteristics(
            lf, self.kept_cols, date_col, self.winsorize_pct[0], self.winsorize_pct[1]
        )

        # Step 2: Impute
        lf = impute_missing(lf, self.kept_cols, date_col, self.impute_method)

        # Step 3: Normalize
        lf = normalize_characteristics(
            lf, self.kept_cols, date_col, self.normalize_method
        )

        return lf

    def transform(
        self,
        lf: pl.LazyFrame,
        date_col: str | list[str] = "date",
    ) -> pl.LazyFrame:
        """
        Transform using previously fitted columns.

        Must call fit_transform first to set kept_cols.
        """
        if not self.kept_cols:
            raise RuntimeError("Must call fit_transform first")

        # Apply same pipeline
        lf = winsorize_characteristics(
            lf, self.kept_cols, date_col, self.winsorize_pct[0], self.winsorize_pct[1]
        )

        lf = impute_missing(lf, self.kept_cols, date_col, self.impute_method)

        lf = normalize_characteristics(
            lf, self.kept_cols, date_col, self.normalize_method
        )

        return lf


# =============================================================================
# Convenience Functions
# =============================================================================


def normalize_barroso(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str | list[str] = "date",
    use_optimized: bool = True,
) -> pl.LazyFrame:
    """
    Apply Barroso et al. (2025) normalization.

    Methodology:
    1. Winsorize at 1st and 99th percentiles (cross-sectional)
    2. Impute missing values with cross-sectional median
    3. Rank transform within each cross-section
    4. Scale to [-0.5, 0.5] range

    Args:
        lf: LazyFrame with raw characteristics
        char_cols: Characteristic columns to normalize
        date_col: Date column for cross-sectional grouping
        use_optimized: If True, uses optimized single-group_by implementation (default)
                       If False, uses original multi-pass implementation

    Returns:
        LazyFrame with normalized characteristics
    """
    if use_optimized:
        # OPTIMIZED: Uses single group_by + join instead of ~310 .over() calls
        return _apply_barroso_transform_fast(
            lf, char_cols, date_col, lower_pct=0.01, upper_pct=0.99
        )
    else:
        # Original implementation (slower but preserved for compatibility)
        normalizer = CharacteristicNormalizer(
            winsorize_pct=(0.01, 0.99),
            normalize_method="rank",
            impute_method="median",
            min_coverage=0.0,  # Don't drop any
        )
        return normalizer.fit_transform(lf, char_cols, date_col)


# =============================================================================
# Column Renaming Utilities
# =============================================================================


def add_suffix_to_columns(
    lf: pl.LazyFrame,
    columns: list[str],
    suffix: str = "_norm",
) -> pl.LazyFrame:
    """
    Rename specified columns by adding a suffix.

    Useful for distinguishing normalized columns from raw columns when
    merging both into a single dataset.

    Args:
        lf: LazyFrame with columns to rename
        columns: List of column names to add suffix to
        suffix: Suffix to append (default "_norm")

    Returns:
        LazyFrame with renamed columns
    """
    schema_names = lf.collect_schema().names()
    rename_map = {col: f"{col}{suffix}" for col in columns if col in schema_names}
    return lf.rename(rename_map)
