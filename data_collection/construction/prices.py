"""
Price-based characteristic construction.

Handles momentum, volatility, beta, and trading characteristics that require
daily or monthly price/return data.

Characteristics covered:
- Past Returns (IDs 1-5): r2_1, r6_2, r12_2, r12_7, r36_13
- Trading (IDs 48-62): Beta, Idio_vol, LME, Spread, etc.
"""

import polars as pl


# =============================================================================
# Momentum / Past Return Characteristics (IDs 1-5)
# =============================================================================


def compute_momentum_characteristics(
    monthly_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Compute past return characteristics.

    IDs computed:
    - 1: r2_1 - Return 1 month before (short-term reversal)
    - 2: r6_2 - Return months 6 to 2 before
    - 3: r12_2 - Return months 12 to 2 before (standard momentum)
    - 4: r12_7 - Return months 12 to 7 before (intermediate momentum)
    - 5: r36_13 - Return months 36 to 13 before (long-term reversal)

    Args:
        monthly_lf: Monthly price/return data with 'ret' and 'date' columns
        id_col: Identifier column name

    Returns:
        LazyFrame with momentum characteristics
    """
    # Sort by id and date for proper lag calculation
    sorted_lf = monthly_lf.sort([id_col, "date"])

    return sorted_lf.with_columns(
        [
            # ID 1: r2_1 - Return at t-1
            pl.col("ret").shift(1).over(id_col).alias("r2_1"),
            # For cumulative returns, we need to compute (1+r) products
            # ID 2: r6_2 - Cumulative return from t-6 to t-2
            (
                (1 + pl.col("ret").shift(2).over(id_col))
                * (1 + pl.col("ret").shift(3).over(id_col))
                * (1 + pl.col("ret").shift(4).over(id_col))
                * (1 + pl.col("ret").shift(5).over(id_col))
                * (1 + pl.col("ret").shift(6).over(id_col))
                - 1
            ).alias("r6_2"),
            # ID 3: r12_2 - Standard momentum (t-12 to t-2)
            # Using rolling product for cleaner code
        ]
    ).with_columns(
        [
            # ID 3: r12_2
            _compute_cumret(sorted_lf, id_col, 2, 12).alias("r12_2"),
            # ID 4: r12_7
            _compute_cumret(sorted_lf, id_col, 7, 12).alias("r12_7"),
            # ID 5: r36_13
            _compute_cumret(sorted_lf, id_col, 13, 36).alias("r36_13"),
        ]
    )


def _compute_cumret(
    lf: pl.LazyFrame,
    id_col: str,
    start_lag: int,
    end_lag: int,
) -> pl.Expr:
    """
    Compute cumulative return from t-end_lag to t-start_lag.

    OPTIMIZED: Uses rolling_sum on log returns instead of iterative multiplication.
    This is O(1) per window instead of O(window_size).

    Returns (1+r_{t-end_lag}) * ... * (1+r_{t-start_lag}) - 1
    """
    window_size = end_lag - start_lag + 1

    # Use log returns for efficient rolling sum, then convert back
    # log(1+r1) + log(1+r2) + ... = log((1+r1)*(1+r2)*...)
    return (
        pl.col("ret")
        .log1p()  # log(1 + ret)
        .shift(start_lag)  # Start from the oldest lag position
        .rolling_sum(window_size=window_size)
        .over(id_col)
        .exp()  # Convert back: exp(sum of logs) = product
        - 1
    )


def compute_cumulative_returns(
    lf: pl.LazyFrame,
    id_col: str,
    windows: list[tuple[int, int, str]],
) -> pl.LazyFrame:
    """
    Compute multiple cumulative return windows efficiently.

    OPTIMIZED: Uses rolling_sum instead of iterative loop.

    Args:
        lf: LazyFrame with returns
        id_col: Identifier column
        windows: List of (start_lag, end_lag, name) tuples

    Returns:
        LazyFrame with cumulative return columns added
    """
    # First compute log returns for easier summation
    result = lf.with_columns([pl.col("ret").log1p().alias("log_ret")])

    cumret_exprs = []
    for start_lag, end_lag, name in windows:
        window_size = end_lag - start_lag + 1
        # OPTIMIZED: Use rolling_sum instead of iterative addition
        cumret_expr = (
            pl.col("log_ret")
            .shift(start_lag)
            .rolling_sum(window_size=window_size)
            .over(id_col)
            .exp()
            - 1
        ).alias(name)
        cumret_exprs.append(cumret_expr)

    return result.with_columns(cumret_exprs).drop("log_ret")


# =============================================================================
# Volatility Characteristics
# =============================================================================


def compute_volatility_characteristics(
    daily_lf: pl.LazyFrame,
    monthly_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Compute volatility-based characteristics from daily data.

    IDs computed:
    - 57: Ret_max - Maximum daily return in previous month
    - 62: Total_vol - Std dev of daily returns
    - 59: Std_Turn - Std dev of daily turnover
    - 60: Std_Vol - Std dev of daily volume

    Args:
        daily_lf: Daily price/return data
        monthly_lf: Monthly data to join results to
        id_col: Identifier column

    Returns:
        Monthly LazyFrame with volatility characteristics
    """
    # Aggregate daily to monthly for volatility metrics
    daily_with_month = daily_lf.with_columns(
        [
            pl.col("date").dt.month_end().alias("month_end"),
        ]
    )

    monthly_vol = (
        daily_with_month.group_by([id_col, "month_end"])
        .agg(
            [
                # ID 57: Maximum daily return
                pl.col("ret").max().alias("Ret_max"),
                # ID 62: Total volatility (std of daily returns)
                pl.col("ret").std().alias("Total_vol"),
                # ID 60: Std of daily volume
                pl.col("vol").std().alias("Std_Vol"),
                # For turnover std, need shares
                (pl.col("vol") / pl.col("shrout")).std().alias("Std_Turn"),
                # Count of trading days (for data quality)
                pl.col("ret").count().alias("n_trading_days"),
            ]
        )
        .rename({"month_end": "date"})
    )

    return monthly_vol


# =============================================================================
# Beta Characteristics
# =============================================================================


def compute_beta_characteristics(
    daily_lf: pl.LazyFrame,
    factors_lf: pl.LazyFrame,
    id_col: str = "permno",
    window: int = 252,  # 1 year of trading days
    min_obs: int = 120,
) -> pl.LazyFrame:
    """
    Compute beta and idiosyncratic volatility.

    For each month-end, uses the last `window` trading days of raw daily returns
    to compute rolling statistics. This is the standard academic approach.

    IDs computed:
    - 50: Beta - CAPM beta from daily returns
    - 49: Beta_Cor - Correlation component (Beta * VolMkt/VolStock)
    - 52: Idio_vol - Idiosyncratic volatility from FF3 model

    Args:
        daily_lf: Daily stock returns
        factors_lf: Fama-French factors (mktrf, smb, hml, rf)
        id_col: Identifier column
        window: Rolling window size in trading days (default 252 = ~12 months)
        min_obs: Minimum observations for valid estimate

    Returns:
        LazyFrame with monthly beta characteristics
    """
    # Join factors to stock returns
    merged = (
        daily_lf.join(
            factors_lf.select(["date", "mktrf", "smb", "hml", "rf"]),
            on="date",
            how="inner",
        )
        .with_columns(
            [
                # Excess return
                (pl.col("ret") - pl.col("rf")).alias("ret_excess")
            ]
        )
        .sort([id_col, "date"])
    )

    # Compute rolling statistics directly on daily returns
    beta_daily = merged.with_columns(
        [
            pl.col("ret_excess")
            .rolling_mean(window_size=window, min_periods=min_obs)
            .over(id_col)
            .alias("mean_ret"),
            pl.col("mktrf")
            .rolling_mean(window_size=window, min_periods=min_obs)
            .over(id_col)
            .alias("mean_mkt"),
            pl.col("ret_excess")
            .rolling_std(window_size=window, min_periods=min_obs)
            .over(id_col)
            .alias("vol_stock"),
            pl.col("mktrf")
            .rolling_std(window_size=window, min_periods=min_obs)
            .over(id_col)
            .alias("vol_mkt"),
            pl.col("mktrf")
            .rolling_var(window_size=window, min_periods=min_obs)
            .over(id_col)
            .alias("var_m"),
            (pl.col("ret_excess") * pl.col("mktrf"))
            .rolling_mean(window_size=window, min_periods=min_obs)
            .over(id_col)
            .alias("mean_prod"),
        ]
    )

    # Sample at month-end dates, then compute beta metrics
    result = (
        beta_daily.filter(pl.col("date") == pl.col("date").dt.month_end())
        .with_columns(
            [
                # Covariance = E[XY] - E[X]*E[Y]
                (pl.col("mean_prod") - pl.col("mean_ret") * pl.col("mean_mkt")).alias(
                    "cov_rm"
                ),
            ]
        )
        .with_columns(
            [
                # ID 50: Beta = Cov(r, rm) / Var(rm)
                (pl.col("cov_rm") / pl.col("var_m")).alias("Beta"),
            ]
        )
        .with_columns(
            [
                # ID 49: Beta_Cor = Beta * (VolMkt / VolStock)
                (pl.col("Beta") * pl.col("vol_mkt") / pl.col("vol_stock")).alias(
                    "Beta_Cor"
                ),
                # ID 52: Idiosyncratic volatility (residual vol)
                (pl.col("vol_stock").pow(2) - pl.col("Beta").pow(2) * pl.col("var_m"))
                .clip(lower_bound=0)
                .sqrt()
                .alias("Idio_vol"),
            ]
        )
        .select([id_col, "date", "Beta", "Beta_Cor", "Idio_vol"])
    )

    return result


# =============================================================================
# Trading Characteristics
# =============================================================================


def compute_trading_characteristics(
    daily_lf: pl.LazyFrame,
    monthly_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Compute trading-related characteristics.

    IDs computed:
    - 51: DTO - Detrended turnover
    - 55: LTurnover - Last month's turnover
    - 56: Rel2High - Price relative to 52-week high
    - 58: Spread - Bid-ask spread

    Note: SUV (ID 61) is computed at fiscal year level in fundamentals.py

    Args:
        daily_lf: Daily trading data
        monthly_lf: Monthly data
        id_col: Identifier column

    Returns:
        LazyFrame with trading characteristics
    """
    # Compute monthly turnover
    daily_with_month = daily_lf.with_columns(
        [
            pl.col("date").dt.month_end().alias("month_end"),
            (pl.col("vol") / pl.col("shrout")).alias("turnover"),
        ]
    )

    monthly_trading = (
        daily_with_month.group_by([id_col, "month_end"])
        .agg(
            [
                # ID 55: LTurnover - Average turnover
                pl.col("turnover").mean().alias("LTurnover"),
                # For DTO: need to compute trend
                pl.col("turnover").sum().alias("total_turnover"),
                # Price for 52-week high
                pl.col("prc").last().alias("prc_last"),
                pl.col("high").max().alias("high_month"),
            ]
        )
        .rename({"month_end": "date"})
        .sort([id_col, "date"])
    )

    # ID 51: DTO - Detrended turnover (12-month linear detrend)
    monthly_trading = monthly_trading.with_columns(
        [
            # 12-month rolling mean as trend proxy
            pl.col("LTurnover")
            .rolling_mean(window_size=12)
            .over(id_col)
            .alias("turnover_trend"),
        ]
    ).with_columns(
        [
            (pl.col("LTurnover") - pl.col("turnover_trend")).alias("DTO"),
        ]
    )

    # ID 56: Rel2High - Price relative to 52-week high
    monthly_trading = monthly_trading.with_columns(
        [
            # 52-week (12-month) high
            pl.col("high_month")
            .rolling_max(window_size=12)
            .over(id_col)
            .alias("high_52w"),
        ]
    ).with_columns(
        [
            (pl.col("prc_last") / pl.col("high_52w")).alias("Rel2High"),
        ]
    )

    return monthly_trading


def compute_spread(
    daily_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Compute bid-ask spread from quoted bid/ask prices.

    ID 58: Spread - Average daily bid-ask spread

    Args:
        daily_lf: Daily data with bid, ask prices
        id_col: Identifier column

    Returns:
        Monthly spread estimates
    """
    return _compute_quoted_spread(daily_lf, id_col)


def _compute_quoted_spread(
    daily_lf: pl.LazyFrame,
    id_col: str,
) -> pl.LazyFrame:
    """Compute spread from quoted bid/ask prices."""
    return (
        daily_lf.with_columns(
            [
                pl.col("date").dt.month_end().alias("month_end"),
                # Quoted spread = (ask - bid) / midpoint
                (
                    (pl.col("ask") - pl.col("bid"))
                    / ((pl.col("ask") + pl.col("bid")) / 2)
                ).alias("daily_spread"),
            ]
        )
        .group_by([id_col, "month_end"])
        .agg(
            [
                pl.col("daily_spread").mean().alias("Spread"),
            ]
        )
        .rename({"month_end": "date"})
    )


# =============================================================================
# Market Equity (for Trading characteristics)
# =============================================================================


def compute_market_equity(
    monthly_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Compute Market Equity (ME) from monthly data.

    ID 53: LME - Market equity (price × shares)
    # Corrected to no log
    ID 54: LME_adj - Industry-adjusted size

    Args:
        monthly_lf: Monthly price data with prc and shrout
        id_col: Identifier column

    Returns:
        LazyFrame with ME and LME
    """
    return monthly_lf.with_columns(
        [
            # ME = price * shares outstanding
            (pl.col("prc").abs() * pl.col("shrout")).alias("ME"),
        ]
    ).with_columns(
        [
            # LME = ME (no log)
            pl.col("ME").alias("LME"),
        ]
    )
