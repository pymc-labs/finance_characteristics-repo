"""
Fundamentals-based characteristic construction.

Handles characteristics derived from annual accounting data (Compustat).

Characteristics covered:
- Investment (IDs 6-11)
- Profitability (IDs 12-28)
- Intangibles (IDs 29-32)
- Value (IDs 33-47)
"""

import polars as pl


# =============================================================================
# Intermediate Variables (0.3xx)
# =============================================================================


def compute_intermediate_variables(
    funda_lf: pl.LazyFrame,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Compute intermediate variables used by multiple characteristics.

    Intermediates computed:
    - 0.302: BE (Book Equity)
    - 0.303: NOA (Net Operating Assets)
    - 0.304: OpAcc (Operating Accruals)
    - 0.305: GP (Gross Profit)

    Note: 0.301 ME (Market Equity) is computed from price data.

    Args:
        funda_lf: Annual fundamentals data
        id_col: Identifier column

    Returns:
        LazyFrame with intermediate variables added
    """
    sorted_lf = funda_lf.sort([id_col, "datadate"])

    return (
        sorted_lf.with_columns(
            [
                # 0.302: BE (Book Equity)
                # Primary: ceq
                # Fallback: at - lt - pstk
                pl.when(pl.col("ceq").is_not_null() & (pl.col("ceq") > 0))
                .then(pl.col("ceq"))
                .otherwise(pl.col("at") - pl.col("lt") - pl.col("pstk"))
                .alias("BE"),
                # 0.303: NOA (Net Operating Assets)
                # NOA = (AT - CHE) - (AT - DLC - DLTT - CEQ)
                # Simplifies to: NOA = DLC + DLTT + CEQ - CHE
                (pl.col("dlc") + pl.col("dltt") + pl.col("ceq") - pl.col("che")).alias(
                    "NOA"
                ),
                # 0.305: GP (Gross Profit)
                (pl.col("sale") - pl.col("cogs")).alias("GP"),
            ]
        )
        # Compute lagged values for changes
        .with_columns(
            [
                pl.col("act").shift(1).over(id_col).alias("act_lag"),
                pl.col("che").shift(1).over(id_col).alias("che_lag"),
                pl.col("lct").shift(1).over(id_col).alias("lct_lag"),
                pl.col("dlc").shift(1).over(id_col).alias("dlc_lag"),
                pl.col("txp").shift(1).over(id_col).alias("txp_lag"),
            ]
        )
        .with_columns(
            [
                # 0.304: OpAcc (Operating Accruals)
                # OpAcc = (Δact - Δche) - (Δlct - Δdlc - Δtxp) - dp
                (
                    (pl.col("act") - pl.col("act_lag"))
                    - (pl.col("che") - pl.col("che_lag"))
                    - (
                        (pl.col("lct") - pl.col("lct_lag"))
                        - (pl.col("dlc") - pl.col("dlc_lag"))
                        - (pl.col("txp") - pl.col("txp_lag"))
                    )
                    - pl.col("dp")
                ).alias("OpAcc"),
                # Working capital changes (balance sheet approximation)
                # Fallback for Compustat wcapch which is null ~95% of firm-years
                # wcapch_calc = Δ(act - che) - Δ(lct - dlc)
                (
                    (pl.col("act") - pl.col("act_lag"))
                    - (pl.col("che") - pl.col("che_lag"))
                    - (
                        (pl.col("lct") - pl.col("lct_lag"))
                        - (pl.col("dlc") - pl.col("dlc_lag"))
                    )
                ).alias("wcapch_calc"),
            ]
        )
        .drop(["act_lag", "che_lag", "lct_lag", "dlc_lag", "txp_lag"])
    )


# =============================================================================
# Investment Characteristics (IDs 6-11)
# =============================================================================


def compute_investment_characteristics(
    funda_lf: pl.LazyFrame,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Compute investment characteristics.

    IDs computed:
    - 6: Investment - % change in AT (Asset Growth)
    - 7: ACEQ - % change in Book Equity
    - 8: DPI2A - Change in PP&E + inventory / lagged AT
    - 9: AShrout - % change in shares (computed from price data)
    - 10: IVC - Change in inventory / average AT
    - 11: NOA - Net operating assets / lagged AT

    Args:
        funda_lf: Annual fundamentals with intermediate variables
        id_col: Identifier column

    Returns:
        LazyFrame with investment characteristics
    """
    sorted_lf = funda_lf.sort([id_col, "datadate"])

    return (
        sorted_lf.with_columns(
            [
                # Lagged values
                pl.col("at").shift(1).over(id_col).alias("at_lag"),
                pl.col("ceq").shift(1).over(id_col).alias("ceq_lag"),
                pl.col("BE").shift(1).over(id_col).alias("BE_lag"),
                pl.col("ppent").shift(1).over(id_col).alias("ppent_lag"),
                pl.col("invt").shift(1).over(id_col).alias("invt_lag"),
                pl.col("NOA").shift(1).over(id_col).alias("NOA_lag"),
            ]
        )
        .with_columns(
            [
                # ID 6: Investment = Δat / at_{t-1}
                ((pl.col("at") - pl.col("at_lag")) / pl.col("at_lag")).alias(
                    "Investment"
                ),
                # ID 7: ACEQ = Δceq / ceq_{t-1}
                ((pl.col("ceq") - pl.col("ceq_lag")) / pl.col("ceq_lag")).alias("ACEQ"),
                # ID 8: DPI2A = (Δppent + Δinvt) / at_{t-1}
                (
                    (pl.col("ppent") - pl.col("ppent_lag"))
                    + (pl.col("invt") - pl.col("invt_lag"))
                )
                .truediv(pl.col("at_lag"))
                .alias("DPI2A"),
                # ID 10: IVC = Δinvt / avg(at)
                (
                    (pl.col("invt") - pl.col("invt_lag"))
                    / ((pl.col("at") + pl.col("at_lag")) / 2)
                ).alias("IVC"),
                # ID 11: NOA = NOA / at_{t-1}
                (pl.col("NOA") / pl.col("at_lag")).alias("NOA_ratio"),
            ]
        )
        .drop(["at_lag", "ceq_lag", "ppent_lag", "invt_lag", "NOA_lag"])
        .rename({"NOA_ratio": "NOA_char"})  # Avoid confusion with intermediate NOA
    )


# =============================================================================
# Profitability Characteristics (IDs 12-28)
# =============================================================================


def compute_profitability_characteristics(
    funda_lf: pl.LazyFrame,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Compute profitability characteristics.

    IDs computed:
    - 12: ATO - Sales to lagged NOA
    - 13: CTO - Sales to lagged AT
    - 14: dGM_dSales - Δ Gross Margin - Δ Sales
    - 15: EPS - Earnings per share
    - 16: IPM - Pretax income over sales
    - 17: PCM - Gross profit margin (GP / sale)
    - 18: PM - Operating profit margin (oiadp / sale)
    - 19: PM_adj - Industry-adjusted PM (computed later with industry data)
    - 20: Prof - Gross profitability over BE
    - 21: RNA - Return on net operating assets
    - 22: ROA - Return on assets
    - 23: ROC - Return on cash
    - 24: ROE - Return on equity
    - 25: ROIC - Return on invested capital
    - 26: S2C - Sales to cash
    - 27: SAT - Sales to assets
    - 28: SAT_adj - Industry-adjusted SAT (computed later)

    Args:
        funda_lf: Annual fundamentals with intermediate variables
        id_col: Identifier column

    Returns:
        LazyFrame with profitability characteristics
    """
    sorted_lf = funda_lf.sort([id_col, "datadate"])

    return (
        sorted_lf.with_columns(
            [
                # Lagged values
                pl.col("at").shift(1).over(id_col).alias("at_lag"),
                pl.col("NOA").shift(1).over(id_col).alias("NOA_lag"),
                pl.col("BE").shift(1).over(id_col).alias("BE_lag"),
                pl.col("sale").shift(1).over(id_col).alias("sale_lag"),
                pl.col("GP").shift(1).over(id_col).alias("GP_lag"),
            ]
        )
        .with_columns(
            [
                # Gross margin
                (pl.col("GP") / pl.col("sale")).alias("gross_margin"),
                (pl.col("GP_lag") / pl.col("sale_lag")).alias("gross_margin_lag"),
            ]
        )
        .with_columns(
            [
                # ID 12: ATO = sale / NOA_{t-1}
                (pl.col("sale") / pl.col("NOA_lag")).alias("ATO"),
                # ID 13: CTO = sale / at_{t-1}
                (pl.col("sale") / pl.col("at_lag")).alias("CTO"),
                # ID 14: dGM_dSales = Δ(GP/sale) - Δsale
                (
                    (pl.col("gross_margin") - pl.col("gross_margin_lag"))
                    - (pl.col("sale") - pl.col("sale_lag"))
                ).alias("dGM_dSales"),
                # ID 15: EPS (directly from data)
                pl.col("eps").alias("EPS"),
                # ID 16: IPM = pi / sale
                (pl.col("pi") / pl.col("sale")).alias("IPM"),
                # ID 17: PCM = GP / sale
                (pl.col("GP") / pl.col("sale")).alias("PCM"),
                # ID 18: PM = oiadp / sale (guard sale=0 to prevent inf in PM_adj)
                (
                    pl.when(pl.col("sale") != 0)
                    .then(pl.col("oiadp") / pl.col("sale"))
                    .otherwise(None)
                ).alias("PM"),
                # ID 20: Prof = GP / BE (exclude non-positive BE)
                (
                    pl.when(pl.col("BE") > 0)
                    .then(pl.col("GP") / pl.col("BE"))
                    .otherwise(None)
                ).alias("Prof"),
                # ID 21: RNA = oiadp / NOA_{t-1}
                (pl.col("oiadp") / pl.col("NOA_lag")).alias("RNA"),
                # ID 22: ROA = ib / at_{t-1} (exclude non-positive at_lag)
                (
                    pl.when(pl.col("at_lag") > 0)
                    .then(pl.col("ib") / pl.col("at_lag"))
                    .otherwise(None)
                ).alias("ROA"),
                # ID 24: ROE = ib / BE_{t-1} (exclude non-positive BE_lag)
                (
                    pl.when(pl.col("BE_lag") > 0)
                    .then(pl.col("ib") / pl.col("BE_lag"))
                    .otherwise(None)
                ).alias("ROE"),
                # ID 25: ROIC = ebit / (ceq + lt - che)
                (pl.col("ebit") / (pl.col("ceq") + pl.col("lt") - pl.col("che"))).alias(
                    "ROIC"
                ),
                # ID 26: S2C = sale / che
                (pl.col("sale") / pl.col("che")).alias("S2C"),
                # ID 27: SAT = sale / at (guard at=0 to prevent inf in SAT_adj)
                (
                    pl.when(pl.col("at") != 0)
                    .then(pl.col("sale") / pl.col("at"))
                    .otherwise(None)
                ).alias("SAT"),
            ]
        )
        .drop(
            [
                "at_lag",
                "NOA_lag",
                "BE_lag",
                "sale_lag",
                "GP_lag",
                "gross_margin",
                "gross_margin_lag",
            ]
        )
    )


def compute_roc(
    funda_lf: pl.LazyFrame,
    me_lf: pl.LazyFrame,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Compute ROC (Return on Cash) which requires market equity.

    ID 23: ROC = (ME + dltt - at) / che

    Args:
        funda_lf: Annual fundamentals
        me_lf: Market equity data with ME column
        id_col: Identifier column

    Returns:
        LazyFrame with ROC
    """
    return funda_lf.join(
        me_lf.select([id_col, "date", "ME"]), on=[id_col, "date"], how="left"
    ).with_columns(
        [
            ((pl.col("ME") + pl.col("dltt") - pl.col("at")) / pl.col("che")).alias(
                "ROC"
            ),
        ]
    )


# =============================================================================
# Intangibles Characteristics (IDs 29-32)
# =============================================================================


def compute_intangibles_characteristics(
    funda_lf: pl.LazyFrame,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Compute intangibles characteristics.

    IDs computed:
    - 29: AOA - Absolute operating accruals
    - 30: OL - Operating leverage
    - 31: Tan - Tangibility
    - 32: OA - Operating accruals

    Args:
        funda_lf: Annual fundamentals with OpAcc computed
        id_col: Identifier column

    Returns:
        LazyFrame with intangibles characteristics
    """
    return funda_lf.with_columns(
        [
            # ID 29: AOA = abs(OpAcc)
            pl.col("OpAcc").abs().alias("AOA"),
            # ID 30: OL = (cogs + xsga) / at
            ((pl.col("cogs") + pl.col("xsga")) / pl.col("at")).alias("OL"),
            # ID 31: Tan = ppent / at
            (pl.col("ppent") / pl.col("at")).alias("Tan"),
            # ID 32: OA = OpAcc (already computed)
            pl.col("OpAcc").alias("OA"),
        ]
    )


# =============================================================================
# Value Characteristics (IDs 33-47)
# =============================================================================


def compute_value_characteristics(
    funda_lf: pl.LazyFrame,
    me_lf: pl.LazyFrame | None,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Compute value characteristics.

    IDs computed:
    - 33: A2ME - Assets to market equity
    - 34: BEME - Book to market
    - 35: BEMEadj - Industry-adjusted B/M (computed later)
    - 36: C - Cash to assets
    - 37: C2D - Cash flow to debt
    - 38: ASO - Log change in split-adjusted shares
    - 39: Debt2P - Debt to price
    - 40: E2P - Earnings to price
    - 41: Free_CF - Free cash flow to BE
    - 42: LDP - Dividend yield
    - 43: NOP - Net payouts to price
    - 44: O2P - Operating payouts to price
    - 45: Q - Tobin's Q
    - 46: S2P - Sales to price
    - 47: Sales_g - Sales growth

    Args:
        funda_lf: Annual fundamentals with intermediate variables (may already have ME)
        me_lf: Market equity data (optional if ME already in funda_lf)
        id_col: Identifier column

    Returns:
        LazyFrame with value characteristics
    """
    sorted_lf = funda_lf.sort([id_col, "datadate"])

    # Join ME to fundamentals only if ME not already present
    if me_lf is not None and "ME" not in funda_lf.collect_schema().names():
        merged = sorted_lf.join(
            me_lf.select([id_col, "date", "ME"]),
            left_on=[id_col, "datadate"],
            right_on=[id_col, "date"],
            how="left",
        )
    else:
        merged = sorted_lf

    return (
        merged.with_columns(
            [
                # Lagged values
                pl.col("sale").shift(1).over(id_col).alias("sale_lag"),
                pl.col("BE").shift(1).over(id_col).alias("BE_lag"),
            ]
        )
        # Explicit Float64 casts to prevent type inference errors in complex query plans
        .with_columns(
            [
                pl.col("at").cast(pl.Float64),
                pl.col("ME").cast(pl.Float64),
                pl.col("BE").cast(pl.Float64),
                pl.col("che").cast(pl.Float64),
                pl.col("ib").cast(pl.Float64),
                pl.col("dp").cast(pl.Float64),
                pl.col("lt").cast(pl.Float64),
                pl.col("dltt").cast(pl.Float64),
                pl.col("dlc").cast(pl.Float64),
                pl.col("ni").cast(pl.Float64),
                pl.col("wcapch").cast(pl.Float64),
                pl.col("capx").cast(pl.Float64),
                pl.col("dvt").cast(pl.Float64),
                pl.col("prstkc").cast(pl.Float64),
                pl.col("ceq").cast(pl.Float64),
                pl.col("sale").cast(pl.Float64),
                pl.col("sale_lag").cast(pl.Float64),
                pl.col("BE_lag").cast(pl.Float64),
            ]
        )
        .with_columns(
            [
                # ID 33: A2ME = at / ME
                (pl.col("at") / pl.col("ME")).alias("A2ME"),
                # ID 34: BEME = BE / ME (guard ME=0 to prevent inf in BEME_adj)
                (
                    pl.when(pl.col("ME") != 0)
                    .then(pl.col("BE") / pl.col("ME"))
                    .otherwise(None)
                ).alias("BEME"),
                # ID 36: C = che / at
                (pl.col("che") / pl.col("at")).alias("C"),
                # ID 37: C2D = (ib + dp) / lt
                ((pl.col("ib") + pl.col("dp")) / pl.col("lt")).alias("C2D"),
                # ID 39: Debt2P = (dltt + dlc) / ME
                ((pl.col("dltt") + pl.col("dlc")) / pl.col("ME")).alias("Debt2P"),
                # ID 40: E2P = ib / ME
                (pl.col("ib") / pl.col("ME")).alias("E2P"),
                # ID 41: Free_CF = (ni + dp - wcapch - capx) / BE (exclude non-positive BE)
                # wcapch fallback: use balance sheet approximation when Compustat wcapch is null
                (
                    pl.when(pl.col("BE") > 0)
                    .then(
                        (
                            pl.col("ni")
                            + pl.col("dp")
                            - pl.col("wcapch").fill_null(pl.col("wcapch_calc"))
                            - pl.col("capx")
                        )
                        / pl.col("BE")
                    )
                    .otherwise(None)
                ).alias("Free_CF"),
                # ID 42: LDP = dvt / ME (dividend yield)
                (pl.col("dvt") / pl.col("ME")).alias("LDP"),
                # ID 43: NOP = (dvt + prstkc) / ME
                ((pl.col("dvt") + pl.col("prstkc")) / pl.col("ME")).alias("NOP"),
                # ID 44: O2P = (ni - ΔBE) / ME
                (
                    (pl.col("ni") - (pl.col("BE") - pl.col("BE_lag"))) / pl.col("ME")
                ).alias("O2P"),
                # ID 45: Q = (at + ME - ceq) / at
                ((pl.col("at") + pl.col("ME") - pl.col("ceq")) / pl.col("at")).alias(
                    "Q"
                ),
                # ID 46: S2P = sale / ME
                (pl.col("sale") / pl.col("ME")).alias("S2P"),
                # ID 47: Sales_g = Δsale / sale_{t-1}
                ((pl.col("sale") - pl.col("sale_lag")) / pl.col("sale_lag")).alias(
                    "Sales_g"
                ),
            ]
        )
        .drop(["sale_lag", "BE_lag"])
        # Drop "date" column if it exists (from ME join with monthly data)
        .pipe(
            lambda df: df.drop("date") if "date" in df.collect_schema().names() else df
        )
    )


def compute_aso(
    funda_lf: pl.LazyFrame,
    id_col: str = "gvkey",
) -> pl.LazyFrame:
    """
    Compute ASO - Log change in split-adjusted shares.

    ID 38: ASO = Δln(csho × ajex)

    Args:
        funda_lf: Annual fundamentals with csho and ajex
        id_col: Identifier column

    Returns:
        LazyFrame with ASO
    """
    sorted_lf = funda_lf.sort([id_col, "datadate"])

    return (
        sorted_lf.with_columns(
            [
                # Split-adjusted shares
                (pl.col("csho") * pl.col("ajex").fill_null(1)).alias("adj_shares"),
            ]
        )
        .with_columns(
            [
                pl.col("adj_shares").log().alias("log_adj_shares"),
                pl.col("adj_shares")
                .log()
                .shift(1)
                .over(id_col)
                .alias("log_adj_shares_lag"),
            ]
        )
        .with_columns(
            [
                # ASO = change in log split-adjusted shares
                (pl.col("log_adj_shares") - pl.col("log_adj_shares_lag")).alias("ASO"),
            ]
        )
        .drop(["adj_shares", "log_adj_shares", "log_adj_shares_lag"])
    )


# =============================================================================
# Industry Adjustments (for _adj characteristics)
# =============================================================================


def compute_industry_adjusted(
    char_lf: pl.LazyFrame,
    industry_lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str = "datadate",
) -> pl.LazyFrame:
    """
    Compute industry-adjusted characteristics.

    For PM_adj (19), SAT_adj (28), BEMEadj (35), LME_adj (54):
    Adjusted = Raw - Industry Mean

    Uses Fama-French 48 industry classification.

    Args:
        char_lf: Characteristics data (must have 'sic' or 'sich' column)
        industry_lf: FF48 industry mapping (sic -> industry)
        char_cols: Columns to adjust
        date_col: Date column for cross-sectional grouping

    Returns:
        LazyFrame with adjusted characteristics
    """
    # First need to join industry classification
    # Handle both 'sic' and 'sich' (Compustat historical SIC) column names
    existing_cols = char_lf.collect_schema().names()

    if "sic" not in existing_cols:
        if "sich" in existing_cols:
            # Rename sich to sic for the join
            char_lf = char_lf.rename({"sich": "sic"})
        else:
            raise ValueError("No SIC column found in data (expected 'sic' or 'sich')")

    merged = char_lf.join(industry_lf, on="sic", how="left")

    # Compute industry means and adjustments
    adj_exprs = []
    for col in char_cols:
        adj_col = f"{col}_adj" if not col.endswith("_adj") else col

        adj_exprs.append(
            (pl.col(col) - pl.col(col).mean().over([date_col, "industry"])).alias(
                adj_col
            )
        )

    return merged.with_columns(adj_exprs)


# =============================================================================
# SUV Characteristic (ID 61) - Fiscal Year Computation
# =============================================================================


def compute_suv_fiscal(
    fiscal_chars_lf: pl.LazyFrame,
    id_col: str = "permno",
) -> pl.LazyFrame:
    """
    Compute SUV (Standard Unexplained Volume) for fiscal years using monthly data.

    ID 61: SUV = standardized residuals from volume prediction model
    Model: Monthly_Volume_m = α + β1 * |Monthly_Ret_m+| + β2 * |Monthly_Ret_m-| + ε
    SUV = mean(residuals / std(residuals)) over 12 months

    This function expects fiscal_chars data that contains daily observations
    with columns: id columns, fyear, datadate, vol, ret, and aggregates them
    to monthly frequency before computing SUV.

    Args:
        fiscal_chars_lf: LazyFrame with fiscal year daily data
        id_col: Identifier column (permno)

    Returns:
        LazyFrame with one SUV value per firm-fiscal year
    """
    import numpy as np

    # First, aggregate daily data to monthly
    # Add year-month column for grouping
    daily_data = fiscal_chars_lf.with_columns(
        [
            pl.col("datadate").dt.month().alias("month"),
            pl.col("datadate").dt.year().alias("year"),
        ]
    )

    monthly_group_cols = [id_col, "gvkey", "fyear", "year", "month"]
    monthly_group_cols = list(dict.fromkeys(monthly_group_cols))

    # Aggregate to monthly: sum volume, compound returns
    monthly_data = (
        daily_data.group_by(monthly_group_cols)
        .agg(
            [
                pl.col("vol").sum().alias("monthly_vol"),
                # Compound returns: (1+r1)*(1+r2)*...-1
                ((pl.col("ret") + 1).product() - 1).alias("monthly_ret"),
                pl.col("datadate").count().alias("n_days"),
                pl.col("datadate").max().alias("max_date"),  # Keep for reference
            ]
        )
        .filter(
            pl.col("n_days") >= 10
        )  # Only keep months with at least 10 trading days
    )

    # Prepare features: absolute returns split by sign (now monthly)
    monthly_with_features = monthly_data.with_columns(
        [
            pl.when(pl.col("monthly_ret") > 0)
            .then(pl.col("monthly_ret").abs())
            .otherwise(0)
            .alias("abs_ret_pos"),
            pl.when(pl.col("monthly_ret") < 0)
            .then(pl.col("monthly_ret").abs())
            .otherwise(0)
            .alias("abs_ret_neg"),
        ]
    )

    group_cols = [id_col, "gvkey", "fyear"]
    group_cols = list(dict.fromkeys(group_cols))

    # Group by firm-fiscal year and collect monthly data
    fiscal_year_data = (
        monthly_with_features.group_by(group_cols)
        .agg(
            [
                pl.col("monthly_vol").alias("vol_data"),
                pl.col("abs_ret_pos").alias("ret_pos_data"),
                pl.col("abs_ret_neg").alias("ret_neg_data"),
                pl.col("monthly_vol").count().alias("n_months"),
                pl.col("max_date").max().alias("datadate"),  # Keep fiscal year end
            ]
        )
        .filter(pl.col("n_months") >= 10)  # Minimum 10 months out of 12
        .collect()
    )

    # Convert to pandas for regression computation
    df = fiscal_year_data.to_pandas()

    def compute_suv_for_fiscal_year(row):
        """Compute SUV from monthly data within fiscal year."""
        vol = np.array(row["vol_data"])
        ret_pos = np.array(row["ret_pos_data"])
        ret_neg = np.array(row["ret_neg_data"])

        # Remove NaN values
        valid_mask = ~(np.isnan(vol) | np.isnan(ret_pos) | np.isnan(ret_neg))
        if valid_mask.sum() < 10:
            return np.nan

        vol = vol[valid_mask]
        ret_pos = ret_pos[valid_mask]
        ret_neg = ret_neg[valid_mask]

        # Build design matrix: [constant, abs_ret_pos, abs_ret_neg]
        X = np.column_stack(
            [
                np.ones(len(vol)),
                ret_pos,
                ret_neg,
            ]
        )

        try:
            # OLS regression: Volume = α + β1*abs_ret_pos + β2*abs_ret_neg + ε
            beta, _, _, _ = np.linalg.lstsq(X, vol, rcond=None)

            # Compute residuals
            y_pred = X @ beta
            resid = vol - y_pred

            # Standardize residuals (ddof=3 for 3 parameters)
            std_resid = np.std(resid, ddof=3)
            if std_resid == 0 or np.isnan(std_resid):
                return np.nan

            # SUV is mean of standardized residuals
            suv = np.mean(resid / std_resid)
            return suv

        except Exception:
            return np.nan

    # Apply to each fiscal year
    df["SUV"] = df.apply(compute_suv_for_fiscal_year, axis=1)

    # Drop aggregated data columns and convert back to Polars
    keep_cols = group_cols + ["datadate", "SUV"]
    result = pl.from_pandas(df[keep_cols]).lazy()

    return result
