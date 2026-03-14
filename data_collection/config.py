"""
Configuration module for Freyberger 62 characteristics (US data).

Contains variable mappings for US (CRSP/Compustat NA) data sources,
as well as characteristic definitions.
"""

from dataclasses import dataclass, field
from pathlib import Path

# =============================================================================
# Variable Mappings (US)
# =============================================================================


@dataclass
class VariableMapping:
    """Maps a concept to its US variable code."""

    concept: str
    us_code: str


# Market / Security Data (0.0xx)
MARKET_MAPPINGS: list[VariableMapping] = [
    VariableMapping("price", "prc"),
    VariableMapping("shares", "csho"),
    VariableMapping("return", "ret"),
    VariableMapping("volume", "vol"),
    VariableMapping("high", "askhi"),
    VariableMapping("low", "bidlo"),
    VariableMapping("bid", "bid"),
    VariableMapping("ask", "ask"),
    VariableMapping("adj_factor", "cfacpr"),
]

# Annual Fundamentals (0.1xx)
FUNDAMENTAL_VARIABLES: list[str] = [
    "act",  # 0.101: Current Assets - Total
    "at",  # 0.102: Assets - Total
    "capx",  # 0.103: Capital Expenditures
    "ceq",  # 0.104: Common/Ordinary Equity
    "che",  # 0.105: Cash & Short-Term Investments
    "cogs",  # 0.106: Cost of Goods Sold
    "dlc",  # 0.107: Debt in Current Liabilities
    "dltt",  # 0.108: Long-Term Debt - Total
    "dp",  # 0.109: Depreciation & Amortization
    "dvt",  # 0.110: Dividends - Total
    "ebit",  # 0.111: Earnings Before Interest & Tax
    "ib",  # 0.113: Income Before Extraordinary Items
    "invt",  # 0.114: Inventories - Total
    "lct",  # 0.115: Current Liabilities - Total
    "lt",  # 0.116: Liabilities - Total
    "ni",  # 0.117: Net Income (Loss)
    "oiadp",  # 0.118: Operating Income After Depreciation
    "pi",  # 0.119: Pretax Income
    "ppent",  # 0.120: PP&E - Net
    "prstkc",  # 0.122: Purchase of Common/Preferred Stock
    "sale",  # 0.123: Sales / Turnover (Net)
    "txp",  # 0.124: Income Taxes Payable
    "wcapch",  # 0.125: Working Capital Changes
    "xsga",  # 0.126: SG&A Expenses
    "pstk",  # Preferred Stock (for BE fallback)
    "ajex",  # Adjustment factor (annual)
    "csho",  # Common Shares Outstanding (for ASO)
    "sich",  # Historical SIC code (for industry-adjusted characteristics)
]

# EPS variable (US uses epspx)
EPS_CODE = "epspx"

# Fiscal year-end price (US uses prcc_f)
PRICE_FISCAL_CODE = "prcc_f"


# =============================================================================
# Characteristic Definitions
# =============================================================================


@dataclass
class CharacteristicDef:
    """Definition of a single characteristic."""

    id: int
    name: str
    category: str
    description: str
    formula: str
    uses_intermediates: list[str] = field(default_factory=list)


# Intermediate Variables (0.3xx)
INTERMEDIATE_DEFINITIONS: dict[str, str] = {
    "ME": "price * shares",  # 0.301
    "BE": "ceq (fallback: at - lt - pstk)",  # 0.302
    "NOA": "(at - che) - (at - dlc - dltt - ceq)",  # 0.303
    "OpAcc": "(Δact - Δche) - (Δlct - Δdlc - Δtxp) - dp",  # 0.304
    "GP": "sale - cogs",  # 0.305
}


# 62 Characteristics organized by category
CHARACTERISTICS: list[CharacteristicDef] = [
    # Past Returns (IDs 1-5)
    CharacteristicDef(
        1, "r2_1", "Past Returns", "Return 1 month before prediction", "ret(t-1)"
    ),
    CharacteristicDef(
        2, "r6_2", "Past Returns", "Return 6 to 2 months before", "cumret(t-6, t-2)"
    ),
    CharacteristicDef(
        3,
        "r12_2",
        "Past Returns",
        "Return 12 to 2 months before",
        "cumret(t-12, t-2)",
    ),
    CharacteristicDef(
        4,
        "r12_7",
        "Past Returns",
        "Return 12 to 7 months before",
        "cumret(t-12, t-7)",
    ),
    CharacteristicDef(
        5,
        "r36_13",
        "Past Returns",
        "Return 36 to 13 months before",
        "cumret(t-36, t-13)",
    ),
    # Investment (IDs 6-11)
    CharacteristicDef(
        6, "Investment", "Investment", "% change in AT", "Δat / at_{t-1}"
    ),
    CharacteristicDef(
        7, "ACEQ", "Investment", "% change in Book Equity", "Δceq / ceq_{t-1}"
    ),
    CharacteristicDef(
        8,
        "DPI2A",
        "Investment",
        "Change in PP&E + inventory / lagged AT",
        "(Δppent + Δinvt) / at_{t-1}",
    ),
    CharacteristicDef(
        9,
        "AShrout",
        "Investment",
        "% change in shares outstanding",
        "Δshares / shares_{t-1}",
    ),
    CharacteristicDef(
        10, "IVC", "Investment", "Change in inventory / average AT", "Δinvt / avg(at)"
    ),
    CharacteristicDef(
        11,
        "NOA",
        "Investment",
        "Net-operating assets / lagged AT",
        "NOA / at_{t-1}",
        ["NOA"],
    ),
    # Profitability (IDs 12-28)
    CharacteristicDef(
        12, "ATO", "Profitability", "Sales to lagged NOA", "sale / NOA_{t-1}", ["NOA"]
    ),
    CharacteristicDef(
        13, "CTO", "Profitability", "Sales to lagged AT", "sale / at_{t-1}"
    ),
    CharacteristicDef(
        14,
        "dGM_dSales",
        "Profitability",
        "Δ Gross Margin - Δ Sales",
        "Δ(GP/sale) - Δsale",
        ["GP"],
    ),
    CharacteristicDef(
        15, "EPS", "Profitability", "Earnings per share", "eps"
    ),
    CharacteristicDef(
        16, "IPM", "Profitability", "Pretax income over sales", "pi / sale"
    ),
    CharacteristicDef(
        17, "PCM", "Profitability", "Sales minus COGS to sales", "GP / sale", ["GP"]
    ),
    CharacteristicDef(
        18, "PM", "Profitability", "Op. inc after dep. over sales", "oiadp / sale"
    ),
    CharacteristicDef(
        19,
        "PM_adj",
        "Profitability",
        "PM - Industry Mean PM",
        "oiadp / sale - ind_mean",
    ),
    CharacteristicDef(
        20,
        "Prof",
        "Profitability",
        "Gross profitability over BE",
        "GP / BE",
        ["GP", "BE"],
    ),
    CharacteristicDef(
        21,
        "RNA",
        "Profitability",
        "Op. inc after dep. to lagged NOA",
        "oiadp / NOA_{t-1}",
        ["NOA"],
    ),
    CharacteristicDef(
        22, "ROA", "Profitability", "Inc before extra to lagged AT", "ib / at_{t-1}"
    ),
    CharacteristicDef(
        23,
        "ROC",
        "Profitability",
        "(Size + LT Debt - AT) to cash",
        "(ME + dltt - at) / che",
        ["ME"],
    ),
    CharacteristicDef(
        24,
        "ROE",
        "Profitability",
        "Inc before extra to lagged BE",
        "ib / BE_{t-1}",
        ["BE"],
    ),
    CharacteristicDef(
        25,
        "ROIC",
        "Profitability",
        "Return on invested capital",
        "ebit / (ceq + lt - che)",
    ),
    CharacteristicDef(26, "S2C", "Profitability", "Sales to cash", "sale / che"),
    CharacteristicDef(27, "SAT", "Profitability", "Sales to total assets", "sale / at"),
    CharacteristicDef(
        28,
        "SAT_adj",
        "Profitability",
        "SAT - Industry Mean SAT",
        "sale / at - ind_mean",
    ),
    # Intangibles (IDs 29-32)
    CharacteristicDef(
        29,
        "AOA",
        "Intangibles",
        "Abs value of operating accruals",
        "abs(OpAcc)",
        ["OpAcc"],
    ),
    CharacteristicDef(
        30,
        "OL",
        "Intangibles",
        "COGS + SG&A to total assets",
        "(cogs + xsga) / at",
    ),
    CharacteristicDef(
        31, "Tan", "Intangibles", "Tangibility (PP&E / AT)", "ppent / at"
    ),
    CharacteristicDef(
        32, "OA", "Intangibles", "Operating accruals", "OpAcc", ["OpAcc"]
    ),
    # Value (IDs 33-47)
    CharacteristicDef(33, "A2ME", "Value", "Total assets to Size", "at / ME", ["ME"]),
    CharacteristicDef(
        34, "BEME", "Value", "Book to market ratio", "BE / ME", ["BE", "ME"]
    ),
    CharacteristicDef(
        35,
        "BEMEadj",
        "Value",
        "BEME - Industry Mean BEME",
        "BE / ME - ind_mean",
        ["BE", "ME"],
    ),
    CharacteristicDef(36, "C", "Value", "Cash to AT", "che / at"),
    CharacteristicDef(
        37, "C2D", "Value", "Cash flow to total liabilities", "(ib + dp) / lt"
    ),
    CharacteristicDef(
        38,
        "ASO",
        "Value",
        "Log chg in split-adj shares",
        "Δln(shares × ajex)",
    ),
    CharacteristicDef(
        39, "Debt2P", "Value", "Total debt to Size", "(dltt + dlc) / ME", ["ME"]
    ),
    CharacteristicDef(
        40, "E2P", "Value", "Inc before extra to Size", "ib / ME", ["ME"]
    ),
    CharacteristicDef(
        41,
        "Free_CF",
        "Value",
        "Free cash flow to BE",
        "(ni + dp - wcapch - capx) / BE",
        ["BE"],
    ),
    CharacteristicDef(
        42, "LDP", "Value", "Trail 12-m dividends to price", "dvt / ME", ["ME"]
    ),
    CharacteristicDef(
        43, "NOP", "Value", "Net payouts to Size", "(dvt + prstkc) / ME", ["ME"]
    ),
    CharacteristicDef(
        44, "O2P", "Value", "Operating payouts to market cap", "(ni - Δbe) / ME", ["ME"]
    ),
    CharacteristicDef(45, "Q", "Value", "Tobin's Q", "(at + ME - ceq) / at", ["ME"]),
    CharacteristicDef(46, "S2P", "Value", "Sales to price", "sale / ME", ["ME"]),
    CharacteristicDef(47, "Sales_g", "Value", "Sales growth", "Δsale / sale_{t-1}"),
    # Trading Frictions (IDs 48-62)
    CharacteristicDef(48, "AT", "Trading", "Total Assets", "at"),
    CharacteristicDef(
        49,
        "Beta_Cor",
        "Trading",
        "Correlation ratio of vols",
        "Beta × (VolMkt/VolStock)",
    ),
    CharacteristicDef(
        50,
        "Beta",
        "Trading",
        "CAPM beta using daily returns",
        "Daily Rolling Regression",
    ),
    CharacteristicDef(
        51, "DTO", "Trading", "De-trended Turnover", "Turnover - Trend"
    ),
    CharacteristicDef(
        52,
        "Idio_vol",
        "Trading",
        "Idio vol of FF3 factor model",
        "Residual StdDev",
    ),
    CharacteristicDef(
        53, "LME", "Trading", "Price × shares outstanding", "ME", ["ME"]
    ),
    CharacteristicDef(
        54, "LME_adj", "Trading", "Size - Industry Mean Size", "ME - ind_mean", ["ME"]
    ),
    CharacteristicDef(
        55, "LTurnover", "Trading", "Last month's vol to shares", "vol / shares"
    ),
    CharacteristicDef(
        56,
        "Rel2High",
        "Trading",
        "Price to 52 week high price",
        "prc / high52",
    ),
    CharacteristicDef(
        57, "Ret_max", "Trading", "Maximum daily return", "Max(Daily Ret)"
    ),
    CharacteristicDef(
        58, "Spread", "Trading", "Average daily bid-ask spread", "(Ask-Bid)/Mid"
    ),
    CharacteristicDef(
        59, "Std_Turn", "Trading", "Std dev of daily turnover", "Std(Vol/Shares)"
    ),
    CharacteristicDef(60, "Std_Vol", "Trading", "Std dev of daily volume", "Std(Vol)"),
    CharacteristicDef(
        61, "SUV", "Trading", "Standard unexplained volume", "Model Residuals"
    ),
    CharacteristicDef(
        62, "Total_vol", "Trading", "Std dev of daily returns", "Std(Daily Ret)"
    ),
]


# =============================================================================
# Category Groupings
# =============================================================================

CATEGORIES: dict[str, list[int]] = {
    "Past Returns": [1, 2, 3, 4, 5],
    "Investment": [6, 7, 8, 9, 10, 11],
    "Profitability": [
        12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
    ],
    "Intangibles": [29, 30, 31, 32],
    "Value": [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47],
    "Trading": [48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62],
}


# =============================================================================
# Data Filters
# =============================================================================


@dataclass
class USFilters:
    """US-specific data filters."""

    exchange_codes: list[int] = field(
        default_factory=lambda: [1, 2, 3]
    )  # NYSE, AMEX, NASDAQ
    share_codes: list[int] = field(default_factory=lambda: [10, 11])  # Common shares


# =============================================================================
# Path Configuration
# =============================================================================


@dataclass
class PathConfig:
    """Data path configuration."""

    # Input paths (to be set by user)
    us_crsp_monthly: Path | None = None
    us_crsp_daily: Path | None = None
    us_compustat: Path | None = None
    us_ccm_link: Path | None = None
    ff_factors: Path | None = None
    ff48_industries: Path | None = None

    # Fiscal year characteristics (computed from daily data during download)
    us_fiscal_chars: Path | None = None  # crsp_fiscal_chars.parquet

    # Output paths
    output_dir: Path = field(default_factory=lambda: Path("output"))

    @property
    def output_raw(self) -> Path:
        return self.output_dir / "characteristics_raw_us.parquet"

    @property
    def output_normalized(self) -> Path:
        return self.output_dir / "characteristics_normalized_us.parquet"

    # Yearly characteristics
    @property
    def output_yearly_raw(self) -> Path:
        """Raw yearly characteristics (before normalization)."""
        return self.output_dir / "yearly_raw_characteristics_us.parquet"

    # Final merged output - unnormalized (raw characteristics only)
    @property
    def output_final_unnormalized(self) -> Path:
        """Final unnormalized characteristics."""
        return self.output_dir / "final_output_unnormalized_us.parquet"

    # Final merged output - normalized (normalized characteristics only)
    @property
    def output_final_normalized(self) -> Path:
        """Final normalized characteristics."""
        return self.output_dir / "final_output_normalized_us.parquet"

    # Monthly prices with raw price characteristics
    @property
    def output_monthly_raw(self) -> Path:
        """Monthly prices with raw price characteristics."""
        return self.output_dir / "monthly_prices_raw_price_charact_us.parquet"


# =============================================================================
# Helper Functions
# =============================================================================


def get_characteristic_by_id(char_id: int) -> CharacteristicDef | None:
    """Get characteristic definition by ID."""
    for char in CHARACTERISTICS:
        if char.id == char_id:
            return char
    return None


def get_characteristics_by_category(category: str) -> list[CharacteristicDef]:
    """Get all characteristics in a category."""
    return [c for c in CHARACTERISTICS if c.category == category]


def get_characteristic_names() -> list[str]:
    """Get list of all characteristic names."""
    return [c.name for c in CHARACTERISTICS]


def get_us_mapping(concept: str) -> str:
    """Get the US variable code for a concept."""
    for mapping in MARKET_MAPPINGS:
        if mapping.concept == concept:
            return mapping.us_code
    raise ValueError(f"Unknown concept: {concept}")


def get_output_columns() -> list[str]:
    """
    Get the list of columns for final output files.

    Returns only essential columns:
    - Identifiers (permno, gvkey)
    - Date
    - Price
    - 62 characteristic columns
    """
    char_cols = get_characteristic_names()
    return ["permno", "gvkey", "date", "prc"] + char_cols
