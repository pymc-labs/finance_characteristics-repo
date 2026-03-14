"""
Constants for the 62 Freyberger characteristics.

This module provides two main constants for use in validation notebooks and scripts:
1. CHARACTERISTIC_COLUMNS: List of 62 characteristic column names
2. CHARACTERISTIC_METADATA: Dictionary with metadata for each characteristic

These constants provide a single source of truth for characteristic definitions.
"""

from data_collection.config import CHARACTERISTICS

# =============================================================================
# Characteristic Column Names (as they appear in output files)
# =============================================================================

# Note: These names match the actual output files (from main.py)
# Some differ from config.py due to renaming in the pipeline:
# - ID 11: "NOA" in config.py -> "NOA_ch" in output
# - ID 35: "BEMEadj" in config.py -> "BEME_adj" in output
# - ID 48: "AT" in config.py -> "AT_raw" in output

CHARACTERISTIC_COLUMNS = [
    # Past Returns (IDs 1-5)
    "r2_1",  # ID 1: Return 1 month before prediction
    "r6_2",  # ID 2: Return 6 to 2 months before
    "r12_2",  # ID 3: Return 12 to 2 months before
    "r12_7",  # ID 4: Return 12 to 7 months before
    "r36_13",  # ID 5: Return 36 to 13 months before
    # Investment (IDs 6-11)
    "Investment",  # ID 6: Asset growth
    "ACEQ",  # ID 7: % change in book equity
    "DPI2A",  # ID 8: Change in PP&E + inventory / lagged AT
    "AShrout",  # ID 9: % change in shares outstanding
    "IVC",  # ID 10: Change in inventory / average AT
    "NOA_ch",  # ID 11: Net-operating assets / lagged AT
    # Profitability (IDs 12-28)
    "ATO",  # ID 12: Sales to lagged NOA
    "CTO",  # ID 13: Sales to lagged AT
    "dGM_dSales",  # ID 14: Δ Gross Margin - Δ Sales
    "EPS",  # ID 15: Earnings per share
    "IPM",  # ID 16: Pretax income over sales
    "PCM",  # ID 17: Gross profit margin
    "PM",  # ID 18: Operating profit margin
    "PM_adj",  # ID 19: Industry-adjusted PM
    "Prof",  # ID 20: Gross profitability over BE
    "RNA",  # ID 21: Return on net operating assets
    "ROA",  # ID 22: Return on assets
    "ROC",  # ID 23: Return on cash
    "ROE",  # ID 24: Return on equity
    "ROIC",  # ID 25: Return on invested capital
    "S2C",  # ID 26: Sales to cash
    "SAT",  # ID 27: Sales to assets
    "SAT_adj",  # ID 28: Industry-adjusted SAT
    # Intangibles (IDs 29-32)
    "AOA",  # ID 29: Absolute operating accruals
    "OL",  # ID 30: Operating leverage
    "Tan",  # ID 31: Tangibility
    "OA",  # ID 32: Operating accruals
    # Value (IDs 33-47)
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
    # Trading/Risk (IDs 48-62)
    "AT_raw",  # ID 48: Total Assets
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
    "SUV",  # ID 61: Standard unexplained volume
    "Total_vol",  # ID 62: Total volatility
]

# =============================================================================
# Characteristic Metadata Dictionary
# =============================================================================

# Build metadata dictionary from config.py CHARACTERISTICS list
# Mapping column names (from output files) to their metadata

# First, create a name mapping to handle discrepancies
_NAME_MAPPING = {
    "NOA": "NOA_ch",  # config.py name -> output name
    "BEMEadj": "BEME_adj",
    "AT": "AT_raw",
}

CHARACTERISTIC_METADATA = {}

for char in CHARACTERISTICS:
    # Get the output column name (handle name mappings)
    output_name = _NAME_MAPPING.get(char.name, char.name)

    # Build metadata dictionary
    metadata = {
        "id": char.id,
        "name": char.description,  # Use description as the readable name
        "description": char.description,
        "category": char.category,
        "formula": char.formula,
    }

    # Add optional fields if they exist
    if char.uses_intermediates:
        metadata["uses_intermediates"] = char.uses_intermediates

    CHARACTERISTIC_METADATA[output_name] = metadata

# =============================================================================
# Helper Functions
# =============================================================================


def get_characteristics_by_category(category: str) -> list[str]:
    """
    Get list of characteristic column names for a specific category.

    Args:
        category: One of "Past Returns", "Investment", "Profitability",
                  "Intangibles", "Value", "Trading"

    Returns:
        List of characteristic column names in that category
    """
    return [
        col
        for col in CHARACTERISTIC_COLUMNS
        if CHARACTERISTIC_METADATA[col]["category"] == category
    ]


def get_characteristic_info(column_name: str) -> dict:
    """
    Get metadata for a specific characteristic.

    Args:
        column_name: Column name of the characteristic

    Returns:
        Dictionary with characteristic metadata
    """
    return CHARACTERISTIC_METADATA.get(column_name, {})


def validate_characteristics_present(
    df_columns: list[str],
) -> tuple[list[str], list[str]]:
    """
    Validate which characteristics are present in a dataframe.

    Args:
        df_columns: List of column names from a dataframe

    Returns:
        Tuple of (present_characteristics, missing_characteristics)
    """
    present = [col for col in CHARACTERISTIC_COLUMNS if col in df_columns]
    missing = [col for col in CHARACTERISTIC_COLUMNS if col not in df_columns]
    return present, missing


# =============================================================================
# Category Lists
# =============================================================================

CATEGORIES = {
    "Past Returns": [1, 2, 3, 4, 5],
    "Investment": [6, 7, 8, 9, 10, 11],
    "Profitability": [
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28,
    ],
    "Intangibles": [29, 30, 31, 32],
    "Value": [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47],
    "Trading": [48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62],
}
