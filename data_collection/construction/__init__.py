"""
Construction submodule for Freyberger 62 characteristics.

Contains:
- prices.py: Momentum, volatility, beta calculations (daily logic)
- fundamentals.py: Ratios like BEME, ROA, Investment (annual logic)
- merge.py: Price + Fundamentals merge with Fama-French timing
"""

from data_collection.construction.prices import (
    compute_momentum_characteristics,
    compute_volatility_characteristics,
    compute_beta_characteristics,
    compute_trading_characteristics,
)
from data_collection.construction.fundamentals import (
    compute_intermediate_variables,
    compute_investment_characteristics,
    compute_profitability_characteristics,
    compute_intangibles_characteristics,
    compute_value_characteristics,
)
from data_collection.construction.merge import (
    merge_price_fundamentals,
    merge_fiscal_chars_to_monthly,
    merge_normalized_yearly_with_monthly,
    merge_normalized_yearly_with_monthly_chunked,
)

__all__ = [
    "compute_momentum_characteristics",
    "compute_volatility_characteristics",
    "compute_beta_characteristics",
    "compute_trading_characteristics",
    "compute_intermediate_variables",
    "compute_investment_characteristics",
    "compute_profitability_characteristics",
    "compute_intangibles_characteristics",
    "compute_value_characteristics",
    "merge_price_fundamentals",
    "merge_fiscal_chars_to_monthly",
    "merge_normalized_yearly_with_monthly",
    "merge_normalized_yearly_with_monthly_chunked",
]
