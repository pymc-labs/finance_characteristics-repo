"""
Freyberger 62 Characteristics Package (US).

A Polars-based implementation of the 62 firm characteristics from
Freyberger, Neuhierl, & Weber (2020) using US (CRSP/Compustat) data.

Usage:
    from data_collection import CharacteristicBuilder, PathConfig

    config = PathConfig(...)
    builder = CharacteristicBuilder(config)
    characteristics = builder.build()
"""

from data_collection.config import (
    PathConfig,
    VariableMapping,
    CharacteristicDef,
    CHARACTERISTICS,
    CATEGORIES,
    MARKET_MAPPINGS,
    FUNDAMENTAL_VARIABLES,
    get_characteristic_names,
    get_characteristic_by_id,
    get_characteristics_by_category,
    get_output_columns,
)

from data_collection.data_loader import (
    DataLoader,
    load_us_crsp_monthly,
    load_us_crsp_daily,
    load_us_compustat,
)

from data_collection.cleaners import (
    DataCleaner,
    clean_us_crsp,
    winsorize,
)

from data_collection.characteristics import (
    CharacteristicBuilder,
    build_us_characteristics,
)

from data_collection.normalization import (
    CharacteristicNormalizer,
    normalize_characteristics,
    normalize_barroso,
    winsorize_characteristics,
    add_suffix_to_columns,
)

__version__ = "0.1.0"

__all__ = [
    # Config
    "PathConfig",
    "VariableMapping",
    "CharacteristicDef",
    "CHARACTERISTICS",
    "CATEGORIES",
    "MARKET_MAPPINGS",
    "FUNDAMENTAL_VARIABLES",
    "get_characteristic_names",
    "get_characteristic_by_id",
    "get_characteristics_by_category",
    "get_output_columns",
    # Data Loading
    "DataLoader",
    "load_us_crsp_monthly",
    "load_us_crsp_daily",
    "load_us_compustat",
    # Cleaning
    "DataCleaner",
    "clean_us_crsp",
    "winsorize",
    # Characteristics
    "CharacteristicBuilder",
    "build_us_characteristics",
    # Normalization
    "CharacteristicNormalizer",
    "normalize_characteristics",
    "normalize_barroso",
    "winsorize_characteristics",
    "add_suffix_to_columns",
]
