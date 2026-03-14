# Missing Values Handling Documentation

**Last Updated:** February 6, 2026

## Overview

This document provides a complete reference for how missing values are handled throughout the characteristics computation pipeline. It maps every location where:

- **Dropping operations**: Rows or columns are filtered/removed due to null values or data quality thresholds
- **Replacement operations**: Missing values are filled or replaced with specific values (0, median, NaN, etc.)
- **Infinity handling**: Infinite values are checked or handled (note: currently no explicit handling)

## Missing Value Philosophy

As of February 6, 2026, the pipeline follows a **minimal intervention approach**:
- Missing values (NaN) propagate naturally through calculations
- No artificial filling with 0 unless mathematically justified
- Characteristics with insufficient data result in NaN
- Downstream users can apply their own imputation strategies
- Preserves data integrity by not making hidden assumptions about missing data

### Summary Statistics

| Operation Type | Total Instances | Files Affected |
|----------------|-----------------|----------------|
| Dropping Operations | 13 | 5 |
| Replacement Operations | 7 | 4 |
| Infinity Handling | 0 | 0 |
| **Total** | **20** | **7 unique files** |

### Files with Missing Value Operations

| File | Dropping | Replacement | Total |
|------|----------|-------------|-------|
| `download_data.py` | 5 | 4 | 9 |
| `data_collection/cleaners.py` | 2 | 1 | 3 |
| `data_collection/data_loader.py` | 4 | 0 | 4 |
| `data_collection/normalization.py` | 0 | 2 | 2 |
| `data_collection/construction/prices.py` | 1 | 0 | 1 |
| `data_collection/construction/fundamentals.py` | 0 | 1 | 1 |

---

## Table of Contents

1. [Dropping Operations](#1-dropping-operations)
   - [1.1 data_collection/cleaners.py](#11-data_collectioncleanerspy)
   - [1.2 data_collection/data_loader.py](#12-data_collectiondata_loaderpy)
   - [1.3 download_data.py](#13-download_datapy)
   - [1.4 data_collection/construction/prices.py](#14-data_collectionconstructionpricespy)
   - [1.5 data_collection/normalization.py](#15-data_collectionnormalizationpy)
2. [Replacement/Filling Operations](#2-replacementfilling-operations)
   - [2.1 data_collection/construction/fundamentals.py](#21-data_collectionconstructionfundamentalspy)
   - [2.2 download_data.py](#22-download_datapy)
   - [2.3 data_collection/normalization.py](#23-data_collectionnormalizationpy)
3. [Infinity Handling](#3-infinity-handling)
4. [Summary and Recommendations](#4-summary-and-recommendations)

---

## 1. Dropping Operations

This section documents all instances where rows or columns are removed due to null values or data quality thresholds.

### 1.1 data_collection/cleaners.py

#### Instance 1.1.1: Filter null/non-positive prices in US CRSP (Line 44)

**Function:** `clean_us_crsp()`

```python
result = lf.filter(pl.col("prc").is_not_null() & (pl.col("prc") > 0))
```

- **What's dropped:** Rows with null or non-positive prices
- **Column affected:** `prc` (price)
- **Rationale:** Ensures valid price data for US CRSP monthly data
- **Impact:** All price-based characteristics require valid prices

---

#### Instance 1.1.2: Filter null assets and fiscal year in US Compustat (Line 66) - MODIFIED

**Function:** `clean_us_compustat()`

```python
return lf.filter(pl.col("at").is_not_null()).filter(
    pl.col("fyear").is_not_null()
)
```

- **What's dropped:** Rows with null total assets or null fiscal year
- **Columns affected:** `at` (total assets), `fyear` (fiscal year)
- **Rationale:** Ensures non-null fundamentals; positive value check removed to allow zero/negative assets
- **Impact:** All fundamental characteristics require valid assets and fiscal year
- **Change (Feb 6, 2026):** Removed `& (pl.col("at") > 0)` condition

---

### 1.2 data_collection/data_loader.py

#### Instance 1.2.1: Filter null prices in US CRSP Monthly (Line 41)

**Function:** `load_us_crsp_monthly()`

```python
.filter(pl.col("prc").is_not_null())
```

- **What's dropped:** Rows with null prices
- **Column affected:** `prc` (price)
- **Rationale:** Ensures valid price data during loading
- **Impact:** Removes missing price observations before characteristic computation

---

#### Instance 1.2.2: Filter null prices in US CRSP Daily (Line 69)

**Function:** `load_us_crsp_daily()`

```python
.filter(pl.col("prc").is_not_null())
```

- **What's dropped:** Rows with null prices
- **Column affected:** `prc` (price)
- **Rationale:** Ensures valid daily price data
- **Impact:** Required for daily-based characteristics (Beta, volatility, SUV)

---

### 1.3 download_data.py

#### Instance 1.3.1: Filter firms with insufficient trading days - US (Line 810)

**Function:** `compute_fiscal_year_characteristics_crsp()`

```python
chars = chars[chars["n_days"] >= 120].copy()
```

- **What's dropped:** Firms with fewer than 120 trading days in the fiscal year
- **Column affected:** `n_days` (count of trading days)
- **Rationale:** Ensures sufficient data for reliable fiscal year characteristics
- **Impact:** Affects IDs 55-62 (trading characteristics: Beta, Total_vol, Ret_max, Std_Vol, Std_Turn, LTurnover, Spread, Rel2High, SUV)
- **Threshold:** 120 days ~ 48% of typical trading year (~250 days)

---

#### Instance 1.3.2: Return NaN for insufficient Beta observations - US (Line 1014)

**Function:** `_compute_beta_for_fiscal_year()`

```python
if len(group) < 60:  # Need at least 60 days
    return pd.Series({"Beta": np.nan, "Beta_Cor": np.nan, "Idio_vol": np.nan})
```

- **What's dropped:** Beta calculations for firms with fewer than 60 days of data (returns NaN)
- **Column affected:** All observations in the group
- **Rationale:** Requires minimum observations for reliable CAPM beta estimates
- **Impact:** IDs 49 (Beta), 50 (Beta_Cor), 52 (Idio_vol) - returns NaN if insufficient data
- **Threshold:** 60 days ~ 24% of trading year

---

#### Instance 1.3.3: Return NaN for insufficient SUV observations (Line 1207)

**Function:** `_compute_suv_for_fiscal_year()`

```python
if len(valid) < 10:
    return np.nan
```

- **What's dropped:** SUV calculations for securities with fewer than 10 months of valid data (returns NaN)
- **Column affected:** All valid observations in the group
- **Rationale:** Requires minimum monthly observations for reliable OLS regression (SUV uses monthly aggregated data)
- **Impact:** ID 61 (SUV) - returns NaN if insufficient data
- **Threshold:** 10 months out of 12 ~ 83% of fiscal year
- **Note:** SUV computation was changed from daily (60 days) to monthly (10 months) for efficiency

---

#### Instance 1.3.4: Filter months with insufficient trading days (Line 1158)

**Function:** `_aggregate_daily_to_monthly()` (for SUV computation)

```python
monthly = monthly[monthly['n_days'] >= 10].copy()
```

- **What's dropped:** Months with fewer than 10 trading days
- **Column affected:** `n_days` (count of trading days per month)
- **Rationale:** Ensures sufficient daily observations for monthly aggregation
- **Impact:** Affects SUV (ID 61) computation - months with < 10 days are excluded
- **Threshold:** 10 days ~ 48% of typical trading month (~21 days)

---

### 1.4 data_collection/construction/prices.py

#### Instance 1.4.1: Filter months with insufficient trading days for Beta (Line 257)

**Function:** `compute_beta_characteristics()`

```python
.filter(pl.col("n_days") >= 15)  # At least 15 trading days in month
```

- **What's dropped:** Months with fewer than 15 trading days
- **Column affected:** `n_days` (count of trading days per month)
- **Rationale:** Ensures sufficient data for monthly return aggregation in beta calculation
- **Impact:** Affects IDs 49 (Beta), 50 (Beta_Cor), 52 (Idio_vol)
- **Threshold:** 15 days ~ 71% of typical trading month (~21 days)

---

### 1.5 data_collection/normalization.py

#### Instance 1.5.1: Drop sparse characteristics with low coverage (Lines 339-376) - REMOVED

**Function:** `drop_sparse_characteristics()` - **DELETED Feb 6, 2026**

~~```python
def drop_sparse_characteristics(
    lf: pl.LazyFrame,
    char_cols: list[str],
    date_col: str = "date",
    min_coverage: float = 0.2,
) -> tuple[pl.LazyFrame, list[str]]:
    # ... (computes coverage per characteristic)
    kept_cols = [col for col in char_cols if coverage_df[col][0] >= min_coverage]
    # ...
    return lf.select(non_char_cols + kept_cols), kept_cols
```~~

- **Status:** REMOVED - Function deleted entirely
- **Rationale:** All 62 characteristics should be kept regardless of sparsity
- **Impact:** Sparse characteristics now remain in output with NaN values
- **Change (Feb 6, 2026):** Entire function removed from `data_collection/normalization.py`

---

## 2. Replacement/Filling Operations

This section documents all instances where missing values are filled or replaced with specific values.

### 2.1 data_collection/construction/fundamentals.py

This file contains replacement operations (13 instances).

#### Instance 2.1.1: Fill null preferred stock for Book Equity (Line 53) - REMOVED

**Function:** `compute_intermediate_variables()` - BE calculation

~~```python
.otherwise(pl.col("at") - pl.col("lt") - pl.col("pstk").fill_null(0))
```~~

- **Status:** REMOVED - `.fill_null(0)` deleted
- **New behavior:** `pstk` NaN propagates to BE calculation
- **Change (Feb 6, 2026):** Now uses `.otherwise(pl.col("at") - pl.col("lt") - pl.col("pstk"))`

---

#### Instance 2.1.2-2.1.5: Fill nulls for NOA calculation (Lines 59-62) - REMOVED

**Function:** `compute_intermediate_variables()` - NOA calculation

~~```python
pl.col("dlc").fill_null(0)
+ pl.col("dltt").fill_null(0)
+ pl.col("ceq").fill_null(0)
- pl.col("che").fill_null(0)
```~~

- **Status:** REMOVED - All `.fill_null(0)` deleted
- **New behavior:** NaN in any component results in NaN for NOA
- **Characteristic:** ID 11 (NOA - Net Operating Assets)
- **Change (Feb 6, 2026):** Now uses direct operations without filling

---

#### Instance 2.1.6-2.1.7: Fill nulls for Gross Profit (Line 65) - REMOVED

**Function:** `compute_intermediate_variables()` - GP calculation

~~```python
(pl.col("sale").fill_null(0) - pl.col("cogs").fill_null(0)).alias("GP"),
```~~

- **Status:** REMOVED - All `.fill_null(0)` deleted
- **New behavior:** NaN in sale or cogs results in NaN for GP
- **Characteristic:** Intermediate variable for profitability ratios
- **Change (Feb 6, 2026):** Now uses `(pl.col("sale") - pl.col("cogs"))`

---

#### Instance 2.1.8-2.1.18: Fill nulls for Operating Accruals (Lines 83-90) - REMOVED

**Function:** `compute_intermediate_variables()` - OpAcc calculation

~~```python
(pl.col("act").fill_null(0) - pl.col("act_lag").fill_null(0))
- (pl.col("che").fill_null(0) - pl.col("che_lag").fill_null(0))
- (
    (pl.col("lct").fill_null(0) - pl.col("lct_lag").fill_null(0))
    - (pl.col("dlc").fill_null(0) - pl.col("dlc_lag").fill_null(0))
    - (pl.col("txp").fill_null(0) - pl.col("txp_lag").fill_null(0))
)
- pl.col("dp").fill_null(0)
```~~

- **Status:** REMOVED - All 11 `.fill_null(0)` deleted
- **New behavior:** NaN in any component results in NaN for OpAcc
- **Characteristics:** ID 29 (AOA - Accruals), ID 31 (OA - Operating Accruals)
- **Change (Feb 6, 2026):** Now uses direct operations without filling

---

#### Instance 2.1.19-2.1.20: Fill nulls for DPI2A (Lines 149-150) - REMOVED

**Function:** `compute_investment_characteristics()` - DPI2A calculation

~~```python
(pl.col("ppent").fill_null(0) - pl.col("ppent_lag").fill_null(0))
+ (pl.col("invt").fill_null(0) - pl.col("invt_lag").fill_null(0))
```~~

- **Status:** REMOVED - All `.fill_null(0)` deleted
- **New behavior:** NaN in ppent or invt results in NaN for DPI2A
- **Characteristic:** ID 8 (DPI2A - Change in PP&E + inventory / assets)
- **Change (Feb 6, 2026):** Now uses direct operations without filling

---

#### Instance 2.1.21-2.1.22: Fill nulls for IVC (Line 156) - REMOVED

**Function:** `compute_investment_characteristics()` - IVC calculation

~~```python
(pl.col("invt").fill_null(0) - pl.col("invt_lag").fill_null(0))
```~~

- **Status:** REMOVED - All `.fill_null(0)` deleted
- **New behavior:** NaN in invt results in NaN for IVC
- **Characteristic:** ID 10 (IVC - Inventory change)
- **Change (Feb 6, 2026):** Now uses direct subtraction without filling

---

#### Instance 2.1.23-2.1.24: Fill nulls for Operating Leverage (Lines 337-338) - REMOVED

**Function:** `compute_intangibles_characteristics()` - OL calculation

~~```python
(pl.col("cogs").fill_null(0) + pl.col("xsga").fill_null(0))
/ pl.col("at")
```~~

- **Status:** REMOVED - All `.fill_null(0)` deleted
- **New behavior:** NaN in cogs or xsga results in NaN for OL
- **Characteristic:** ID 30 (OL - Operating leverage)
- **Change (Feb 6, 2026):** Now uses `(pl.col("cogs") + pl.col("xsga"))`

---

#### Instance 2.1.25: Fill null for C2D (Line 439) - REMOVED

**Function:** `compute_value_characteristics()` - C2D calculation

~~```python
((pl.col("ib") + pl.col("dp").fill_null(0)) / pl.col("lt")).alias("C2D")
```~~

- **Status:** REMOVED - `.fill_null(0)` deleted
- **New behavior:** NaN in dp results in NaN for C2D
- **Characteristic:** ID 35 (C2D - Cash flow to debt)
- **Change (Feb 6, 2026):** Now uses `((pl.col("ib") + pl.col("dp")) / pl.col("lt"))`

---

#### Instance 2.1.26-2.1.27: Fill nulls for Debt2P (Line 444) - REMOVED

**Function:** `compute_value_characteristics()` - Debt2P calculation

~~```python
(pl.col("dltt").fill_null(0) + pl.col("dlc").fill_null(0))
```~~

- **Status:** REMOVED - All `.fill_null(0)` deleted
- **New behavior:** NaN in dltt or dlc results in NaN for Debt2P
- **Characteristic:** ID 36 (Debt2P - Debt-to-price)
- **Change (Feb 6, 2026):** Now uses `(pl.col("dltt") + pl.col("dlc"))`

---

#### Instance 2.1.28-2.1.30: Fill nulls for Free Cash Flow (Lines 452-455) - REMOVED

**Function:** `compute_value_characteristics()` - Free_CF calculation

~~```python
pl.col("ni")
+ pl.col("dp").fill_null(0)
- pl.col("wcapch").fill_null(0)
- pl.col("capx").fill_null(0)
```~~

- **Status:** REMOVED - All `.fill_null(0)` deleted
- **New behavior:** NaN in dp, wcapch, or capx results in NaN for Free_CF
- **Characteristic:** ID 41 (Free_CF - Free cash flow)
- **Change (Feb 6, 2026):** Now uses direct operations without filling
- **Change (Feb 10, 2026):** Added balance sheet fallback for `wcapch`. Compustat's `wcapch` field is null for ~95% of firm-years, causing Free_CF to be ~96% null. When `wcapch` is null, it is now approximated from balance sheet changes: `wcapch_calc = Δ(act - che) - Δ(lct - dlc)`. This is computed as an intermediate variable in `compute_intermediate_variables()` and applied via `pl.col("wcapch").fill_null(pl.col("wcapch_calc"))`. The `.fill_null(0)` removal from Feb 6 is preserved — only `wcapch` gets this targeted fallback, not dp or capx.

---

#### Instance 2.1.31: Fill null for dividend yield (Line 460) - REMOVED

**Function:** `compute_value_characteristics()` - LDP calculation

~~```python
(pl.col("dvt").fill_null(0) / pl.col("ME")).alias("LDP")
```~~

- **Status:** REMOVED - `.fill_null(0)` deleted
- **New behavior:** NaN in dvt results in NaN for LDP (not zero dividend yield)
- **Characteristic:** ID 40 (LDP - Dividend yield)
- **Change (Feb 6, 2026):** Now uses `(pl.col("dvt") / pl.col("ME"))`

---

#### Instance 2.1.32-2.1.33: Fill nulls for Net Payout (Line 463) - REMOVED

**Function:** `compute_value_characteristics()` - NOP calculation

~~```python
(pl.col("dvt").fill_null(0) + pl.col("prstkc").fill_null(0))
```~~

- **Status:** REMOVED - All `.fill_null(0)` deleted
- **New behavior:** NaN in dvt or prstkc results in NaN for NOP
- **Characteristic:** ID 41 (NOP - Net payout yield)
- **Change (Feb 6, 2026):** Now uses `(pl.col("dvt") + pl.col("prstkc"))`

---

#### Instance 2.1.34: Fill null adjustment factor (Line 512)

**Function:** `compute_aso()` - ASO calculation

```python
(pl.col("csho") * pl.col("ajex").fill_null(1)).alias("adj_shares")
```

- **Column affected:** `ajex` (adjustment factor for splits)
- **Replacement value:** `1`
- **Purpose:** Split-adjusted shares = csho x ajex, defaulting to 1 if ajex is missing (no adjustment)
- **Characteristic:** ID 39 (ASO - Share issuance)

---

### 2.2 download_data.py

This file contains replacement operations for preventing division by zero.

#### Instance 2.2.1: Replace 0 with NaN for spread (Lines 777-779)

**Function:** `compute_fiscal_year_characteristics_crsp()`

```python
midpoint = (fy_data["ask"] + fy_data["bid"]) / 2
fy_data["daily_spread"] = (fy_data["ask"] - fy_data["bid"]) / midpoint.replace(
    0, np.nan
)
```

- **Column affected:** `midpoint` (calculated as (ask + bid) / 2)
- **Replacement value:** `np.nan`
- **Purpose:** Prevent division by zero in spread calculation
- **Characteristic:** ID 58 (Spread - Bid-ask spread)
- **Impact:** Days with zero midpoint get NaN spread

---

#### Instance 2.2.2: Replace 0 with NaN for Rel2High (Line 807)

**Function:** `compute_fiscal_year_characteristics_crsp()`

```python
chars["Rel2High"] = chars["last_prc"] / chars["high_52w"].replace(0, np.nan)
```

- **Column affected:** `high_52w` (52-week high price)
- **Replacement value:** `np.nan`
- **Purpose:** Prevent division by zero in Rel2High calculation
- **Characteristic:** ID 56 (Rel2High - Price relative to 52-week high)
- **Impact:** Firms with zero 52-week high get NaN Rel2High

---

### 2.3 data_collection/normalization.py

This file contains 2 instances related to the Barroso normalization process.

#### Instance 2.3.1: Fill nulls with cross-sectional median (Line 231)

**Function:** `_apply_barroso_transform_fast()` - Step 2 of Barroso normalization

```python
imputed = winsorized.fill_null(pl.col(f"{col}__med"))
```

- **Columns affected:** All characteristic columns (`char_cols`)
- **Replacement value:** Cross-sectional median (`{col}__med`) computed per date
- **Purpose:** Step 2 of Barroso et al. (2025) normalization - impute missing values with cross-sectional median before rank transformation
- **Context:** Part of the rank normalization pipeline
- **Citation:** Barroso, Saxena, & Wang (2025) Appendix A.1

---

#### Instance 2.3.2: Fill nulls with median/mean/zero (Line 333)

**Function:** `impute_missing()` - General imputation function

```python
imputed = pl.col(col).fill_null(fill_value)
```

- **Columns affected:** Characteristic columns specified in `char_cols`
- **Replacement value:** Depends on `method` parameter:
  - `"median"`: Cross-sectional median per date (`.median().over(date_col)`)
  - `"mean"`: Cross-sectional mean per date (`.mean().over(date_col)`)
  - `"zero"`: Literal `0.0`
- **Purpose:** General missing value imputation function with multiple strategies
- **Use case:** Can be called independently or as part of normalization pipeline

---

## 3. Infinity Handling

### 3.1 Zero-Denominator Guards for Industry-Adjusted Characteristics

**Change (Feb 10, 2026):** Three base characteristics now have explicit zero-denominator guards to prevent `inf` propagation in industry-demeaned (`_adj`) variables:

| Characteristic | Guard | Reason |
|:---|:---|:---|
| **PM** (ID 18): `oiadp / sale` | `sale != 0` | Prevents inf in **PM_adj** (ID 19) |
| **SAT** (ID 27): `sale / at` | `at != 0` | Prevents inf in **SAT_adj** (ID 28) |
| **BEME** (ID 34): `BE / ME` | `ME != 0` | Prevents inf in **BEME_adj** (ID 35) |

**Why only these 3?** Industry-adjusted characteristics use `.mean().over([datadate, industry])`. A single `inf` value in any industry-date group makes the group mean `inf`, then `char - inf = NaN` for **every firm in that group**. One bad observation poisons the entire industry-date cross-section. These 3 are the only characteristics that feed into `_adj` demeaning. Other standalone divisions (IPM, PCM, A2ME, E2P, S2C, ROC, C2D, etc.) may still produce `inf` from zero denominators, but these are handled by the Barroso normalization winsorization (1st/99th percentile clipping).

### 3.2 Other Division Operations

For characteristics **not** feeding into industry adjustments, there is no explicit handling of infinite values. Division by zero may produce `inf` values (e.g., turnover when `shrout = 0`). These are handled downstream by the cross-sectional rank normalization step, which converts values to ranks before normalizing.

---

## 4. Summary and Recommendations

### Changes Implemented (February 6, 2026)

**Removed Operations:**
- 2 positive asset value checks in `cleaners.py`
- 1 sparse characteristics dropping function in `normalization.py`
- 13 `.fill_null(0)` operations in `fundamentals.py`
- 2 `.fillna()` / `.replace()` operations in `download_data.py`

**Total Reduction:** 18 operations removed

### Current Philosophy

The pipeline now follows a **minimal intervention approach** to missing values:

1. **Transparency:** Missing data is visible as NaN in outputs
2. **No Hidden Assumptions:** Don't assume missing dividends = 0, missing debt = 0, etc.
3. **Natural Propagation:** NaN values flow through calculations naturally
4. **User Control:** Downstream users can apply their own imputation strategies
5. **Data Integrity:** Prevents incorrect calculations from artificial fills

### Remaining Operations

**Justified Dropping (13 instances):**
- Valid price/asset/fiscal year filters (data quality)
- Minimum observation thresholds for statistical reliability (Beta, SUV)
- Months with insufficient trading days (data completeness)

**Justified Replacement (7 instances):**
- Normalization-specific imputation (Barroso method - cross-sectional median)
- General imputation function (optional, user-controlled)
- Split adjustment factor filling (`.fill_null(1)` for `ajex` - mathematically correct default)
- Division by zero prevention (spread calculation - still retained for `midpoint`)

### Impact on Characteristics

**Most Affected Characteristics:**
- **Investment (8, 10, 11):** DPI2A, IVC, NOA - more NaN if components missing
- **Value (35, 36, 38, 40, 41):** C2D, Debt2P, Free_CF, LDP, NOP - missing dividends/debt no longer assumed zero
- **Intangibles (30):** OL - missing cogs/xsga produce NaN
- **Trading (61):** SUV - NaN returns handled differently

**Expected Outcome:**
- Higher NaN counts in characteristics with sparse input data
- More accurate representation of data availability
- Better data quality signals for downstream analysis

### Testing Recommendations

1. Compare NaN counts before/after for all 62 characteristics
2. Verify no crashes or unexpected errors in pipeline execution
3. Spot-check firms with known missing data to verify NaN propagation
4. Validate that output files remain valid and readable
5. Document any characteristics with significantly reduced coverage

---

**Document Version:** 2.0
**Last Updated:** February 6, 2026
**Changes:** Major revision - removed 18 missing value operations to adopt minimal intervention philosophy
