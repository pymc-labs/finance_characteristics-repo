# Normalization Methodology

This document describes the cross-sectional normalization procedure applied to the 62 Freyberger characteristics in the US pipeline.

---

## 1. Overview

The normalization follows **Barroso, Saxena & Wang (2025), Appendix A.1**. The goal is to transform raw characteristic values into cross-sectionally comparable, uniformly distributed scores on [-0.5, 0.5]. This ensures that:

- Characteristics with different scales (e.g., total assets in millions vs. return ratios near zero) are directly comparable
- Outliers are bounded (via winsorization)
- Missing values are handled systematically (via cross-sectional median imputation)
- The resulting normalized characteristics are robust inputs for machine learning models

---

## 2. Normalization Steps

For each characteristic $c$ at each cross-section $t$, the following four steps are applied **in order**:

### Step 1: Winsorization

Clip values to the cross-sectional 1st and 99th percentiles:

$$c_{i,t}^{W} = \text{clip}(c_{i,t}, \; P_{0.01}(c_t), \; P_{0.99}(c_t))$$

This bounds extreme outliers (including $\pm\infty$ from division by zero) to the empirical tails of the cross-sectional distribution.

### Step 2: Imputation

Fill remaining missing values with the cross-sectional median:

$$c_{i,t}^{I} = \begin{cases} c_{i,t}^{W} & \text{if not null} \\ \text{median}(c_t^{W}) & \text{if null} \end{cases}$$

After this step, all stocks in the cross-section have a value for characteristic $c$.

### Step 3: Rank Transform

Assign cross-sectional ranks using the **average method** for ties:

$$R_{i,t} = \text{rank}(c_{i,t}^{I}) \quad \text{within cross-section } t$$

Ranks range from 1 to $N_t$ (total number of stocks in the cross-section).

### Step 4: Scale to [-0.5, 0.5]

$$\tilde{c}_{i,t} = \frac{R_{i,t}}{N_t + 1} - 0.5$$

Where $N_t$ is the total number of stocks in the cross-section (after imputation). The resulting values lie in the open interval $\left(\frac{1}{N_t+1} - 0.5, \; \frac{N_t}{N_t+1} - 0.5\right) \approx (-0.5, \; 0.5)$.

---

## 3. Cross-Section Definition

| Parameter | Value |
|:---|:---|
| Cross-section grouping | `date` (calendar month) |
| Universe | All US stocks with valid identifiers at that month |
| Normalization call | `normalize_barroso(lf, char_cols, date_col="date")` |

All US stocks are pooled into a single cross-section per month. This follows the standard approach in the US asset pricing literature.

---

## 4. Monthly-First Architecture

The pipeline uses a **monthly-first normalization architecture**:

1. Raw annual accounting characteristics (IDs 6-48, 61) are computed at the yearly level
2. These are forward-filled to monthly frequency via Fama-French timing (FYE in calendar year $t$ applied to returns July $t+1$ through June $t+2$)
3. Raw monthly price characteristics (IDs 1-5, 49-60, 62) are computed directly from daily/monthly market data
4. **All 62 characteristics are normalized together at the monthly level**

This means that even accounting characteristics (which update annually) receive different normalized values each month, because the cross-sectional ranks change as the stock universe evolves.

---

## 5. Implementation Details

### Code References

| Function | File | Purpose |
|:---|:---|:---|
| `normalize_barroso()` | `normalization.py` | Main entry point; dispatches to optimized or standard path |
| `_apply_barroso_transform_fast()` | `normalization.py` | Optimized: single `group_by` + join for all statistics |
| `_compute_cross_sectional_stats()` | `normalization.py` | Computes p01, p99, median, and group size in one aggregation |
| `_rank_normalize()` | `normalization.py` | Non-optimized fallback (multi-pass `.over()` calls) |
| Step 5 in `process_region()` | `main.py` | Pipeline orchestration; sets `date_col` based on region |

### Optimized Implementation

The default (optimized) path uses a single `group_by` to compute all cross-sectional statistics (percentiles, medians, group sizes) in one pass, then joins them back to the main data. This replaces ~310 separate `.over()` calls with a single aggregation, significantly improving performance for large datasets.

### Output Files

| Unnormalized Output | Normalized Output |
|:---|:---|
| `final_output_unnormalized_us.parquet` | `final_output_normalized_us.parquet` |

In the normalized output, characteristic columns have a `_norm` suffix (e.g., `BEME_norm`, `Beta_norm`).

---

## 6. Edge Cases

| Scenario | Behavior |
|:---|:---|
| **All null** for a characteristic in a cross-section | Median is null; imputation has no effect; ranks are null |
| **Single non-null** value | After imputation, all stocks get the same value; all ranks are tied (average rank) |
| **$\pm\infty$ values** | Clipped to p99/p01 during winsorization (Step 1) |
| **Ties** | Handled via `rank(method="average")` — tied values share the mean of their ranks |

---

**Reference:** Barroso, P., Saxena, K., & Wang, Y. (2025). *Predicting Returns Out-of-Sample: A Naive Combination Approach.* Appendix A.1.
