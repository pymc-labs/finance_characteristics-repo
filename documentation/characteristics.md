# Characteristics and data structure

This document presents the structure of the datasets used and how the characteristics are constructed using Compustat North America (NA) and CRSP data. We also detail the intermediate variables required to compute the 62 characteristics.

# 1. Data Source Strategy

### Data Sources
| Price/Returns Source | Fundamentals Source |
| :--- | :--- |
| **CRSP** (Monthly `msf` / Daily `dsf`) | **Compustat NA** Annual (`funda`) |

### Key Considerations

1.  **Identifiers:**
    * Merges via `PERMNO` (CRSP) and `GVKEY` (Compustat) using the CCM Link Table.

2.  **Fiscal Timing:**
    * Follow standard Fama-French timing: Match Fiscal Year ending in calendar year $t-1$ with returns starting in July of year $t$.

---

# 2. Data Dictionary: Required Input Variables

**Objective:** To calculate the 62 characteristics from Freyberger, Neuhierl, & Weber (2020), three types of requirements must be computed:

* Market Data (CRSP)
* Fundamental Data (Compustat Annual)
* Intermediate Calculated Variables

To keep track of each one of the 62 characteristics, and the requirements to compute them, we use an ID system:

**ID Structure:**
* `0.0xx`: Market Data (CRSP)
* `0.1xx`: Fundamental Data (Compustat Annual)
* `0.3xx`: Intermediate Calculated Variables
* `xx` : Characteristics

Now we present in detail each one of the variables required for the characteristics computation:


## 2.1 Market / Security Data (Daily & Monthly Inputs)

| ID | Variable Code | Description | CRSP Code | Used in ID |
| :--- | :--- | :--- | :--- | :--- |
| **0.001** | **price** | Daily Closing Price | `prc` | 0.301, 56 |
| **0.002** | **shares** | Shares Outstanding | `csho` | 0.301, 9, 55 |
| **0.003** | **return** | Total Return | `ret` | 1, 2, 3, 4, 5, 50, 52, 57, 62 |
| **0.004** | **volume** | Share Volume | `vol` | 51, 55, 59, 60, 61 |
| **0.005** | **bid_ask** | Ask & Bid Prices | `ask`, `bid` | 58 |
| **0.006** | **high_low** | High & Low Prices | `askhi`, `bidlo` | 58 |

## 2.2 Annual Fundamentals (Raw Inputs)

| ID | Variable Code | Description | Compustat Table | Used in ID |
| :--- | :--- | :--- | :--- | :--- |
| **0.101** | **act** | Current Assets - Total | `funda` | 0.304 |
| **0.102** | **at** | Assets - Total | `funda` | 0.303, 6, 8, 10, 13, 22, 23, 27, 28, 30, 31, 33, 36, 45, 48 |
| **0.103** | **capx** | Capital Expenditures | `funda` | 41 |
| **0.104** | **ceq** | Common/Ordinary Equity | `funda` | 0.302, 0.303, 7 |
| **0.105** | **che** | Cash & Short-Term Inv. | `funda` | 0.303, 0.304, 23, 25, 26, 36 |
| **0.106** | **cogs** | Cost of Goods Sold | `funda` | 0.305, 30 |
| **0.107** | **dlc** | Debt in Current Liab. | `funda` | 0.303, 0.304, 18, 23, 39 |
| **0.108** | **dltt** | Long-Term Debt - Total | `funda` | 0.303, 18, 23, 39 |
| **0.109** | **dp** | Depreciation & Amort. | `funda` | 0.304, 37, 41 |
| **0.110** | **dvt** | Dividends - Total | `funda` | 42, 43 |
| **0.111** | **ebit** | Earnings Before Int/Tax | `funda` | 25 |
| **0.112** | **eps** | Earnings Per Share | `epspx` | 15 |
| **0.113** | **ib** | Income Before Extra | `funda` | 22, 24, 37, 40 |
| **0.114** | **invt** | Inventories - Total | `funda` | 8, 10 |
| **0.115** | **lct** | Current Liab. - Total | `funda` | 0.304 |
| **0.116** | **lt** | Liabilities - Total | `funda` | 25, 37, 39 |
| **0.117** | **ni** | Net Income (Loss) | `funda` | 41, 44 |
| **0.118** | **oiadp** | Op. Income After Dep. | `funda` | 18, 19, 21 |
| **0.119** | **pi** | Pretax Income | `funda` | 16 |
| **0.120** | **ppent** | PP&E - Net | `funda` | 8, 31 |
| **0.121** | **prc_f** | Price Close (Fiscal Year) | `prcc_f` | 53 |
| **0.122** | **prstkc** | Purch. Common/Pref. | `funda` | 0.302, 43 |
| **0.123** | **sale** | Sales / Turnover (Net) | `funda` | 0.305, 12, 13, 16, 18, 19, 26, 27, 28, 46, 47 |
| **0.124** | **txp** | Income Taxes Payable | `funda` | 0.304 |
| **0.125** | **wcapch** | Work. Cap Changes | `funda` | 41 |
| **0.126** | **xsga** | SG&A Expenses | `funda` | 30 |

## 2.3 Intermediate Calculated Values

These variables are derived from the raw inputs (`0.0xx` and `0.1xx`) and serve as inputs for the final 62 characteristics.

| ID | Variable | Formula | Used in ID |
| :--- | :--- | :--- | :--- |
| **0.301** | **ME** (Market Equity) | `price * shares` | 33, 34, 35, 39, 40, 43, 44, 45, 46, 53, 54 |
| **0.302** | **BE** (Book Equity) | `ceq` (fallback: `at - lt - pstk`) | 20, 24, 34, 35, 41 |
| **0.303** | **NOA** (Net Op Assets) | `(at - che) - (at - dlc - dltt - ceq)` | 11, 12, 21 |
| **0.304** | **OpAcc** (Op Accruals) | `(Δact - Δche) - (Δlct - Δdlc - Δtxp) - dp` | 29, 32 |
| **0.305** | **GP** (Gross Profit) | `sale - cogs` | 14, 17, 20 |

---

# 3. Master Characteristic Equivalence Table (Freyberger et al., 2020)

**Source:** Freyberger, Neuhierl, & Weber (2020), Table 1.
**Note:** `_adj` variables denote the base characteristic minus the industry mean (Fama-French 48).

| Category | ID | Characteristic | Definition (Source) | Compustat NA (US) Inputs |
| :--- | :--- | :--- | :--- | :--- |
| **Past Returns** | 1 | **r2_1** | Return 1 month before prediction | `ret` (t-1) |
| **Past Returns** | 2 | **r6_2** | Return 6 to 2 months before prediction | `ret` (t-6 to t-2) |
| **Past Returns** | 3 | **r12_2** | Return 12 to 2 months before prediction | `ret` (t-12 to t-2) |
| **Past Returns** | 4 | **r12_7** | Return 12 to 7 months before prediction | `ret` (t-12 to t-7) |
| **Past Returns** | 5 | **r36_13** | Return 36 to 13 months before prediction | `ret` (t-36 to t-13) |
| **Investment** | 6 | **Investment** | % change in AT | $\Delta at / at_{t-1}$ |
| **Investment** | 7 | **ACEQ** | % change in Book Equity | $\Delta ceq / ceq_{t-1}$ |
| **Investment** | 8 | **DPI2A** | Change in PP&E and inventory / lagged AT | $(\Delta ppent + \Delta invt) / at_{t-1}$ |
| **Investment** | 9 | **AShrout** | % change in shares outstanding | $\Delta csho / csho_{t-1}$ |
| **Investment** | 10 | **IVC** | Change in inventory / average AT | $\Delta invt / \text{avg}(at)$ |
| **Investment** | 11 | **NOA** | Net-operating assets / lagged AT | `NOA / at`$_{t-1}$ |
| **Profit** | 12 | **ATO** | Sales to lagged net operating assets | `sale / NOA`$_{t-1}$ |
| **Profit** | 13 | **CTO** | Sales to lagged total assets | `sale / at`$_{t-1}$ |
| **Profit** | 14 | **dGM_dSales**| $\Delta$ Gross Margin - $\Delta$ Sales | $\Delta(GP/sale) - \Delta sale$ |
| **Profit** | 15 | **EPS** | Earnings per share | `eps` |
| **Profit** | 16 | **IPM** | Pretax income over sales | `pi / sale` |
| **Profit** | 17 | **PCM** | Sales minus COGS to sales | `GP / sale` |
| **Profit** | 18 | **PM** | Op. inc after dep. over sales | `oiadp / sale` |
| **Profit** | 19 | **PM_adj** | PM - Industry Mean PM | `oiadp / sale` |
| **Profit** | 20 | **Prof** | Gross profitability over BE | `GP / BE` |
| **Profit** | 21 | **RNA** | Op. inc after dep. to lagged NOA | `oiadp / NOA`$_{t-1}$ |
| **Profit** | 22 | **ROA** | Inc before extra to lagged AT | `ib / at`$_{t-1}$ |
| **Profit** | 23 | **ROC** | (Size + LT Debt - AT) to cash | `(me + dltt - at) / che` |
| **Profit** | 24 | **ROE** | Inc before extra to lagged BE | `ib / BE`$_{t-1}$ |
| **Profit** | 25 | **ROIC** | Return on invested capital | `ebit / (ceq + lt - che)` |
| **Profit** | 26 | **S2C** | Sales to cash | `sale / che` |
| **Profit** | 27 | **SAT** | Sales to total assets | `sale / at` |
| **Profit** | 28 | **SAT_adj** | SAT - Industry Mean SAT | `sale / at` |
| **Intang** | 29 | **AOA** | Abs value of operating accruals | `abs(OpAcc)` |
| **Intang** | 30 | **OL** | COGS + SG&A to total assets | `(cogs + xsga) / at` |
| **Intang** | 31 | **Tan** | Tangibility (PP&E / AT) | `ppent / at` |
| **Intang** | 32 | **OA** | Operating accruals | `OpAcc` |
| **Value** | 33 | **A2ME** | Total assets to Size | `at / ME` |
| **Value** | 34 | **BEME** | Book to market ratio | `BE / ME` |
| **Value** | 35 | **BEMEadj** | BEME - Industry Mean BEME | `BE / ME` |
| **Value** | 36 | **C** | Cash to AT | `che / at` |
| **Value** | 37 | **C2D** | Cash flow to total liabilities | `(ib + dp) / lt` |
| **Value** | 38 | **ASO** | Log chg in split-adj shares | $\Delta \ln(csho \times ajex)$ |
| **Value** | 39 | **Debt2P** | Total debt to Size | `(dltt+dlc) / ME` |
| **Value** | 40 | **E2P** | Inc before extra to Size | `ib / ME` |
| **Value** | 41 | **Free_CF** | Free cash flow to BE | `(ni+dp-wcapch-capx)/BE` |
| **Value** | 42 | **LDP** | Trail 12-m dividends to price | `dvt / ME` |
| **Value** | 43 | **NOP** | Net payouts to Size | `(dvt + prstkc) / ME` |
| **Value** | 44 | **O2P** | Operating payouts to market cap | `(ni - delta_be) / ME` |
| **Value** | 45 | **Q** | Tobin's Q | `(at + ME - ceq) / at` |
| **Value** | 46 | **S2P** | Sales to price | `sale / ME` |
| **Value** | 47 | **Sales_g** | Sales growth | $\Delta sale / sale_{t-1}$ |
| **Trading** | 48 | **AT** | Total Assets | `at` |
| **Trading** | 49 | **Beta_Cor**| Correlation ratio of vols | `Beta * (VolMkt/VolStock)` |
| **Trading** | 50 | **Beta** | CAPM beta using daily returns | Daily Rolling Reg |
| **Trading** | 51 | **DTO** | De-trended Turnover | `Turnover - Trend` |
| **Trading** | 52 | **Idio_vol**| Idio vol of FF3 factor model | Residual StdDev |
| **Trading** | 53 | **LME** | Price times shares outstanding | `ME` |
| **Trading** | 54 | **LME_adj** | Size - Industry Mean Size | `ME` |
| **Trading** | 55 | **LTurnover**| Last month's vol to shares | `vol / csho` |
| **Trading** | 56 | **Rel2High**| Price to 52 week high price | `prc / high52` |
| **Trading** | 57 | **Ret_max** | Maximum daily return | Max(Daily Ret) |
| **Trading** | 58 | **Spread** | Average daily bid-ask spread | `(Ask-Bid)/Mid` |
| **Trading** | 59 | **Std_Turn**| Std dev of daily turnover | Std(Vol/Shares) |
| **Trading** | 60 | **Std_Vol** | Std dev of daily volume | Std(Vol) |
| **Trading** | 61 | **SUV** | Standard unexplained volume | Model Residuals |
| **Trading** | 62 | **Total_vol**| Std dev of daily returns | Std(Daily Ret) |

---

# 4. Notes

## Beta computation

Beta (ID 50) and related characteristics (e.g., Beta_Cor, Idio_vol) are computed by regressing stock excess returns against the US market factor (Fama-French Mkt-RF), with excess returns calculated by subtracting the US risk-free rate (FF RF).
