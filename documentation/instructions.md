# Step-by-Step Instructions

This guide walks you through the complete process of generating the 62 firm characteristics.

## Step 1 Prerequisites

### 1.1 Install Pixi

Pixi is a fast, cross-platform package manager built on conda-forge.

**Windows (PowerShell):**
```powershell
iwr -useb https://pixi.sh/install.ps1 | iex
```

**Windows terminal**

```bash
powershell -ExecutionPolicy ByPass -c "irm https://pixi.sh/install.ps1 | iex"
```

**macOS/Linux:**
```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

Verify installation:
```bash
pixi --version
```

### 1.2 Set Up the Environment
To set up the environment, run in terminal:

```bash
pixi install
```
This will create the environment `data_collection`. With the packages required to download the data from Wharton's Research Data Set (WRDS), and process it:

- Python ≥3.11
- Polars ≥1.0
- PyArrow ≥14.0
- ConnectorX (for fast WRDS queries)
- Click (CLI)
- And development tools

 To activate the environment just do:

```bash
pixi shell
```

**Step 2: Create a `.env` file** (in the `characteristics_repo` folder)

Open your text editor and create a new file called `.env` (check the example file `.env.example` for an example). This file must contain your WRDS user name, and the paths where you want to store the data inputs and outputs:

```env
# Your WRDS username
WRDS_USER=your_actual_wrds_username

# Where to save downloaded data (relative to project folder)
DATA_DIR=./data/inputs

# Where to save output characteristics (relative to project folder)
OUTPUT_DIR=./data/outputs
```

**Note** that you shouldn't define the variables as strings.

> **Important**: Replace `your_actual_wrds_username` with your real WRDS username!

**Alternative: Set environment variables in Terminal**

If you don't want to create a `.env` file, you can set variables directly in your terminal session:


## Step 3: Prepare Your Data

To run the pipeline, you will need the CRSP and Compustat North America data sets from WRDS using the automated download script.

### Use WRDS Library (Automated Download)

We've provided a ready-to-use script: `download_data.py` To use it, you will need to create the `.env` file of step 3.


**Step 3.1: Access your WRDS account**

You must go to WRDS [webpage](https://wrds-www.wharton.upenn.edu/) and connect to your account.

**Step 3.2: Run the download script** (in Terminal)

Open a terminal (PowerShell in Windows) and run:

```powershell
# Navigate to the project folder (characteristics_repo directory)
cd path/to/characteristics_repo

# Activate the pixi environment
pixi shell

# Run the download script (uses default date range)
python download_data.py

# Or specify custom date range
python download_data.py --start-date 1987-01-01 --end-date 2024-12-31

# Force re-download (ignore existing files)
python download_data.py --force
```

**Command-line options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--start-date` | `1987-01-01` | Start date in YYYY-MM-DD format |
| `--end-date` | Dynamic* | End date in YYYY-MM-DD format |
| `--force` | False | Force re-download even if files exist |
| `--skip-factors` | False | Skip downloading Fama-French factors from Ken French's website |
| `--max-workers` | `5` | Maximum parallel download workers (recommended: 5-8) |
| `--no-parallel` | False | Disable parallel downloading (use sequential mode) |

*Default end date is December 31st of the last complete year. If running before March, it uses year-2 (since annual data may not yet be published); from March onwards, it uses year-1.

**Parallel Downloading (for large daily data files):**

For date ranges spanning multiple years, the download script automatically uses **parallel downloading** for CRSP Daily data. This:
- Splits the request into yearly chunks
- Downloads chunks simultaneously using multiple WRDS connections
- Significantly speeds up downloads and prevents timeouts

> **Important:** Parallel downloading requires `.pgpass` to be configured (see Step 3.3 below) since worker processes cannot prompt for passwords interactively.

**Step 3.3: Create .pgpass file (REQUIRED for parallel downloads)**

On the first run, the WRDS library will prompt you to create a `.pgpass` file to store your credentials:

```
Enter your WRDS username [your_username]:
Enter your WRDS password:
Create .pgpass file now [y/n]?: y
```

Type `y` and press Enter. This creates a file at:
- **Windows:** `C:\Users\<your_username>\.pgpass`
- **macOS/Linux:** `~/.pgpass`

The file stores your WRDS credentials securely so you won't need to enter your password on subsequent runs.

> **Important:** The `.pgpass` file is **required** for parallel downloading. When downloading large daily data files (CRSP Daily), the script spawns multiple worker processes that each need to authenticate with WRDS. Without `.pgpass`, these workers cannot prompt for your password and will fail.

> **Note:** If you need to update your password later, edit the `.pgpass` file directly or delete it and re-run the script.

The script will:
1. Connect to WRDS (prompts for password on first run)
2. Download all US data (CRSP monthly, daily, Compustat, CCM link)
3. Download Fama-French factors from Kenneth French's website (FF3 daily, FF48 industries)
4. Save everything as parquet files in your DATA_DIR
5. Skip files that already exist (use `--force` to re-download)

**Expected output:**
```
============================================================
WRDS Data Downloader for Freyberger 62 Characteristics
============================================================

Data will be saved to: ./data/inputs
Date range: 1987-01-01 to 2023-12-31

Connecting to WRDS as your_username...
  Connection successful!

========================================
Downloading US Data
========================================
Downloading CRSP Monthly Stock File...
  Saved 4,521,234 rows to ./data/inputs/us/crsp_monthly.parquet
...
```

**Alternative: Set environment variables in Terminal**

If you don't want to create a `.env` file, you can set variables directly in your terminal session:

**Windows PowerShell:**
```powershell
$env:DATA_DIR = "./data/inputs"
$env:OUTPUT_DIR = "./data/outputs"
```

**Bash (macOS/Linux):**
```bash
export DATA_DIR="./data/inputs"
export OUTPUT_DIR="./data/outputs"
```

> **Note**: Environment variables set in terminal only last for that session. The `.env` file is persistent.

---

## Step 4: Validate Data Paths

#### Where to do this: **Terminal**

Before running the main pipeline, verify all required files exist:

```powershell
# Make sure you're in the project directory (characteristics_repo folder)
cd path/to/characteristics_repo

# Activate pixi environment (if not already active)
pixi shell

# Validate paths
python main.py --validate-only
```

**Expected output if everything is set up correctly:**
```
Freyberger 62 Characteristics Builder
=====================================

All required files found. Validation passed.
```

**If files are missing, you'll see:**
```
Missing data files:
  - ./data/inputs/us/crsp_monthly.parquet
  - ./data/inputs/factors/ff_factors_daily.parquet

Validation failed.
```

## Step 5: Run the Pipeline

**Command-line options for `main.py`:**

| Option | Default | Description |
|--------|---------|-------------|
| `--no-normalize` | False | Skip Barroso et al. (2025) normalization |
| `--fundamentals-only` | False | Output only yearly fundamentals (skip price merge) |
| `--data-dir` | From `.env` | Override input data directory |
| `--output-dir` | From `.env` | Override output directory |
| `--quiet` | False | Suppress progress messages |
| `--validate-only` | False | Only validate paths, don't process |

### Process US Data

```bash
pixi run us
```

Or with full options:
```bash
pixi shell
python main.py
```

### Skip Normalization

If you only want raw characteristics:
```bash
python main.py --no-normalize
```

### Custom Directories

```bash
python main.py --data-dir ./mydata --output-dir ./results
```

### Fundamentals Only (Fast Mode)

If you only need yearly fundamental characteristics without the expensive price merge:

```bash
python main.py --fundamentals-only
```

This is **much faster** because it:
- Skips loading daily price data
- Skips computing price-based characteristics (beta, volatility, momentum)
- Skips the price-fundamental merge
- Outputs yearly data instead of monthly

**Characteristics included in fundamentals-only mode:**
- Investment (IDs 6-11): Investment, ACEQ, DPI2A, AShrout, IVC, NOA
- Profitability (IDs 12-28): ATO, CTO, PM, ROA, ROE, etc.
- Intangibles (IDs 29-32): AOA, OL, Tan, OA

**NOT included** (require price data):
- Past Returns (IDs 1-5)
- Value (IDs 33-47) - require Market Equity from prices
- Trading (IDs 48-62) - require daily price data

## Step 6: Check Output

After processing, you'll have:

```
output/
├── characteristics_raw_us.parquet           # Full monthly characteristics (US)
├── characteristics_normalized_us.parquet    # Normalized monthly (US)
└── characteristics_fundamentals_us.parquet  # Yearly fundamentals only (US)
```

> **Note:** The `characteristics_fundamentals_us.parquet` file is only created when using the `--fundamentals-only` flag.


## Troubleshooting

### Memory Issues
The data sets processed are heavy, and memory issues may appear.

1. Process in chunks by year
2. Use `sink_parquet()` instead of `collect().write_parquet()`
3. Reduce the date range.


## Support

For issues or questions:
1. Check the README for common solutions
2. Review the config.py for variable definitions
3. Examine the construction/ modules for formula details
