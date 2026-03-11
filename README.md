# FundOperations ETL Pipeline

Extract, Transform, Load (ETL). This pipeline collects data from various sources and inserts them into Harvest's FundOperations Database.
The Database enforces strict data type casting, and validates entries before insertion.

---
## Data Architecture

The data architecture for this project follows the medallion architecture framework: **Bronze**, **Silver**, **Gold** layers:
![Data Architecture](docs/DB_Architecture_FundOperations-pipeline%20rules.drawio.png)

1. **Bronze Layer**: Tabularizes source data with minimal transformations. Meant to show data as came from source. Holds full history.
2. **Silver Layer**: Takes data from bronze layer, cleans, transforms, and validates the entries and updates. Serves as the source of truth.
3. **Gold Layer**: Serves as a highly aggregated view of Silver layer. Different tables serve different reporting needs. Data for analysis only.

---
## Prerequisites
- Python 3.10+
- 8GB RAM recommended
- Basic git commands knowledge

## Installation

### Clone and Setup
```bash
# Clone repo
git clone https://github.com/mmoin3/ETL-pipeline.git

# Set up virtual environment
python -m venv myvenv
```

### Activate Virtual Environment

<details>
<summary><b>macOS/Linux</b></summary>

```bash
source myvenv/bin/activate
```
</details>

<details>
<summary><b>Windows PowerShell</b></summary>

```powershell
& .\myvenv\Scripts\Activate.ps1
```
</details>

### Install Dependencies

```bash
pip install -e .
```

### Environment Setup

```bash
# Set up environment (create .env with your database credentials)
# cp .env.example .env  # if .env.example exists
```

### Optional: Bloomberg API Setup

> **Note:** `blpapi` (Bloomberg API) must be installed separately as it's proprietary software and not available via pip. This is required only to use Bloomberg Tools.

```powershell
python -m pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple/ blpapi
```

This is required to use the `BloombergClient` for data extraction via BDH and BDP functions.

## Bloomberg Tools (Simple)

```python
from src.services.bloomberg_client import BloombergClient

with BloombergClient() as client:
    hist = client.BDH(["XIU CN Equity"], ["PX_LAST"], "20260101", "20260220")
    snap = client.BDP(["XIU CN Equity"], ["PX_LAST", "VOLUME"])
```

## Running Tests & Imports (Windows)

When tests are run from subfolders, Python may fail to resolve `src` imports.
Ensure you've installed the project with `pip install -e .` and run tests from the project root.

```powershell
python -m unittest discover -s tests -p "test_clean.py" -v
```

```powershell
pytest
```

Notes:
- Avoid running `cd tests` then `python test_clean.py`.
- Keep test imports as `from src.dataframe_cleaner import DataFrameCleaner`.