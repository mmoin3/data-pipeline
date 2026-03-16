# ETL Pipeline Conventions

## Python Code Conventions

### Naming

| Element | Convention | Example |
|---|---|---|
| **Classes** | PascalCase | `DataFrameCleaner`, `INAVBskt`, `Extractor` |
| **Functions/Methods** | snake_case | `extract()`, `load_to_bronze()`, `clean_data()` |
| **Helper Methods** | snake_case with leading underscore | `_parse_dates()`, `_clean_str()` |
| **Public Attributes** | snake_case | `self.schema`, `self.file_path` |
| **Constants** | UPPER_SNAKE_CASE | `NULL_LIKE_VALUES`, `DB_PATH` |
| **Variables** | snake_case | `raw_files`, `metrics_df`, `file_path` |

### Class Structure

```python
class MyExtractor:
    """Brief description of class purpose."""
    
    def __init__(self):
        """Initialize instance attributes."""
        self._engine = create_engine(DB_CONN_STR)
        self._raw_dir = Path(RAW_DATA_DIR)
    
    def public_method(self):
        """Public API method."""
        self._helper_method()
    
    def _helper_method(self):
        """Private helper method."""
        pass
```

### Function Documentation

Every function must have a docstring:

```python
def extract(self, file_path: Path) -> pd.DataFrame:
    """Extract data from raw file.
    
    Args:
        file_path: Path to raw data file.
    
    Returns:
        DataFrame with extracted data.
    """
    pass
```

---

## Database Conventions

### Database Naming

Use **PascalCase** for database names:

```sql
OperationsDB          -- Fund operations data
HarvestStoreDB        -- Bloomberg/market data
ReportsDB             -- Reporting layer
```

### Schema Organization

```
OperationsDB (database)
  ├── bronze                         ← raw ingestion layer
  │   ├── fund_metrics
  │   ├── fund_holdings
  │   └── mft_trades
  │
  ├── silver                         ← cleaned & enriched layer
  │   ├── fund_metrics_clean
  │   ├── fund_holdings_enriched
  │   ├── ref_funds                  ← reference/lookup tables
  │   ├── ref_securities
  │   ├── ref_currencies
  │   └── ref_asset_classes
  │
  └── gold                           ← business logic & aggregations
      ├── daily_nav
      ├── fund_performance
      └── portfolio_composition
```

### Table Naming

Use **snake_case_plural** for table names:

**Transactional Tables:**
```sql
bronze.fund_metrics
bronze.fund_holdings
bronze.mft_trades
silver.fund_metrics_clean
silver.fund_holdings_enriched
gold.daily_nav
gold.fund_performance
```

**Reference Tables** (use `ref_` prefix in silver schema):
```sql
silver.ref_funds              -- Master fund list
silver.ref_securities         -- Security master (tickers, ISINs, etc)
silver.ref_currencies         -- Currency codes
silver.ref_asset_classes      -- Asset classification
silver.ref_counterparties     -- Counterparty master
```

### Column Naming

Use **snake_case_singular** for column names:

```sql
CREATE TABLE bronze.fund_metrics (
    fund_metric_id INT PRIMARY KEY,         -- Singular (fund_metric not fund_metrics)
    fund_name VARCHAR(255),
    nav_value DECIMAL(18, 2),
    as_of_date DATE,
    file_name VARCHAR(255),
    loaded_at DATETIME
);
```

### Primary Key Convention

- Primary keys **always end with `_id`**
- Format: `{table_name_singular}_id` (use singular form of table name)
- Examples:
  - `fund_metric_id` (table: `bronze.fund_metrics`)
  - `trade_id` (table: `bronze.trades`)
  - `holding_id` (table: `silver.holdings_enriched`)
  - `fund_id` (table: `silver.ref_funds`)

### Foreign Key Convention

- Foreign keys reference the primary key by name
- Examples:
  - `fund_metric_id` → references `bronze.fund_metrics(fund_metric_id)`
  - `fund_holding_id` → references `bronze.fund_holdings(fund_holding_id)`
  - `fund_id` → references `silver.ref_funds(fund_id)`

### Reference Table Structure

Reference tables in silver layer act as master/dimension tables:

```sql
CREATE TABLE silver.ref_funds (
    fund_id INT PRIMARY KEY,
    fund_code VARCHAR(10) UNIQUE NOT NULL,
    fund_name VARCHAR(255) NOT NULL,
    fund_type VARCHAR(50),
    inception_date DATE,
    is_active BIT DEFAULT 1,
    loaded_at DATETIME
);

CREATE TABLE silver.ref_securities (
    security_id INT PRIMARY KEY,
    ticker VARCHAR(20),
    isin VARCHAR(12) UNIQUE,
    security_name VARCHAR(255),
    asset_class_id INT,
    loaded_at DATETIME,
    CONSTRAINT fk_ref_securities_asset_class 
        FOREIGN KEY (asset_class_id) 
        REFERENCES silver.ref_asset_classes(asset_class_id)
);
```

### Metadata Columns

**Bronze Layer** (raw ingestion):
```sql
file_name VARCHAR(255)            -- Name of source file
loaded_at DATETIME                -- When data was loaded
```

**Silver Layer** (cleaned & enriched):
```sql
file_name VARCHAR(255)            -- Original source file
loaded_at DATETIME                -- When transformation occurred
```

**Silver Reference Tables** (optional):
```sql
loaded_at DATETIME                -- When reference was last updated
```

**Gold Layer** (optional):
```sql
loaded_at DATETIME                -- When metric was last calculated
```

### Using Reference Tables

Gold layer queries **join with silver reference tables**:

```sql
SELECT 
    g.daily_nav_id,
    f.fund_name,
    s.security_name,
    g.nav_value,
    g.loaded_at
FROM gold.daily_navs g
JOIN silver.ref_funds f ON g.fund_id = f.fund_id
JOIN silver.ref_securities s ON g.security_id = s.security_id
WHERE g.loaded_at >= DATEADD(DAY, -30, GETDATE());
```

---

## Summary Table

| Item | Convention | Example |
|---|---|---|
| Class | PascalCase | `DataFrameCleaner` |
| Function | snake_case | `extract_data()` |
| Helper method | _snake_case | `_parse_dates()` |
| Constant | UPPER_SNAKE_CASE | `NULL_LIKE_VALUES` |
| Variable | snake_case | `raw_files` |
| Database | PascalCase | `OperationsDB` |
| Transactional Table | snake_case_plural | `fund_metrics` |
| Reference Table | ref_snake_case_plural | `ref_funds` |
| Column | snake_case_singular | `fund_name` |
| Primary Key | *_id (singular) | `fund_metric_id` |
| Foreign Key | *_id (matches PK) | `fund_id` |
| Metadata (Bronze) | file_name, loaded_at | — |
| Metadata (Silver) | file_name, loaded_at | — |