# ETL Pipeline Conventions

## Python Code Conventions

### Naming

| Element | Convention | Example(s) |
|---|---|---|
| **Project Name** | kebab-case usually singular| `myproject`,`data-pipeline` |
| **Folder Names** | snake_case usually plural| `parsers`, `external_services` |
| **File Names** | snake_case usually singular | `dataframe_cleaner.py`, `statestreet_mft_client.py` |
| **Classes** | PascalCase based on file name. Limit one class to one file | `DataframeCleaner`, `StatestreetMFTClient` |
| **Functions or Class Methods** | snake_case with leading underscore for private methods| `my_func()`, `self.public_method()`,`self._private_method()`|
| **Variables or Class Attributes** | snake_case with leading underscore for private attributes| `my_var`, `self.public_attribute`,`self._private_attribute` |
| **Constants** | UPPER_SNAKE_CASE | `MY_CONSTANT` |

### Class Structure

```python
class MyExtractor:
    """Brief description of class purpose."""
    
    def __init__(self):
        """Initialize instance attributes."""
        self.file_path = Path(RAW_DATA_DIR)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def public_method(self) -> pd.DataFrame:
        """Public API method."""
        return self._helper_method()
    
    def _helper_method(self):
        """Private helper method."""
        pass
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit."""
        return False
```

### Function Documentation

Every function must have a docstring with Args (no Returns):

```python
def extract(self, file_path: Path) -> pd.DataFrame:
    """Extract data from raw file.
    
    Args:
        file_path: Path to raw data file.
    """
    pass
```

---

## Data Architecture

### Bronze Layer (Landing Zone)

Bronze is a **file storage layer** with no schema enforcement. Files are stored locally (currently) with plans to migrate to Azure Data Lake.

```
bronze/
  ├── raw/            ← New files land here
  ├── processed/      ← Successfully validated files
  └── quarantined/    ← Files that failed validation
```

### Silver Layer (Transformation & Curation)

Silver is where all **data transformation, validation, and schema enforcement** occurs.

**Responsibilities:**
- Type enforcement and validation
- Deduplication and aggregation
- Data quality checks
- Reference key linking
- Audit metadata

```sql
OperationsDB
  └── silver
      ├── fund_metrics
      ├── fund_holdings
      ├── mft_trades
      ├── ref_funds              ← Reference/lookup tables
      ├── ref_securities
      ├── ref_currencies
      └── ref_asset_classes
```

### Gold Layer (Business Logic)

Gold contains **aggregated, business-ready metrics** for reporting and analytics.

```sql
OperationsDB
  └── gold
      ├── daily_nav
      ├── fund_performance
      └── portfolio_composition
```

---

## Database Conventions

### Database Naming

Use **PascalCase** for database names:

```sql
OperationsDB          -- Fund operations data
```

### Table Naming

Use **snake_case_plural** for table names:

**Transactional Tables:**
```sql
silver.fund_metrics
silver.fund_holdings
silver.mft_trades
gold.daily_nav
```

**Reference Tables** (use `ref_` prefix):
```sql
silver.ref_funds
silver.ref_securities
silver.ref_currencies
silver.ref_asset_classes
```

### Column Naming

Use **snake_case_singular** for column names:

```sql
CREATE TABLE silver.fund_metrics (
    fund_metric_id INT PRIMARY KEY,
    fund_name VARCHAR(255) NOT NULL,
    net_asset_value DECIMAL(18, 2),
    as_of_date DATE,
    share_count INT,
    _sourced_from VARCHAR(255),
    _loaded_at DATETIME,
    _updated_at DATETIME
);
```

### Primary Key Convention

- Format: `{table_name_singular}_id`
- Examples: `fund_metric_id`, `trade_id`, `holding_id`, `fund_id`

### Foreign Key Convention

- Foreign keys match their primary key names
- Example: `fund_id` → references `silver.ref_funds(fund_id)`

### Metadata Columns

Metadata columns use a **leading underscore** to denote system/audit columns:

```sql
_sourced_from VARCHAR(255)    -- Original source file
_loaded_at DATETIME           -- When data entered transformation
_updated_at DATETIME          -- When record was last modified
```

---

## Summary Table

| Item | Convention | Example |
|---|---|---|
| Class | PascalCase | `StateStreetMFTClient` |
| Public Method | snake_case | `download()` |
| Private Method | _snake_case | `_login()` |
| Constant | UPPER_SNAKE_CASE | `RAW_DATA_DIR` |
| Database | PascalCase | `OperationsDB` |
| Table | snake_case_plural | `fund_metrics` |
| Reference Table | ref_snake_case_plural | `ref_funds` |
| Column | snake_case_singular | `share_count` |
| Primary Key | *_id | `fund_metric_id` |
| Metadata | _snake_case | `_sourced_from` |