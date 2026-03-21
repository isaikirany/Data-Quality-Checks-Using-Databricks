# DataGuard 🛡️
### Config-Driven Data Quality Framework on Azure Databricks

> Validate any structured dataset through a single reusable pipeline — no code changes between datasets. Drop in a JSON config and go.

---

## Overview

DataGuard is a plug-and-play data quality framework built on **PySpark** and **Azure Databricks**. It ingests raw data, runs a configurable chain of validation checks, separates clean records from bad ones, and writes a full audit trail — all driven by a JSON config file.

Onboarding a new dataset means writing a new config file. Not touching the notebook.

---

## Architecture

```
ADLS Gen2 (Landing Zone)
        │
        ▼
┌──────────────────────────────────────────┐
│           DataGuard Pipeline             │
│                                          │
│  JSON Config ──► Parameter Extraction    │
│                        │                 │
│              ┌─────────▼──────────┐      │
│              │  Source Data Read  │      │
│              └─────────┬──────────┘      │
│                        │                 │
│              ┌─────────▼──────────┐      │
│              │  Duplicate Check   │      │
│              └─────────┬──────────┘      │
│                        │                 │
│              ┌─────────▼──────────┐      │
│              │    Null Check      │      │
│              └─────────┬──────────┘      │
│                        │                 │
│              ┌─────────▼──────────┐      │
│              │  Negative Check    │      │
│              └─────────┬──────────┘      │
│                        │                 │
│              ┌─────────▼──────────┐      │
│              │ Date Format Check  │      │
│              └─────────┬──────────┘      │
│                        │                 │
│              ┌─────────▼──────────┐      │
│              │  Cast + Select     │      │
│              └──────┬──────┬──────┘      │
└─────────────────────┼──────┼─────────────┘
                      │      │
          ┌───────────▼─┐  ┌─▼────────────┐
          │ Clean Data  │  │  Rejected +  │
          │  (Parquet)  │  │  Audit (CSV) │
          └─────────────┘  └──────────────┘
```

---

## Features

| Feature | Detail |
|---|---|
| **Config-driven** | All validation rules defined in a JSON file — zero code changes per dataset |
| **Multi-format support** | CSV, JSON, Parquet, Delta Lake |
| **Sequential validation** | Records exit the pipeline at first failure — no duplicate reject entries |
| **Secrets management** | OAuth credentials stored in Databricks Secret Scopes, never hardcoded |
| **Conditional checks** | Each check can be enabled/disabled per dataset via config flags |
| **Append-mode audit log** | Full timestamped run history — never overwrites prior records |
| **Parameterized execution** | Widget-driven `config_filepath` and `processed_date` — ready for ADF/orchestration |

---

## Validation Checks

**1. Duplicate Check**  
Partitions across all columns. A record is only flagged if every field is identical. Controlled by the `duplicate_check` config flag.

**2. Null Check**  
Flags records where any column in `null_check` list is null. One reject tag per record — no double-counting.

**3. Negative Value Check**  
Flags records where any column in `no_negative_value` list contains a value less than zero.

**4. Date Format Check**  
Uses `to_date()` to validate parsability. Records with unparseable dates are rejected with the column name and expected format in the reason string.

**5. Schema Cast**  
Applies column type casting after all rejection checks. Invalid casts are skipped with a warning, not a crash.

---

## Config File Structure

Each dataset gets its own JSON config file stored in ADLS. Example for an `orders` dataset:

```json
{
  "sourcefile":       "/mnt/global/india/landing/orders/",
  "targetfile":       "/mnt/global/india/silver/orders/",
  "pendingfile":      "/mnt/global/india/reject/orders/",
  "auditfile":        "/mnt/global/india/auditdata/orders/",
  "source_format":    "csv",
  "duplicate_check":  "yes",
  "required_cols":    ["orderid", "ordername", "orderdate", "orderprice", "orderaddress"],
  "null_check":       ["ordername", "email"],
  "no_negative_value":["orderprice"],
  "dateformatchecks": ["orderdate"],
  "cols_datatype": [
    {
      "orderid":    "int",
      "orderprice": "int",
      "orderdate":  "date"
    }
  ]
}
```

To onboard a new dataset, create a new config file. The notebook stays untouched.

---

## Project Structure

```
DataGuard/
│
├── notebooks/
│   └── Data_Quality_Framework.ipynb   # Main Databricks notebook
│
├── configs/
│   ├── orders.json                    # Sample config — orders dataset
│   └── customers.json                 # Sample config — customers dataset
│
└── README.md
```

---

## Setup

### Prerequisites
- Azure Databricks workspace
- Azure Data Lake Storage Gen2
- Databricks Secret Scope configured with the following keys:

| Secret Key | Description |
|---|---|
| `client-id` | Azure AD App (Service Principal) client ID |
| `client-secret` | Service Principal client secret |
| `tenant-id` | Azure AD tenant ID |

### Create a Databricks Secret Scope
```bash
databricks secrets create-scope --scope adls-secret-scope
databricks secrets put --scope adls-secret-scope --key client-id
databricks secrets put --scope adls-secret-scope --key client-secret
databricks secrets put --scope adls-secret-scope --key tenant-id
```

### Run the Notebook
1. Import `Data_Quality_Framework.ipynb` into your Databricks workspace
2. Upload your dataset config JSON to ADLS
3. Set the widgets:
   - `config_filepath` → path to your JSON config in ADLS
   - `processed_date` → partition date, e.g. `2024/08/10`
4. Run all cells

---

## Output

| Output | Format | Location | Mode |
|---|---|---|---|
| Clean records | Parquet | `targetfile` path + date | Overwrite |
| Rejected records | CSV with `reject_reason` column | `pendingfile` path + date | Overwrite |
| Audit log | CSV | `auditfile` path + date | **Append** |

### Audit Log Schema

```
table_name | source_count | reject_count | written_count | processed_date | load_timestamp | config_filepath | source_format
```

---

## Tech Stack

- **Azure Databricks** — Compute & notebook execution
- **Apache PySpark** — Distributed data processing
- **Azure Data Lake Storage Gen2** — Source and target storage
- **Azure Active Directory** — OAuth 2.0 service principal auth
- **Databricks Secrets** — Credential management
- **Python 3** — Pipeline logic and config parsing

---

## Why DataGuard

Most data quality scripts are dataset-specific — you copy, paste, and modify them for every new source. DataGuard treats the validation logic as infrastructure and the dataset rules as configuration. The result is one notebook that handles every dataset, a clear separation between logic and rules, and an audit trail that actually persists.

---

## Author

**[Your Name]**  
[LinkedIn](https://linkedin.com/in/yourprofile) · [Portfolio](https://yourwebsite.com)
