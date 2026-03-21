# Databricks notebook source
# =============================================================================
# PROJECT      : Plug-and-Play Data Quality Framework
# DESCRIPTION  : Config-driven validation pipeline for any structured dataset.
#                Supports CSV, JSON, Parquet, and Delta Lake sources.
#                Performs duplicate, null, negative value, date format, and
#                schema type checks. Outputs clean, rejected, and audit data.
# AUTHOR       : <Your Name>
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Mount ADLS Gen2 Storage
# MAGIC Credentials are loaded from Databricks Secrets — never hardcoded.

# COMMAND ----------

def mount_storage(container: str, storage_account: str, mount_point: str, scope: str):
    """
    Mounts an ADLS Gen2 container using OAuth credentials stored in Databricks Secrets.

    Args:
        container      : ADLS container name
        storage_account: Storage account name
        mount_point    : Databricks mount path (e.g. /mnt/mydata)
        scope          : Databricks secret scope name
    """
    already_mounted = any(m.mountPoint == mount_point for m in dbutils.fs.mounts())
    if already_mounted:
        print(f"[INFO] Already mounted: {mount_point}")
        return

    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type":
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id":
            dbutils.secrets.get(scope=scope, key="client-id"),
        "fs.azure.account.oauth2.client.secret":
            dbutils.secrets.get(scope=scope, key="client-secret"),
        "fs.azure.account.oauth2.client.endpoint":
            f"https://login.microsoftonline.com/"
            f"{dbutils.secrets.get(scope=scope, key='tenant-id')}/oauth2/token",
    }

    dbutils.fs.mount(
        source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs,
    )
    print(f"[INFO] Successfully mounted: {mount_point}")


# Example call — update scope/container/account to match your environment
mount_storage(
    container="mnt",
    storage_account="dataqualitytrg",
    mount_point="/mnt/global",
    scope="adls-secret-scope",       # <-- your Databricks secret scope
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Widget Parameters

# COMMAND ----------

dbutils.widgets.text("config_filepath", "", "Config File Path")
dbutils.widgets.text("processed_date", "", "Processed Date (YYYY/MM/DD)")

config_filepath  = dbutils.widgets.get("config_filepath")
processed_date   = dbutils.widgets.get("processed_date")

if not config_filepath:
    raise ValueError("[ERROR] config_filepath widget is empty. Provide a valid path.")
if not processed_date:
    raise ValueError("[ERROR] processed_date widget is empty. Provide a date string.")

print(f"Config  : {config_filepath}")
print(f"Date    : {processed_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load & Parse Config

# COMMAND ----------

def load_config(path: str) -> dict:
    """
    Reads a JSON config file from ADLS and returns a flat Python dict.
    Validates that all required keys exist before proceeding.
    """
    required_keys = [
        "sourcefile", "targetfile", "pendingfile", "auditfile",
        "source_format", "duplicate_check", "required_cols",
        "null_check", "no_negative_value", "dateformatchecks", "cols_datatype"
    ]
    try:
        df = (spark.read.format("json")
                   .option("multiLine", True)
                   .load(path))
        params = df.rdd.map(lambda x: x.asDict()).first()
    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to read config file at '{path}': {e}")

    missing = [k for k in required_keys if k not in params]
    if missing:
        raise KeyError(f"[ERROR] Missing config keys: {missing}")

    # Flatten nested cols_datatype (list of Row -> single dict)
    if params.get("cols_datatype"):
        params["cols_datatype"] = {
            k: v
            for d in params["cols_datatype"]
            for k, v in (d.asDict() if hasattr(d, "asDict") else d).items()
        }
    else:
        params["cols_datatype"] = {}

    return params


params = load_config(config_filepath)
print("[INFO] Config loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resolve Paths

# COMMAND ----------

def resolve_path(base: str, date: str) -> str:
    """Appends date partition to a base path. Handles trailing slashes."""
    return base.rstrip("/") + "/" + date.strip("/")


source_path  = resolve_path(params["sourcefile"],  processed_date)
target_path  = resolve_path(params["targetfile"],  processed_date)
pending_path = resolve_path(params["pendingfile"], processed_date)
audit_path   = resolve_path(params["auditfile"],   processed_date)
table_name   = config_filepath.split("/")[-1].split(".")[0]

print(f"Source  : {source_path}")
print(f"Target  : {target_path}")
print(f"Pending : {pending_path}")
print(f"Audit   : {audit_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read Source Data
# MAGIC Supports: csv | json | parquet | delta

# COMMAND ----------

SUPPORTED_FORMATS = {"csv", "json", "parquet", "delta"}

def read_source(path: str, fmt: str) -> "DataFrame":
    """
    Reads source data in a format-agnostic way.
    Raises clearly if the format is unsupported or the path is missing.
    """
    fmt = fmt.lower().strip()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(
            f"[ERROR] Unsupported format '{fmt}'. Choose from: {SUPPORTED_FORMATS}"
        )
    try:
        reader = spark.read.format(fmt)
        if fmt == "csv":
            reader = reader.option("header", True).option("inferSchema", False)
        if fmt == "json":
            reader = reader.option("multiLine", True)
        return reader.load(path)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Cannot read source at '{path}': {e}")


source_format = params.get("source_format", "csv")
df_source     = read_source(source_path, source_format)
source_count  = df_source.count()

print(f"[INFO] Source rows loaded: {source_count}")
df_source.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality Checks
# MAGIC Each check is isolated. A record is only flagged once (first-match wins).
# MAGIC This prevents inflated reject counts.

# COMMAND ----------

from pyspark.sql               import DataFrame
from pyspark.sql.functions     import col, row_number, lit, date_format, current_timestamp, to_date
from pyspark.sql.window        import Window
from functools                 import reduce


def check_duplicates(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Splits df into (unique_records, duplicate_records).
    Partition across ALL columns — a row is a duplicate only if every field matches.
    """
    cols       = df.columns
    window_spec = Window.partitionBy(*cols).orderBy(cols[0])
    df_ranked  = df.withColumn("_rn", row_number().over(window_spec))
    duplicates = (df_ranked.filter(col("_rn") > 1)
                           .drop("_rn")
                           .withColumn("reject_reason", lit("Duplicate record")))
    unique     = df_ranked.filter(col("_rn") == 1).drop("_rn")
    return unique, duplicates


def check_nulls(df: DataFrame, null_cols: list) -> tuple[DataFrame, DataFrame]:
    """
    Returns (clean_records, null_records).
    A record is flagged if ANY of the specified columns is null.
    Only one reject_reason tag is applied per record.
    """
    if not null_cols:
        return df, df.limit(0).withColumn("reject_reason", lit(""))

    null_condition = reduce(
        lambda a, b: a | b,
        [col(c).isNull() for c in null_cols]
    )
    rejected = df.filter(null_condition).withColumn("reject_reason", lit("Null value"))
    clean    = df.filter(~null_condition)
    return clean, rejected


def check_negatives(df: DataFrame, neg_cols: list) -> tuple[DataFrame, DataFrame]:
    """
    Returns (clean_records, negative_records).
    Flags rows where ANY of the specified numeric columns has a value < 0.
    """
    if not neg_cols:
        return df, df.limit(0).withColumn("reject_reason", lit(""))

    neg_condition = reduce(
        lambda a, b: a | b,
        [col(c).cast("double") < 0 for c in neg_cols]
    )
    rejected = df.filter(neg_condition).withColumn("reject_reason", lit("Negative value"))
    clean    = df.filter(~neg_condition)
    return clean, rejected


def check_date_formats(df: DataFrame, date_cols: list, fmt: str = "yyyy-MM-dd") -> tuple[DataFrame, DataFrame]:
    """
    Returns (clean_records, bad_date_records).
    A date is invalid if it cannot be parsed to the specified format.
    """
    if not date_cols:
        return df, df.limit(0).withColumn("reject_reason", lit(""))

    temp_col = "_date_check"
    current  = df
    bad_dfs  = []

    for column in date_cols:
        flagged = (current
                   .withColumn(temp_col, to_date(col(column), fmt))
                   .filter(col(temp_col).isNull())
                   .drop(temp_col)
                   .withColumn("reject_reason", lit(f"Invalid date format in '{column}' (expected {fmt})")))
        bad_dfs.append(flagged)
        # Remove bad rows from the working dataframe before the next column check
        current = (current
                   .withColumn(temp_col, to_date(col(column), fmt))
                   .filter(col(temp_col).isNotNull())
                   .drop(temp_col))

    rejected = reduce(DataFrame.union, bad_dfs) if bad_dfs else df.limit(0).withColumn("reject_reason", lit(""))
    return current, rejected


def cast_columns(df: DataFrame, cast_map: dict) -> DataFrame:
    """
    Casts columns to target types based on the cols_datatype config map.
    Skips any column not present in the dataframe.
    """
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))
        else:
            print(f"[WARN] Cast column '{col_name}' not found in dataframe — skipped.")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Execute Pipeline (Checks in Sequence)
# MAGIC Order: Duplicates → Nulls → Negatives → Date Format → Cast → Select

# COMMAND ----------

reject_frames = []

# --- Step 1: Duplicate check (conditional) ---
if params.get("duplicate_check", False):
    df_clean, df_dup = check_duplicates(df_source)
    dup_count = df_dup.count()
    reject_frames.append(df_dup)
    print(f"[CHECK] Duplicates flagged : {dup_count}")
else:
    df_clean = df_source
    print("[CHECK] Duplicate check skipped (disabled in config).")

# --- Step 2: Null check ---
null_cols = params.get("null_check") or []
df_clean, df_null = check_nulls(df_clean, null_cols)
null_count = df_null.count()
if null_count > 0:
    reject_frames.append(df_null)
print(f"[CHECK] Null records flagged  : {null_count}")

# --- Step 3: Negative value check ---
neg_cols = params.get("no_negative_value") or []
df_clean, df_neg = check_negatives(df_clean, neg_cols)
neg_count = df_neg.count()
if neg_count > 0:
    reject_frames.append(df_neg)
print(f"[CHECK] Negative rows flagged : {neg_count}")

# --- Step 4: Date format check ---
date_cols = params.get("dateformatchecks") or []
df_clean, df_date = check_date_formats(df_clean, date_cols)
date_count = df_date.count()
if date_count > 0:
    reject_frames.append(df_date)
print(f"[CHECK] Bad date rows flagged : {date_count}")

# --- Step 5: Cast column types ---
df_clean = cast_columns(df_clean, params.get("cols_datatype", {}))

# --- Step 6: Select required columns only ---
required_cols = params.get("required_cols") or []
if required_cols:
    missing_cols = [c for c in required_cols if c not in df_clean.columns]
    if missing_cols:
        raise ValueError(f"[ERROR] required_cols not found in data: {missing_cols}")
    df_clean = df_clean.select(required_cols)

written_count = df_clean.count()
print(f"\n[SUMMARY] Source: {source_count} | Rejected: {sum([dup_count if params.get('duplicate_check') else 0, null_count, neg_count, date_count])} | Written: {written_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Build Rejected Records Frame

# COMMAND ----------

if reject_frames:
    df_reject = reduce(DataFrame.union, reject_frames)
    reject_count = df_reject.count()
else:
    # Empty reject frame with schema matching source + reject_reason column
    df_reject = df_source.limit(0).withColumn("reject_reason", lit(""))
    reject_count = 0

print(f"[INFO] Total rejected records: {reject_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Outputs

# COMMAND ----------

# Clean / target data → Parquet (columnar, efficient for downstream queries)
df_clean.write.mode("overwrite").format("parquet").save(target_path)
print(f"[WRITE] Clean data written to  : {target_path}")

# Rejected records → CSV (human-readable for ops/data teams to review)
if reject_count > 0:
    df_reject.write.mode("overwrite").format("csv").option("header", True).save(pending_path)
    print(f"[WRITE] Rejected data written to: {pending_path}")
else:
    print("[WRITE] No rejected records — pending file not created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Audit Log

# COMMAND ----------

df_audit = (
    spark.createDataFrame(
        [(table_name, source_count, reject_count, written_count)],
        ["table_name", "source_count", "reject_count", "written_count"],
    )
    .withColumn("processed_date",   lit(processed_date))
    .withColumn("load_timestamp",   current_timestamp())
    .withColumn("config_filepath",  lit(config_filepath))
    .withColumn("source_format",    lit(source_format))
)

df_audit.show(truncate=False)
df_audit.write.mode("append").format("csv").option("header", True).save(audit_path)
print(f"[WRITE] Audit log written to   : {audit_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete ✓
