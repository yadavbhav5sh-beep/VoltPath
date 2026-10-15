from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ─────────────────────────────────────────────
# 1. Standardize & apply data quality rules
# ─────────────────────────────────────────────
def standardize_tariffs(df: DataFrame) -> DataFrame:
    """
    Read the bronze tariffs DataFrame and apply standardization
    and data-type casting rules so every column is clean and consistent.

    Standardization rules:
      • Trim leading/trailing whitespace on all string columns
      • connector_type  → cast STRING + TRIM + UPPER   ("ccs2" → "CCS2")
      • time_band       → cast STRING + TRIM + UPPER   (" off_peak " → "OFF_PEAK")
      • rate_per_kwh    → cast DOUBLE
      • idle_fee_per_min → cast DECIMAL(10,2)
      • free_idle_minutes → cast INT
      • Drop _rescued_data, _source_file, _ingestion_at
    """
    return (
        df
        .withColumns({
            "connector_type":    F.upper(F.trim(F.col("connector_type").cast("string"))),
            "time_band":         F.upper(F.trim(F.col("time_band").cast("string"))),
            "rate_per_kwh":      F.col("rate_per_kwh").cast("double"),
            "idle_fee_per_min":  F.col("idle_fee_per_min").cast("decimal(10,2)"),
            "free_idle_minutes": F.col("free_idle_minutes").cast("int"),
        })
        .drop("_rescued_data", "_source_file", "_ingestion_at")
    )

# ─────────────────────────────────────────────
# 2. Quarantine invalid records
# ─────────────────────────────────────────────
def quarantine_invalid_tariffs(df: DataFrame) -> DataFrame:
    """
    Identify records that fail data-quality checks and write
    them to silver.quarantine_tariffs.

    A record is quarantined when:
      • connector_type is NULL or empty
      • time_band      is NULL or empty

    Returns the quarantined DataFrame (caller can inspect counts).
    """
    invalid_df = df.filter(
        F.col("connector_type").isNull()
        | (F.trim(F.col("connector_type")) == "")
        | F.col("time_band").isNull()
        | (F.trim(F.col("time_band")) == "")
    )

    (
        invalid_df.write
        .mode("overwrite")
        .saveAsTable("vp_by20021205.silver.quarantine_tariffs")
    )

    return invalid_df


# ─────────────────────────────────────────────
# 3. Save valid records to silver
# ─────────────────────────────────────────────
def save_valid_tariffs(df: DataFrame) -> DataFrame:
    """
    Keep only valid records and write them to silver.tariffs.

    A record is valid when:
      • connector_type is NOT NULL and non-empty
      • time_band      is NOT NULL and non-empty

    Returns the valid DataFrame (caller can inspect counts).
    """
    valid_df = df.filter(
        F.col("connector_type").isNotNull()
        & (F.trim(F.col("connector_type")) != "")
        & F.col("time_band").isNotNull()
        & (F.trim(F.col("time_band")) != "")
    )

    (
        valid_df.write
        .mode("overwrite")
        .saveAsTable("vp_by20021205.silver.tariffs")
    )

    return valid_df


# ─────────────────────────────────────────────
# 4. Main pipeline
# ─────────────────────────────────────────────
def main():
    # Read from bronze
    bronze_df = spark.read.table("vp_by20021205.bronze.tariffs")

    # Apply standardization rules
    standardized_df = standardize_tariffs(bronze_df)

    # Quarantine invalid records
    invalid_df = quarantine_invalid_tariffs(standardized_df)

    # Save valid records to silver
    valid_df = save_valid_tariffs(standardized_df)

    # Print counts
    invalid_count = invalid_df.count()
    valid_count   = valid_df.count()

    print("=" * 40)
    print(f"  Valid records   : {valid_count}")
    print(f"  Invalid records : {invalid_count}")
    print(f"  Total records   : {valid_count + invalid_count}")
    print("=" * 40)


if __name__ == "__main__":
    main()