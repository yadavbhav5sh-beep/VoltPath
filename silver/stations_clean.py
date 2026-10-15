from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ──────────────────────────────────────────────
# 1. Standardize & apply data quality rules
# ──────────────────────────────────────────────
def standardize_stations(df: DataFrame) -> DataFrame:
    """
    Read the bronze stations DataFrame and apply standardization
    and data-type casting rules so every column is clean and consistent.

    Standardization rules:
      • Trim leading/trailing whitespace on all string columns
      • station_id   → cast STRING + UPPER
      • station_name → cast STRING + TRIM + INITCAP
      • city         → cast STRING + TRIM + INITCAP   ("  jaipur " → "Jaipur")
      • site_type    → cast STRING + TRIM + UPPER     ("highway" → "HIGHWAY")
      • opened_on    → cast DATE
      • Drop _rescued_data, _source_file, _ingestion_at
    """
    return (
        df
        .withColumns({
            "station_id":   F.upper(F.trim(F.col("station_id").cast("string"))),
            "station_name": F.initcap(F.trim(F.col("station_name").cast("string"))),
            "city":         F.initcap(F.trim(F.col("city").cast("string"))),
            "site_type":    F.upper(F.trim(F.col("site_type").cast("string"))),
            "opened_on":   F.col("opened_on").cast("date"),
        })
        .drop("_rescued_data", "_source_file", "_ingestion_at")
    )

# ──────────────────────────────────────────────
# 2. Quarantine invalid records
# ──────────────────────────────────────────────
def quarantine_invalid_stations(df: DataFrame) -> DataFrame:
    """
    Identify records that fail data-quality checks and write
    them to silver.quarantine_stations.

    A record is quarantined when:
      • station_id   is NULL or empty
      • station_name is NULL or empty

    Returns the quarantined DataFrame (caller can inspect counts).
    """
    invalid_df = df.filter(
        F.col("station_id").isNull()
        | (F.trim(F.col("station_id")) == "")
        | F.col("station_name").isNull()
        | (F.trim(F.col("station_name")) == "")
    )

    (
        invalid_df.write
        .mode("overwrite")
        .saveAsTable("vp_by20021205.silver.quarantine_stations")
    )

    return invalid_df


# ──────────────────────────────────────────────
# 3. Save valid records to silver
# ──────────────────────────────────────────────
def save_valid_stations(df: DataFrame) -> DataFrame:
    """
    Keep only valid records and write them to silver.stations.

    A record is valid when:
      • station_id   is NOT NULL and non-empty
      • station_name is NOT NULL and non-empty

    Returns the valid DataFrame (caller can inspect counts).
    """
    valid_df = df.filter(
        F.col("station_id").isNotNull()
        & (F.trim(F.col("station_id")) != "")
        & F.col("station_name").isNotNull()
        & (F.trim(F.col("station_name")) != "")
    )

    (
        valid_df.write
        .mode("overwrite")
        .saveAsTable("vp_by20021205.silver.stations")
    )

    return valid_df


# ──────────────────────────────────────────────
# 4. Main pipeline
# ──────────────────────────────────────────────
def main():
    # Read from bronze
    bronze_df = spark.read.table("vp_by20021205.bronze.stations")

    # Apply standardization rules
    standardized_df = standardize_stations(bronze_df)

    # Quarantine invalid records
    invalid_df = quarantine_invalid_stations(standardized_df)

    # Save valid records to silver
    valid_df = save_valid_stations(standardized_df)

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