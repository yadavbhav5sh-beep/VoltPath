from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window
from delta.tables import DeltaTable

CATALOG = "vp_by20021205"

BRONZE_TABLE = f"{CATALOG}.bronze.chargers"
STATIONS_TABLE = f"{CATALOG}.silver.stations"

STAGING_TABLE = f"{CATALOG}.silver.chargers_source"
TARGET_TABLE = f"{CATALOG}.silver.chargers"
QUARANTINE_TABLE = f"{CATALOG}.silver.quarantine_chargers"

# ─────────────────────────────────────────────
# 1. Standardize and unpack nested connector
# ─────────────────────────────────────────────
def standardize_chargers(df: DataFrame) -> DataFrame:
    """
    Clean charger data and unpack the nested connector STRUCT.

    connector.type      -> connector_type
    connector.power_kw  -> max_kw

    Cast columns to the required Silver data types.
    """

    return (
        df
        .withColumn(
            "charger_id",
            F.upper(F.trim(F.col("charger_id").cast("string")))
        )
        .withColumn(
            "station_id",
            F.upper(F.trim(F.col("station_id").cast("string")))
        )
        .withColumn(
            "connector_type",
            F.upper(
                F.trim(F.col("connector.type").cast("string"))
            )
        )
        .withColumn(
            "max_kw",
            F.col("connector.max_kw").cast("decimal(6,1)")
        )
        .withColumn(
            "is_active",
            F.col("is_active").cast("boolean")
        )
        .withColumn(
            "commissioned_on",
            F.to_date("commissioned_on")
        )
        .withColumn(
            "snapshot_date",
            F.to_date("snapshot_date")
        )
        .drop(
            "connector",
            "_rescued_data",
            "_source_file",
            "_ingestion_at"
        )
    )


# ─────────────────────────────────────────────
# 2. Deduplicate: latest snapshot per charger
# ─────────────────────────────────────────────
def split_latest_and_duplicates(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Keep the latest snapshot per charger_id.

    Older snapshots are quarantined.
    """

    window_spec = (
        Window
        .partitionBy("charger_id")
        .orderBy(F.col("snapshot_date").desc_nulls_last())
    )

    ranked_df = df.withColumn(
        "_row_num",
        F.row_number().over(window_spec)
    )

    latest_df = (
        ranked_df
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    duplicates_df = (
        ranked_df
        .filter(F.col("_row_num") > 1)
        .drop("_row_num")
        .withColumn(
            "reject_reason",
            F.lit("OLDER_CHARGER_SNAPSHOT")
        )
    )

    return latest_df, duplicates_df


# ─────────────────────────────────────────────
# 3. Validate station references
# ─────────────────────────────────────────────
def split_valid_and_invalid_stations(
    latest_df: DataFrame,
    stations_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    A charger is valid only when station_id exists
    in the cleaned Silver stations table.

    Invalid station references are quarantined.
    """

    station_ids = (
        stations_df
        .select(
            F.upper(F.trim(F.col("station_id"))).alias("station_id")
        )
        .filter(F.col("station_id").isNotNull())
        .distinct()
    )

    valid_df = (
        latest_df
        .filter(
            F.col("station_id").isNotNull()
            & (F.trim(F.col("station_id")) != "")
            & F.col("charger_id").isNotNull()
            & (F.trim(F.col("charger_id")) != "")
            & F.col("snapshot_date").isNotNull()
        )
        .join(station_ids, on="station_id", how="left_semi")
    )

    invalid_df = (
        latest_df
        .join(station_ids, on="station_id", how="left_anti")
        .withColumn(
            "reject_reason",
            F.lit("UNKNOWN_STATION")
        )
    )

    return valid_df, invalid_df

# ─────────────────────────────────────────────
# 4. Create target once, if it doesn't exist
# ─────────────────────────────────────────────
def create_target_if_not_exists():
    """
    Create the target table only when it does not exist.
    Never overwrite or recreate the target.
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            charger_id STRING,
            station_id STRING,
            connector_type STRING,
            max_kw DECIMAL(6,1),
            is_active BOOLEAN,
            commissioned_on DATE,
            last_changed_snapshot_date DATE
        )
        USING DELTA
    """)


# ─────────────────────────────────────────────
# 5. Create staging table and load valid source
# ─────────────────────────────────────────────
def save_valid_staging(valid_df: DataFrame):
    """
    The staging table holds the current run's valid source.
    It can be refreshed; the Silver target is never overwritten.
    """

    source_df = (
        valid_df
        .select(
            "charger_id",
            "station_id",
            "connector_type",
            "max_kw",
            "is_active",
            "commissioned_on",
            F.col("snapshot_date").alias(
                "last_changed_snapshot_date"
            )
        )
    )

    # One source row per charger is required for MERGE.
    source_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(STAGING_TABLE)

# ─────────────────────────────────────────────
# 6. MERGE source into Silver target
# ─────────────────────────────────────────────
def merge_chargers():
    """
    Insert new chargers.
    Update existing chargers only if a business value changes.

    Missing chargers are not deleted.
    last_changed_snapshot_date does not trigger an update by itself.
    """

    source = spark.table(STAGING_TABLE)

    target = DeltaTable.forName(spark, TARGET_TABLE)

    business_change = """
        NOT (t.station_id <=> s.station_id)
        OR NOT (t.connector_type <=> s.connector_type)
        OR NOT (t.max_kw <=> s.max_kw)
        OR NOT (t.is_active <=> s.is_active)
        OR NOT (t.commissioned_on <=> s.commissioned_on)
    """

    (
        target.alias("t")
        .merge(
            source.alias("s"),
            "t.charger_id = s.charger_id"
        )
        .whenMatchedUpdate(
            condition=business_change,
            set={
                "station_id": "s.station_id",
                "connector_type": "s.connector_type",
                "max_kw": "s.max_kw",
                "is_active": "s.is_active",
                "commissioned_on": "s.commissioned_on",
                "last_changed_snapshot_date":
                    "s.last_changed_snapshot_date"
            }
        )
        .whenNotMatchedInsert(
            values={
                "charger_id": "s.charger_id",
                "station_id": "s.station_id",
                "connector_type": "s.connector_type",
                "max_kw": "s.max_kw",
                "is_active": "s.is_active",
                "commissioned_on": "s.commissioned_on",
                "last_changed_snapshot_date":
                    "s.last_changed_snapshot_date"
            }
        )
        .execute()
    )


# ─────────────────────────────────────────────
# 7. Save quarantine records
# ─────────────────────────────────────────────
def save_quarantine(df: DataFrame):
    """
    Save rejected records with their reasons.
    """

    quarantine_df = df.withColumn(
        "quarantined_at",
        F.current_timestamp()
    )

    if not spark.catalog.tableExists(QUARANTINE_TABLE):
        (
            quarantine_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(QUARANTINE_TABLE)
        )
    else:
        # Basic append implementation.
        # Use a keyed MERGE instead if reruns must not duplicate
        # the same quarantine record.
        (
            quarantine_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(QUARANTINE_TABLE)
        )


# ─────────────────────────────────────────────
# 8. Main pipeline
# ─────────────────────────────────────────────
def main():

    # Read Bronze
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Standardize
    standardized_df = standardize_chargers(bronze_df)

    # Latest snapshot and older duplicates
    latest_df, duplicates_df = split_latest_and_duplicates(
        standardized_df
    )

    # Validate latest snapshot against cleaned stations
    stations_df = spark.read.table(STATIONS_TABLE)

    valid_df, invalid_stations_df = split_valid_and_invalid_stations(
        latest_df,
        stations_df
    )

    # Combine rejected records
    quarantine_df = (
        duplicates_df
        .unionByName(
            invalid_stations_df,
            allowMissingColumns=True
        )
    )

    # Create target once
    create_target_if_not_exists()

    # Save quarantine and current valid source
    save_quarantine(quarantine_df)
    save_valid_staging(valid_df)

    # Merge into persistent Silver target
    merge_chargers()

    print("Valid latest chargers:", valid_df.count())
    print("Older duplicate snapshots:", duplicates_df.count())
    print("Invalid station references:", invalid_stations_df.count())


if __name__ == "__main__":
    main()
