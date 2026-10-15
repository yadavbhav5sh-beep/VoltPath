from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────
CATALOG = "vp_by20021205"

SESSIONS_TABLE = f"{CATALOG}.silver.sessions"
FACT_TABLE = f"{CATALOG}.gold.fact_sessions"
DATE_TABLE = f"{CATALOG}.gold.dim_date"
STATION_TABLE = f"{CATALOG}.gold.dim_station"
CHARGER_TABLE = f"{CATALOG}.gold.dim_charger"

TARGET_TABLE = f"{CATALOG}.gold.agg_daily_station"

# ─────────────────────────────────────────────
# 2. Build daily station aggregation
# ─────────────────────────────────────────────
def build_agg_daily_station():

    # Source session date range
    sessions_df = spark.read.table(SESSIONS_TABLE)

    date_range = sessions_df.agg(
        F.min("session_date").alias("first_date"),
        F.max("session_date").alias("last_date")
    ).first()

    if date_range["first_date"] is None:
        raise ValueError(
            "Cannot build agg_daily_station: no session dates found."
        )

    first_date = date_range["first_date"]
    last_date = date_range["last_date"]

    # ─────────────────────────────────────────
    # Generate every date from first to last
    # ─────────────────────────────────────────
    dates_df = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.lit(first_date).cast("date"),
                    F.lit(last_date).cast("date"),
                    F.expr("INTERVAL 1 DAY")
                )
            ).alias("session_date")
        )
    )

    # ─────────────────────────────────────────
    # Read stations and charger counts
    # ─────────────────────────────────────────
    stations_df = (
        spark.read.table(STATION_TABLE)
        .select("station_id", "city")
        .dropDuplicates(["station_id"])
    )

    charger_counts_df = (
        spark.read.table(CHARGER_TABLE)
        .groupBy("station_id")
        .agg(
            F.countDistinct("charger_id").alias("charger_count")
        )
    )

    # Every station × every date
    station_date_df = (
        dates_df.crossJoin(stations_df)
        .join(
            charger_counts_df,
            on="station_id",
            how="left"
        )
        .withColumn(
            "charger_count",
            F.coalesce(F.col("charger_count"), F.lit(0))
        )
    )

    # ─────────────────────────────────────────
    # Read fact sessions and attach session_date
    # ─────────────────────────────────────────
    fact_df = spark.read.table(FACT_TABLE)

    date_dim_df = (
        spark.read.table(DATE_TABLE)
        .select("date_key", "calendar_date")
    )

    session_fact_df = (
        fact_df.alias("f")
        .join(
            date_dim_df.alias("d"),
            F.col("f.date_key") == F.col("d.date_key"),
            "inner"
        )
        .select(
            F.col("d.calendar_date").alias("session_date"),
            F.col("f.station_id").alias("station_id"),
            F.col("f.session_id").alias("session_id"),
            F.upper(F.col("f.status")).alias("status"),
            F.col("f.energy_kwh").alias("energy_kwh"),
            F.col("f.duration_minutes").alias("duration_minutes"),
            F.col("f.billed_amount").alias("billed_amount"),
            F.col("f.amount_paid").alias("amount_paid"),
            F.col("f.billing_status").alias("billing_status"),
            F.col("f.fault_count").alias("fault_count")
        )
    )

    # ─────────────────────────────────────────
    # Aggregate metrics per station per date
    # ─────────────────────────────────────────
    daily_metrics_df = (
        session_fact_df
        .groupBy("session_date", "station_id")
        .agg(
            # Completed sessions
            F.count(
                F.when(F.col("status") == "COMPLETED", 1)
            ).alias("completed_sessions"),

            # Failed sessions
            F.count(
                F.when(F.col("status") == "FAILED", 1)
            ).alias("failed_sessions"),

            # These three metrics are COMPLETED only
            F.sum(
                F.when(
                    F.col("status") == "COMPLETED",
                    F.coalesce(
                        F.col("energy_kwh"),
                        F.lit(0)
                    )
                ).otherwise(F.lit(0))
            ).alias("energy_kwh"),

            F.sum(
                F.when(
                    F.col("status") == "COMPLETED",
                    F.coalesce(
                        F.col("billed_amount"),
                        F.lit(0)
                    )
                ).otherwise(F.lit(0))
            ).alias("billed_amount"),

            F.sum(
                F.when(
                    F.col("status") == "COMPLETED",
                    F.coalesce(
                        F.col("amount_paid"),
                        F.lit(0)
                    )
                ).otherwise(F.lit(0))
            ).alias("amount_paid"),

            # Completed sessions with billing mismatch
            F.count(
                F.when(
                    (F.col("status") == "COMPLETED")
                    & (F.col("billing_status") == "MISMATCH"),
                    1
                )
            ).alias("mismatch_sessions"),

            # Total faults associated with sessions
            F.sum(
                F.coalesce(F.col("fault_count"), F.lit(0))
            ).alias("fault_count"),

            # Needed for utilisation calculation
            F.sum(
                F.when(
                    F.col("status") == "COMPLETED",
                    F.coalesce(
                        F.col("duration_minutes"),
                        F.lit(0)
                    )
                ).otherwise(F.lit(0))
            ).alias("_completed_duration_minutes")
        )
    )

    # ─────────────────────────────────────────
    # Join aggregates to the full station-date grid
    # ─────────────────────────────────────────
    result_df = (
        station_date_df
        .join(
            daily_metrics_df,
            on=["session_date", "station_id"],
            how="left"
        )
    )

    # Fill missing daily metrics with zero
    metric_columns = [
        "completed_sessions",
        "failed_sessions",
        "energy_kwh",
        "billed_amount",
        "amount_paid",
        "mismatch_sessions",
        "fault_count",
        "_completed_duration_minutes"
    ]

    for column in metric_columns:
        result_df = result_df.withColumn(
            column,
            F.coalesce(F.col(column), F.lit(0))
        )

    # ─────────────────────────────────────────
    # Utilisation percentage
    # 100 × completed duration / (chargers × 1440)
    # ─────────────────────────────────────────
    utilisation = F.when(
        F.col("charger_count") > 0,
        F.lit(100.0)
        * F.col("_completed_duration_minutes")
        / (
            F.col("charger_count") * F.lit(1440)
        )
    ).otherwise(F.lit(0.0))

    result_df = result_df.withColumn(
        "utilisation_pct",
        F.round(utilisation, 2).cast("decimal(5,2)")
    )

    # ─────────────────────────────────────────
    # Final schema
    # ─────────────────────────────────────────
    return result_df.select(
        F.col("session_date").cast("date"),
        F.col("station_id"),
        F.col("city"),
        F.col("completed_sessions").cast("long"),
        F.col("failed_sessions").cast("long"),
        F.col("energy_kwh").cast("decimal(12,3)"),
        F.col("billed_amount").cast("decimal(12,2)"),
        F.col("amount_paid").cast("decimal(12,2)"),
        F.col("mismatch_sessions").cast("long"),
        F.col("fault_count").cast("long"),
        F.col("utilisation_pct")
    )


# ─────────────────────────────────────────────
# 3. Write Gold table
# ─────────────────────────────────────────────
def main():

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

    agg_df = build_agg_daily_station()

    (
        agg_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    print(f"Created: {TARGET_TABLE}")
    print(f"Total rows: {agg_df.count()}")

    agg_df.orderBy(
        "session_date", "station_id"
    ).show(20, truncate=False)

if __name__ == "__main__":
    main()
