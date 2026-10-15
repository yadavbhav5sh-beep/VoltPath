from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────
CATALOG = "vp_by20021205"

FACT_TABLE = f"{CATALOG}.gold.fact_sessions"
VIEW_NAME = f"{CATALOG}.gold.v_kpi_summary"

# ─────────────────────────────────────────────
# 2. Build KPI summary
# ─────────────────────────────────────────────
def build_kpi_summary():

    fact_df = spark.read.table(FACT_TABLE)

    # Completed and failed session counts
    completed_sessions = F.sum(
        F.when(F.upper(F.col("status")) == "COMPLETED", 1)
         .otherwise(0)
    )

    failed_sessions = F.sum(
        F.when(F.upper(F.col("status")) == "FAILED", 1)
         .otherwise(0)
    )

    # Calculate all KPIs from unrounded values.
    # Round percentages and averages only once.
    kpi_df = fact_df.agg(
        completed_sessions.alias("completed_sessions"),
        failed_sessions.alias("failed_sessions"),

        # Total energy from completed sessions only
        F.sum(
            F.when(
                F.upper(F.col("status")) == "COMPLETED",
                F.coalesce(F.col("energy_kwh"), F.lit(0))
            ).otherwise(F.lit(0))
        ).alias("total_energy_kwh"),

        # Total billed amount from completed sessions only
        F.sum(
            F.when(
                F.upper(F.col("status")) == "COMPLETED",
                F.coalesce(F.col("billed_amount"), F.lit(0))
            ).otherwise(F.lit(0))
        ).alias("total_billed"),

        # Total amount paid from completed sessions only
        F.sum(
            F.when(
                F.upper(F.col("status")) == "COMPLETED",
                F.coalesce(F.col("amount_paid"), F.lit(0))
            ).otherwise(F.lit(0))
        ).alias("total_paid"),

        # Completed sessions with billing mismatch
        F.sum(
            F.when(
                (F.upper(F.col("status")) == "COMPLETED")
                & (F.col("billing_status") == "MISMATCH"),
                1
            ).otherwise(0)
        ).alias("mismatch_sessions"),

        # Distinct registered members with completed sessions.
        # NULL member IDs represent guests and are excluded.
        F.countDistinct(
            F.when(
                (F.upper(F.col("status")) == "COMPLETED")
                & F.col("member_id").isNotNull(),
                F.col("member_id")
            )
        ).alias("active_members")
    )

    # ─────────────────────────────────────────
    # 3. Success rate
    # completed / (completed + failed) * 100
    # ─────────────────────────────────────────
    kpi_df = kpi_df.withColumn(
        "success_rate_pct",
        F.when(
            (F.col("completed_sessions") + F.col("failed_sessions")) > 0,
            F.round(
                F.col("completed_sessions") * F.lit(100.0)
                / (
                    F.col("completed_sessions")
                    + F.col("failed_sessions")
                ),
                2
            )
        ).otherwise(F.lit(0.0))
    )

    # ─────────────────────────────────────────
    # 4. Revenue leakage = billed - paid
    # ─────────────────────────────────────────
    kpi_df = kpi_df.withColumn(
        "revenue_leakage",
        (
            F.col("total_billed") - F.col("total_paid")
        ).cast("decimal(12,2)")
    )

    # Final types
    return kpi_df.select(
        F.col("completed_sessions").cast("long"),
        F.col("failed_sessions").cast("long"),
        F.col("success_rate_pct").cast("decimal(5,2)"),
        F.col("total_energy_kwh").cast("decimal(12,3)"),
        F.col("total_billed").cast("decimal(12,2)"),
        F.col("total_paid").cast("decimal(12,2)"),
        F.col("revenue_leakage").cast("decimal(12,2)"),
        F.col("mismatch_sessions").cast("long"),
        F.col("active_members").cast("long")
    )

# ─────────────────────────────────────────────
# 5. Create or replace Gold view
# ─────────────────────────────────────────────
def main():

    kpi_df = build_kpi_summary()

    (
        kpi_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(VIEW_NAME)
    )

    print(f"Created table: {VIEW_NAME}")
    spark.table(VIEW_NAME).show(truncate=False)


if __name__ == "__main__":
    main()
