from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────
CATALOG = "vp_by20021205"

SESSIONS_TABLE = f"{CATALOG}.silver.sessions"
CHARGERS_TABLE = f"{CATALOG}.gold.dim_charger"
DATE_TABLE = f"{CATALOG}.gold.dim_date"
MEMBERS_TABLE = f"{CATALOG}.gold.dim_member"
TARIFFS_TABLE = f"{CATALOG}.silver.tariffs"
FAULTS_TABLE = f"{CATALOG}.silver.session_faults"

FACT_TABLE = f"{CATALOG}.gold.fact_sessions"

# ─────────────────────────────────────────────
# 2. Build fact_sessions
# ─────────────────────────────────────────────
def build_fact_sessions():

    # Read source tables
    sessions_df = spark.read.table(SESSIONS_TABLE).alias("s")
    chargers_df = spark.read.table(CHARGERS_TABLE).alias("c")
    dates_df = spark.read.table(DATE_TABLE).alias("d")
    members_df = spark.read.table(MEMBERS_TABLE).alias("m")
    tariffs_df = spark.read.table(TARIFFS_TABLE).alias("t")

    # Count faults: one aggregated row per session
    faults_df = (
        spark.read.table(FAULTS_TABLE)
        .groupBy("session_id")
        .agg(
            F.count("*").cast("int").alias("fault_count")
        )
        .alias("f")
    )

    # ─────────────────────────────────────────
    # Join date dimension
    # ─────────────────────────────────────────
    joined_df = (
        sessions_df
        .join(
            dates_df,
            F.col("s.session_date") == F.col("d.calendar_date"),
            "left"
        )
    )

    # ─────────────────────────────────────────
    # Join charger dimension
    # Provides station_id and connector_type
    # ─────────────────────────────────────────
    joined_df = (
        joined_df
        .join(
            chargers_df,
            F.col("s.charger_id") == F.col("c.charger_id"),
            "left"
        )
    )

    # ─────────────────────────────────────────
    # Join applicable SCD Type 2 member version
    # effective_from <= session_date
    # session_date < effective_to OR current
    # ─────────────────────────────────────────
    member_condition = (
        (F.col("s.member_id") == F.col("m.member_id"))
        & (
            F.col("s.session_date")
            >= F.col("m.effective_from")
        )
        & (
            F.col("m.effective_to").isNull()
            | (
                F.col("s.session_date")
                < F.col("m.effective_to")
            )
        )
    )

    joined_df = joined_df.join(
        members_df,
        member_condition,
        "left"
    )

    # ─────────────────────────────────────────
    # Join tariff using connector_type + time_band
    # ─────────────────────────────────────────
    tariff_condition = (
        (
            F.upper(F.trim(F.col("c.connector_type")))
            == F.upper(F.trim(F.col("t.connector_type")))
        )
        & (
            F.upper(F.trim(F.col("s.time_band")))
            == F.upper(F.trim(F.col("t.time_band")))
        )
    )

    joined_df = joined_df.join(
        tariffs_df,
        tariff_condition,
        "left"
    )

    # ─────────────────────────────────────────
    # Join aggregated session faults
    # ─────────────────────────────────────────
    joined_df = joined_df.join(
        faults_df,
        F.col("s.session_id") == F.col("f.session_id"),
        "left"
    )

    # ─────────────────────────────────────────
    # 3. Session-level attributes
    # ─────────────────────────────────────────
    joined_df = (
        joined_df
        .withColumn(
            "plan_at_session",
            F.when(
                F.col("s.member_id").isNull(),
                F.lit("GUEST")
            ).otherwise(F.col("m.plan"))
        )
        .withColumn(
            "fault_count",
            F.coalesce(F.col("f.fault_count"), F.lit(0))
        )
    )

    # ─────────────────────────────────────────
    # 4. Billing calculations
    # Only COMPLETED sessions are billed.
    # Money uses DECIMAL(12,2).
    # ─────────────────────────────────────────
    is_completed = F.upper(F.col("s.status")) == "COMPLETED"

    energy_charge_calc = F.round(
        F.col("s.energy_kwh").cast("decimal(20,6)")
        * F.col("t.rate_per_kwh").cast("decimal(20,6)"),
        2
    ).cast("decimal(12,2)")

    discount_rate = (
        F.when(F.upper(F.col("m.plan")) == "BASIC", F.lit(0.00))
         .when(F.upper(F.col("m.plan")) == "PRO", F.lit(0.10))
         .when(F.upper(F.col("m.plan")) == "FLEET", F.lit(0.15))
         .otherwise(F.lit(0.00))
    )

    plan_discount_calc = F.round(
        energy_charge_calc * discount_rate,
        2
    ).cast("decimal(12,2)")

    billable_idle_minutes = F.greatest(
        F.coalesce(F.col("s.idle_minutes"), F.lit(0))
        - F.coalesce(F.col("t.free_idle_minutes"), F.lit(0)),
        F.lit(0)
    )

    idle_fee_calc = F.round(
        billable_idle_minutes
        * F.coalesce(
            F.col("t.idle_fee_per_min").cast("decimal(20,6)"),
            F.lit(0).cast("decimal(20,6)")
        ),
        2
    ).cast("decimal(12,2)")

    zero_money = F.lit(0).cast("decimal(12,2)")

    joined_df = (
        joined_df
        .withColumn(
            "energy_charge",
            F.when(is_completed, energy_charge_calc)
             .otherwise(zero_money)
        )
        .withColumn(
            "plan_discount",
            F.when(is_completed, plan_discount_calc)
             .otherwise(zero_money)
        )
        .withColumn(
            "idle_fee",
            F.when(is_completed, idle_fee_calc)
             .otherwise(zero_money)
        )
        .withColumn(
            "billed_amount",
            F.when(
                is_completed,
                (
                    energy_charge_calc
                    - plan_discount_calc
                    + idle_fee_calc
                ).cast("decimal(12,2)")
            ).otherwise(zero_money)
        )
    )

    # ─────────────────────────────────────────
    # 5. Billing status
    # OK when difference <= 0.50
    # Otherwise MISMATCH
    # Non-completed sessions -> NULL
    # ─────────────────────────────────────────
    joined_df = joined_df.withColumn(
        "billing_status",
        F.when(
            ~is_completed,
            F.lit(None).cast("string")
        ).when(
            F.col("s.amount_paid").isNotNull()
            & (
                F.abs(
                    F.col("s.amount_paid").cast("decimal(12,2)")
                    - F.col("billed_amount")
                ) <= F.lit(0.50)
            ),
            F.lit("OK")
        ).otherwise(F.lit("MISMATCH"))
    )

    # ─────────────────────────────────────────
    # 6. Select final required columns
    # ─────────────────────────────────────────
    fact_df = joined_df.select(
        F.col("s.session_id").alias("session_id"),
        F.col("d.date_key").cast("int").alias("date_key"),
        F.col("s.charger_id").alias("charger_id"),
        F.col("c.station_id").alias("station_id"),
        F.col("s.member_id").alias("member_id"),
        F.col("m.member_sk").cast("long").alias("member_sk"),
        F.col("s.status").alias("status"),
        F.col("s.time_band").alias("time_band"),
        F.col("s.energy_kwh").cast("decimal(12,3)").alias("energy_kwh"),
        F.col("s.duration_minutes").alias("duration_minutes"),
        F.col("s.idle_minutes").alias("idle_minutes"),
        F.col("s.amount_paid").cast("decimal(12,2)").alias("amount_paid"),
        F.col("plan_at_session").alias("plan_at_session"),
        F.col("energy_charge").cast("decimal(12,2)").alias("energy_charge"),
        F.col("plan_discount").cast("decimal(12,2)").alias("plan_discount"),
        F.col("idle_fee").cast("decimal(12,2)").alias("idle_fee"),
        F.col("billed_amount").cast("decimal(12,2)").alias("billed_amount"),
        F.col("billing_status").alias("billing_status"),
        F.col("fault_count").cast("int").alias("fault_count")
    )

    return fact_df


# ─────────────────────────────────────────────
# 7. Create target table
# ─────────────────────────────────────────────
def main():

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

    fact_df = build_fact_sessions()

    (
        fact_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(FACT_TABLE)
    )

    print(f"Created: {FACT_TABLE}")
    print(f"Fact rows: {fact_df.count()}")

    fact_df.show(10, truncate=False)


if __name__ == "__main__":
    main()