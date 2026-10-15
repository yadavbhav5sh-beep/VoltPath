from pyspark.sql import functions as F
from decimal import Decimal

# ─────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────
CATALOG = "vp_by20021205"

BRONZE_SESSIONS = f"{CATALOG}.bronze.sessions"
SILVER_SESSIONS = f"{CATALOG}.silver.sessions"
QUARANTINE_SESSIONS = f"{CATALOG}.silver.quarantine_sessions"
SILVER_MEMBERS = f"{CATALOG}.silver.members"

DIM_MEMBER = f"{CATALOG}.gold.dim_member"
AGG_DAILY_STATION = f"{CATALOG}.gold.agg_daily_station"

TARGET_TABLE = f"{CATALOG}.ops.reconciliation_results"

# ─────────────────────────────────────────────
# 2. Create ops schema and target table
# ─────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    run_ts TIMESTAMP,
    check_name STRING,
    source_value DECIMAL(18,3),
    target_value DECIMAL(18,3),
    status STRING
)
USING DELTA
""")

# ─────────────────────────────────────────────
# 3. Helper: safely get a numeric scalar
# ─────────────────────────────────────────────
def get_value(df, expression, alias_name):
    row = df.agg(expression.alias(alias_name)).first()
    value = row[alias_name]

    if value is None:
        return Decimal("0")

    return Decimal(str(value))


# ─────────────────────────────────────────────
# 4. SESSION_COUNT
# Source: distinct standardized session_id in Bronze
# Target: Silver sessions + quarantine rows
# ─────────────────────────────────────────────
def check_session_count():

    bronze_df = spark.read.table(BRONZE_SESSIONS)

    standardized_ids = F.upper(
        F.trim(F.col("session_id").cast("string"))
    )

    source_df = bronze_df.select(
        F.when(
            standardized_ids == "",
            F.lit(None).cast("string")
        ).otherwise(standardized_ids).alias("session_id")
    )

    source_value = get_value(
        source_df,
        F.countDistinct("session_id"),
        "value"
    )

    silver_count = get_value(
        spark.read.table(SILVER_SESSIONS),
        F.count("*"),
        "value"
    )

    quarantine_count = get_value(
        spark.read.table(QUARANTINE_SESSIONS),
        F.count("*"),
        "value"
    )

    target_value = silver_count + quarantine_count

    return ("SESSION_COUNT", source_value, target_value)


# ─────────────────────────────────────────────
# 5. ENERGY_KWH
# Source: COMPLETED energy in silver.sessions
# Target: total energy in gold.agg_daily_station
# ─────────────────────────────────────────────
def check_energy_kwh():

    sessions_df = spark.read.table(SILVER_SESSIONS)

    source_value = get_value(
        sessions_df.filter(
            F.upper(F.col("status")) == "COMPLETED"
        ),
        F.sum("energy_kwh"),
        "value"
    )

    daily_df = spark.read.table(AGG_DAILY_STATION)

    target_value = get_value(
        daily_df,
        F.sum("energy_kwh"),
        "value"
    )

    return ("ENERGY_KWH", source_value, target_value)


# ─────────────────────────────────────────────
# 6. CURRENT_MEMBERS
# Source: distinct member_id in silver.members
# Target: current rows in gold.dim_member
# ─────────────────────────────────────────────
def check_current_members():

    members_df = spark.read.table(SILVER_MEMBERS)

    source_value = get_value(
        members_df,
        F.countDistinct("member_id"),
        "value"
    )

    dim_member_df = spark.read.table(DIM_MEMBER)

    target_value = get_value(
        dim_member_df.filter(
            F.col("is_current") == True
        ),
        F.count("*"),
        "value"
    )

    return ("CURRENT_MEMBERS", source_value, target_value)


# ─────────────────────────────────────────────
# 7. Run checks and append exactly 3 results
# ─────────────────────────────────────────────
def main():

    results = [
        check_session_count(),
        check_energy_kwh(),
        check_current_members()
    ]

    output_rows = []

    for check_name, source_value, target_value in results:

        # Normalize to DECIMAL(18,3) precision before comparison
        source_value = source_value.quantize(Decimal("0.001"))
        target_value = target_value.quantize(Decimal("0.001"))

        status = (
            "PASS"
            if source_value == target_value
            else "FAIL"
        )

        output_rows.append((
            check_name,
            source_value,
            target_value,
            status
        ))

    result_schema = """
        check_name STRING,
        source_value DECIMAL(18,3),
        target_value DECIMAL(18,3),
        status STRING
    """

    results_df = spark.createDataFrame(
        output_rows,
        schema=result_schema
    ).withColumn(
        "run_ts",
        F.current_timestamp()
    ).select(
        "run_ts",
        "check_name",
        "source_value",
        "target_value",
        "status"
    )

    # Append only: do not overwrite previous runs
    (
        results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(TARGET_TABLE)
    )

    print("Reconciliation results for this run:")
    results_df.show(truncate=False)

if __name__ == "__main__":
    main()
