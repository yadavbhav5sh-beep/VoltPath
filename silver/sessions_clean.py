from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window
from delta.tables import DeltaTable

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CATALOG = "vp_by20021205"

BRONZE_TABLE = f"{CATALOG}.bronze.sessions"
CHARGERS_TABLE = f"{CATALOG}.silver.chargers"
MEMBERS_TABLE = f"{CATALOG}.silver.members"

SESSIONS_TABLE = f"{CATALOG}.silver.sessions"
FAULTS_TABLE = f"{CATALOG}.silver.session_faults"
QUARANTINE_TABLE = f"{CATALOG}.silver.quarantine_sessions"

# ─────────────────────────────────────────────
# 1. Standardize sessions (S1-S7)
# ─────────────────────────────────────────────
def standardize_sessions(df: DataFrame) -> DataFrame:
    """
    Standardization happens before deduplication and validation.

    S1: IDs trim + uppercase; empty -> NULL
    S2: status trim + uppercase
    S3: strict timestamp formats
    S4: meter.end_kwh - meter.start_kwh
    S5: whole duration minutes, rounded down
    S6: PEAK hours 18-21; otherwise OFF_PEAK
    S7: missing idle_minutes -> 0; fault codes uppercase
    """

    # Preserve the complete original Bronze row as JSON.
    df = df.withColumn(
        "raw_record",
        F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )

    # S1
    for c in ["session_id", "charger_id", "member_id"]:
        df = df.withColumn(
            c,
            F.when(
                F.trim(F.col(c).cast("string")) == "",
                F.lit(None).cast("string")
            ).otherwise(
                F.upper(F.trim(F.col(c).cast("string")))
            )
        )

    # S2
    df = df.withColumn(
        "status",
        F.upper(F.trim(F.col("status").cast("string")))
    )

    # Preserve raw timestamp text for strict validation.
    df = (
        df
        .withColumn(
            "started_at_raw",
            F.trim(F.col("started_at").cast("string"))
        )
        .withColumn(
            "ended_at_raw",
            F.trim(F.col("ended_at").cast("string"))
        )
    )

    # S3: IMPORTANT - quote the literal T in the pattern.
    df = (
        df
        .withColumn(
            "started_at_ts",
            F.coalesce(
                F.try_to_timestamp(
                    F.col("started_at_raw"),
                    F.lit("yyyy-MM-dd HH:mm:ss")
                ),
                F.try_to_timestamp(
                    F.col("started_at_raw"),
                    F.lit("yyyy-MM-dd'T'HH:mm:ss")
                )
            )
        )
        .withColumn(
            "ended_at_ts",
            F.coalesce(
                F.try_to_timestamp(
                    F.col("ended_at_raw"),
                    F.lit("yyyy-MM-dd HH:mm:ss")
                ),
                F.try_to_timestamp(
                    F.col("ended_at_raw"),
                    F.lit("yyyy-MM-dd'T'HH:mm:ss")
                )
            )
        )
        .withColumn(
            "updated_at_ts",
            F.try_to_timestamp(F.col("updated_at").cast("string"))
        )
        .withColumn(
            "session_date",
            F.to_date("started_at_ts")
        )
    )

    # S4
    df = df.withColumn(
        "energy_kwh",
        F.when(
            F.col("meter.start_kwh").isNotNull()
            & F.col("meter.end_kwh").isNotNull(),
            (
                F.col("meter.end_kwh").cast("decimal(20,6)")
                - F.col("meter.start_kwh").cast("decimal(20,6)")
            ).cast("decimal(10,3)")
        ).otherwise(F.lit(None).cast("decimal(10,3)"))
    )

    # S5
    df = df.withColumn(
        "duration_minutes",
        F.when(
            F.col("started_at_ts").isNotNull()
            & F.col("ended_at_ts").isNotNull(),
            F.floor(
                (
                    F.col("ended_at_ts").cast("long")
                    - F.col("started_at_ts").cast("long")
                ) / F.lit(60)
            ).cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S6
    df = df.withColumn(
        "time_band",
        F.when(
            F.hour("started_at_ts").between(18, 21),
            F.lit("PEAK")
        ).otherwise(F.lit("OFF_PEAK"))
    )

    # S7
    df = df.withColumn(
        "idle_minutes",
        F.coalesce(
            F.col("idle_minutes").cast("int"),
            F.lit(0)
        )
    )
    # C4: amount_paid from nested payment struct
    df = df.withColumn(
        "amount_paid",
        F.col("payment.amount_inr").cast("decimal(10,2)")
    )

    df = df.withColumn(
        "faults",
        F.transform(
            F.col("faults"),
            lambda x: x.withField(
                "code",
                F.upper(F.trim(x["code"].cast("string")))
            )
        )
    )

    # Keep _ingested_at until deduplication has completed.
    return df.drop(
        "_rescued_data",
        "_source_file",
        "_ingested_at"
    )

# ─────────────────────────────────────────────
# 2. Latest version per session_id (C2)
# ─────────────────────────────────────────────
def latest_session_versions(df: DataFrame) -> DataFrame:
    """
    Validate only the latest updated_at version.

    _ingested_at is used as a tie-breaker when available.
    """

    order_columns = [
        F.col("updated_at_ts").desc_nulls_last()
    ]

    if "_ingested_at" in df.columns:
        order_columns.append(
            F.col("_ingested_at").desc_nulls_last()
        )

    window_spec = (
        Window
        .partitionBy("session_id")
        .orderBy(*order_columns)
    )

    return (
        df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ─────────────────────────────────────────────
# 3. Validate dependencies and rules (C3)
# ─────────────────────────────────────────────
def validate_sessions(
    df: DataFrame,
    chargers_df: DataFrame,
    members_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Reject the entire latest session version if any rule fails.

    R1 UNKNOWN_CHARGER
    R2 UNKNOWN_MEMBER
    R3 INVALID_TIMESTAMP
    R4 INVALID_DURATION
    R5 INVALID_METER

    Lowest-numbered failed rule determines reject_reason.
    """

    charger_ids = (
        chargers_df
        .select(
            F.upper(F.trim(F.col("charger_id"))).alias("_known_charger")
        )
        .filter(F.col("_known_charger").isNotNull())
        .distinct()
    )

    # Only current member versions are valid references.
    member_ids = (
        members_df
        .filter(F.col("is_current") == True)
        .select(
            F.upper(F.trim(F.col("member_id"))).alias("_known_member")
        )
        .filter(F.col("_known_member").isNotNull())
        .distinct()
    )

    checked = (
        df
        .join(
            charger_ids,
            F.col("charger_id") == F.col("_known_charger"),
            "left"
        )
        .join(
            member_ids,
            F.col("member_id") == F.col("_known_member"),
            "left"
        )
        # R1
        .withColumn(
            "_r1",
            F.col("charger_id").isNull()
            | F.col("_known_charger").isNull()
        )
        # R2: NULL member_id is a valid guest session.
        .withColumn(
            "_r2",
            F.col("member_id").isNotNull()
            & F.col("_known_member").isNull()
        )
        # R3
        .withColumn(
            "_r3",
            F.col("started_at_ts").isNull()
        )
        # R4: applies to COMPLETED and FAILED sessions.
        .withColumn(
            "_r4",
            F.col("status").isin("COMPLETED", "FAILED")
            & (
                F.col("ended_at_ts").isNull()
                | F.col("started_at_ts").isNull()
                | (
                    F.col("ended_at_ts")
                    <= F.col("started_at_ts")
                )
            )
        )
        # R5: applies to COMPLETED sessions.
        .withColumn(
            "_r5",
            (F.col("status") == "COMPLETED")
            & (
                F.col("energy_kwh").isNull()
                | (F.col("energy_kwh") < 0)
            )
        )
        .withColumn(
            "reject_reason",
            F.when(F.col("_r1"), "UNKNOWN_CHARGER")
             .when(F.col("_r2"), "UNKNOWN_MEMBER")
             .when(F.col("_r3"), "INVALID_TIMESTAMP")
             .when(F.col("_r4"), "INVALID_DURATION")
             .when(F.col("_r5"), "INVALID_METER")
        )
        .drop("_known_charger", "_known_member")
    )

    invalid_df = checked.filter(
        F.col("reject_reason").isNotNull()
    )

    valid_df = checked.filter(
        F.col("reject_reason").isNull()
    )

    return valid_df, invalid_df


# ─────────────────────────────────────────────
# 4. Create output tables only if missing
# ─────────────────────────────────────────────
def create_tables_if_not_exists():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SESSIONS_TABLE} (
            session_id STRING,
            charger_id STRING,
            member_id STRING,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            session_date DATE,
            status STRING,
            time_band STRING,
            energy_kwh DECIMAL(10,3),
            duration_minutes INT,
            idle_minutes INT,
            amount_paid DECIMAL(10,2),
            updated_at TIMESTAMP
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FAULTS_TABLE} (
            session_id STRING,
            fault_seq INT,
            fault_code STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            session_id STRING,
            reject_reason STRING,
            updated_at TIMESTAMP,
            raw_record STRING,
            quarantined_at TIMESTAMP
        ) USING DELTA
    """)


# ─────────────────────────────────────────────
# 5. Prepare valid and quarantine outputs
# ─────────────────────────────────────────────
def prepare_outputs(
    valid_df: DataFrame,
    invalid_df: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame]:

    sessions_out = valid_df.select(
        "session_id",
        "charger_id",
        "member_id",
        F.col("started_at_ts").alias("started_at"),
        F.col("ended_at_ts").alias("ended_at"),
        "session_date",
        "status",
        "time_band",
        F.col("energy_kwh").cast("decimal(10,3)").alias("energy_kwh"),
        "duration_minutes",
        "idle_minutes",
        F.col("amount_paid").cast("decimal(10,2)").alias("amount_paid"),
        F.col("updated_at_ts").alias("updated_at")
    )

    faults_out = (
        valid_df
        .select(
            "session_id",
            F.posexplode_outer("faults").alias("pos", "fault")
        )
        .filter(F.col("fault").isNotNull())
        .select(
            "session_id",
            (F.col("pos") + 1).cast("int").alias("fault_seq"),
            F.col("fault.code").alias("fault_code")
        )
    )

    quarantine_out = invalid_df.select(
        "session_id",
        "reject_reason",
        F.col("updated_at_ts").alias("updated_at"),
        "raw_record",
        F.current_timestamp().alias("quarantined_at")
    )

    return sessions_out, faults_out, quarantine_out


# ─────────────────────────────────────────────
# 6. MERGE sessions and remove stale invalid rows
# ─────────────────────────────────────────────
def merge_sessions(
    latest_df: DataFrame,
    sessions_out: DataFrame
):
    """
    A session must be in Silver or quarantine, never both.
    If its latest version is rejected, remove its old Silver row.
    """

    latest_df.select("session_id").distinct().createOrReplaceTempView(
        "_latest_session_ids"
    )

    sessions_out.select("session_id").distinct().createOrReplaceTempView(
        "_valid_session_ids"
    )

    spark.sql(f"""
        MERGE INTO {SESSIONS_TABLE} AS t
        USING (
            SELECT l.session_id
            FROM _latest_session_ids l
            LEFT ANTI JOIN _valid_session_ids v
              ON l.session_id = v.session_id
        ) AS s
        ON t.session_id = s.session_id
        WHEN MATCHED THEN DELETE
    """)

    sessions_out.createOrReplaceTempView("_sessions_source")

    spark.sql(f"""
        MERGE INTO {SESSIONS_TABLE} AS t
        USING _sessions_source AS s
        ON t.session_id = s.session_id

        WHEN MATCHED AND (
            NOT (t.charger_id <=> s.charger_id)
            OR NOT (t.member_id <=> s.member_id)
            OR NOT (t.started_at <=> s.started_at)
            OR NOT (t.ended_at <=> s.ended_at)
            OR NOT (t.session_date <=> s.session_date)
            OR NOT (t.status <=> s.status)
            OR NOT (t.time_band <=> s.time_band)
            OR NOT (t.energy_kwh <=> s.energy_kwh)
            OR NOT (t.duration_minutes <=> s.duration_minutes)
            OR NOT (t.idle_minutes <=> s.idle_minutes)
            OR NOT (t.amount_paid <=> s.amount_paid)
            OR NOT (t.updated_at <=> s.updated_at)
        )
        THEN UPDATE SET
            t.charger_id = s.charger_id,
            t.member_id = s.member_id,
            t.started_at = s.started_at,
            t.ended_at = s.ended_at,
            t.session_date = s.session_date,
            t.status = s.status,
            t.time_band = s.time_band,
            t.energy_kwh = s.energy_kwh,
            t.duration_minutes = s.duration_minutes,
            t.idle_minutes = s.idle_minutes,
            t.amount_paid = s.amount_paid,
            t.updated_at = s.updated_at

        WHEN NOT MATCHED THEN INSERT (
            session_id, charger_id, member_id,
            started_at, ended_at, session_date,
            status, time_band, energy_kwh,
            duration_minutes, idle_minutes,
            amount_paid, updated_at
        )
        VALUES (
            s.session_id, s.charger_id, s.member_id,
            s.started_at, s.ended_at, s.session_date,
            s.status, s.time_band, s.energy_kwh,
            s.duration_minutes, s.idle_minutes,
            s.amount_paid, s.updated_at
        )
    """)


# ─────────────────────────────────────────────
# 7. MERGE session faults incrementally
# ─────────────────────────────────────────────
def merge_session_faults(
    latest_df: DataFrame,
    faults_out: DataFrame
):
    """
    Reconcile fault rows only for sessions processed this run.
    Identical rows are not deleted/reinserted.
    """

    latest_df.select("session_id").distinct().createOrReplaceTempView(
        "_affected_session_ids"
    )

    fault_limits = (
        latest_df.select("session_id").distinct()
        .join(
            faults_out.groupBy("session_id").agg(
                F.max("fault_seq").alias("max_fault_seq")
            ),
            "session_id",
            "left"
        )
        .fillna({"max_fault_seq": 0})
    )

    fault_limits.createOrReplaceTempView("_fault_limits")

    # Remove obsolete fault positions only.
    spark.sql(f"""
        MERGE INTO {FAULTS_TABLE} AS t
        USING _fault_limits AS s
        ON t.session_id = s.session_id
        WHEN MATCHED AND t.fault_seq > s.max_fault_seq
        THEN DELETE
    """)

    # Remove old faults for sessions that are now invalid.
    spark.sql(f"""
        DELETE FROM {FAULTS_TABLE}
        WHERE session_id IN (
            SELECT a.session_id
            FROM _affected_session_ids a
            LEFT ANTI JOIN (
                SELECT DISTINCT session_id
                FROM _sessions_source
            ) v
            ON a.session_id = v.session_id
        )
    """)

    faults_out.createOrReplaceTempView("_faults_source")

    spark.sql(f"""
        MERGE INTO {FAULTS_TABLE} AS t
        USING _faults_source AS s
        ON t.session_id = s.session_id
           AND t.fault_seq = s.fault_seq

        WHEN MATCHED AND NOT (t.fault_code <=> s.fault_code)
        THEN UPDATE SET t.fault_code = s.fault_code

        WHEN NOT MATCHED THEN INSERT (
            session_id, fault_seq, fault_code
        )
        VALUES (
            s.session_id, s.fault_seq, s.fault_code
        )
    """)


# ─────────────────────────────────────────────
# 8. MERGE quarantine and recover valid sessions
# ─────────────────────────────────────────────
def merge_quarantine(
    latest_df: DataFrame,
    quarantine_out: DataFrame
):
    """
    Keep one quarantine record per rejected session.
    Remove quarantine records for sessions now valid.
    """

    latest_df.select("session_id").distinct().createOrReplaceTempView(
        "_processed_ids"
    )

    quarantine_out.select("session_id").distinct().createOrReplaceTempView(
        "_rejected_ids"
    )

    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE}
        WHERE session_id IN (
            SELECT p.session_id
            FROM _processed_ids p
            LEFT ANTI JOIN _rejected_ids r
              ON p.session_id = r.session_id
        )
    """)

    quarantine_out.createOrReplaceTempView("_quarantine_source")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING _quarantine_source AS s
        ON t.session_id = s.session_id

        WHEN MATCHED AND (
            NOT (t.reject_reason <=> s.reject_reason)
            OR NOT (t.updated_at <=> s.updated_at)
            OR NOT (t.raw_record <=> s.raw_record)
        )
        THEN UPDATE SET
            t.reject_reason = s.reject_reason,
            t.updated_at = s.updated_at,
            t.raw_record = s.raw_record,
            t.quarantined_at = s.quarantined_at

        WHEN NOT MATCHED THEN INSERT (
            session_id, reject_reason, updated_at,
            raw_record, quarantined_at
        )
        VALUES (
            s.session_id, s.reject_reason, s.updated_at,
            s.raw_record, s.quarantined_at
        )
    """)


# ─────────────────────────────────────────────
# 9. Main pipeline
# ─────────────────────────────────────────────
def main():

    # Read Bronze
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Standardize first
    standardized_df = standardize_sessions(bronze_df)

    # Keep latest version per session_id
    latest_df = latest_session_versions(standardized_df)

    # Read dependencies
    chargers_df = spark.read.table(CHARGERS_TABLE)
    members_df = spark.read.table(MEMBERS_TABLE)

    # Validate latest version only
    valid_df, invalid_df = validate_sessions(
        latest_df,
        chargers_df,
        members_df
    )

    # Prepare output DataFrames
    sessions_out, faults_out, quarantine_out = prepare_outputs(
        valid_df,
        invalid_df
    )

    # Create tables once
    create_tables_if_not_exists()

    # Ensure views are available to the incremental fault logic.
    sessions_out.createOrReplaceTempView("_sessions_source")

    # Incremental target updates
    merge_sessions(latest_df, sessions_out)
    merge_session_faults(latest_df, faults_out)
    merge_quarantine(latest_df, quarantine_out)

    print("=" * 45)
    print("Latest session versions:", latest_df.count())
    print("Valid sessions:", sessions_out.count())
    print("Rejected sessions:", quarantine_out.count())
    print("Valid fault rows:", faults_out.count())
    print("=" * 45)

if __name__ == "__main__":
    main()