from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DateType, BooleanType
)
import pandas as pd

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CATALOG = "vp_by20021205"

BRONZE_TABLE = f"{CATALOG}.bronze.members"
TARGET_TABLE = f"{CATALOG}.silver.members"

SNAPSHOT_DATE = "2026-04-01"

# ─────────────────────────────────────────────
# 1. Standardize Bronze members
# ─────────────────────────────────────────────
def standardize_members(df: DataFrame) -> DataFrame:
    """
    Apply standardization before processing SCD Type 2.

    member_id       -> TRIM + UPPER
    full_name       -> TRIM
    email           -> TRIM + LOWER
    phone           -> digits only, last 10 digits
    city            -> TRIM
    plan            -> TRIM + UPPER
    joined_on       -> DATE, accepts yyyy-MM-dd or dd/MM/yyyy
    change_effective_date -> DATE
    """

    return (
        df
        .withColumn(
            "member_id",
            F.upper(F.trim(F.col("member_id").cast("string")))
        )
        .withColumn(
            "full_name",
            F.trim(F.col("full_name").cast("string"))
        )
        .withColumn(
            "email",
            F.lower(F.trim(F.col("email").cast("string")))
        )
        .withColumn(
            "phone",
            F.expr(
                "right(regexp_replace(cast(phone as string), "
                "'[^0-9]', ''), 10)"
            )
        )
        .withColumn(
            "city",
            F.trim(F.col("city").cast("string"))
        )
        .withColumn(
            "plan",
            F.upper(F.trim(F.col("plan").cast("string")))
        )
        .withColumn(
            "joined_on",
            F.coalesce(
                F.try_to_date("joined_on", "yyyy-MM-dd"),
                F.try_to_date("joined_on", "dd/MM/yyyy")
            )
        )
        .withColumn(
            "change_effective_date",
            F.to_date("change_effective_date")
        )
        .filter(
            F.col("member_id").isNotNull()
            & (F.col("member_id") != "")
        )
        .select(
            "member_id",
            "full_name",
            "email",
            "phone",
            "city",
            "plan",
            "joined_on",
            "change_effective_date"
        )
        # Identical standardized records count only once
        .dropDuplicates()
    )

# ─────────────────────────────────────────────
# 2. Output schema for SCD Type 2 history
# ─────────────────────────────────────────────
def get_history_schema() -> StructType:
    return StructType([
        StructField("member_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("joined_on", DateType(), True),
        StructField("effective_from", DateType(), False),
        StructField("effective_to", DateType(), True),
        StructField("is_current", BooleanType(), False)
    ])

# ─────────────────────────────────────────────
# 3. Build member version history
# ─────────────────────────────────────────────
def build_member_history(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Process one member's events in date order.

    Snapshot rows:
        effective_from = 2026-04-01

    Plan/city change:
        Close current version and open a new version.

    Name/email/phone-only change:
        Update current version without creating a new version.

    No standardized change:
        Do nothing.
    """

    columns = [
        "member_id", "full_name", "email", "phone",
        "city", "plan", "joined_on",
        "effective_from", "effective_to", "is_current"
    ]

    pdf = pdf.copy()

    snapshot = pdf[
        pdf["change_effective_date"].isna()
    ].copy()

    changes = pdf[
        pdf["change_effective_date"].notna()
    ].copy()

    # If snapshot rows exist, use one baseline record.
    # Exact duplicates have already been removed.
    if not snapshot.empty:
        snapshot = snapshot.sort_values(
            ["member_id", "full_name", "email", "phone",
             "city", "plan"],
            na_position="last"
        )
        base = snapshot.iloc[0].to_dict()
        base_date = pd.Timestamp(SNAPSHOT_DATE).date()
    elif not changes.empty:
        changes = changes.sort_values(
            "change_effective_date"
        )
        base = changes.iloc[0].to_dict()
        base_date = pd.Timestamp(
            base["change_effective_date"]
        ).date()
        changes = changes.iloc[1:].copy()
    else:
        return pd.DataFrame(columns=columns)

    state = {
        "member_id": base["member_id"],
        "full_name": base["full_name"],
        "email": base["email"],
        "phone": base["phone"],
        "city": base["city"],
        "plan": base["plan"],
        "joined_on": base["joined_on"]
    }

    history = [{
        **state,
        "effective_from": base_date,
        "effective_to": None,
        "is_current": True
    }]

    # Process changes chronologically
    changes = changes.sort_values(
        "change_effective_date"
    )

    for _, event in changes.iterrows():

        event_date = pd.Timestamp(
            event["change_effective_date"]
        ).date()

        # Ignore an event dated before the baseline.
        if event_date < history[0]["effective_from"]:
            continue

        new_state = {
            "member_id": event["member_id"],
            "full_name": event["full_name"],
            "email": event["email"],
            "phone": event["phone"],
            "city": event["city"],
            "plan": event["plan"],
            "joined_on": event["joined_on"]
        }

        # Determine what changed after standardization
        tracked_fields = [
            "full_name", "email", "phone",
            "city", "plan"
        ]

        changed = [
            field for field in tracked_fields
            if new_state[field] != state[field]
            and not (
                pd.isna(new_state[field])
                and pd.isna(state[field])
            )
        ]

        # No change: do nothing
        if not changed:
            continue

        current = history[-1]

        # Plan or city change => new SCD Type 2 version
        if "plan" in changed or "city" in changed:

            current["effective_to"] = event_date
            current["is_current"] = False

            state = new_state

            history.append({
                **state,
                "effective_from": event_date,
                "effective_to": None,
                "is_current": True
            })

        # Only full_name/email/phone changed
        elif all(
            field in ["full_name", "email", "phone"]
            for field in changed
        ):
            state = new_state

            current.update({
                "full_name": state["full_name"],
                "email": state["email"],
                "phone": state["phone"]
            })

        # joined_on-only changes do not create a version.
        # joined_on remains the original member attribute.

    return pd.DataFrame(history, columns=columns)

# ─────────────────────────────────────────────
# 4. Create target table once
# ─────────────────────────────────────────────
def create_target_if_not_exists():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            member_id STRING,
            full_name STRING,
            email STRING,
            phone STRING,
            city STRING,
            plan STRING,
            joined_on DATE,
            effective_from DATE,
            effective_to DATE,
            is_current BOOLEAN
        )
        USING DELTA
    """)

# ─────────────────────────────────────────────
# 5. Merge history into persistent Silver
# ─────────────────────────────────────────────
def merge_member_history(history_df: DataFrame):
    """
    Match by member_id + effective_from.

    Update existing versions when attributes or validity
    dates change. Insert new versions. Never overwrite
    or recreate the target table.
    """

    history_df.createOrReplaceTempView(
        "members_history_source"
    )

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING members_history_source AS s
        ON  t.member_id = s.member_id
        AND t.effective_from = s.effective_from

        WHEN MATCHED AND (
            NOT (t.full_name <=> s.full_name)
            OR NOT (t.email <=> s.email)
            OR NOT (t.phone <=> s.phone)
            OR NOT (t.city <=> s.city)
            OR NOT (t.plan <=> s.plan)
            OR NOT (t.joined_on <=> s.joined_on)
            OR NOT (t.effective_to <=> s.effective_to)
            OR NOT (t.is_current <=> s.is_current)
        )
        THEN UPDATE SET
            t.full_name = s.full_name,
            t.email = s.email,
            t.phone = s.phone,
            t.city = s.city,
            t.plan = s.plan,
            t.joined_on = s.joined_on,
            t.effective_to = s.effective_to,
            t.is_current = s.is_current

        WHEN NOT MATCHED THEN INSERT (
            member_id,
            full_name,
            email,
            phone,
            city,
            plan,
            joined_on,
            effective_from,
            effective_to,
            is_current
        )
        VALUES (
            s.member_id,
            s.full_name,
            s.email,
            s.phone,
            s.city,
            s.plan,
            s.joined_on,
            s.effective_from,
            s.effective_to,
            s.is_current
        )
    """)


# ─────────────────────────────────────────────
# 6. Main pipeline
# ─────────────────────────────────────────────
def main():

    # Read Bronze
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Standardize and remove identical rows
    standardized_df = standardize_members(bronze_df)

    # Create history independently for each member
    history_df = (
        standardized_df
        .groupBy("member_id")
        .applyInPandas(
            build_member_history,
            schema=get_history_schema()
        )
    )

    # Create target once, then merge incrementally
    create_target_if_not_exists()
    merge_member_history(history_df)

    # Counts
    print("Standardized source rows:", standardized_df.count())
    print("Generated member versions:", history_df.count())

    display(
        spark.table(TARGET_TABLE)
        .orderBy("member_id", "effective_from")
    )

if __name__ == "__main__":
    main()
