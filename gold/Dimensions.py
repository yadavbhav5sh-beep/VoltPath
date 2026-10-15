from pyspark.sql import functions as F
from pyspark.sql import Window

# ─────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────
CATALOG = "vp_by20021205"

STATIONS_TABLE = f"{CATALOG}.silver.stations"
CHARGERS_TABLE = f"{CATALOG}.silver.chargers"
MEMBERS_TABLE = f"{CATALOG}.silver.members"

DIM_DATE_TABLE = f"{CATALOG}.gold.dim_date"
DIM_STATION_TABLE = f"{CATALOG}.gold.dim_station"
DIM_CHARGER_TABLE = f"{CATALOG}.gold.dim_charger"
DIM_MEMBER_TABLE = f"{CATALOG}.gold.dim_member"

# ─────────────────────────────────────────────
# 2. Create Gold schema if missing
# ─────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# ─────────────────────────────────────────────
# 3. gold.dim_date
# One row per day of April 2026
# ─────────────────────────────────────────────
def create_dim_date():
    dates_df = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit("2026-04-01")),
                    F.to_date(F.lit("2026-04-30")),
                    F.expr("INTERVAL 1 DAY")
                )
            ).alias("calendar_date")
        )
    )

    dim_date_df = dates_df.select(
        F.date_format(
            "calendar_date", "yyyyMMdd"
        ).cast("int").alias("date_key"),

        F.col("calendar_date"),

        F.date_format(
            "calendar_date", "EEEE"
        ).alias("day_name"),

        F.weekofyear(
            "calendar_date"
        ).alias("iso_week"),

        F.dayofweek(
            "calendar_date"
        ).isin(1, 7).alias("is_weekend")
    )

    (
        dim_date_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_DATE_TABLE)
    )

    print("Created:", DIM_DATE_TABLE)

# ─────────────────────────────────────────────
# 4. gold.dim_station
# All columns from silver.stations
# ─────────────────────────────────────────────
def create_dim_station():

    stations_df = spark.read.table(STATIONS_TABLE)

    (
        stations_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_STATION_TABLE)
    )

    print("Created:", DIM_STATION_TABLE)

# ─────────────────────────────────────────────
# 5. gold.dim_charger
# All silver.chargers columns except
# last_changed_snapshot_date
# Plus station city
# ─────────────────────────────────────────────
def create_dim_charger():

    chargers_df = spark.read.table(CHARGERS_TABLE)
    stations_df = spark.read.table(STATIONS_TABLE)

    # Select charger columns except the snapshot tracking column.
    charger_columns = [
        F.col(f"c.`{column}`")
        for column in chargers_df.columns
        if column != "last_changed_snapshot_date"
    ]

    # Join stations to get the station's city.
    dim_charger_df = (
        chargers_df.alias("c")
        .join(
            stations_df.select(
                "station_id", "city"
            ).alias("s"),
            F.col("c.station_id") == F.col("s.station_id"),
            "left"
        )
        .select(
            *charger_columns,
            F.col("s.city").alias("city")
        )
    )

    (
        dim_charger_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_CHARGER_TABLE)
    )

    print("Created:", DIM_CHARGER_TABLE)

# ─────────────────────────────────────────────
# 6. gold.dim_member
# All silver.members columns + member_sk BIGINT
# One unique key per member version
# ─────────────────────────────────────────────
def create_dim_member():

    members_df = spark.read.table(MEMBERS_TABLE)

    # Deterministic ordering for member versions.
    order_columns = [
        F.col("member_id").asc_nulls_first(),
        F.col("effective_from").asc_nulls_first()
    ]

    # Add tie-breakers if multiple versions have the same start date.
    for column in [
        "effective_to",
        "full_name",
        "email",
        "phone",
        "city",
        "plan",
        "joined_on",
        "is_current"
    ]:
        if column in members_df.columns:
            order_columns.append(
                F.col(column).asc_nulls_first()
            )

    window_spec = Window.orderBy(*order_columns)

    dim_member_df = (
        members_df
        .withColumn(
            "member_sk",
            F.row_number().over(window_spec).cast("long")
        )
        .select(
            "member_sk",
            *members_df.columns
        )
    )

    (
        dim_member_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_MEMBER_TABLE)
    )

    print("Created:", DIM_MEMBER_TABLE)

# ─────────────────────────────────────────────
# 7. Main execution
# ─────────────────────────────────────────────
def main():

    create_dim_date()
    create_dim_station()
    create_dim_charger()
    create_dim_member()

    print("=" * 45)
    print("All Gold dimension tables created.")
    print("=" * 45)


if __name__ == "__main__":
    main()
