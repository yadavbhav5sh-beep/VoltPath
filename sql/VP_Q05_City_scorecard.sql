WITH city_metrics AS (
    SELECT
        SUM(
            CASE WHEN UPPER(f.status) = 'COMPLETED'
                 THEN 1 ELSE 0 END
        ) AS completed_sessions,

        SUM(
            CASE WHEN UPPER(f.status) = 'FAILED'
                 THEN 1 ELSE 0 END
        ) AS failed_sessions,

        SUM(
            CASE WHEN UPPER(f.status) = 'COMPLETED'
                 THEN COALESCE(f.energy_kwh, 0)
                 ELSE 0 END
        ) AS energy_kwh,

        SUM(
            CASE WHEN UPPER(f.status) = 'COMPLETED'
                 THEN COALESCE(f.billed_amount, 0)
                 ELSE 0 END
        ) AS total_billed

    FROM vp_by20021205.gold.fact_sessions f
    INNER JOIN vp_by20021205.gold.dim_station s
        ON f.station_id = s.station_id
    INNER JOIN vp_by20021205.gold.dim_date d
        ON f.date_key = d.date_key
    WHERE LOWER(TRIM(s.city)) = LOWER(TRIM(:city))
      AND d.calendar_date BETWEEN
          COALESCE(try_cast(:start_date AS DATE), DATE '1900-01-01')
          AND COALESCE(try_cast(:end_date AS DATE), DATE '9999-12-31')
)
SELECT
    COALESCE(completed_sessions, 0) AS completed_sessions,
    CAST(COALESCE(energy_kwh, 0) AS DECIMAL(12,3))
        AS energy_kwh,
    CAST(COALESCE(total_billed, 0) AS DECIMAL(12,2))
        AS total_billed,
    CAST(
        CASE
            WHEN COALESCE(completed_sessions, 0)
               + COALESCE(failed_sessions, 0) > 0
            THEN 100.0 * completed_sessions
                 / (completed_sessions + failed_sessions)
            ELSE 0.0
        END
        AS DECIMAL(5,2)
    ) AS success_rate_pct
FROM city_metrics;