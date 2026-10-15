
WITH station_days AS (
    SELECT
        station_id,
        COUNT(DISTINCT session_date) AS total_days
    FROM vp_by20021205.gold.agg_daily_station
    GROUP BY station_id
),
charger_counts AS (
    SELECT
        station_id,
        COUNT(DISTINCT charger_id) AS charger_count
    FROM vp_by20021205.gold.dim_charger
    GROUP BY station_id
),
station_metrics AS (
    SELECT
        f.station_id,
        SUM(
            CASE WHEN UPPER(f.status) = 'COMPLETED'
                 THEN 1 ELSE 0 END
        ) AS completed_sessions,
        SUM(
            CASE WHEN UPPER(f.status) = 'COMPLETED'
                 THEN COALESCE(f.energy_kwh, 0)
                 ELSE 0 END
        ) AS energy_kwh,
        SUM(
            CASE WHEN UPPER(f.status) = 'COMPLETED'
                 THEN COALESCE(f.duration_minutes, 0)
                 ELSE 0 END
        ) AS completed_minutes
    FROM vp_by20021205.gold.fact_sessions f
    GROUP BY f.station_id
)
SELECT
    s.station_id,
    s.city,
    COALESCE(m.completed_sessions, 0) AS completed_sessions,
    CAST(COALESCE(m.energy_kwh, 0) AS DECIMAL(12,3))
        AS energy_kwh,
    CAST(
        CASE
            WHEN COALESCE(c.charger_count, 0) > 0
             AND COALESCE(d.total_days, 0) > 0
            THEN 100.0 * COALESCE(m.completed_minutes, 0)
                 / (c.charger_count * 1440.0 * d.total_days)
            ELSE 0.0
        END
        AS DECIMAL(5,2)
    ) AS utilisation_pct
FROM vp_by20021205.gold.dim_station s
LEFT JOIN station_metrics m
    ON s.station_id = m.station_id
LEFT JOIN charger_counts c
    ON s.station_id = c.station_id
LEFT JOIN station_days d
    ON s.station_id = d.station_id
ORDER BY utilisation_pct DESC, s.station_id ASC;
