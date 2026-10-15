
WITH charger_metrics AS (
    SELECT
        charger_id,
        SUM(
            CASE WHEN UPPER(status) = 'COMPLETED'
                 THEN 1 ELSE 0 END
        ) AS completed_sessions,
        SUM(
            CASE WHEN UPPER(status) = 'FAILED'
                 THEN 1 ELSE 0 END
        ) AS failed_sessions
    FROM vp_by20021205.gold.fact_sessions
    WHERE UPPER(status) IN ('COMPLETED', 'FAILED')
    GROUP BY charger_id
)
SELECT
    charger_id,
    completed_sessions,
    failed_sessions,
    CAST(
        100.0 * failed_sessions
        / (completed_sessions + failed_sessions)
        AS DECIMAL(5,2)
    ) AS failure_rate_pct
FROM charger_metrics
WHERE completed_sessions + failed_sessions >= 10
ORDER BY failure_rate_pct DESC, charger_id ASC
LIMIT 10;
