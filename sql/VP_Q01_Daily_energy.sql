
SELECT
    session_date,
    SUM(completed_sessions) AS completed_sessions,
    CAST(SUM(energy_kwh) AS DECIMAL(12,3)) AS energy_kwh,
    CAST(SUM(billed_amount) AS DECIMAL(12,2)) AS total_billed,
    CAST(SUM(amount_paid) AS DECIMAL(12,2)) AS total_paid
FROM vp_by20021205.gold.agg_daily_station
GROUP BY session_date
ORDER BY session_date ASC;
