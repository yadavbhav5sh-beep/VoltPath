
SELECT
    plan_at_session,
    COUNT(*) AS completed_sessions,
    SUM(
        CASE WHEN billing_status = 'MISMATCH'
             THEN 1 ELSE 0 END
    ) AS mismatch_sessions,
    CAST(
        SUM(COALESCE(billed_amount, 0))
        - SUM(COALESCE(amount_paid, 0))
        AS DECIMAL(12,2)
    ) AS revenue_leakage
FROM vp_by20021205.gold.fact_sessions
WHERE UPPER(status) = 'COMPLETED'
GROUP BY plan_at_session
ORDER BY CASE UPPER(plan_at_session)
    WHEN 'BASIC' THEN 1
    WHEN 'PRO' THEN 2
    WHEN 'FLEET' THEN 3
    WHEN 'GUEST' THEN 4
    ELSE 5
END;
