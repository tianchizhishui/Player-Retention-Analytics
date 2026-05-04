-- mart_game_performance_metrics.sql
-- Grain: one row per metric_date + platform

WITH base AS (
    SELECT
        session_date AS metric_date,
        platform,
        session_id,

        had_crash,
        had_anr,
        avg_latency_ms,
        p95_latency_ms,
        startup_time_ms

    FROM fact_sessions --FROM {{ ref('fact_sessions') }} for dbt models
),

daily_metrics AS (
    SELECT
        metric_date,
        platform,

        COUNT(DISTINCT session_id) AS total_sessions,

        SUM(had_crash) AS crash_sessions,
        SUM(had_anr) AS anr_sessions,

        SUM(had_crash) * 1.0 / COUNT(DISTINCT session_id) AS crash_session_rate,
        1.0 - (SUM(had_crash) * 1.0 / COUNT(DISTINCT session_id)) AS crash_free_session_rate,

        SUM(had_anr) * 1.0 / COUNT(DISTINCT session_id) AS anr_session_rate,

        AVG(avg_latency_ms) AS avg_latency_ms,
        QUANTILE_CONT(p95_latency_ms, 0.95) AS p95_latency_ms,

        AVG(startup_time_ms) AS avg_startup_time_ms

    FROM base
    GROUP BY
        metric_date,
        platform
)

SELECT *
FROM daily_metrics
ORDER BY
    metric_date,
    platform;