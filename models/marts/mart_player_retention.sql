-- mart_player_retention.sql
-- Grain: one row per install_date + platform + acquisition_source

WITH installs AS (
    SELECT
        player_id,
        MIN(event_date) AS install_date,
        platform,
        acquisition_source
    FROM fact_sessions
    GROUP BY
        player_id,
        platform,
        acquisition_source
),

sessions AS (
    SELECT
        player_id,
        session_date
    FROM fact_sessions --FROM {{ ref('fact_sessions') }} for dbt models
),

retention_flags AS (
    SELECT
        i.install_date,
        i.platform,
        i.acquisition_source,
        i.player_id,

        MAX(CASE 
            WHEN s.session_date = i.install_date + INTERVAL 1 DAY 
            THEN 1 ELSE 0 
        END) AS retained_d1,

        MAX(CASE 
            WHEN s.session_date = i.install_date + INTERVAL 7 DAY 
            THEN 1 ELSE 0 
        END) AS retained_d7

    FROM installs i
    LEFT JOIN sessions s
        ON i.player_id = s.player_id
    GROUP BY
        i.install_date,
        i.platform,
        i.acquisition_source,
        i.player_id
),

cohort_metrics AS (
    SELECT
        install_date,
        platform,
        acquisition_source,

        COUNT(DISTINCT player_id) AS installs,

        SUM(retained_d1) AS retained_d1_players,
        SUM(retained_d7) AS retained_d7_players,

        SUM(retained_d1) * 1.0 / COUNT(DISTINCT player_id) AS d1_retention_rate,
        SUM(retained_d7) * 1.0 / COUNT(DISTINCT player_id) AS d7_retention_rate

    FROM retention_flags
    GROUP BY
        install_date,
        platform,
        acquisition_source
)

SELECT *
FROM cohort_metrics
ORDER BY
    install_date,
    platform,
    acquisition_source;