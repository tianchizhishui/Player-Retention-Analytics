-- fact_game_performance.sql
-- Grain: one row per performance event

WITH source AS (
    SELECT *
    FROM read_csv_auto('data/raw/game_performance_events.csv') -- {{ ref('stg_game_performance_events') }}
),

cleaned AS (
    SELECT
        event_id,
        player_id,
        session_id,

        CAST(event_time AS TIMESTAMP) AS event_time,
        CAST(event_date AS DATE) AS event_date,

        LOWER(platform) AS platform,
        app_version,
        device_model,
        LOWER(event_type) AS event_type,

        latency_ms,
        startup_time_ms,

        CASE WHEN is_crash = 1 THEN 1 ELSE 0 END AS is_crash,
        CASE WHEN is_anr = 1 THEN 1 ELSE 0 END AS is_anr

    FROM source
)

SELECT *
FROM cleaned;