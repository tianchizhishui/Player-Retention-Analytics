-- fact_sessions.sql

WITH player_sessions AS (
    SELECT
        session_id,
        player_id,
        event_time AS session_start_time,
        event_date AS session_date,
        platform,
        country,
        acquisition_source,
        session_length_sec
    From read_csv_auto('data/raw/player_events.csv')
    WHERE event_name = 'session_start'
),

level_events AS (
    SELECT
        session_id,
        COUNT(CASE WHEN event_name = 'level_start' THEN 1 END) AS level_start_count,
        COUNT(CASE WHEN event_name = 'level_complete' THEN 1 END) AS level_complete_count,
        MAX(level_num) AS max_level_reached
    FROM read_csv_auto('data/raw/player_events.csv')
    WHERE event_name = 'level_up'
    GROUP BY session_id
),

performance_events AS (
    SELECT
        session_id,
        MAX(CASE WHEN is_crash = 1 THEN 1 ELSE 0 END) AS had_crash,
        MAX(CASE WHEN is_anr = 1 THEN 1 ELSE 0 END) AS had_anr,
        AVG(latency_ms) AS avg_latency_ms,
        QUANTILE(latency_ms, 0.95) AS p95_latency_ms,
        MAX(startup_time_ms) AS startup_time-ms
    FROM read_csv_auto('data/raw/game_performance_events.csv')
    WHERE session_id IS NOT NULL
    GROUP BY session_id
)

SELECT
    ps.session_id,
    ps.player_id,
    CAST(ps.session_start_time AS TIMESTAMP) AS session_start_time,
    CAST(ps.session_date AS DATE) AS session_date   ,
    LOWER(ps.platform) AS platform,
    LOWER(ps.country) AS country,
    LOWER(ps.acquisition_source) AS acquisition_source,

    ps.session_length_sec,

    COALESCE(le.level_start_count, 0) AS level_start_count,
    COALESCE(le.level_complete_count, 0) AS level_complete_count,
    COALESCE(le.max_level_reached, 0) AS max_level_reached,
    
    COALESCE(pe.had_crash, 0) AS had_crash,
    COALESCE(pe.had_anr, 0) AS had_anr,
    pe.avg_latency_ms,
    pe.p95_latency_ms,
    pe.startup_time_ms
FROM player_sessions ps
LEFT JOIN level_events le 
    ON ps.session_id = le.session_id
LEFT JOIN performance_events pe 
    ON ps.session_id = pe.session_id

