-- data_quality_checks.sql
-- Purpose: basic data quality checks for the analytics engineering project

-- 1. player_events: event_id should not be null

SELECT
    'player_events_event_id_not_null' AS test_name,
    COUNT(*) AS failed_rows
FROM {{ ref('stg_game_performance_events') }}
WHERE event_id IS NULL;


-- 2. player_events: player_id should not be null
SELECT
    'player_events_player_id_not_null' AS test_name,
    COUNT(*) AS failed_rows
FROM {{ ref('stg_game_performance_events') }}
WHERE player_id IS NULL;


-- 3. player_events: event_id should not be unique
SELECT
    'player_events_event_id_unique' AS test_name,
    COUNT(*) AS failed_rows
FROM (
    SELECT event_id
    FROM {{ ref('stg_game_performance_events') }}
    GROUP BY event_id
    HAVING COUNT(*) > 1
);

-- 4. session_start events should have session_id
SELECT
    'session_start_session_id_not_null' AS test_name,
    COUNT(*) AS failed_rows
FROM {{ ref('stg_game_performance_events') }}
WHERE event_name = 'session_start' 
    AND session_id IS NULL;

-- 5. game_performance_events: event_id should be unique
SELECT
    'game_performance_event_id_unique' AS test_name,
    COUNT(*) AS failed_rows
FROM (
    SELECT event_id
    FROM {{ ref('stg_game_performance_events') }}
    GROUP BY event_id
    HAVING COUNT(*) > 1
);


-- 6. game_performance_events: session_id should not be null
SELECT
    'game_performance_session_id_not_null' AS test_name,
    COUNT(*) AS failed_rows
FROM {{ ref('stg_game_performance_events') }}
WHERE session_id IS NULL;


-- 7. game_performance_events: event_type should be valid
SELECT
    'game_performance_valid_event_type' AS test_name,
    COUNT(*) AS failed_rows
FROM {{ ref('stg_game_performance_events') }}
WHERE event_type NOT IN ('startup', 'latency', 'crash', 'anr');


-- 8. latency events should have latency_ms
SELECT
    'latency_event_latency_ms_not_null' AS test_name,
    COUNT(*) AS failed_rows
FROM {{ ref('stg_game_performance_events') }}
WHERE event_type = 'latency' 
    AND latency_ms IS NULL; 


-- 9. startup events should have startup_time_ms
SELECT
    'startup_event_startup_time_ms_not_null' AS test_name,
    COUNT(*) AS failed_rows
FROM {{ ref('stg_game_performance_events') }}
WHERE event_type = 'startup'
    AND startup_time_ms IS NULL;


-- 10. crash / ANR flags should only be 0 or 1
SELECT
    'performance_flags_valid_values' AS test_name,
    COUNT(*) AS failed_rows
FROM {{ ref('stg_game_performance_events') }}
WHERE is_crash NOT IN (0, 1)
    OR is_anr NOT IN (0, 1);