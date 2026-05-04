-- stg_player_events.sql

WITH source AS (
    SELECT *
    FROM read_csv_auto('data/raw/player_events.csv')
)

cleaned AS (
    SELECT
        event_id,
        player_id,
        session_id,
        CAST(event_time AS TIMESTAMP) AS event_time,
        CAST(event_data AS DATE) AS event_date,
        LOWER(event_name) AS event_name,
        LOWER(platform) AS platform,
        LOWER(country) AS country,
        LOWER(acquisition_source) AS acquisition_source,
        level_num,
        session_length_sec
    FROM source
)

SELECT *
FROM cleaned