-- dim_players.sql
-- Grain: one row per player_id

WITH player_first_seen AS (
    SELECT
        player_id,
        MIN(session_date) AS install_date
    FROM fact_sessions
    GROUP BY player_id
),

player_attributes AS (
    SELECT
        player_id,
        platform,
        country,
        acquisition_source,

        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY session_date
        ) rn

    FROM fact_sessions
)

SELECT
    pa.player_id,
    pfs.install_date,
    pa.platform,
    pa.country,
    pa.acquisition_source

FROM player_attributes pa
JOIN player_first_seen pfs
    ON pa.player_id = pfs.player_id
WHERE pa.rn = 1
