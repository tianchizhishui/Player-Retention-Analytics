-- ============================================
-- Player Retention Analytics SQL Queries
-- ============================================

-- Table: player_events
-- Fields:
-- player_id, install_date, event_time, platform, country,
-- acquisition_source, event_name, session_id, level_num, session_length_sec


-- ============================================
-- 1. Daily Active Users (DAU)
-- ============================================
-- Definition:
-- Number of distinct active players per day

SELECT
    DATE(event_time) AS date,
    COUNT(DISTINCT player_id) AS dau
FROM player_events
GROUP BY date
ORDER BY date;


-- ============================================
-- 2. D1 Retention (Cohort-based)
-- ============================================
-- Definition:
-- Percentage of users who return 1 day after install

WITH installs AS (
    SELECT DISTINCT
        player_id,
        DATE(install_date) AS install_date
    FROM player_events
),
returns_d1 AS (
    SELECT DISTINCT
        i.player_id,
        i.install_date
    FROM installs i
    JOIN player_events e
        ON i.player_id = e.player_id
       AND DATE(e.event_time) = i.install_date + INTERVAL '1 day'
       AND e.event_name = 'session_start'
)
SELECT
    i.install_date,
    COUNT(DISTINCT i.player_id) AS installers,
    COUNT(DISTINCT r.player_id) AS retained,
    ROUND(
        COUNT(DISTINCT r.player_id) * 1.0 / COUNT(DISTINCT i.player_id),
        3
    ) AS d1_retention
FROM installs i
LEFT JOIN returns_d1 r
    ON i.player_id = r.player_id
   AND i.install_date = r.install_date
GROUP BY i.install_date
ORDER BY i.install_date;


-- ============================================
-- 3. D1 Retention by Platform
-- ============================================

WITH installs AS (
    SELECT DISTINCT
        player_id,
        DATE(install_date) AS install_date,
        platform
    FROM player_events
),
returns_d1 AS (
    SELECT DISTINCT
        i.player_id,
        i.install_date
    FROM installs i
    JOIN player_events e
        ON i.player_id = e.player_id
       AND DATE(e.event_time) = i.install_date + INTERVAL '1 day'
       AND e.event_name = 'session_start'
)
SELECT
    i.install_date,
    i.platform,
    COUNT(DISTINCT i.player_id) AS installers,
    COUNT(DISTINCT r.player_id) AS retained,
    ROUND(
        COUNT(DISTINCT r.player_id) * 1.0 / COUNT(DISTINCT i.player_id),
        3
    ) AS d1_retention
FROM installs i
LEFT JOIN returns_d1 r
    ON i.player_id = r.player_id
   AND i.install_date = r.install_date
GROUP BY i.install_date, i.platform
ORDER BY i.install_date, i.platform;


-- ============================================
-- 4. Funnel Analysis
-- ============================================
-- Install → Session Start → D1 Retained

WITH installs AS (
    SELECT DISTINCT player_id
    FROM player_events
    WHERE event_name = 'install'
),
sessions AS (
    SELECT DISTINCT player_id
    FROM player_events
    WHERE event_name = 'session_start'
),
d1_retained AS (
    SELECT DISTINCT i.player_id
    FROM player_events i
    JOIN player_events e
        ON i.player_id = e.player_id
       AND DATE(e.event_time) = DATE(i.install_date) + INTERVAL '1 day'
       AND e.event_name = 'session_start'
    WHERE i.event_name = 'install'
)
SELECT
    (SELECT COUNT(*) FROM installs) AS installs,
    (SELECT COUNT(*) FROM sessions) AS sessions,
    (SELECT COUNT(*) FROM d1_retained) AS d1_retained;


-- ============================================
-- 5. D7 Retention
-- ============================================
-- Definition:
-- Percentage of users who return 7 days after install

WITH installs AS (
    SELECT DISTINCT
        player_id,
        DATE(install_date) AS install_date
    FROM player_events
),
returns_d7 AS (
    SELECT DISTINCT
        i.player_id,
        i.install_date
    FROM installs i
    JOIN player_events e
        ON i.player_id = e.player_id
       AND DATE(e.event_time) = i.install_date + INTERVAL '7 day'
       AND e.event_name = 'session_start'
)
SELECT
    i.install_date,
    COUNT(DISTINCT i.player_id) AS installers,
    COUNT(DISTINCT r.player_id) AS retained,
    ROUND(
        COUNT(DISTINCT r.player_id) * 1.0 / COUNT(DISTINCT i.player_id),
        3
    ) AS d7_retention
FROM installs i
LEFT JOIN returns_d7 r
    ON i.player_id = r.player_id
   AND i.install_date = r.install_date
GROUP BY i.install_date
ORDER BY i.install_date;


-- ============================================
-- 6. Session Depth (Engagement)
-- ============================================
-- Definition:
-- Number of sessions per player

SELECT
    player_id,
    COUNT(DISTINCT session_id) AS session_count
FROM player_events
WHERE event_name = 'session_start'
GROUP BY player_id
ORDER BY session_count DESC, player_id;