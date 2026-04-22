import os
import pandas as pd
import duckdb
import matplotlib.pyplot as plt


def main():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    VISUALS_DIR = os.path.join(BASE_DIR, "visuals")
    os.makedirs(VISUALS_DIR, exist_ok=True)

    DATA_PATH = os.path.join(BASE_DIR, "data", "player_events_mock_v2.csv")
    df = pd.read_csv(DATA_PATH)

    with duckdb.connect() as con:
        con.register("player_events", df)

        print("Running Player Retention Analytics Project...\n")

        # 1. Daily Active Users (DAU)
        dau = con.execute("""
        SELECT
            DATE(event_time) AS date,
            COUNT(DISTINCT player_id) AS dau
        FROM player_events
        GROUP BY date
        ORDER BY date
        """).df()

        print("=== DAU ===")
        print(dau.head(), "\n")

        # 2. D1 retention
        d1_retention = con.execute("""
        WITH installs AS (
            SELECT DISTINCT player_id, DATE(install_date) AS install_date
            FROM player_events
        ),
        returns_d1 AS (
            SELECT DISTINCT i.player_id, i.install_date
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
            ROUND(COUNT(DISTINCT r.player_id) * 1.0 / COUNT(DISTINCT i.player_id), 3) AS d1_retention
        FROM installs i
        LEFT JOIN returns_d1 r
        ON i.player_id = r.player_id
        AND i.install_date = r.install_date
        GROUP BY i.install_date
        ORDER BY i.install_date
        """).df()

        print("=== D1 Retention ===")
        print(d1_retention.head(), "\n")

        # 3. D1 retention by platform
        retention_platform = con.execute("""
        WITH installs AS (
            SELECT DISTINCT player_id, DATE(install_date) AS install_date, platform
            FROM player_events
        ),
        returns_d1 AS (
            SELECT DISTINCT i.player_id, i.install_date
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
            ROUND(COUNT(DISTINCT r.player_id) * 1.0 / COUNT(DISTINCT i.player_id), 3) AS d1_retention
        FROM installs i
        LEFT JOIN returns_d1 r
        ON i.player_id = r.player_id
        AND i.install_date = r.install_date
        GROUP BY i.install_date, i.platform
        ORDER BY i.install_date, i.platform
        """).df()

        print("=== D1 Retention by Platform ===")
        print(retention_platform.head(), "\n")

        # 4. Funnel analysis
        funnel = con.execute("""
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
            (SELECT COUNT(*) FROM d1_retained) AS d1_retained
        """).df()

        print("=== Funnel ===")
        print(funnel, "\n")

        # 5. D7 retention
        d7_retention = con.execute("""
        WITH installs AS (
            SELECT DISTINCT player_id, DATE(install_date) AS install_date
            FROM player_events
        ),
        returns_d7 AS (
            SELECT DISTINCT i.player_id, i.install_date
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
            ROUND(COUNT(DISTINCT r.player_id) * 1.0 / COUNT(DISTINCT i.player_id), 3) AS d7_retention
        FROM installs i
        LEFT JOIN returns_d7 r
        ON i.player_id = r.player_id
        AND i.install_date = r.install_date
        GROUP BY i.install_date
        ORDER BY i.install_date
        """).df()

        print("=== D7 Retention ===")
        print(d7_retention.head(), "\n")

        # 6. Session depth
        session_depth = con.execute("""
        SELECT
            player_id,
            COUNT(DISTINCT session_id) AS session_count
        FROM player_events
        WHERE event_name = 'session_start'
        GROUP BY player_id
        ORDER BY session_count DESC, player_id
        """).df()

        print("=== Session Depth ===")
        print(session_depth.head(), "\n")

        # 7. Save DAU trend chart
        plt.figure(figsize=(8, 4))
        plt.plot(dau["date"], dau["dau"])
        plt.xticks(rotation=45)
        plt.title("DAU Trend")
        plt.xlabel("Date")
        plt.ylabel("DAU")
        plt.tight_layout()
        plt.savefig(os.path.join(VISUALS_DIR, "dau_trend.png"))
        plt.close()

        # 8. Save session distribution chart
        plt.figure(figsize=(8, 4))
        plt.hist(session_depth["session_count"], bins=10)
        plt.title("Session Distribution per Player")
        plt.xlabel("Sessions")
        plt.ylabel("Number of Players")
        plt.tight_layout()
        plt.savefig(os.path.join(VISUALS_DIR, "session_distribution.png"))
        plt.close()

        print("Project execution completed successfully.")

if __name__ == "__main__":
    main()