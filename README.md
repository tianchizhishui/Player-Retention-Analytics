# Player Retention & Game Performance Analytics Engineering Project

## Overview

This project simulates a modern analytics engineering workflow for a mobile game environment. It models raw player behavior events and game performance telemetry into reusable, self-serve analytics datasets using a layered data modeling approach inspired by modern data stack practices.

The project demonstrates:

- Analytics engineering workflow design
- Data modeling using fact and dimension tables
- Session-level and event-level modeling
- Retention and game health KPI modeling
- Self-serve semantic-layer-style marts
- Data quality validation practices
- Mobile game performance analytics

---

# Project Goals

The primary goals of this project are:

1. Simulate raw player and performance telemetry data
2. Build a structured analytics data model
3. Standardize game analytics KPIs
4. Create reusable mart-layer datasets for downstream analytics
5. Demonstrate modern analytics engineering principles

---

# Tech Stack

| Component | Technology |
|---|---|
| Programming | Python |
| Data Processing | Pandas |
| SQL Engine | DuckDB |
| Data Modeling | SQL |
| Visualization / Exploration | Jupyter Notebook |
| Version Control | Git |

---

# Architecture

```text
Raw Event Data
    ↓
Staging Models
    ↓
Fact / Dimension Models
    ↓
Mart Layer (Semantic Layer)
    ↓
Self-Serve Analytics / Dashboarding
```

This architecture follows a modern analytics engineering pattern similar to workflows built with dbt + Looker.

---

# Project Structure

```text
player-retention-analytics/
  data/
    raw/
    processed/

  src/
    main.py
    generate_mock_data.py

  sql/
    retention_queries.sql

  models/
    staging/
      stg_player_events.sql
      stg_game_performance_events.sql

    marts/
      dim_players.sql
      fact_sessions.sql
      fact_game_performance.sql
      mart_player_retention.sql
      mart_game_performance_metrics.sql

  tests/
    data_quality_checks.sql

  notebooks/
    retention_analysis.ipynb

  README.md
```

---

# Raw Data Sources

## player_events.csv

Simulates player gameplay and engagement events.

### Example Event Types

- install
- session_start
- level_start
- level_complete

### Example Fields

- player_id
- session_id
- event_time
- platform
- country
- acquisition_source
- level_num
- session_length_sec

---

## game_performance_events.csv

Simulates mobile game telemetry and performance monitoring events.

### Example Event Types

- startup
- latency
- crash
- anr

### Example Fields

- player_id
- session_id
- platform
- app_version
- device_model
- latency_ms
- startup_time_ms
- is_crash
- is_anr

---

# Data Modeling

## Fact Tables

### fact_sessions

Grain:

```text
One row per player session
```

Purpose:

Aggregates gameplay behavior and performance telemetry into a reusable session-level fact table.

Key Metrics:

- session_length_sec
- level_start_count
- level_complete_count
- had_crash
- had_anr
- avg_latency_ms
- p95_latency_ms
- startup_time_ms

---

### fact_game_performance

Grain:

```text
One row per performance event
```

Purpose:

Stores detailed performance telemetry including startup, latency, crash, and ANR events.

---

## Dimension Tables

### dim_players

Grain:

```text
One row per player
```

Purpose:

Stores reusable player attributes for downstream analytics.

Attributes:

- platform
- country
- acquisition_source
- install_date

---

# Mart Layer (Semantic Layer)

The mart layer simulates a Looker-style semantic layer by exposing reusable, standardized metrics for self-serve analytics.

---

## mart_player_retention

Grain:

```text
One row per install_date + platform + acquisition_source
```

Metrics:

- installs
- retained_d1_players
- retained_d7_players
- d1_retention_rate
- d7_retention_rate

Purpose:

Supports cohort retention analysis for product and analytics teams.

---

## mart_game_performance_metrics

Grain:

```text
One row per metric_date + platform
```

Metrics:

- total_sessions
- crash_sessions
- anr_sessions
- crash_session_rate
- crash_free_session_rate
- anr_session_rate
- avg_latency_ms
- p95_latency_ms
- avg_startup_time_ms

Purpose:

Supports engineering and product monitoring for mobile game health and player experience.

---

# Data Quality Checks

The project includes SQL-based data quality validation checks.

Implemented validations:

- Not-null checks
- Unique key validation
- Accepted event type validation
- Session integrity checks
- Metric-specific business rule checks

Example validations:

- event_id uniqueness
- player_id presence
- valid performance event types
- latency events contain latency values
- startup events contain startup metrics

---

# Example Analytics Use Cases

## Product Analytics

- DAU tracking
- D1 / D7 retention analysis
- Cohort analysis
- Session engagement analysis

---

## Engineering Analytics

- Crash-free session rate monitoring
- ANR monitoring
- Startup time monitoring
- Latency percentile analysis
- Platform performance comparison

---

# Key Analytics Engineering Concepts Demonstrated

- Layered data modeling
- Fact and dimension modeling
- Star schema principles
- Semantic layer design
- KPI standardization
- Self-serve analytics enablement
- Data quality validation
- Mobile game telemetry modeling
- SQL-based transformation workflows

---

# Future Improvements

Potential future enhancements include:

- dbt migration
- Looker dashboard integration
- Airflow orchestration
- Incremental data models
- Real-time streaming ingestion
- Advanced anomaly detection
- CI/CD pipeline integration
- Automated lineage documentation

---

# Resume Summary

This project demonstrates hands-on experience with:

- Analytics engineering workflows
- Modern data modeling practices
- Mobile game analytics
- Self-serve analytics design
- Data quality engineering
- SQL transformation pipelines
- KPI standardization and semantic-layer mod