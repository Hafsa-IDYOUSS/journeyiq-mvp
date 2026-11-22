# JourneyIQ — E-commerce Customer Journey Analytics

An end-to-end data engineering and analytics pipeline built to process Google Analytics 4 (GA4) e-commerce event data and analyze customer journeys, conversion rates, and drop-off across the purchasing funnel.

## Tech Stack

**BigQuery** · **Databricks** · **PySpark** · **Delta Lake** · **dbt** · **SQL** · **Databricks AI/BI Dashboards**

## Architecture

**BigQuery (GA4) → Databricks Bronze → PySpark Silver → dbt Gold → Analytics Dashboard**

The pipeline follows the **Medallion Architecture**:

- **Bronze — Data Ingestion:** Extracted GA4 e-commerce events from BigQuery and ingested them into Databricks as Delta tables.
- **Silver — Data Transformation:** Used PySpark to clean and structure events, convert timestamps, parse nested JSON attributes, and construct unique session identifiers.
- **Gold — Analytics Modeling:** Built dbt SQL models for session-level funnel analysis, conversion metrics, and drop-off calculations, with automated data-quality tests.
- **Visualization:** Developed a Databricks AI/BI dashboard to explore customer journey performance and conversion trends. (an overview of shopping sessions, funnel progression, conversion rates, and customer drop-off.)

## Key Results

- Processed **328,463 GA4 events** across **25,453 sessions**.
- Modeled a session-based conversion funnel: **Product View → Checkout → Purchase**.
- Calculated stage-level conversion rates, overall conversion, and drop-off metrics.
- Implemented **10 dbt data-quality tests**, all passing.
- Built an interactive dashboard highlighting funnel performance and purchasing behavior.

## Project Structure

```text
JourneyIQ/
├── notebooks/
│   ├── 01_bronze_ingestion
│   └── 02_silver_transformations
├── dbt/
│   ├── models/
│   │   └── journeyiq/
│   │       ├── stg_ga4_events.sql
│   │       ├── int_session_funnel.sql
│   │       ├── fct_funnel_metrics.sql
│   │       ├── _sources.yml
│   │       └── _models.yml
│   └── tests/
│       └── assert_funnel_progression.sql
└── README.md
```

**Dataset:** Google Analytics 4 Obfuscated Sample E-commerce Dataset (BigQuery Public Data).
