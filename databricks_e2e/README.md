# Databricks Marketing Demo

This demo showcases Databricks-native capabilities for marketing analytics and campaign management scenarios using native Databricks features.

## What you will build
- End-to-end pipeline following the medallion architecture (e.g., bronze/silver/gold) using declarative pipelines
- Define Semantic Layer for channel and campaign performance KPIs
- A conversion propensity model with AutoML and MLflow model registry
- An AI/BI dashboard and a scheduled Workflow to keep our data products discoverable and fresh

## Quickstart
1) Pick your catalog and schema for the demo (default: `main`.`marketing_demo`).
2) Open and run `sql/01_setup_uc.sql` to create the UC schema and a `raw` Volume.
3) Open and run `python/02_generate_data.py` to write synthetic raw JSON files into the `raw` Volume.
4) Create a Delta Live Tables pipeline that references `dlt/03_dlt_pipeline.sql`.
   - Set `Continuous` off for a batch demo, or on for streaming.
   - Set storage and target to your chosen catalog and schema.
5) Trigger the DLT pipeline to build bronze/silver/gold marketing tables.
6) Open and run `sql/04_analytics.sql` to create analytics views (ROAS, CAC, LTV, channel performance).
7) (Optional) Open and run `python/05_ml_propensity.py` to train an AutoML model and register it with MLflow.
8) Follow `docs/06_dashboard_and_workflows.md` to build a Lakeview dashboard and a Workflows job.

## Data model (simplified)
- Campaigns and daily spend
- Events: impressions and clicks
- Customers and orders

Bronze = raw ingested JSON, Silver = cleaned/typed, Gold = business aggregates for reporting and ML features.

## Files
- `sql/01_setup_uc.sql`: Creates schema and a `raw` Volume
- `python/02_generate_data.py`: Writes raw JSON files per source into the Volume
- `dlt/03_dlt_pipeline.sql`: DLT pipeline (Auto Loader + expectations + SCD-ready tables)
- `sql/04_analytics.sql`: KPI views (ROAS, CAC, LTV, channel and campaign performance)
- `python/05_ml_propensity.py`: AutoML classification for conversion propensity
- `docs/06_dashboard_and_workflows.md`: Setup Lakeview dashboard and Workflows job
- `docs/demo_script.md`: 10–20 min guided demo script

## Notes
- This demo uses only Databricks-native capabilities. No external tools or libraries are required beyond standard Databricks runtimes.
- If your workspace uses a different default catalog or limited privileges, replace `main` with your catalog in the SQL and notebooks.


