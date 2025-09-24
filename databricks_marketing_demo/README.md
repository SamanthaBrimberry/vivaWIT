# Databricks Marketing Demo

This demo showcases Databricks-native capabilities for common marketing analytics and campaign management scenarios using the Lakehouse: Delta Lake, Unity Catalog, Delta Live Tables (DLT), Auto Loader, SQL, Workflows, MLflow/AutoML, and Lakeview dashboards.

Audience: Beginner to mid-level stakeholders (marketing analysts, marketing ops, data practitioners starting on Databricks).

## What you will build
- End-to-end pipeline: synthetic raw marketing data → bronze/silver/gold using DLT
- KPIs for channel and campaign performance: ROAS, CAC, CTR, CVR, LTV
- A simple conversion propensity model with AutoML and MLflow model registry
- A Lakeview dashboard and a scheduled Workflow to refresh everything

## Prerequisites
- A Databricks workspace with Unity Catalog (recommended) and Lakeview enabled
- Permission to create schemas and volumes in your chosen catalog
- A cluster or SQL Warehouse with access to Unity Catalog

If Unity Catalog is not available, please let us know; we can adapt to workspace-local default behaviors. Volumes and UC models are recommended for governance.

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
9) Use `docs/demo_script.md` for a concise talk track.

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


