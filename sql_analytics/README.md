# Databricks Marketing Demo

This demo showcases Databricks-native capabilities for marketing analytics and campaign management scenarios using native Databricks features.

## What you will build
- End-to-end pipeline following the medallion architecture (e.g., bronze/silver/gold)
- Create channel and campaign performance KPIs
- A conversion propensity model with AutoML and MLflow model registry
- An AI/BI dashboard and a scheduled Workflow to keep our data products discoverable and fresh

## Lab Agenda
| Step | Topic | Notebook | What you'll do |
| --- | --- | --- | --- |
| 1 | Build Silver/Gold tables | [sql/01_ETL](sql/01_ETL.ipynb) | Clean raw data into Silver, aggregate KPIs into Gold (CTR, CVR, ROAS) |
| 2 | Explore KPIs | [sql/02_analytics](sql_analytics/sql/02_analytics.ipynb) | EDA, completeness checks, trends (MA7), seasonality, spend↔revenue correlation |
| 3 | Create revenue forecast| [sql/03_rev_forecast](sql_analytics/sql/03_rev_forecast.sql.dbquery.ipynb)  | Create 14 day revenue forecast using AI functions |
| 4 | Curate analytics views | [sql/04_analytics](sql_analytics/sql/04_analytics.ipynb)  | Create reusable views: channel KPIs, campaign performance, LTV by signup cohort 
| 5 (optional) | Train propensity model | [python/05_ml_propensity](sql_analytics/ml/05_propensity_model.ipynb) | AutoML training and MLflow registration |

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
