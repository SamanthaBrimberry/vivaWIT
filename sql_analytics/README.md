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
