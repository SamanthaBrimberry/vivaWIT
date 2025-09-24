# Lakeview Dashboard and Workflows Setup

This guide shows how to create a Lakeview dashboard and an automated Workflow to refresh data and analytics for the marketing demo.

## Lakeview Dashboard
1. In the Databricks UI, go to Dashboards → Lakeview → New Dashboard.
2. Name it "Marketing Performance" and select your SQL Warehouse.
3. Add Tiles:
   - SQL Table: `SELECT * FROM main.marketing_demo.vw_channel_kpis ORDER BY date DESC, channel`.
   - KPI Tiles:
     - ROAS (Today): `SELECT ROUND(SUM(revenue_usd)/NULLIF(SUM(spend_usd),0),2) AS roas FROM main.marketing_demo.channel_daily_gold WHERE date = current_date();`
     - Spend (7d): `SELECT ROUND(SUM(spend_usd),0) FROM main.marketing_demo.channel_daily_gold WHERE date >= date_sub(current_date(), 7);`
     - Revenue (7d): `SELECT ROUND(SUM(revenue_usd),0) FROM main.marketing_demo.channel_daily_gold WHERE date >= date_sub(current_date(), 7);`
   - Line Chart: `SELECT date, channel, roas FROM main.marketing_demo.channel_daily_gold ORDER BY date` (Pivot by channel).

## Workflows (Jobs)
Create a multi-task job to execute end-to-end:
1. Task: `Generate Data` (Notebook) → `python/02_generate_data.py` on an All-Purpose cluster or Job cluster.
2. Task: `DLT` (Delta Live Tables Pipeline) → point to `dlt/03_dlt_pipeline.sql` pipeline; set `Development` mode for faster runs.
3. Task: `Analytics Views` (SQL) → `sql/04_analytics.sql` on SQL Warehouse.
4. (Optional) Task: `AutoML Propensity` (Notebook) → `python/05_ml_propensity.py`.

Schedule the job daily or hourly for demo. Ensure the dashboard points to `main.marketing_demo` tables.


