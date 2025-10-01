# Marketing Demo Talk Track (10–20 minutes)

- Objective: Show Databricks-native capabilities for marketing analytics and campaign management.

1) Framing (1 min)
- Lakehouse unifies data, analytics, and ML. Today: synthetic marketing data → KPIs → model → dashboard.

2) Setup (1–2 min)
- Open `sql/01_setup_uc.sql`. Run to create schema and raw Volume.
- Note governance with Unity Catalog.

3) Data generation (2–3 min)
- Open `python/02_generate_data.py`. Run to land JSON into the Volume.
- Call out channels, campaigns, impressions/clicks, customers, orders.

4) Ingestion and transformation with DLT (4–6 min)
- Open DLT pipeline referencing `dlt/03_dlt_pipeline.sql`.
- Show bronze/silver/gold lineage, expectations, quality, and refresh.

5) Analytics (2–3 min)
- Open `sql/04_analytics.sql`. Run views, query `vw_channel_kpis` and `vw_campaign_performance`.
- Show ROAS, CTR, CAC proxy (spend/clicks), LTV by cohort.

6) Modeling (optional, 3–5 min)
- Open `python/05_ml_propensity.py`. Run AutoML. Show experiment and best model in Registry.

7) Dashboard and Workflow (2–3 min)
- Open Lakeview dashboard with KPI and line tiles.
- Show Workflows job chaining data → DLT → SQL → AutoML.

8) Wrap (1 min)
- Emphasize native stack, governance, scalability. Discuss next steps: real connectors, attribution, treatment effects.
