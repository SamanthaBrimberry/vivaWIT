-- Databricks Marketing Demo - Analytics views
-- Run after the DLT pipeline has created gold tables.

USE CATALOG main;
USE SCHEMA marketing_demo;

-- View: ROAS, CAC, LTV by channel and day
CREATE OR REPLACE VIEW vw_channel_kpis AS
WITH daily AS (
  SELECT * FROM channel_daily_gold
)
SELECT
  date,
  channel,
  spend_usd,
  impressions,
  clicks,
  ctr,
  revenue_usd,
  roas,
  CASE WHEN clicks > 0 THEN spend_usd / clicks ELSE NULL END AS cpc_estimate,
  CASE WHEN revenue_usd > 0 THEN spend_usd / revenue_usd ELSE NULL END AS cost_to_revenue_ratio
FROM daily;

-- Campaign performance summary
CREATE OR REPLACE VIEW vw_campaign_performance AS
SELECT
  d.date,
  c.campaign_id,
  c.campaign_name,
  c.channel,
  d.spend_usd,
  d.impressions,
  d.clicks,
  d.ctr,
  d.revenue_usd,
  d.roas
FROM daily_channel_metrics_gold d
JOIN campaigns_silver c USING (campaign_id);

-- Simple customer LTV by signup cohort (last NUM_DAYS window)
CREATE OR REPLACE VIEW vw_ltv_by_cohort AS
WITH orders AS (
  SELECT o.user_id, o.order_date, o.revenue_usd FROM orders_silver o
), customers AS (
  SELECT user_id, signup_date, first_touch_channel FROM customers_silver
)
SELECT
  DATE_TRUNC('week', c.signup_date) AS signup_week,
  c.first_touch_channel AS channel,
  COUNT(DISTINCT c.user_id) AS signups,
  COUNT(DISTINCT o.user_id) AS buyers,
  SUM(o.revenue_usd) AS revenue_usd,
  CASE WHEN COUNT(DISTINCT o.user_id) > 0 THEN SUM(o.revenue_usd) / COUNT(DISTINCT o.user_id) ELSE 0 END AS ltv_per_buyer
FROM customers c
LEFT JOIN orders o ON o.user_id = c.user_id
GROUP BY 1,2
ORDER BY 1 DESC;


