-- Databricks Marketing Demo - Exploratory Analysis and Forecasting
-- Run after the DLT pipeline has created gold tables.

USE CATALOG lab;
USE SCHEMA data;

-- -----------------------------------------------
-- Basic EDA on gold aggregates
-- -----------------------------------------------

-- Peek at channel-level KPIs
SELECT *
FROM channel_daily_gold
ORDER BY date DESC, channel
LIMIT 20;

-- Completeness checks on key columns
SELECT
  COUNT(*) AS total_rows,
  COUNT(date) AS non_null_dates,
  COUNT(revenue_usd) AS non_null_revenue,
  COUNT(spend_usd) AS non_null_spend
FROM channel_daily_gold;

-- Summary by channel
SELECT
  channel,
  COUNT(*) AS days,
  SUM(spend_usd) AS spend_usd,
  SUM(revenue_usd) AS revenue_usd,
  AVG(ctr) AS avg_ctr,
  AVG(roas) AS avg_roas
FROM channel_daily_gold
GROUP BY channel
ORDER BY revenue_usd DESC;

-- Create a daily total KPI view across all channels
CREATE OR REPLACE VIEW vw_daily_total_kpis AS
SELECT
  date,
  SUM(spend_usd) AS spend_usd,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END AS ctr,
  SUM(revenue_usd) AS revenue_usd
FROM channel_daily_gold
GROUP BY date;

-- Trend with a 7-day moving average (total revenue)
SELECT
  date,
  revenue_usd,
  AVG(revenue_usd) OVER (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS revenue_usd_ma7
FROM vw_daily_total_kpis
ORDER BY date;

-- Weekday seasonality (across whole series)
SELECT
  DAYOFWEEK(date) AS dow,
  DATE_FORMAT(date, 'E') AS weekday,
  AVG(revenue_usd) AS avg_revenue,
  AVG(spend_usd) AS avg_spend,
  AVG(clicks) AS avg_clicks
FROM vw_daily_total_kpis
GROUP BY 1,2
ORDER BY 1;

-- Correlation between spend and revenue (total series)
SELECT corr(spend_usd, revenue_usd) AS corr_spend_revenue
FROM vw_daily_total_kpis;

-- Channel share over the last 7 days
WITH recent AS (
  SELECT *
  FROM channel_daily_gold
  WHERE date >= DATEADD(day, -7, CURRENT_DATE())
)
SELECT
  channel,
  SUM(revenue_usd) AS revenue_usd,
  SUM(spend_usd) AS spend_usd,
  SUM(clicks) AS clicks
FROM recent
GROUP BY channel
ORDER BY revenue_usd DESC;

-- -----------------------------------------------
-- Forecasting with Databricks SQL AI functions
-- -----------------------------------------------

-- 14-day forecast for total revenue
WITH series AS (
  SELECT date, revenue_usd
  FROM vw_daily_total_kpis
  ORDER BY date
)
SELECT *
FROM AI_FORECAST(
  TABLE(series),
  horizon => DATEADD(day, 14, CURRENT_DATE()),
  time_col => 'date',
  value_col => 'revenue_usd',
  prediction_interval_width => 0.90
);

-- Optional: create a view materializing the total revenue forecast
CREATE OR REPLACE VIEW vw_revenue_forecast_14d AS
WITH series AS (
  SELECT date, revenue_usd
  FROM vw_daily_total_kpis
  ORDER BY date
)
SELECT *
FROM AI_FORECAST(
  TABLE(series),
  horizon => DATEADD(day, 14, CURRENT_DATE()),
  time_col => 'date',
  value_col => 'revenue_usd',
  prediction_interval_width => 0.90
);

-- Per-channel 14-day revenue forecast
-- Note: requires Databricks SQL AI functions support for grouping via id_cols
WITH by_channel AS (
  SELECT
    date,
    channel,
    SUM(revenue_usd) AS revenue_usd
  FROM channel_daily_gold
  GROUP BY 1,2
)
SELECT *
FROM AI_FORECAST(
  TABLE(by_channel),
  horizon => DATEADD(day, 14, CURRENT_DATE()),
  time_col => 'date',
  value_col => 'revenue_usd',
  id_cols => ARRAY('channel'),
  prediction_interval_width => 0.90
);