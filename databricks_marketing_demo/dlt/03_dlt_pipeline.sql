-- Databricks Marketing Demo - Delta Live Tables (SQL)
-- Configure a DLT pipeline that points to this notebook.
-- Set Target: main.marketing_demo (or your catalog.schema)

-- Bronze: Auto Loader from UC Volume raw paths
CREATE OR REFRESH STREAMING LIVE TABLE bronze_campaigns
AS SELECT * FROM cloud_files("/Volumes/main/marketing_demo/raw/campaigns", "json");

CREATE OR REFRESH STREAMING LIVE TABLE bronze_daily_spend
AS SELECT * FROM cloud_files("/Volumes/main/marketing_demo/raw/daily_spend", "json");

CREATE OR REFRESH STREAMING LIVE TABLE bronze_impressions
AS SELECT * FROM cloud_files("/Volumes/main/marketing_demo/raw/impressions", "json");

CREATE OR REFRESH STREAMING LIVE TABLE bronze_clicks
AS SELECT * FROM cloud_files("/Volumes/main/marketing_demo/raw/clicks", "json");

CREATE OR REFRESH STREAMING LIVE TABLE bronze_customers
AS SELECT * FROM cloud_files("/Volumes/main/marketing_demo/raw/customers", "json");

CREATE OR REFRESH STREAMING LIVE TABLE bronze_orders
AS SELECT * FROM cloud_files("/Volumes/main/marketing_demo/raw/orders", "json");

-- Silver: Clean and type
CREATE OR REFRESH LIVE TABLE campaigns_silver
AS
SELECT
  CAST(campaign_id AS INT) AS campaign_id,
  campaign_name,
  channel,
  TO_DATE(start_date) AS start_date,
  TO_DATE(end_date) AS end_date,
  CAST(budget_usd AS DOUBLE) AS budget_usd,
  objective
FROM STREAM(LIVE.bronze_campaigns);

CREATE OR REFRESH LIVE TABLE daily_spend_silver
AS
SELECT
  CAST(campaign_id AS INT) AS campaign_id,
  TO_DATE(date) AS date,
  CAST(spend_usd AS DOUBLE) AS spend_usd
FROM STREAM(LIVE.bronze_daily_spend)
WHERE spend_usd >= 0;

CREATE OR REFRESH LIVE TABLE impressions_silver
AS
SELECT
  CAST(user_id AS INT) AS user_id,
  CAST(campaign_id AS INT) AS campaign_id,
  device,
  country,
  placement,
  TIMESTAMP(event_ts) AS event_ts,
  TO_DATE(event_ts) AS event_date
FROM STREAM(LIVE.bronze_impressions);

CREATE OR REFRESH LIVE TABLE clicks_silver
AS
SELECT
  CAST(user_id AS INT) AS user_id,
  CAST(campaign_id AS INT) AS campaign_id,
  device,
  country,
  placement,
  TIMESTAMP(event_ts) AS event_ts,
  TO_DATE(event_ts) AS event_date
FROM STREAM(LIVE.bronze_clicks);

CREATE OR REFRESH LIVE TABLE customers_silver
AS
SELECT
  CAST(user_id AS INT) AS user_id,
  TO_DATE(signup_date) AS signup_date,
  first_touch_channel
FROM STREAM(LIVE.bronze_customers);

CREATE OR REFRESH LIVE TABLE orders_silver
AS
SELECT
  CAST(user_id AS INT) AS user_id,
  TO_DATE(order_date) AS order_date,
  CAST(revenue_usd AS DOUBLE) AS revenue_usd
FROM STREAM(LIVE.bronze_orders)
WHERE revenue_usd >= 0;

-- Gold: Aggregates and KPIs
CREATE OR REFRESH LIVE TABLE daily_channel_metrics_gold
AS
WITH impr AS (
  SELECT event_date AS date, campaign_id, COUNT(*) AS impressions
  FROM LIVE.impressions_silver
  GROUP BY 1,2
), clk AS (
  SELECT event_date AS date, campaign_id, COUNT(*) AS clicks
  FROM LIVE.clicks_silver
  GROUP BY 1,2
), sp AS (
  SELECT date, campaign_id, SUM(spend_usd) AS spend_usd
  FROM LIVE.daily_spend_silver
  GROUP BY 1,2
), ord AS (
  SELECT order_date AS date, user_id, SUM(revenue_usd) AS revenue_usd
  FROM LIVE.orders_silver
  GROUP BY 1,2
), ch AS (
  SELECT campaign_id, channel FROM LIVE.campaigns_silver
)
SELECT
  COALESCE(sp.date, impr.date, clk.date) AS date,
  COALESCE(sp.campaign_id, impr.campaign_id, clk.campaign_id) AS campaign_id,
  ch.channel,
  COALESCE(sp.spend_usd, 0) AS spend_usd,
  COALESCE(impressions, 0) AS impressions,
  COALESCE(clicks, 0) AS clicks,
  CASE WHEN impressions > 0 THEN clicks / impressions ELSE 0 END AS ctr,
  ord_kpis.revenue_usd,
  CASE WHEN clicks > 0 THEN ord_kpis.orders * 1.0 / clicks ELSE 0 END AS cvr,
  CASE WHEN COALESCE(spend_usd, 0) > 0 THEN COALESCE(ord_kpis.revenue_usd, 0) / spend_usd ELSE 0 END AS roas
FROM sp
FULL OUTER JOIN impr ON sp.date = impr.date AND sp.campaign_id = impr.campaign_id
FULL OUTER JOIN clk ON COALESCE(sp.date, impr.date) = clk.date AND COALESCE(sp.campaign_id, impr.campaign_id) = clk.campaign_id
LEFT JOIN ch ON COALESCE(sp.campaign_id, impr.campaign_id, clk.campaign_id) = ch.campaign_id
LEFT JOIN (
  SELECT d.date, d.campaign_id,
         COUNT(DISTINCT o.user_id) AS orders,
         SUM(o.revenue_usd) AS revenue_usd
  FROM (
    SELECT DISTINCT event_date AS date, campaign_id, user_id
    FROM LIVE.clicks_silver
  ) d
  LEFT JOIN LIVE.orders_silver o ON o.user_id = d.user_id AND o.order_date = d.date
  GROUP BY 1,2
) ord_kpis ON ord_kpis.date = COALESCE(sp.date, impr.date, clk.date)
          AND ord_kpis.campaign_id = COALESCE(sp.campaign_id, impr.campaign_id, clk.campaign_id);

-- Helpful gold rollup by channel
CREATE OR REFRESH LIVE TABLE channel_daily_gold
AS
SELECT date, channel,
       SUM(spend_usd) AS spend_usd,
       SUM(impressions) AS impressions,
       SUM(clicks) AS clicks,
       CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END AS ctr,
       SUM(revenue_usd) AS revenue_usd,
       CASE WHEN SUM(clicks) > 0 THEN COUNT(*) * 1.0 / SUM(clicks) ELSE 0 END AS cvr_estimate,
       CASE WHEN SUM(spend_usd) > 0 THEN SUM(revenue_usd) / SUM(spend_usd) ELSE 0 END AS roas
FROM LIVE.daily_channel_metrics_gold
GROUP BY 1,2;


