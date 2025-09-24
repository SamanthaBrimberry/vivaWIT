# Databricks Marketing Demo - Synthetic data generator
# Writes JSON files into a UC Volume for Auto Loader / DLT ingestion

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timedelta
import random

# ---------- Configuration ----------
# Adjust to your environment if needed
CATALOG = "main"
SCHEMA = "marketing_demo"
VOLUME = "raw"  # created by 01_setup_uc.sql

RAW_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Controls size of the demo
NUM_CAMPAIGNS = 12
NUM_DAYS = 30
USERS_PER_DAY = 2000

random.seed(7)

# ---------- Helpers ----------
def date_range(days: int):
    today = datetime.utcnow().date()
    for i in range(days):
        yield today - timedelta(days=days - 1 - i)


channels = ["search", "social", "email", "display", "affiliate"]
devices = ["mobile", "desktop", "tablet"]
countries = ["US", "CA", "GB", "DE", "FR", "AU"]
objectives = ["awareness", "traffic", "conversions"]


# ---------- Campaigns ----------
campaign_rows = []
start_date = datetime.utcnow().date() - timedelta(days=NUM_DAYS)
for cid in range(1, NUM_CAMPAIGNS + 1):
    ch = random.choice(channels)
    obj = random.choice(objectives)
    duration = random.randint(14, max(14, NUM_DAYS))
    end_date = start_date + timedelta(days=duration)
    budget = random.randint(5000, 50000)
    campaign_rows.append((
        cid,
        f"Campaign {cid} - {ch}",
        ch,
        str(start_date),
        str(end_date),
        float(budget),
        obj
    ))

campaigns_schema = T.StructType([
    T.StructField("campaign_id", T.IntegerType(), False),
    T.StructField("campaign_name", T.StringType(), False),
    T.StructField("channel", T.StringType(), False),
    T.StructField("start_date", T.StringType(), False),
    T.StructField("end_date", T.StringType(), False),
    T.StructField("budget_usd", T.DoubleType(), False),
    T.StructField("objective", T.StringType(), False),
])

campaigns_df = spark.createDataFrame(campaign_rows, schema=campaigns_schema)

# Write campaigns as small JSON snapshot (DLT will treat as streaming if desired)
campaigns_path = f"{RAW_BASE}/campaigns/date={datetime.utcnow().date()}"
campaigns_df.coalesce(1).write.mode("overwrite").json(campaigns_path)


# ---------- Daily spend ----------
spend_rows = []
for d in date_range(NUM_DAYS):
    for cid in range(1, NUM_CAMPAIGNS + 1):
        ch = campaigns_df.where(F.col("campaign_id") == cid).select("channel").first()[0]
        base = {
            "search": 800,
            "social": 600,
            "email": 200,
            "display": 300,
            "affiliate": 250,
        }[ch]
        spend = max(50, random.gauss(base, base * 0.25))
        spend_rows.append((cid, str(d), float(round(spend, 2))))

spend_schema = T.StructType([
    T.StructField("campaign_id", T.IntegerType(), False),
    T.StructField("date", T.StringType(), False),
    T.StructField("spend_usd", T.DoubleType(), False),
])

spend_df = spark.createDataFrame(spend_rows, schema=spend_schema)
spend_df.write.mode("overwrite").partitionBy("date").json(f"{RAW_BASE}/daily_spend")


# ---------- Impressions and clicks ----------
def gen_events_for_day(d):
    # impressions
    impressions = []
    clicks = []
    users_today = USERS_PER_DAY + random.randint(-200, 200)
    for _ in range(max(200, users_today)):
        user_id = random.randint(1, 50000)
        device = random.choice(devices)
        country = random.choice(countries)
        campaign_id = random.randint(1, NUM_CAMPAIGNS)
        placements = ["feed", "search", "stories", "sidebar"]
        placement = random.choice(placements)
        # each user sees 1-5 impressions per day avg
        n_impr = max(1, int(random.gauss(3, 1)))
        for _ in range(n_impr):
            ts = datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
            impressions.append((
                user_id,
                campaign_id,
                device,
                country,
                placement,
                ts.isoformat()
            ))
            # click-through probability varies by channel
            ch = campaigns_df.where(F.col("campaign_id") == campaign_id).select("channel").first()[0]
            base_ctr = {
                "search": 0.06,
                "social": 0.03,
                "email": 0.08,
                "display": 0.01,
                "affiliate": 0.04,
            }[ch]
            ctr = max(0.0005, random.gauss(base_ctr, base_ctr * 0.3))
            if random.random() < ctr:
                clicks.append((
                    user_id,
                    campaign_id,
                    device,
                    country,
                    placement,
                    ts.isoformat()
                ))
    return impressions, clicks


impr_schema = T.StructType([
    T.StructField("user_id", T.IntegerType(), False),
    T.StructField("campaign_id", T.IntegerType(), False),
    T.StructField("device", T.StringType(), False),
    T.StructField("country", T.StringType(), False),
    T.StructField("placement", T.StringType(), False),
    T.StructField("event_ts", T.StringType(), False),
])

click_schema = impr_schema

for d in date_range(NUM_DAYS):
    impr_rows, click_rows = gen_events_for_day(d)
    if impr_rows:
        spark.createDataFrame(impr_rows, schema=impr_schema).coalesce(1).write.mode("overwrite").json(f"{RAW_BASE}/impressions/date={d}")
    if click_rows:
        spark.createDataFrame(click_rows, schema=click_schema).coalesce(1).write.mode("overwrite").json(f"{RAW_BASE}/clicks/date={d}")


# ---------- Customers and orders ----------
cust_rows = []
order_rows = []
user_base = 20000
for uid in range(1, user_base + 1):
    signup_day = random.randint(0, NUM_DAYS - 1)
    sd = list(date_range(NUM_DAYS))[signup_day]
    first_touch_channel = random.choice(channels)
    cust_rows.append((uid, str(sd), first_touch_channel))

    # probability of converting influenced by channel
    base_cvr = {
        "search": 0.08,
        "social": 0.03,
        "email": 0.10,
        "display": 0.01,
        "affiliate": 0.05,
    }[first_touch_channel]
    cvr = max(0.001, random.gauss(base_cvr, base_cvr * 0.4))
    if random.random() < cvr:
        order_day = signup_day + random.randint(0, 7)
        order_day = min(order_day, NUM_DAYS - 1)
        od = list(date_range(NUM_DAYS))[order_day]
        revenue = max(10.0, random.gauss(120.0, 40.0))
        campaign_id = random.randint(1, NUM_CAMPAIGNS)
        order_rows.append((uid, str(od), float(round(revenue, 2))),)

customers_schema = T.StructType([
    T.StructField("user_id", T.IntegerType(), False),
    T.StructField("signup_date", T.StringType(), False),
    T.StructField("first_touch_channel", T.StringType(), False),
])

orders_schema = T.StructType([
    T.StructField("user_id", T.IntegerType(), False),
    T.StructField("order_date", T.StringType(), False),
    T.StructField("revenue_usd", T.DoubleType(), False),
])

spark.createDataFrame(cust_rows, schema=customers_schema).write.mode("overwrite").json(f"{RAW_BASE}/customers")
spark.createDataFrame(order_rows, schema=orders_schema).write.mode("overwrite").partitionBy("order_date").json(f"{RAW_BASE}/orders")

print(f"Wrote synthetic data to {RAW_BASE}")


