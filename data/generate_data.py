# Databricks Marketing Demo - Synthetic data generator
# Writes CSV files into the current working directory (no Spark required)
!pip install faker
from datetime import datetime, timedelta
import random
import os
import csv
from faker import Faker

# ---------- Configuration ----------
OUTPUT_DIR = os.getcwd()

# Controls size of the demo
NUM_CAMPAIGNS = 12
NUM_DAYS = 30
USERS_PER_DAY = 2000

random.seed(7)
fake = Faker()
fake.seed_instance(7)

# ---------- Helpers ----------
def date_range(days: int):
    today = datetime.utcnow().date()
    for i in range(days):
        yield today - timedelta(days=days - 1 - i)


# Precompute day list for seasonality/anomalies
all_days = list(date_range(NUM_DAYS))

# Light anomalies: 1 spike day and 1 dip day
spike_day = random.choice(all_days)
dip_candidates = [d for d in all_days if d != spike_day]
dip_day = random.choice(dip_candidates) if dip_candidates else spike_day

def day_multiplier(d):
    """Return a simple multiplier for weekend effects and anomalies.
    - Weekends (Sat/Sun) have lower spend/traffic
    - One spike day and one dip day introduce light anomalies
    """
    weekend_mult = 0.7 if d.weekday() >= 5 else 1.0
    anomaly_mult = 1.0
    if d == spike_day:
        anomaly_mult = 1.8
    elif d == dip_day:
        anomaly_mult = 0.6
    return weekend_mult * anomaly_mult


channels = ["search", "social", "email", "display", "affiliate"]
devices = ["mobile", "desktop", "tablet"]
countries = ["US", "CA", "GB", "DE", "FR", "AU"]
objectives = ["awareness", "traffic", "conversions"]

# Size of known customer user ID space used across events and customers
user_base = 20000


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
        f"{fake.company()} {fake.bs().split()[0].title()} - {ch}",
        ch,
        str(start_date),
        str(end_date),
        float(budget),
        obj
    ))

# Helpful lookup for channel by campaign
campaign_id_to_channel = {row[0]: row[2] for row in campaign_rows}


# ---------- Daily spend ----------
spend_rows = []
for d in date_range(NUM_DAYS):
    for cid in range(1, NUM_CAMPAIGNS + 1):
        ch = campaign_id_to_channel[cid]
        base = {
            "search": 800,
            "social": 600,
            "email": 200,
            "display": 300,
            "affiliate": 250,
        }[ch]
        mult = day_multiplier(d)
        spend = max(50, random.gauss(base * mult, base * 0.25))
        spend_rows.append((cid, str(d), float(round(spend, 2))))

# defer CSV writing to the end


# ---------- Impressions and clicks ----------
def gen_events_for_day(d):
    # impressions
    impressions = []
    clicks = []
    traffic_mult = day_multiplier(d)
    users_today = int((USERS_PER_DAY + random.randint(-200, 200)) * traffic_mult)
    for _ in range(max(200, users_today)):
        # Bias events toward known customers for realism
        if random.random() < 0.8:
            user_id = random.randint(1, user_base)
        else:
            user_id = random.randint(user_base + 1, 50000)
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
            ch = campaign_id_to_channel[campaign_id]
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


all_impressions = []
all_clicks = []
for d in date_range(NUM_DAYS):
    impr_rows, click_rows = gen_events_for_day(d)
    if impr_rows:
        all_impressions.extend(impr_rows)
    if click_rows:
        all_clicks.extend(click_rows)


# ---------- Customers and orders ----------
cust_rows = []
order_rows = []
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
        # Device bias: more purchases on desktop; device-specific AOV
        purchase_device = random.choices(population=devices, weights=[0.3, 0.6, 0.1], k=1)[0]
        aov_means = {"mobile": 110.0, "desktop": 140.0, "tablet": 120.0}
        revenue = max(10.0, random.gauss(aov_means[purchase_device], 40.0))
        order_rows.append((uid, str(od), purchase_device, float(round(revenue, 2))))

# ---------- CSV outputs ----------
def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

write_csv(os.path.join(OUTPUT_DIR, "campaigns.csv"),
          ["campaign_id", "campaign_name", "channel", "start_date", "end_date", "budget_usd", "objective"],
          campaign_rows)

write_csv(os.path.join(OUTPUT_DIR, "daily_spend.csv"),
          ["campaign_id", "date", "spend_usd"],
          spend_rows)

write_csv(os.path.join(OUTPUT_DIR, "impressions.csv"),
          ["user_id", "campaign_id", "device", "country", "placement", "event_ts"],
          all_impressions)

write_csv(os.path.join(OUTPUT_DIR, "clicks.csv"),
          ["user_id", "campaign_id", "device", "country", "placement", "event_ts"],
          all_clicks)

write_csv(os.path.join(OUTPUT_DIR, "customers.csv"),
          ["user_id", "signup_date", "first_touch_channel"],
          cust_rows)

write_csv(os.path.join(OUTPUT_DIR, "orders.csv"),
          ["user_id", "order_date", "device", "revenue_usd"],
          order_rows)

print(f"Wrote CSV datasets to {OUTPUT_DIR}")


