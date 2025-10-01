# Databricks Marketing Demo - Conversion propensity (AutoML)
# Trains a simple classifier and registers the best model with MLflow Registry.

import mlflow
from databricks import automl
from pyspark.sql import functions as F

CATALOG = "main"
SCHEMA = "marketing_demo"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Build a labeled training set from silver tables
clicks = spark.table("clicks_silver").select("user_id", "event_date")
orders = spark.table("orders_silver").select("user_id", "order_date")

label = clicks.join(orders, (clicks.user_id == orders.user_id) & (clicks.event_date == orders.order_date), "left") \
              .withColumn("converted", F.when(F.col("order_date").isNotNull(), F.lit(1)).otherwise(F.lit(0))) \
              .select("user_id", F.col("event_date").alias("date"), "converted")

features = (
    spark.table("impressions_silver").groupBy("user_id", "event_date").agg(F.count("*").alias("impressions"))
    .join(spark.table("clicks_silver").groupBy("user_id", "event_date").agg(F.count("*").alias("clicks")), ["user_id", "event_date"], "left")
    .withColumnRenamed("event_date", "date")
)

train_df = features.join(label, ["user_id", "date"], "left").fillna({"clicks": 0, "converted": 0})

display_cols = ["user_id", "date", "impressions", "clicks", "converted"]

mlflow.set_experiment(f"/Shared/marketing_demo/propensity")

summary = automl.classify(
    train_df.select(*display_cols).toPandas(),
    target_col="converted",
    experiment_dir=f"/Shared/marketing_demo/propensity",
    timeout_minutes=10,
    max_trials=20,
)

print("Best trial run_id:", summary.best_trial.mlflow_run_id)
print("Best model URI:", summary.best_trial.model_path)


