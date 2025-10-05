# Databricks notebook source
# MAGIC %pip install --quiet databricks-vectorsearch mlflow-skinny[databricks] langgraph==0.3.4 databricks-langchain databricks-agents uv
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updating Lab's config in Vocareum
# MAGIC In order to upload custom lab content we need to update the `content.src` and `workspace_setup.src`.
# MAGIC
# MAGIC This notebook covers the `workspace_setup.src`... Where you will:
# MAGIC - Set up catalog, schema, tables
# MAGIC - Grant permissions to lab users to access catalog assets
# MAGIC - Download synthetic data located in a GitHub Repository
# MAGIC - Create any infrastructure like VS Endpoints, etc.
# MAGIC
# MAGIC `
# MAGIC {
# MAGIC     "content": {
# MAGIC         "src": "agent_workshop.zip",
# MAGIC         "entry": "01_create_tools/01_create_tools"
# MAGIC     },
# MAGIC "workspace_setup": {
# MAGIC   "entry": "agent_setup",
# MAGIC   "src": "agent_setup.zip"
# MAGIC }
# MAGIC },`

# COMMAND ----------

import requests
import pandas as pd
import io
import time
from databricks.sdk import WorkspaceClient

# Initialize clients
w = WorkspaceClient()

# Global Variables
CATALOG = "lab"
SCHEMA = "data"
base_url = "https://raw.githubusercontent.com/SamanthaBrimberry/vivaWIT/main/data"

csv_files = {
    "campaigns": f"{base_url}/campaigns.csv",
    "clicks": f"{base_url}/clicks.csv", 
    "customers": f"{base_url}/customers.csv",
    "daily_spend": f"{base_url}/daily_spend.csv",
    "impressions": f"{base_url}/impressions.csv",
    "orders": f"{base_url}/orders.csv",
}

# Create catalog if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Download and load each CSV file
for table_name, url in csv_files.items():
    response = requests.get(url)
    response.raise_for_status()
    # pandas supports reading from github url spark does not
    df = pd.read_csv(io.StringIO(response.text))
    spark_df = spark.createDataFrame(df)
    (
        spark_df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}")
    )
    print(f"Table: {table_name} created")


# --------- Grant access to UC assets --------------
print("\nGranting all users access to catalog...")
try:
    # Grant USE CATALOG permission to all users
    spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`")
    print("Granted USE CATALOG permission")
    
    # Grant USE SCHEMA permission on the schemas
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `account users`")
    print("Granted USE SCHEMA permission")

    # Grant CREATE SCHEMA permission on the catalog
    spark.sql(f"GRANT CREATE SCHEMA ON CATALOG {CATALOG} TO `account users`")
    
except Exception as e:
    print(f"Error granting permissions: {e}")
    # Try alternative syntax if the above fails
    try:
        spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO ALL USERS")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO ALL USERS")
        print("Successfully granted permissions using alternative syntax")
    except Exception as alt_error:
        print(f"Alternative syntax also failed: {alt_error}")


# COMMAND ----------

DATABRICKS_INSTANCE = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WAREHOUSE_NAME = "viva_wit_warehouse"
USER_GROUP = ''

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "name": WAREHOUSE_NAME,
    "cluster_size": "Large",
    "warehouse_type": "PRO",
    "enable_serverless_compute": True,
    "auto_stop_mins": 10,
    "min_num_clusters": 1,
    "max_num_clusters": 2
}

response = requests.post(
    f"{DATABRICKS_INSTANCE}/api/2.0/sql/warehouses",
    headers=headers,
    json=payload
)

warehouse_id = response.json().get("id")
print(f"Created warehouse with ID: {warehouse_id}")

payload = {
  "access_control_list": [
    {
      "group_name": USER_GROUP,
      "permission_level": "CAN_USE"
    }
  ]
}

response = requests.post(
    f"{DATABRICKS_INSTANCE}/api/2.0/sql/warehouses/{warehouse_id}",
    headers=headers,
    json=payload
)

print(f"Added account users to warehouse: {response.json()}")
