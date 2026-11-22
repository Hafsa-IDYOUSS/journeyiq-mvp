# Databricks notebook source
from pyspark.sql.functions import col
bronze_table = "workspace.journeyiq.bronze_ga4_events"
bronze_df = spark.table(bronze_table)
print(bronze_df.count())
print(bronze_df.filter(col("event_name").isNull()).count())
print(bronze_df.filter(col("event_timestamp").isNull()).count())
print(bronze_df.filter(col("user_pseudo_id").isNull()).count())

bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date, col, timestamp_micros
spark.conf.set("spark.sql.session.timeZone", "UTC")

silver_df = bronze_df.withColumn("event_date", to_date(col("event_date"), "yyyyMMdd"))
silver_df = silver_df.withColumn("event_timestamp", timestamp_micros(col("event_timestamp").cast("long")))
print(silver_df.count())
silver_df.printSchema()
display(silver_df.select("event_date", "event_timestamp", "event_name").limit(5))


# COMMAND ----------

from pyspark.sql.functions import from_json

param_schema = """
ARRAY<STRUCT<
    key:STRING,
    value: STRUCT<
        string_value: STRING,
        int_value: BIGINT,
        float_value: DOUBLE,
        double_value: DOUBLE
    >
>>"""
silver_df = silver_df.withColumn("event_params", from_json("event_params_json", param_schema))
display(silver_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import filter as array_filter, get

session_col = array_filter(col("event_params"), lambda p: p["key"]=="ga_session_id")
session_id = get(session_col, 0).getField("value").getField("int_value")
silver_df = silver_df.withColumn("session_id", session_id)
display(silver_df.select("event_name", "user_pseudo_id", "event_timestamp", "session_id").limit(10))

# COMMAND ----------

from pyspark.sql.functions import concat_ws, when
event_count = silver_df.count()
missing_id = silver_df.filter('session_id IS NULL').count()
missing_percent = (missing_id/event_count)*100
print("Missing percentage", missing_percent, "%")

combined_key = concat_ws("_", col("user_pseudo_id"), col("session_id").cast("string"))
silver_df = silver_df.withColumn("session_key", when(col("session_id").isNotNull(), combined_key).otherwise(None))
display(silver_df.select("event_name", "user_pseudo_id", "session_key").limit(5))

# COMMAND ----------

from pyspark.sql.functions import from_json

item_schema = """
ARRAY<STRUCT<
    item_id: STRING,
    item_name: STRING,
    item_brand: STRING,
    item_category: STRING,
    price: DOUBLE,
    quantity: BIGINT,
    item_revenue: DOUBLE
    >
>
"""
silver_df = silver_df.withColumn("items", from_json("items_json", item_schema))
silver_df.printSchema()
records_count = silver_df.filter("items_json IS NOT NULL AND items IS NULL").count()
print(records_count)
display(silver_df.filter("event_name = 'purchase'").select("event_name", "items_json", "items").limit(5))

# COMMAND ----------


from pyspark.sql.functions import col

total_rows = silver_df.count()

missing_dates = silver_df.filter(col("event_date").isNull()).count()
missing_timestamps = silver_df.filter(col("event_timestamp").isNull()).count()
missing_users = silver_df.filter(col("user_pseudo_id").isNull()).count()
missing_sessions = silver_df.filter(col("session_key").isNull()).count()

invalid_params = silver_df.filter(
    col("event_params_json").isNotNull() &
    col("event_params").isNull()
).count()

invalid_items = silver_df.filter(
    col("items_json").isNotNull() &
    col("items").isNull()
).count()

distinct_sessions = (
    silver_df
    .filter(col("session_key").isNotNull())
    .select("session_key")
    .distinct()
    .count()
)

source_columns = [
    "event_date",
    "event_timestamp",
    "event_name",
    "user_pseudo_id",
    "event_params_json",
    "items_json"
]

unique_rows = silver_df.dropDuplicates(source_columns).count()
potential_duplicates = total_rows - unique_rows

metrics = {
    "Total rows": total_rows,
    "Missing dates": missing_dates,
    "Missing timestamps": missing_timestamps,
    "Missing user IDs": missing_users,
    "Missing session keys": missing_sessions,
    "Invalid event parameters": invalid_params,
    "Invalid item arrays": invalid_items,
    "Distinct sessions": distinct_sessions,
    "Potential duplicate records": potential_duplicates
}

for metric, value in metrics.items():
    print(f"{metric}: {value:,}")


# COMMAND ----------


assert total_rows == 328463, "Unexpected Silver row count"
assert missing_dates == 0, "Invalid event dates detected"
assert missing_timestamps == 0, "Invalid timestamps detected"
assert missing_users == 0, "Missing user IDs detected"
assert missing_sessions == 0, "Missing session keys detected"
assert invalid_params == 0, "Event parameter parsing failed"
assert invalid_items == 0, "Item parsing failed"

silver_table = "workspace.journeyiq.silver_ga4_events"

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

print(f"Silver table saved: {silver_table}")


# COMMAND ----------

stored_silver_df = spark.table(silver_table)

print("Silver rows:", stored_silver_df.count())
print("Silver columns:", len(stored_silver_df.columns))
print("Row difference:", total_rows - stored_silver_df.count())

stored_silver_df.printSchema()

display(
    stored_silver_df.select(
        "event_date",
        "event_timestamp",
        "event_name",
        "user_pseudo_id",
        "session_key"
    ).limit(10)
)
