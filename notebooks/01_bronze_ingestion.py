# Databricks notebook source
# MAGIC %md
# MAGIC ### Test ingestion

# COMMAND ----------

bronze_df = spark.read.option("header", True).csv('/Volumes/workspace/journeyiq/raw_files/journeyiq_ga4_test.csv')
print(bronze_df.count())
display(bronze_df.limit(5))

# COMMAND ----------

bronze_df.write.format("delta").mode("overwrite").saveAsTable('workspace.journeyiq.bronze_ga4_events_test')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full ingestion

# COMMAND ----------

source_path = '/Volumes/workspace/journeyiq/raw_files/journeyiq_ga4_20201101_20201107.csv'
target_table = 'workspace.journeyiq.bronze_ga4_events'

# COMMAND ----------

bronze_df = spark.read.option("header", True).option("escape", '"').option("multiLine", True).csv(source_path)
print(bronze_df.count())
bronze_df.printSchema()
display(bronze_df.limit(5))
display(bronze_df.filter(bronze_df.event_name == "purchase").select("event_name", "items_json").limit(5))

# COMMAND ----------

bronze_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

stored_df = spark.table(target_table)

print("Source CSV : ", bronze_df.count())
print("Bronze Delta table : ", stored_df.count())
print("Column count : ", len(bronze_df.columns))
print("Row difference : ", bronze_df.count() - stored_df.count())

# COMMAND ----------

import json
test = stored_df.filter("event_name = 'purchase'").select("items_json").first()
test = json.loads(test["items_json"])
print(type(test))