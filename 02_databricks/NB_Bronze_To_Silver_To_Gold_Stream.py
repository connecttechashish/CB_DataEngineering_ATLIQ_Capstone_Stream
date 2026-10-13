# Databricks notebook source
KAFKA_BOOTSTRAP="pkc-xxxx.us-east-2.aws.confluent.cloud:9092"
KAFKA_API_KEY="xxxxxxxxxxx"
KAFKA_API_SECRET="xxxxxxxxxxxxxxxxxxxxxxxxxxxxx/xxxxx"
KAFKA_TOPIC="atliq.orders.events"

CATALOG, SCHEMA = "atliq_dbx", "streaming"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# One-time setup (catalog already exists, so we don't create it)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC BRONZE PROCESSING

# COMMAND ----------

from pyspark.sql import functions as F, types as T

jaas = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";')

raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas)
    .load()
    .select(F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            "topic", "partition", "offset", "timestamp"))

q_bronze = (raw.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/bronze")
    .trigger(availableNow=True)          # required on serverless
    .toTable(f"{CATALOG}.{SCHEMA}.bronze_order_events"))
q_bronze.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER PROCESSING

# COMMAND ----------

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("order_id", T.LongType()),
    T.StructField("customer_id", T.LongType()),
    T.StructField("city", T.StringType()),
    T.StructField("product_id", T.LongType()),
    T.StructField("quantity", T.LongType()),
    T.StructField("order_amount", T.DoubleType()),
    T.StructField("payment_method", T.StringType()),
])

silver = (spark.readStream.table(f"{CATALOG}.{SCHEMA}.bronze_order_events")
    .select(F.from_json("value", schema).alias("e")).select("e.*")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["event_id"]))

q_silver = (silver.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/silver")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.silver_order_events"))
q_silver.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD PROCESSING

# COMMAND ----------

gold = (spark.readStream.table(f"{CATALOG}.{SCHEMA}.silver_order_events")
    .filter(F.col("event_type") == "payment_received")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes"))
    .agg(F.count("*").alias("orders_paid"),
         F.sum("order_amount").alias("revenue"))
    .select(F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "orders_paid", "revenue"))

q_gold = (gold.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/gold")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.gold_revenue_5min"))
q_gold.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Checks/Validate
# MAGIC
# MAGIC SELECT event_type, COUNT(*) FROM atliq_dbx.streaming.silver_order_events GROUP BY event_type;
# MAGIC
# MAGIC SELECT * FROM atliq_dbx.streaming.gold_revenue_5min ORDER BY window_start DESC LIMIT 12;