# app/create_kpis.py
import os
from pyspark.sql import SparkSession, functions as F

def create_gold_layer():
    spark = None
    try:
        spark = SparkSession.builder.appName("Create Gold KPIs").getOrCreate()

        acct = os.environ["SNOWFLAKE_ACCOUNT"]
        user = os.environ["SNOWFLAKE_USER"]
        pwd  = os.environ["SNOWFLAKE_PASSWORD"]
        wh   = os.environ["SNOWFLAKE_WAREHOUSE"]
        db   = os.environ["SNOWFLAKE_DATABASE"]

        silver = {"sfURL": f"{acct}.snowflakecomputing.com", "sfUser": user, "sfPassword": pwd,
                  "sfWarehouse": wh, "sfDatabase": db, "sfSchema": "SILVER_LAYER_FRAUD"}
        gold   = {"sfURL": f"{acct}.snowflakecomputing.com", "sfUser": user, "sfPassword": pwd,
                  "sfWarehouse": wh, "sfDatabase": db, "sfSchema": "GOLD_LAYER_FRAUD"}

        silver_df = (spark.read.format("net.snowflake.spark.snowflake")
                     .options(**silver).option("dbtable","FRAUD_TRANSACTIONS_SILVER").load())
        print("Silver data read.")

        # Add a date column once
        silver_df = silver_df.withColumn("transaction_date", F.to_date("transaction_timestamp"))

        # Daily stats per user
        user_daily = (silver_df.groupBy("user_id","transaction_date")
                      .agg(F.count("transaction_id").alias("daily_transaction_count"),
                           F.sum("transaction_amount").alias("daily_total_amount")))

        # User-level stats
        user_stats = (silver_df.groupBy("user_id")
                      .agg(F.avg("transaction_amount").alias("avg_transaction_value"),
                           F.count("transaction_id").alias("total_transactions_per_user")))

        # Join back
        gold_df = (silver_df
                   .join(user_daily, ["user_id","transaction_date"], "left")
                   .join(user_stats, "user_id", "left"))

        gold_df = gold_df.withColumn(
            "is_suspicious_pattern",
            F.when((F.col("daily_transaction_count") > 5) & (F.col("daily_total_amount") > 1000), F.lit(True)).otherwise(F.lit(False))
        )

        (gold_df.write.format("net.snowflake.spark.snowflake")
            .options(**gold).option("dbtable","FRAUD_ANALYSIS_GOLD")
            .mode("overwrite").save())

        print("✔ Gold written: FRAUD_ANALYSIS_GOLD")
    except Exception as e:
        print(f"Error in Gold KPIs: {e}")
    finally:
        if spark: spark.stop()

if __name__ == "__main__":
    create_gold_layer()
