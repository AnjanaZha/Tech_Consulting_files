# app/transform_data.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, to_timestamp

def transform_to_silver():
    spark = None
    try:
        spark = SparkSession.builder.appName("Bronze → Silver").getOrCreate()

        acct   = os.environ["SNOWFLAKE_ACCOUNT"]
        user   = os.environ["SNOWFLAKE_USER"]
        pwd    = os.environ["SNOWFLAKE_PASSWORD"]
        wh     = os.environ["SNOWFLAKE_WAREHOUSE"]
        db     = os.environ["SNOWFLAKE_DATABASE"]

        bronze = {"sfURL": f"{acct}.snowflakecomputing.com", "sfUser": user, "sfPassword": pwd,
                  "sfWarehouse": wh, "sfDatabase": db, "sfSchema": "BRONZE_LAYER_FRAUD"}
        silver = {"sfURL": f"{acct}.snowflakecomputing.com", "sfUser": user, "sfPassword": pwd,
                  "sfWarehouse": wh, "sfDatabase": db, "sfSchema": "SILVER_LAYER_FRAUD"}

        tx   = (spark.read.format("net.snowflake.spark.snowflake").options(**bronze)
                .option("dbtable","TRANSACTIONS_BRONZE").load())
        users= (spark.read.format("net.snowflake.spark.snowflake").options(**bronze)
                .option("dbtable","USERS_BRONZE").load())
        paym = (spark.read.format("net.snowflake.spark.snowflake").options(**bronze)
                .option("dbtable","PAYMENT_METHODS_BRONZE").load())
        print("Bronze tables loaded.")

        denorm = tx.join(users, "user_id").join(paym, "method_id")
        enriched = denorm.withColumn("transaction_hour", hour(to_timestamp("transaction_timestamp")))

        (enriched.write.format("net.snowflake.spark.snowflake")
            .options(**silver).option("dbtable","FRAUD_TRANSACTIONS_SILVER")
            .mode("overwrite").save())
        print("✔ Silver written: FRAUD_TRANSACTIONS_SILVER")
    except Exception as e:
        print(f"Error in Silver transform: {e}")
    finally:
        if spark: spark.stop()

if __name__ == "__main__":
    transform_to_silver()
