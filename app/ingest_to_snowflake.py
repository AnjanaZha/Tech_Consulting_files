# app/ingest_to_snowflake.py
import os
from pyspark.sql import SparkSession
import psycopg2
import pandas as pd

DB_NAME = "fraud_detect"
DB_USER = "root"
DB_PASS = "root"
DB_HOST = "postgres"
DB_PORT = "5432"

def to_spark(df_pd, spark):
    return spark.createDataFrame(df_pd)

def ingest_to_bronze_layer():
    spark = None
    conn = None
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        print("Postgres connection OK.")

        # 1) Read all three tables from Postgres (via pandas)
        users_pd = pd.read_sql_query("SELECT * FROM users;", conn)
        paym_pd  = pd.read_sql_query("SELECT * FROM payment_methods;", conn)
        tx_pd    = pd.read_sql_query("SELECT * FROM transactions;", conn)
        # print(f"Rows → users={len(users_pd)}, payment_methods={len(paym_pd)}, transactions={len(tx_pd)}")
        print(f"Rows → users={len(users_pd)}, payment_methods={len(paym_pd)}, transactions={len(tx_pd)}")


        # 2) Spark session
        spark = SparkSession.builder.appName("Postgres → Snowflake Bronze").getOrCreate()

        users_sdf = to_spark(users_pd, spark)
        paym_sdf  = to_spark(paym_pd,  spark)
        tx_sdf    = to_spark(tx_pd,    spark)

        # 3) Snowflake options
        sf_opts = {
            "sfURL":        f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
            "sfUser":       os.environ["SNOWFLAKE_USER"],
            "sfPassword":   os.environ["SNOWFLAKE_PASSWORD"],
            "sfWarehouse":  os.environ["SNOWFLAKE_WAREHOUSE"],
            "sfDatabase":   os.environ["SNOWFLAKE_DATABASE"],
            "sfSchema":     "BRONZE_LAYER_FRAUD",
        }

        # 4) Write all bronze tables
        (users_sdf.write
            .format("net.snowflake.spark.snowflake")
            .options(**sf_opts).option("dbtable", "USERS_BRONZE")
            .mode("overwrite").save())
        print("Wrote USERS_BRONZE")

        (paym_sdf.write
            .format("net.snowflake.spark.snowflake")
            .options(**sf_opts).option("dbtable", "PAYMENT_METHODS_BRONZE")
            .mode("overwrite").save())
        print("Wrote PAYMENT_METHODS_BRONZE")

        (tx_sdf.write
            .format("net.snowflake.spark.snowflake")
            .options(**sf_opts).option("dbtable", "TRANSACTIONS_BRONZE")
            .mode("overwrite").save())
        print("Wrote TRANSACTIONS_BRONZE")

        print("✔ Bronze layer loaded.")

    except Exception as e:
        print(f"Error in Bronze ingest: {e}")
    finally:
        if conn: conn.close()
        if spark: spark.stop()

if __name__ == "__main__":
    ingest_to_bronze_layer()
