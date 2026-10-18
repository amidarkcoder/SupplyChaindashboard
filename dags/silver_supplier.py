from pyspark.sql.functions import F
from pyspark.sql import SparkSession, Window
import os
from datetime import datetime

JDBC_JAR = "/opt/postgresql-42.7.7.jar"

def sliver_suppiler():
    spark = (
        SparkSession.builder
        .appName("coldchain-silver")
        .master("local[*]")
        .config("spark.jars", JDBC_JAR)
        .config("spark.driver.extraClassPath", JDBC_JAR)
        .getOrCreate()
    )
    host=os.getenv("DB_HOST", "localhost")
    database=os.getenv("DB_NAME")
    user=os.getenv("DB_USER")
    password=os.getenv("DB_PASSWORD")
    port=os.getenv("DB_PORT", "5432")
    table_inventory = "fact.fact_inventory"
    table_po = "fact.fact_purchase_order"
    table_sale = "fact.fact_sales"

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    df_inventory = spark.read.format("jdbc").option("url",jdbc_url) \
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", table_inventory)\
            .option("user", user)\
            .option("password", password)\
            .load()

    df_order = spark.read.format("jdbc").option("url",jdbc_url) \
            .option("driver", "org.postgressql.Driver")\
            .option("dbtable", table_po)\
            .option("usesr", user)\
            .option("possword", password)\
            .load()

    df_sale = spark.read.format("jdbc").option("url",jdbc_url) \
            .option("driver", "org.postgressql.Driver")\
            .option("dbtable", table_sale)\
            .option("usesr", user)\
            .option("possword", password)\
            .load()
    
    df_a= (
        df_order.withColumn("otif",
                            F.when(F.col("promised_date") > F.col("delivery_date"),"No")
                            .otherwise("Yes")
    )
    )

    df_otif = (
        df_a.groupby("suppiler_id")
        .agg(F.count("*").alias("total_count"),
             F.sum(F.when("otif" == "Yes",1).otherwise(0)).alias("yes_count"),
             F.sum(F.when("otif" == "No",1).otherwise(0)).alias("no_count")
             )
        .withColumn("pts_yes",F.round(F.col("yes_count") / F.col("total_count") * 100,2))
        .withColumn("pts_no",F.round(F.col("no_count") / F.col("total_count") * 100,2))
    )

    df_lead_time = (
        df_order.withColumn("leadtime",F.datediff(F.col("delivery_date") - F.col("ordered_qty"),2))
                .groupBy("suppiler_id")
                .agg(F.round(F.avg("leadtime"),2).alias("avg_leadtime"))
    )