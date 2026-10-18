
import os
import re

import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

JDBC_JAR = "/opt/postgresql-42.7.7.jar"


def norm(name):
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def read_table(spark, options, table):
    df = spark.read.format("jdbc").options(**options).option("dbtable", table).load()
    names = [norm(c) for c in df.columns]
    if len(names) != len(set(names)):
        raise ValueError(f"Duplicate normalized column names in {table}: {names}")
    return df.toDF(*names)


def rename(df, target, aliases=(), required=True):
    for name in (target, *aliases):
        if name in df.columns:
            return df if name == target else df.withColumnRenamed(name, target)
    if required:
        raise ValueError(f"Missing column {target}; available columns: {df.columns}")
    return df


def clean_id(df, name):
    return df.withColumn(name, F.trim(F.col(name).cast("string")))


def check_unique(df, keys, label):
    df = df.dropDuplicates()
    duplicate = (
        df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).collect()
    )
    if duplicate:
        raise ValueError(f"{label} has conflicting rows for {keys}: {duplicate[0]}")
    return df


def write_table(df, options, schema, table):
    (df.write.format("jdbc").options(**options)
        .option("dbtable", f"{schema}.{table}")
        .option("batchsize", "5000")
        .mode("overwrite").save())


def to_silver():
    host = os.getenv("DB_HOST") or "postgres"
    port = os.getenv("DB_PORT") or "5432"
    database = os.getenv("DB_NAME") or "airflow"
    user = os.getenv("DB_USER") or "airflow"
    password = os.getenv("DB_PASSWORD") or "airflow"
    output_schema = os.getenv("OUTPUT_SCHEMA") or "silver"

    if not os.path.isfile(JDBC_JAR):
        raise FileNotFoundError(f"JDBC JAR not found: {JDBC_JAR}")

    options = {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "driver": "org.postgresql.Driver",
        "user": user,
        "password": password,
    }

    # Ensure the output schema exists.
    conn = psycopg2.connect(
        host=host, port=port, dbname=database, user=user, password=password
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(output_schema)
                )
            )
        conn.commit()
    finally:
        conn.close()

    spark = (
        SparkSession.builder.appName("coldchain-silver")
        .master("local[*]")
        .config("spark.jars", JDBC_JAR)
        .config("spark.driver.extraClassPath", JDBC_JAR)
        .getOrCreate()
    )

    try:
        # Read source dimensions.
        p = read_table(spark, options, os.getenv("PRODUCT_DIM_TABLE", "dim.dim_product"))
        p = rename(p, "product_id", ("sku_id",))
        p = rename(p, "product_name", ("name",), required=False)
        p = rename(p, "category", ("product_category",), required=False)
        p = rename(p, "unit_cost", ("standard_cost", "cost_per_unit", "cost"))
        if "product_name" not in p.columns:
            p = p.withColumn("product_name", F.lit(None).cast("string"))
        if "category" not in p.columns:
            p = p.withColumn("category", F.lit("Unknown"))
        p = clean_id(p, "product_id").select(
            "product_id", "product_name", "category", "unit_cost"
        )
        p = (p.withColumn("product_name", F.coalesce("product_name", "product_id"))
              .withColumn("category", F.coalesce("category", F.lit("Unknown")))
              .withColumn("unit_cost", F.col("unit_cost").cast("double")))
        p = check_unique(p, ["product_id"], "Product dimension")

        sdim = read_table(spark, options, os.getenv(
            "SUPPLIER_DIM_TABLE", "dim.dim_suppiler"
        ))
        sdim = rename(sdim, "supplier_id", ("suppiler_id", "vendor_id"))
        sdim = rename(sdim, "supplier_name", ("suppiler_name", "name"), False)
        if "supplier_name" not in sdim.columns:
            sdim = sdim.withColumn("supplier_name", F.lit(None).cast("string"))
        sdim = clean_id(sdim, "supplier_id").select("supplier_id", "supplier_name")
        sdim = sdim.withColumn(
            "supplier_name", F.coalesce("supplier_name", "supplier_id")
        )
        sdim = check_unique(sdim, ["supplier_id"], "Supplier dimension")

        wdim = read_table(spark, options, os.getenv(
            "WAREHOUSE_DIM_TABLE", "dim.dim_warehouse"
        ))
        wdim = rename(wdim, "warehouse_id", ("warehosue_id",))
        wdim = rename(wdim, "warehouse_name", ("warehosue_name", "name"), False)
        if "warehouse_name" not in wdim.columns:
            wdim = wdim.withColumn("warehouse_name", F.lit(None).cast("string"))
        wdim = clean_id(wdim, "warehouse_id").select(
            "warehouse_id", "warehouse_name"
        )
        wdim = wdim.withColumn(
            "warehouse_name", F.coalesce("warehouse_name", "warehouse_id")
        )
        wdim = check_unique(wdim, ["warehouse_id"], "Warehouse dimension")

        # Inventory: one row per snapshot, product, and warehouse.
        inv = read_table(spark, options, os.getenv(
            "INVENTORY_TABLE", "fact.fact_inventory"
        ))
        inv = rename(inv, "snapshot_date", ("inventory_date", "date"))
        inv = rename(inv, "product_id", ("sku_id",))
        inv = rename(inv, "warehouse_id", ("warehosue_id",))
        inv = rename(inv, "stock_qty", ("quantity_on_hand", "on_hand_qty", "qty_on_hand"))
        inv = (inv.withColumn("snapshot_date", F.to_date("snapshot_date"))
                  .withColumn("stock_qty", F.col("stock_qty").cast("double")))
        inv = clean_id(clean_id(inv, "product_id"), "warehouse_id")
        inv = inv.select("snapshot_date", "product_id", "warehouse_id", "stock_qty")
        inv = check_unique(
            inv, ["snapshot_date", "product_id", "warehouse_id"],
            "Inventory snapshot"
        )

        # Sales and rolling periods, anchored to the latest valid sales date.
        sales = read_table(spark, options, os.getenv("SALES_TABLE", "fact.fact_sales"))
        sales = rename(sales, "sale_date", ("sales_date", "date"))
        sales = rename(sales, "product_id", ("sku_id",))
        sales = rename(sales, "qty_sold", ("quantity_sold", "quantity", "qty"))
        sales = rename(sales, "sales_value", ("sales_amount", "amount", "revenue"))
        sales = rename(sales, "sale_id", ("transaction_id",), False)
        sales = rename(sales, "warehouse_id", ("warehosue_id",), False)
        has_sale_id = "sale_id" in sales.columns
        has_sales_wh = "warehouse_id" in sales.columns

        if not has_sales_wh:
            sales = sales.withColumn("warehouse_id", F.lit(None).cast("string"))
        else:
            sales = clean_id(sales, "warehouse_id")
        sales = clean_id(sales, "product_id")
        sales = (sales.withColumn("sale_date", F.to_date("sale_date"))
                      .withColumn("qty_sold", F.col("qty_sold").cast("double"))
                      .withColumn("sales_value", F.col("sales_value").cast("double")))

        sales_cols = ["sale_date", "product_id", "warehouse_id", "qty_sold", "sales_value"]
        if has_sale_id:
            sales = clean_id(sales, "sale_id")
            sales_cols.append("sale_id")
        sales = sales.select(*sales_cols).filter(
            F.col("sale_date").isNotNull()
            & F.col("product_id").isNotNull()
            & (F.col("qty_sold") > 0)
            & (F.col("sales_value") >= 0)
        ).dropDuplicates()

        anchor = sales.agg(F.max("sale_date")).first()[0]
        if anchor is None:
            raise ValueError("No valid sales rows found.")

        sales12 = sales.filter(F.col("sale_date").between(
            F.add_months(F.lit(anchor), -12), F.lit(anchor)
        ))
        sales90 = sales.filter(F.col("sale_date").between(
            F.date_sub(F.lit(anchor), 89), F.lit(anchor)
        ))
        sales30 = sales.filter(F.col("sale_date").between(
            F.date_sub(F.lit(anchor), 29), F.lit(anchor)
        ))

        # ABC class by 12-month revenue.
        abc = p.select("product_id", "product_name", "category").join(
            sales12.groupBy("product_id").agg(
                F.sum("sales_value").alias("revenue_12m")
            ),
            "product_id", "left"
        ).withColumn("revenue_12m", F.coalesce("revenue_12m", F.lit(0.0)))

        total_revenue = abc.agg(F.sum("revenue_12m")).first()[0] or 0.0
        cw = Window.orderBy(F.col("revenue_12m").desc(), F.col("product_id")) \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        abc = (abc.withColumn("cumulative_revenue", F.sum("revenue_12m").over(cw))
            .withColumn("cumulative_pct", F.when(
                F.lit(total_revenue) > 0,
                F.col("cumulative_revenue") / F.lit(total_revenue) * 100
            ).otherwise(0.0))
            .withColumn("abc_class", F.when(F.col("cumulative_pct") <= 80, "A")
                .when(F.col("cumulative_pct") <= 95, "B").otherwise("C")))

        abc_summary = abc.groupBy("abc_class").agg(
            F.sum("revenue_12m").alias("revenue_12m"),
            F.countDistinct("product_id").alias("sku_count")
        )

        # Product ADS, recent-sale flags, and last sale date.
        ads = sales90.groupBy("product_id").agg(
            F.sum("qty_sold").alias("qty90"),
            F.countDistinct("sale_date").alias("days_sold90")
        ).withColumn(
            "ads90", F.when(F.col("days_sold90") > 0, F.col("qty90") / F.col("days_sold90"))
                     .otherwise(0.0)
        ).select("product_id", "ads90")

        sold90 = sales90.select("product_id").distinct().withColumn("sold90", F.lit(True))
        sold30 = sales30.select("product_id").distinct().withColumn("sold30", F.lit(True))
        last_sale = sales.groupBy("product_id").agg(
            F.max("sale_date").alias("last_sale_date")
        )

        # Warehouse ADS is only calculated when sales includes warehouse IDs.
        has_sales_wh = has_sales_wh and sales.filter(
            F.col("warehouse_id").isNotNull()
        ).limit(1).count() > 0
        if has_sales_wh:
            wads = (sales90.filter(F.col("warehouse_id").isNotNull())
                .groupBy("product_id", "warehouse_id")
                .agg(F.sum("qty_sold").alias("qty90"),
                     F.countDistinct("sale_date").alias("days90"))
                .withColumn("warehouse_ads90",
                    F.when(F.col("days90") > 0, F.col("qty90") / F.col("days90"))
                     .otherwise(0.0))
                .select("product_id", "warehouse_id", "warehouse_ads90"))

        # Latest inventory snapshot and turnover.
        latest = inv.agg(F.max("snapshot_date")).first()[0]
        if latest is None:
            raise ValueError("No inventory snapshots found.")
        current = inv.filter(F.col("snapshot_date") == F.lit(latest))
        inv12 = inv.filter(F.col("snapshot_date").between(
            F.add_months(F.lit(latest), -12), F.lit(latest)
        ))
        avg_stock = (inv12.groupBy("product_id", "snapshot_date")
            .agg(F.sum("stock_qty").alias("daily_stock"))
            .groupBy("product_id")
            .agg(F.avg("daily_stock").alias("avg_stock12")))
        turnover = (sales12.groupBy("product_id")
            .agg(F.sum("qty_sold").alias("qty12"))
            .join(avg_stock, "product_id", "full")
            .withColumn("turnover12", F.when(
                F.col("avg_stock12") > 0,
                F.round(F.coalesce(F.col("qty12"), F.lit(0.0)) / F.col("avg_stock12"), 2)
            )))

        # Current inventory health, dead stock, and stock-outs.
        metrics = (current.join(wdim, "warehouse_id", "left")
            .join(p, "product_id", "left")
            .join(abc.select("product_id", "revenue_12m", "cumulative_pct", "abc_class"),
                  "product_id", "left")
            .join(ads, "product_id", "left")
            .join(sold90, "product_id", "left")
            .join(sold30, "product_id", "left")
            .join(last_sale, "product_id", "left")
            .join(turnover.select("product_id", "avg_stock12", "turnover12"),
                  "product_id", "left")
            .withColumn("ads90", F.coalesce("ads90", F.lit(0.0)))
            .withColumn("days_of_cover", F.when(
                F.col("ads90") > 0, F.round(F.col("stock_qty") / F.col("ads90"), 2)
            ).otherwise(999.0))
            .withColumn("no_sale_90d", F.col("sold90").isNull())
            .withColumn("dead_stock_value", F.when(
                F.col("sold90").isNull() & (F.col("stock_qty") > 0)
                & F.col("unit_cost").isNotNull(),
                F.col("stock_qty") * F.col("unit_cost")
            ).otherwise(0.0))
            .withColumn("is_stock_out",
                (F.col("stock_qty") <= 0) & F.coalesce("sold30", F.lit(False)))
        )

        if has_sales_wh:
            metrics = (metrics.join(wads, ["product_id", "warehouse_id"], "left")
                .withColumn("warehouse_ads90", F.coalesce("warehouse_ads90", F.lit(0.0)))
                .withColumn("warehouse_days_of_cover", F.when(
                    F.col("warehouse_ads90") > 0,
                    F.round(F.col("stock_qty") / F.col("warehouse_ads90"), 2)
                ).otherwise(999.0)))
            warehouse_cover = metrics.groupBy("warehouse_id", "warehouse_name").agg(
                F.sum("stock_qty").alias("stock_qty"),
                F.sum("warehouse_ads90").alias("avg_daily_demand")
            ).withColumn("days_of_cover", F.when(
                F.col("avg_daily_demand") > 0,
                F.round(F.col("stock_qty") / F.col("avg_daily_demand"), 2)
            ).otherwise(999.0))
        else:
            metrics = metrics.withColumn(
                "warehouse_days_of_cover", F.lit(None).cast("double")
            )
            warehouse_cover = current.join(wdim, "warehouse_id", "left").groupBy(
                "warehouse_id", "warehouse_name"
            ).agg(F.sum("stock_qty").alias("stock_qty")).withColumn(
                "avg_daily_demand", F.lit(None).cast("double")
            ).withColumn("days_of_cover", F.lit(None).cast("double"))

        dead = metrics.filter(F.col("dead_stock_value") > 0)
        stock_out = metrics.filter(F.col("is_stock_out"))
        dead_category = dead.groupBy("category").agg(
            F.sum("dead_stock_value").alias("dead_stock_value"),
            F.countDistinct("product_id").alias("sku_count")
        )

        # Purchase orders: normalize the source typo once, then use received_qty.
        po = read_table(spark, options, os.getenv(
            "PO_TABLE", "fact.fact_purchase_order"
        ))
        po = rename(po, "po_no", ("po_number", "purchase_order_no", "po_id"))
        po = rename(po, "supplier_id", ("suppiler_id", "vendor_id"))
        po = rename(po, "product_id", ("sku_id",))
        po = rename(po, "received_qty",
                    ("reveived_qty", "quantity_received", "received_quantity"))
        po = rename(po, "ordered_qty", ("quantity_ordered", "order_qty"))
        po = rename(po, "po_unit_price", ("unit_price", "po_price", "price"))
        po = rename(po, "invoice_unit_price", ("invoice_price",), False)
        for c, aliases in (
            ("order_date", ("po_date",)),
            ("promised_date", ("promised_delivery_date", "promise_date")),
            ("delivery_date", ("actual_delivery_date",)),
        ):
            po = rename(po, c, aliases, False)
            if c not in po.columns:
                po = po.withColumn(c, F.lit(None).cast("date"))
            else:
                po = po.withColumn(c, F.to_date(c))
        if "invoice_unit_price" not in po.columns:
            po = po.withColumn("invoice_unit_price", F.lit(None).cast("double"))

        for c in ("po_no", "supplier_id", "product_id"):
            po = clean_id(po, c)
        po = (po.withColumn("ordered_qty", F.col("ordered_qty").cast("double"))
            .withColumn("received_qty", F.col("received_qty").cast("double"))
            .withColumn("po_unit_price", F.col("po_unit_price").cast("double"))
            .withColumn("invoice_unit_price", F.col("invoice_unit_price").cast("double"))
            .dropDuplicates())

        closed = (po.filter(
            F.col("delivery_date").isNotNull() & (F.col("received_qty") > 0)
        ).withColumn("otif", F.when(
            F.col("promised_date").isNotNull()
            & (F.col("delivery_date") <= F.col("promised_date"))
            & (F.col("received_qty") >= F.col("ordered_qty")), 1
        ).otherwise(0))
         .withColumn("actual_lead_days", F.datediff("delivery_date", "order_date"))
         .withColumn("promised_lead_days", F.datediff("promised_date", "order_date"))
         .withColumn("spend", F.col("received_qty") * F.col("po_unit_price")))

        closed = (closed.join(sdim, "supplier_id", "left")
            .join(p.select("product_id", "product_name", "category"), "product_id", "left")
            .withColumn("supplier_name", F.coalesce("supplier_name", "supplier_id"))
            .withColumn("product_name", F.coalesce("product_name", "product_id"))
            .withColumn("category", F.coalesce("category", F.lit("Unknown"))))

        supplier_agg = closed.groupBy("supplier_id").agg(
            F.count("*").alias("closed_lines"),
            F.sum("otif").alias("otif_lines"),
            F.avg("actual_lead_days").alias("actual_lead_days"),
            F.avg("promised_lead_days").alias("promised_lead_days"),
            F.sum("spend").alias("closed_spend")
        )
        supplier_ids = sdim.select("supplier_id").unionByName(
            supplier_agg.select("supplier_id")
        ).distinct()
        rank = Window.orderBy(F.col("closed_spend").desc(), F.col("supplier_id"))
        supplier_metrics = (supplier_ids.join(sdim, "supplier_id", "left")
            .join(supplier_agg, "supplier_id", "left")
            .withColumn("supplier_name", F.coalesce("supplier_name", "supplier_id"))
            .withColumn("closed_lines", F.coalesce("closed_lines", F.lit(0)))
            .withColumn("otif_lines", F.coalesce("otif_lines", F.lit(0)))
            .withColumn("closed_spend", F.coalesce("closed_spend", F.lit(0.0)))
            .withColumn("otif_pct", F.when(
                F.col("closed_lines") > 0,
                F.round(F.col("otif_lines") / F.col("closed_lines") * 100, 2)
            ).otherwise(0.0))
            .withColumn("spend_rank", F.row_number().over(rank)))

        spend_category = closed.groupBy(
            "supplier_id", "supplier_name", "category"
        ).agg(F.sum("spend").alias("closed_spend"))

        variance = (closed.filter(
            F.col("invoice_unit_price").isNotNull()
            & F.col("po_unit_price").isNotNull()
        ).withColumn(
            "variance_inr",
            (F.col("invoice_unit_price") - F.col("po_unit_price")) * F.col("received_qty")
        ).withColumn(
            "variance_pct",
            F.when(F.col("po_unit_price") != 0,
                (F.col("invoice_unit_price") - F.col("po_unit_price"))
                / F.col("po_unit_price") * 100)
        ).select(
            "supplier_id", "supplier_name", "product_id", "product_name",
            "po_no", "po_unit_price", "invoice_unit_price", "received_qty",
            "variance_inr", "variance_pct"
        ))

        # Executive KPI cards.
        executive = (closed.agg(
            F.count("*").alias("closed_lines"),
            F.sum("otif").alias("otif_lines"),
            F.sum("spend").alias("total_closed_spend")
        ).crossJoin(dead.agg(
            F.sum("dead_stock_value").alias("dead_stock_value")
        )).crossJoin(stock_out.agg(
            F.countDistinct("product_id").alias("stock_out_skus")
        )).withColumn(
            "overall_otif_pct",
            F.when(F.col("closed_lines") > 0,
                F.round(F.col("otif_lines") / F.col("closed_lines") * 100, 2)
            ).otherwise(0.0)
        ))

        # Superset-ready outputs.
        outputs = {
            "dashboard_executive_kpis": executive,
            "dashboard_inventory_health": metrics,
            "dashboard_abc_products": abc,
            "dashboard_abc_summary": abc_summary,
            "dashboard_dead_stock": dead,
            "dashboard_dead_stock_category": dead_category,
            "dashboard_stock_out": stock_out,
            "dashboard_warehouse_cover": warehouse_cover,
            "dashboard_supplier_metrics": supplier_metrics,
            "dashboard_top_10_suppliers": supplier_metrics.filter(
                F.col("spend_rank") <= 10
            ),
            "dashboard_supplier_category_spend": spend_category,
            "dashboard_price_variance": variance,
        }
        for table, df in outputs.items():
            write_table(df, options, output_schema, table)

    finally:
        spark.stop()


if __name__ == "__main__":
    to_silver() 