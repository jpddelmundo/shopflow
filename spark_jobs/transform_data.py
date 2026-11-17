# Clean raw orders, join products, write partitioned parquet (silver), and daily sales (gold).

import sys

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DataType

spark = (SparkSession.builder
            .appName("shopflow-etl")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

path = ("data/raw/orders/" if "all" else f"data/raw/orders/dt={sys.argv[1]}")

raw = spark.read.json(path, )

# prints the DataFrame schema, 
# schema is infered automatically when using
# spark.read.json()
# raw.printSchema()

# print the row count 
# print(raw.count())

# raw.show()

# duplicate_orders = (raw.groupBy("order_id")
#                     .agg(F.count("order_id").alias("order_count"))
#                     .where(F.col("order_count") > 1))

# show orders with duplicates
# duplicate_orders.show()
# drop duplicate rows
orders = raw.dropDuplicates(["order_id"])
print(f"order_without_dups: {orders.count()} orders_with_dups: {raw.count()}")

lines = (orders.withColumn("item", F.explode("items"))
         .select("order_id", "customer_id", "status",
                 F.to_timestamp("ordered_at").alias("ordered_at"),
                 F.col("item.sku").alias("sku"),
                 F.col("item.qty").alias("qty")))

# lines.show()


products = (spark.read
            .option("multiline", True)
            .option("inferSchema", True)
            .json("data/raw/products.json"))

# products.show(5)

joined = (lines.join(F.broadcast(products.select("sku", "category", "price")), "sku", "left")
          .withColumn("order_date", F.to_date("ordered_at")))

# joined.printSchema()

is_bad = (F.col("qty") <= 0) | F.col("price").isNull() | F.col("status").isNull()

bad = joined.filter(is_bad)
silver = (joined.filter(~is_bad)
          .withColumn("amount", F.round(F.col("qty") * F.col("price"), 2)))

# silver.show(5)



silver.write.mode("overwrite").partitionBy("order_date").parquet("lake/silver/order_lines")
bad.write.mode("overwrite").partitionBy("order_date").parquet("lake/quarantined/order_lines")

paid = spark.read.parquet("lake/silver/order_lines").filter(F.col("status") == "paid")

# paid = silver.filter(F.col("status") == "paid")
daily = (paid.groupBy("order_date", "category")
            .agg(F.count_distinct("order_id").alias("orders"),
                  F.sum("qty").alias("units"),
                  F.round(F.sum("amount"), 2).alias("revenue")))

# daily.orderBy("order_date").show()
daily.write.mode("overwrite").parquet("lake/gold/daily_category_sales")

w = Window.partitionBy("order_date").orderBy(F.desc("units"))
top = (paid.groupBy("order_date", "sku")
      #  .agg(F.round(F.sum("amount"), 2).alias("revenue"))
      .agg(F.sum("qty").alias("units"))
       .withColumn("rank", F.row_number().over(w))
       .filter("rank <= 3"))

top.orderBy("order_date", "rank").show()

print(f"day {path}: raw {raw.count()}, orders {orders.count()}, lines = {lines.count()}" \
      f"clean lines: {silver.count()}, quaranteed lines: {bad.count()}")



spark.stop()   
