from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("shopflow")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

spark.read.parquet("lake/silver/order_lines").createOrReplaceTempView("order_lines")

query = """SELECT category, count(*) AS lines, round(sum(amount)) AS revenue
        FROM order_lines
        WHERE status = 'paid'
        GROUP BY category ORDER BY revenue DESC"""

result = spark.sql(query)

result.show()

result.explain()

spark.stop()