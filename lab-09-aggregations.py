########################################################################################################
# Title               : Lab-09 (Aggregations)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 1 ----------
# Read data, cache, and create table
# -----------------------------

# we use coalesce function to repartition the data 
# to have far fewer partitions 
# because we know it’s a small volume of data stored in a lot of small files
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("data/retail-data/all/*.csv")\
  .coalesce(5)
df.show()

# caching the results for rapid access.
# the data will persist in memory
# and spark doesn't have to read from disk 
# every time the data accessed in the future
df.cache()

# create table for SQL processing
df.createOrReplaceTempView("dfTable")


# ---------- STEP 2 ----------
# Count
# -----------------------------

# count all
from pyspark.sql.functions import count
df.select(count("StockCode")).show() # 541909


# count distinct
from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show() # 4070


# Often, we find ourselves working with large datasets 
# and the exact distinct count is irrelevant. 
# There are times when an approximation to a certain degree of accuracy 
# will work just fine, and for that, you can use the approx_count_distinct function
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364


# ---------- STEP 3 ----------
# First & Last, Min & Max
# -----------------------------

# first & last
from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()

# min & max
from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()


# ---------- STEP 4 ----------
# Sum
# -----------------------------

# sum all
from pyspark.sql.functions import sum
df.select(sum("Quantity")).show() # 5176450

# sum disitinct
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show() # 29310


# ---------- STEP 5 ----------
# Average
# -----------------------------

from pyspark.sql.functions import sum, count, avg, expr

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()


# ---------- STEP 6 ----------
# Grouping
# -----------------------------

# perform count() aggregation on group level
# in this case, count by InvoiceNo
from pyspark.sql.functions import count
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()

# Group by InvoiceNo and CustomerId
df.groupBy("InvoiceNo", "CustomerId").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()


# ---------- STEP 7 ----------
# Window function
# -----------------------------

from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
  .partitionBy("CustomerId", "date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)


# COMMAND ----------

from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)


# COMMAND ----------

from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)


# COMMAND ----------

from pyspark.sql.functions import col

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


# COMMAND ----------

dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")


# COMMAND ----------

rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
  .orderBy("Date")
rolledUpDF.show()


# COMMAND ----------

from pyspark.sql.functions import sum

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()


# COMMAND ----------

pivoted = dfWithDate.groupBy("date").pivot("Country").sum()


# COMMAND ----------

