########################################################################################################
# Title               : Lab-3 (Spark SQL)
# Pre-requisite       : Finished Lab-2
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 1 ----------
# You can make any DataFrame into a table or view with one simple method call
# -----------------------------

flightData2015.createOrReplaceTempView("flight_data_2015")


# ---------- STEP 2 ----------
# We will compare the underlying plans generated from SQL query and DataFrame code
# -----------------------------

# Express the logic in SQL query
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

# Express the logic in DataFrame code
dataFrameWay = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .count()

# output explain plan from SQL query
sqlWay.explain()

# output explain plan from DataFrame code
dataFrameWay.explain()

# EXAMINE: the both outputs must be identical


# ---------- STEP 3 ----------
# We will use the max function, to establish the maximum number of flights to and from any given location
# -----------------------------

# use SQL query
spark.sql("SELECT max(count) from flight_data_2015").take(1)

# use DataFrame code
from pyspark.sql.functions import max

flightData2015.select(max("count")).take(1)

# EXAMINE: both will give the same result: 370,002


# ---------- STEP 4 ----------
# find the top five destination countries in the data
# -----------------------------

# use SQL query
maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()


# use DataFrame code
from pyspark.sql.functions import desc

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .show()


# ---------- STEP 5 ----------
# analyze the execution plan
# -----------------------------

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .explain()

