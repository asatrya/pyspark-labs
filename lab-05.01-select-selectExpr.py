########################################################################################################
# Title               : Lab-5.01 (DataFrame Transformations -- select and selectExpr)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 1 ----------
# See the columns and rows of a DataFrame
# -----------------------------

df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")

# see the list of columns in the DataFrame
df.columns

# see the first Row of the DataFrame
df.first()


# ---------- STEP 2 ----------
# Create a table from a DataFrame 
# so that we can process it with SQL
# -----------------------------

df.createOrReplaceTempView("dfTable")


# ---------- STEP 3 ----------
# Manipulate DataFrame columns
# -----------------------------

# this is equivalent with SQL query:
# SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2
df.select("DEST_COUNTRY_NAME").show(2)


# this is equivalent with SQL query:
# SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


# ---------- STEP 4 ----------
# Refer to columns in a number of different ways:
# 1. using col function
# 2. using expr function
# 
# keep in mind that you can use them interchangeably
# -----------------------------

from pyspark.sql.functions import expr, col

df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"))\
  .show(2)


# expr is the most flexible reference that we can use. 
# It can refer to a plain column or a string manipulation of a column. 
# To illustrate, let’s change the column name
# -- in SQL: SELECT DEST_COUNTRY_NAME as destination FROM dfTable LIMIT 2
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)


# You can further manipulate the result of your expression as another expression
# To illustrate, from previous example, change it back by the alias method on the column
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)


# ---------- STEP 5 ----------
# Because select followed by a series of expr is such a common pattern, 
# Spark has a shorthand for doing this efficiently: selectExpr
# -----------------------------

# -- in SQL:
# SELECT DEST_COUNTRY_NAME as newColumnName, DEST_COUNTRY_NAME FROM dfTable LIMIT 2
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

# -- in SQL:
# SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry FROM dfTable LIMIT 2
df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)


# With select expression, we can also specify aggregations over the entire DataFrame
# -- in SQL:
# SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
