########################################################################################################
# Title               : Lab-6 (Working with Different Types of Data)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 0 ----------
# Read data from CSV
# -----------------------------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("data/retail-data/by-day/2010-12-01.csv")

# examine the schema
df.printSchema()

# create table for SQL processing
df.createOrReplaceTempView("dfTable")


# ---------- STEP 1 ----------
# Working with Booleans
# -----------------------------

from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
  .select("InvoiceNo", "Description")\
  .show(5, False)


# In Spark, you should always chain together and filters as a sequential filter.
# The reason for this is that even if Boolean statements are expressed serially 
# (one after the other), Spark will flatten all of these filters into 
# one statement and perform the filter at the same time, creating the and statement for us.
from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT"))\
  .where(priceFilter | descripFilter)\
  .show()


# Boolean expressions are not just reserved to filters. 
# To filter a DataFrame, you can also just specify a Boolean column:
from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice", "isExpensive").show(5)


# ---------- STEP 2 ----------
# Working with Numeric
# -----------------------------

# Rounding
from pyspark.sql.functions import lit, round, bround
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)


# Correlation of two columns
# --
# For example, we can see the Pearson correlation coefficient for two columns 
# to see if cheaper things are typically bought in greater quantities.
from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()


# Compute summary statistics for a column or set of columns. 
# --
# We can use the describe method to achieve exactly this. 
# This will take all numeric columns and
# calculate the count, mean, standard deviation, min, and max.
df.describe().show()


# ---------- STEP 3 ----------
# Working with Strings
# -----------------------------

# The initcap function will capitalize every word 
# in a given string when that word is separated from another by a space.
from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()


# cast strings in uppercase and lowercase
from pyspark.sql.functions import lower, upper
df.select(
  col("Description"),
  lower(col("Description")),
  upper(lower(col("Description")))).show(2)


# adding or removing spaces around a string. 
# You can do this by using lpad, ltrim, rpad and rtrim, trim
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("    HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)


# Using regex, replace any "BLACK|WHITE|RED|GREEN|BLUE" words with "COLOR"
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
  col("Description")).show(2)


# Replace given characters with other characters using translate
from pyspark.sql.functions import translate
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\
  .show(2)


# Extract string
from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
     regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
     col("Description")).show(2)


# Check for string existence
from pyspark.sql.functions import instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
  .where("hasSimpleColor")\
  .select("Description").show(3, False)


# ---------- STEP 4 ----------
# Working with Dates and Timestamps
# -----------------------------

# Let’s begin with the basics and get the current date and the current timestamps
from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.show()

# examine the schema
dateDF.printSchema()

# create table for SQL processing
dateDF.createOrReplaceTempView("dateTable")


# let’s add and subtract five days from toda
from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)


# take a look at the difference between two dates
from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)

# get the number of months between two dates with months_between
dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end"))).show(1)


# The to_date function allows you to convert a string to a date, 
# optionally with a specified format. We specify our format in 
# the Java SimpleDateFormat 
from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
  .select(to_date(col("date"))).show(1)


# convert a string to a date with a specified format
from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.show()

# create table for SQL processing
cleanDateDF.createOrReplaceTempView("dateTable2")


# convert date to timestamp
from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()


# ---------- STEP 5 ----------
# Working with NULL values
# -----------------------------

# select the first non-null value from a set of columns by using the coalesce function
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()


# removes rows that contain nulls
df.na.drop("all", subset=["StockCode", "InvoiceNo"])


# to fill all null values in columns "StockCode" and "InvoiceNo"
df.na.fill("all", subset=["StockCode", "InvoiceNo"])


# to fill all null values in columns "StockCode" and "Description"
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)
