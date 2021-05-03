########################################################################################################
# Title               : Lab-4 (Schema)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 1 ----------
# Create DataFrame from JSON file and look at the Schema
# -----------------------------

df = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
df.printSchema()


# ---------- STEP 2 ----------
# Examine the Schema object
# -----------------------------

spark.read.format("json").load("data/flight-data/json/2015-summary.json").schema

# Output:
#
# StructType(List(StructField(DEST_COUNTRY_NAME,StringType,true), 
# StructField(ORIGIN_COUNTRY_NAME,StringType,true), 
# StructField(count,LongType,true)))


# Explanation:
# 
# A schema is a StructType made up of a number of fields, StructFields, 
# that have a name, type, a Boolean flag which specifies whether that 
# column can contain missing or null values.


# ---------- STEP 3 ----------
# Create and enforce a specific schema on a DataFrame.
#
# Pro tip: In production, it's better to specifically define the schema instead of
# using Spark's infer schema function
# -----------------------------

from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema).load("data/flight-data/json/2015-summary.json")
