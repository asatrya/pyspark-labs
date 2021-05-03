########################################################################################################
# Title               : Lab-7 (Working with Complex Types)
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
# Structs
# -----------------------------

# We can create a struct by wrapping a set of columns in parenthesis in a query
from pyspark.sql.functions import struct
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.show()
complexDF.createOrReplaceTempView("complexDF")

# We can query it using a dot syntax
complexDF.select("complex.Description").show()

# We can also query all values in the struct by using * 
# This brings up all the columns to the top- level DataFrame
complexDF.select("complex.*").show()


# ---------- STEP 2 ----------
# Array
# -----------------------------

# split
# --
# The first task is to turn our Description column into a complex type, an array. split
# We do this by using the split function and specify the delimiter:
from pyspark.sql.functions import split, col
df.select(split(col("Description"), " ")).show(2)


# Spark allows us to manipulate this complex type as another column. 
# We can also query the values of the array using Python-like syntax
df.select(split(col("Description"), " ").alias("array_col"))\
  .selectExpr("array_col[0]").show(2)


# explode
# --
# The explode function takes a column that consists of arrays 
# and creates one row (with the rest of the values duplicated) 
# per value in the array
from pyspark.sql.functions import split, explode

df.withColumn("splitted", split(col("Description"), " "))\
  .withColumn("exploded", explode(col("splitted")))\
  .select("Description", "InvoiceNo", "exploded").show(2)


# ---------- STEP 3 ----------
# Maps
# -----------------------------

# Maps are created by using the map function and key-value pairs of columns. 
# You then can select them just like you might select from an array:
from pyspark.sql.functions import create_map
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .show(2)

# You can query them by using the proper key. A missing key returns null:
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)


# You can also explode map types, which will turn them into columns
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("explode(complex_map)").show(2)


# ---------- STEP 4 ----------
# JSON
# -----------------------------

# Let’s begin by creating a JSON column
jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

# show it
jsonDF.show()


# You can use the get_json_object to inline query a JSON object
# You can use json_tuple if this object has only one level of nesting
from pyspark.sql.functions import get_json_object, json_tuple

jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]").alias('column'),
    json_tuple(col("jsonString"), "myJSONKey")).show(2)


# You can also turn a StructType into a JSON string by using the to_json function
from pyspark.sql.functions import to_json
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct"))).show()
