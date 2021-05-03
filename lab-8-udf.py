########################################################################################################
# Title               : Lab-8 (User Defined Function)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 0 ----------
# Create sample DataFrame
# -----------------------------

udfExampleDF = spark.range(5).toDF("num")


# ---------- STEP 1 ----------
# Define Python function
# -----------------------------

def power3(double_value):
  return double_value ** 3

# test the function
power3(2.0)


# ---------- STEP 2 ----------
# Define Python function as Spark UDF
# -----------------------------

from pyspark.sql.functions import udf
power3udf = udf(power3)


# ---------- STEP 3 ----------
# Use the UDF in the DataFrame
# -----------------------------

from pyspark.sql.functions import col
udfExampleDF.select(power3udf(col("num"))).show(2)
