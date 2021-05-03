########################################################################################################
# Title               : Lab-5.03 (DataFrame Transformations -- rows operations)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 1 ----------
# Filtering rows
# -----------------------------

# -- in SQL
# SELECT * FROM dfTable WHERE count < 2 LIMIT 2
df.where(col("count") < 2).show(2)

# -- in SQL
# SELECT * FROM dfTable WHERE count < 2 AND ORIGIN_COUNTRY_NAME != "Croatia" LIMIT 2
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)


# ---------- STEP 2 ----------
# Getting unique rows
# -----------------------------

# -- in SQL
# SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM dfTable
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()


# -- in SQL
# SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME) FROM dfTable
df.select("ORIGIN_COUNTRY_NAME").distinct().count()


# ---------- STEP 3 ----------
# Sorting rows
# -----------------------------

# -- in SQL
# SELECT * FROM dfTable ORDER BY count LIMIT 5
df.sort("count").show(5)

# -- in SQL
# SELECT * FROM dfTable ORDER BY count, DEST_COUNTRY_NAME LIMIT 5
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)

# equivalent with above
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

# To more explicitly specify sort direction, you need to use the asc and desc functions
from pyspark.sql.functions import desc, asc

# -- in SQL
# SELECT * FROM dfTable ORDER BY count DESC, DEST_COUNTRY_NAME ASC LIMIT 2
df.orderBy(expr("count desc")).show(2)

# equivalent with above
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)
