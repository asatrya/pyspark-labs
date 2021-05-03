########################################################################################################
# Title               : Lab-5.02 (DataFrame Transformations -- columns operations)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 1 ----------
# Pass explicit values into Spark 
# that are just a value (rather than a new column)
# using literals
# -----------------------------

from pyspark.sql.functions import lit

# -- in SQL
# SELECT *, 1 as One FROM dfTable LIMIT 2
df.select(expr("*"), lit(1).alias("One")).show(2)


# ---------- STEP 2 ----------
# Adding Columns
# -----------------------------

# -- in SQL
# SELECT *, 1 as numberOne FROM dfTable LIMIT 2
df.withColumn("numberOne", lit(1)).show(2)

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)


# ---------- STEP 3 ----------
# Renaming Columns
# -----------------------------

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns


# ---------- STEP 4 ----------
# Dropping Columns
# -----------------------------

# drop single columns
df.drop("ORIGIN_COUNTRY_NAME").columns

# drop multiple columns
df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")


# ---------- STEP 4 ----------
# Changing a Column's type (cast)
# -----------------------------

# -- in SQL
# SELECT *, cast(count as long) AS count2 FROM dfTable
df.withColumn("count2", col("count").cast("long"))
