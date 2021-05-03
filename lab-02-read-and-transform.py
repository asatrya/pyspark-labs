########################################################################################################
# Title               : Lab-2 (Read data and perform transformations)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 1 ----------
# Read data from CSV
# -----------------------------

flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("data/flight-data/csv/2015-summary.csv")

# show the data
flightData2015.show()


# ---------- STEP 2 ----------
# Get the number of partitions
# -----------------------------

flightData2015.rdd.getNumPartitions()


# ---------- STEP 3 ----------
# Perform wide transformation
# -----------------------------

df = flightData2015.sort("count")
df.rdd.getNumPartitions()


# ---------- STEP 4 ----------
# Perform wide transformation with configuring Shuffle partitions
# -----------------------------

spark.conf.set("spark.sql.shuffle.partitions", "5")
df = flightData2015.sort("count")
df.rdd.getNumPartitions()