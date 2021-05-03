########################################################################################################
# Title               : Lab-1 (Starting Spark)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 1 ----------
# Start Spark interactive session
# -----------------------------

# Run this command in the Terminal
# $ pyspark

# Look and examine the SparkSession object
# below code will generate something like:
# <pyspark.sql.session.SparkSession at 0x7efda4c1ccd0>
spark


# ---------- STEP 2 ----------
# Create a DataFrame
# -----------------------------

myRange = spark.range(1000).toDF("number")

# show in console
myRange.show()


# ---------- STEP 3 ----------
# Define a transformation, with
# business logic: to find all even numbers in DataFrame 
# -----------------------------

divisBy2 = myRange.where("number % 2 = 0")


# ---------- STEP 4 ----------
# Define an Action to view data in the console
# -----------------------------
divisBy2.count()
