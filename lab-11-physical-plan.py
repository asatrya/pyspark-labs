########################################################################################################
# Title               : Lab-11 (Physical Plan)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# Creating a SparkSession in Python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Word Count")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()


# Define series of logical steps
df1 = spark.range(2, 10000000, 2)
df2 = spark.range(2, 10000000, 4)
step1 = df1.repartition(5)
step12 = df2.repartition(6)
step2 = step1.selectExpr("id * 5 as id")
step3 = step2.join(step12, ["id"])
step4 = step3.selectExpr("sum(id)")

# trigger an action
step4.collect() # 2500000000000

# see the physical plan
step4.explain()
