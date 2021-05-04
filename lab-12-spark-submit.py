########################################################################################################
# Title               : Lab-12 (Spark Submit)
# General instruction : Run this script using spark-submit
########################################################################################################

# run using this command:
"""
spark-submit \
    --master local\
    --deploy-mode client\
    lab-12-spark-submit.py
"""

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    print(spark.range(5000).where("id > 500").selectExpr("sum(id)").collect())
