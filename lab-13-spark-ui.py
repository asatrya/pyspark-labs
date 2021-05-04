########################################################################################################
# Title               : Lab-13 (Spark Submit)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################

spark.read\
  .option("header", "true")\
  .csv("data/retail-data/all/online-retail-dataset.csv")\
  .repartition(2)\
  .selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")\
  .groupBy("is_glass")\
  .count()\
  .collect()
