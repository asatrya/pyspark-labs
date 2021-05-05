########################################################################################################
# Title               : Lab-12 (Spark Submit)
# General instruction : Run this script using spark-submit
########################################################################################################


# ---------- STEP 1 ----------
# Run in local
# -----------------------------

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


# ---------- STEP 2 ----------
# Run in cluster
# -----------------------------

# Setup the cluster in directory `pyspark-lab-clsuter`, an the run this command:
"""
# copy the generated SSH private key from inside the container
sudo docker-compose exec -T node-master bash -c "cat ~/.ssh/id_rsa" > ~/.ssh/pyspark_lab_cluster_key

# use SSH to run spark-submit remotely
ssh -i ~/.ssh/pyspark_lab_cluster_key root@localhost -p 2222 cd /root/lab \
    && export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    && spark-submit \
        --master yarn\
        --deploy-mode client\
        pyspark/count_glass.py
"""