# Databricks notebook source
mydata = spark.read.format("csv") \
    .option("header","true") \
        .load("/Workspace/Users/nitishsurve887@gmail.com/challenge.csv")

# COMMAND ----------

from pyspark.sql.functions import *

mydata = mydata.withColumn("Mexico?", when(mydata.Country == "Mexico", "yes").otherwise("no"))

# COMMAND ----------

display(mydata)

# COMMAND ----------

import pyspark.sql.functions as sf

# COMMAND ----------

df_bytes = mydata.groupBy("Mexico?").agg(sf.sum("Bytes_used").alias("sum_bytes"))

# COMMAND ----------

display(df_bytes)

# COMMAND ----------

df_ip = mydata.groupBy("Country").agg(sf.countDistinct("ip_address").alias("unique_ips"))

# COMMAND ----------

display(df_ip).sort("unique_ips", ascending=True)