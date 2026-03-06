# Databricks notebook source
mydata = spark.read.format("csv") \
    .option("header" , "true") \
        .load("/Workspace/Users/nitishsurve887@gmail.com/original.csv")

# COMMAND ----------

display(mydata)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Handling null values from City column

# COMMAND ----------

from pyspark.sql.functions import *

mydata2 = mydata.withColumn("clean_city", when(mydata.City.isNull(),'Unknown').otherwise(mydata.City))


# COMMAND ----------

display(mydata2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Cleaning null values from JobTitle column

# COMMAND ----------

mydata2 = mydata2.filter(mydata2.JobTitle.isNotNull())

# COMMAND ----------

display(mydata2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Cleaning null values from salary column by putting mean values where there is null

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### and also handling $ using substr function

# COMMAND ----------

mydata2 = mydata2.withColumn('clean_salary', mydata2.Salary.substr(2,100).cast('float'))

# COMMAND ----------

mydata2.show()

# COMMAND ----------

mean = mydata2.groupBy().avg('clean_salary').take(1)[0][0]

# COMMAND ----------

display(mean)

# COMMAND ----------

from pyspark.sql.functions import lit

mydata2 = mydata2.withColumn('new_salary',when(mydata2.clean_salary.isNull(), lit(mean)).otherwise(mydata2.clean_salary))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Populating median values for latitude and longitude

# COMMAND ----------

import numpy as np
latitudes = mydata2.select('Latitude')

# COMMAND ----------

latitudes = latitudes.filter(latitudes.Latitude.isNotNull())

# COMMAND ----------

latitudes = latitudes.withColumn('latitude2', latitudes.Latitude.cast('float')).select('latitude2')

# COMMAND ----------

median = np.median(latitudes.collect())

# COMMAND ----------

mydata2 = mydata2.withColumn('lat', when(mydata2.Latitude.isNull(), lit(median)).otherwise(mydata2.Latitude))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Average Salary for males and females

# COMMAND ----------

import pyspark.sql.functions as sqlfunc

genders = mydata2.groupBy('gender').agg(sqlfunc.avg("new_salary").alias("AvgSalary"))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Per job role, whether males or females are paid more

# COMMAND ----------

df = mydata2.withColumn('female_salary', when(mydata2.gender == 'Female', mydata2.new_salary).otherwise(lit(0)))

# COMMAND ----------

df = df.withColumn('male_salary', when(mydata2.gender == 'Male', mydata2.new_salary).otherwise(lit(0)))

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.groupBy('JobTitle').agg(sqlfunc.avg('female_salary').alias('final_female_salary'), sqlfunc.avg('male_salary').alias("final_male_salary"))

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn('delta', df.final_female_salary - df.final_male_salary)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Which city has the highest average salary

# COMMAND ----------

df2 = mydata2.groupBy('clean_city').agg(sqlfunc.round(sqlfunc.avg('new_salary'),2).alias("highest_avg_salary")) \
    .orderBy("highest_avg_salary",ascending = False)

# COMMAND ----------

display(df2)

# COMMAND ----------

df_select = mydata2.select("City", "clean_city")
display(df_select)

# COMMAND ----------

mydata2.createTempView("temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from temp;

# COMMAND ----------

df_agg = mydata2.groupBy("gender","city").agg(sqlfunc.avg('clean_salary'),
                                       sqlfunc.sum('clean_salary'),
                                       sqlfunc.max('clean_salary'),
                                       sqlfunc.min('clean_salary'))

# COMMAND ----------

df_agg.show()