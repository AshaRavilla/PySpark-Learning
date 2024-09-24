# Databricks notebook source
#code to load the data into a spark DataFrame

fire_df = spark.read\
                .format("csv")\
                .option("header", "True")\
                .option("inferSchema","True")\
                .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# spark.read  In this spark is a spark session object, it is an entry point to the spark. 
#Every spark program starts with the Spark Session. Becasue Spark APIs are availbale through spark sessions only
# read is the attribute of the spark for Dataframe reader
# the rest like the format, option , load are the methods of the read attribute

# COMMAND ----------

fire_df.show(10)

# COMMAND ----------

display(fire_df)

# COMMAND ----------

fire_df.createGlobalTempView("fire_service_calls_views")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.fire_service_calls_views
