# Databricks notebook source
# MAGIC %sql create schema powerbiwritebackdemo

# COMMAND ----------

df_train = spark.read.option("header", "true").csv('/FileStore/alex.young@databricks.com/titanic/train.csv')
df_test = spark.read.option("header", "true").csv('/FileStore/alex.young@databricks.com/titanic/test.csv')

# COMMAND ----------

df_train.write.mode('overwrite').saveAsTable("powerbiwritebackdemo.titanic_train")
df_train.write.mode('overwrite').saveAsTable("powerbiwritebackdemo.titanic_test")

# COMMAND ----------


