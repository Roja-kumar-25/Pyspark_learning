from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()
data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
df.show()

#to check the current date
df.select(current_date().alias("current_date")).show()

#to check the date format
df.select(col("input"),date_format(col("input"),"MM-dd-yyyy").alias("date-format")).show()

#to convert the input column to date column
df.select(col("input"),to_date(col("input"),"yyyy-MM-dd").alias("to_date")).show()

df.select(col("input")).withColumn("date-difference",date_diff(current_date(),col("input"))).show()

# df.select(col("input")).withColumn("mnth-between",months_between(current_date(),col("input"))).orderBy("mnth_between").show()

df.select(col("input"),current_timestamp().alias("current-time")).show()

