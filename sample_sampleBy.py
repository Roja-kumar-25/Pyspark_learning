from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[1]").appName("sampleBy.com").getOrCreate()

df=spark.range(100)
print(df.sample(0.06).collect())
print(df.sample( True,0.1,123).collect())