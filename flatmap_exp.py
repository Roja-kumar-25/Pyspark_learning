#creating RDD
# performing the flatmap function
from pyspark.sql import SparkSession
# from pyspark.sql.functions import
spark= SparkSession.builder.master("local[1]").appName("flatmap.com").getOrCreate()
 
data  = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)
    
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)