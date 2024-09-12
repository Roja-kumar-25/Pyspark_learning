from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, transform

transform_df=[("mongodb",4500,10),
              ("pyspark",6000,5),
              ("datascience",7000,15)
              ]
columns=["courseName","fee","discount"]
spark=SparkSession.builder.appName("transform_exp.com").getOrCreate()
trans_df=spark.createDataFrame(data=transform_df,schema=columns)
trans_df.show()