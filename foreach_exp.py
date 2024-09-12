from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[1]").appName("foreach.com").getOrCreate()

data=[(1,"siva"),
      (2,"komala"),
      (3,"dhivya"),
      (4,"sowmiya")
      ]
columns=["Seqno","Name"]

df=spark.createDataFrame(data=data,schema=columns)

#foreach function used in dataframe
def fun(df):
    print(df.Seqno)
df.foreach(fun)
accum=spark.sparkContext.accumulator(0)
df.foreach(lambda x : accum.add(int(x.Seqno)))
print(accum.value)

#foreach function used in RDD
accum1=spark.sparkContext.accumulator(0)
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x: accum1.add(x))
print(accum1.value)
