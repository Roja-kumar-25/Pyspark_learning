# map is basically one of the transformations used with RDD.
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("map_exp.com").getOrCreate()

data=["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]

# creating rdd
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())


rdd_map=rdd.map(lambda x:(x,1))
for i in rdd_map.collect():
   print(i) 

#creating a dataframe 
#converting to RDD 
#applying the map function

data=[("siva","guru",21,"developer"),
      ("dhivya","shree",20,"mongodbDev"),
      ("roja","kumar",21,"data engineer")
      ]
columns=["fname","lname","age","role"]
df=spark.createDataFrame(data=data,schema=columns)
df.show()


rdd2=df.rdd.map(lambda x: 
    (x[0]+","+x[1],x[2]*2,x[3])
    )  
df2=rdd2.toDF(["name","age","role"]   )
df2.show()