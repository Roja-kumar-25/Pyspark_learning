#1.first letw discuss about the window ranking functions:
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,rank,dense_rank,percent_rank,ntile,when,col

spark=SparkSession.builder.appName("windowOperations.com").getOrCreate()

data=[("john","Marketing",50000),
      ("amy","Finance",40000),
      ("smith","Development",60000),
      ("robert","Technology",35000),
      ("maria","Sales",30000),
    ("Jen", "Finance", 39000),    
    ("Jeff", "Marketing", 30000), 
    ("Kumar", "Marketing", 20000),
    ("Saif", "Sales", 41000) ,
    ("James", "Sales", 30000),    
    ("Scott", "Finance", 33000), 
    ]

columns=["Names","Departments","Salary"]
df= spark.createDataFrame(data=data,schema=columns)

# df.show()

#performing window operations
# 1. row_number
windowSpec=Window.partitionBy("Departments").orderBy("salary")
# df.withColumn("row_number",row_number().over(windowSpec)).show()



# 2.rank
df=df.withColumn("dense-rank",dense_rank().over(windowSpec))
df.show()
df.filter(col("dense-rank")==2).show()

# # 3.dense_rank
# df.withColumn("dense_rank",dense_rank().over(windowSpec)).show()

# #4. percent rank
# df.withColumn("percent_rank",percent_rank().over(windowSpec)).show()

# #5.ntile 
# df.withColumn("ntile",ntile(3).over(windowSpec)).show()

# df.withColumn("ntile",ntile(2).over(windowSpec)).show()
