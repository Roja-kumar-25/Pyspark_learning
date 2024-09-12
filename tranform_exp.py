from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, transform

transform_df=[("mongodb",4500,10),
              ("pyspark",6000,5),
              ("datascience",7000,15)
              ]
columns=["courseName","fee","discount"]
spark=SparkSession.builder.appName("transform_exp.com").getOrCreate()
trans_df=spark.createDataFrame(data=transform_df,schema=columns)
# trans_df.show()

def ucaseCourse(trans_df):
    return trans_df.withColumn("CourseName",upper(trans_df.courseName))
def reduce_price(trans_df,reduceBy):
    return trans_df.withColumn("new_price",trans_df.fee- reduceBy)
def discountedPrice(trans_df):
    return trans_df.withColumn("Discounted Price",trans_df.new_price-(trans_df.new_price*trans_df.discount)/100)

obj=trans_df.transform(ucaseCourse)\
    .transform(reduce_price,1000)\
        .transform(discountedPrice)
    
# obj.show()


data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"]),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"]),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"])
]
df = spark.createDataFrame(data=data,schema=["Name","Languages1","Languages2"])
df.printSchema()
df.show()

# using transform() function
from pyspark.sql.functions import upper
from pyspark.sql.functions import transform
df.select(transform("Languages1", lambda x: upper(x)).alias("languages1")) \
  .show()