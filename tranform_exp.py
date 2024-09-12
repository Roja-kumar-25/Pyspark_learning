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

def ucaseCourse(trans_df):
    return trans_df.withColumn("CourseName",upper(trans_df.courseName))
def reduce_price(trans_df,reduceBy):
    return trans_df.withColumn("new_price",trans_df.fee- reduceBy)
def discountedPrice(trans_df):
    return trans_df.withColumn("Discounted Price",trans_df.new_price-(trans_df.new_price*trans_df.discount)/100)

obj=trans_df.transform(ucaseCourse)\
    .transform(reduce_price,1000)\
        .transform(discountedPrice)
    
obj.show()