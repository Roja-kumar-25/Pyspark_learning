from pyspark.sql import SparkSession
from pyspark.sql.functions import when,mean,sum,max
from pyspark.sql.types import Row
spark=SparkSession.builder.master("local[1]").appName("task1.com").getOrCreate()

data = [
    Row(id=1, name='Alice', marks=98),
    Row(id=2, name='Bob', marks=74),
    Row(id=3, name='Charlie', marks=87),
    Row(id=4, name='David', marks=93),
    Row(id=5, name='Eva', marks=73),
    Row(id=6, name='Frank', marks=61),
    Row(id=7, name='Grace', marks=95),
    Row(id=8, name='Hank', marks=90),
    Row(id=9, name='Ivy', marks=70),
    Row(id=10, name='Jack', marks=40)
]

marks_df = spark.createDataFrame(data)
#marks_df.show()

#new column using withcolumn and when

grade_df=marks_df.withColumn("Grade",
                             when(marks_df.marks>=95,"S")
                            .when(marks_df.marks>=90,"A")
                            .when(marks_df.marks>=85,"B")
                            .when(marks_df.marks>=80,"C")
                            .when(marks_df.marks>=70,"D")
                            .when(marks_df.marks>=60,"E")
                            .otherwise("F"))
# grade_df.show()

#retieving students who scored "S" and "A" grade
grade_view=grade_df.createOrReplaceTempView("marks")

spark.sql("SELECT * FROM marks WHERE grade =='S' or grade =='A'").show()

# partitioning files based on grades

# grade_df.write.partitionBy("grade").parquet("/home/ai/Downloads/Pyspark_learning/grades.parquet")


#grouping out based on grades and performing out some of the aggregations task
grade_df.groupby("grade")\
    .agg(max("marks").alias("Max_Marks"),
        mean("marks").alias("Average"),
        sum("marks").alias("total_marks"))\
        .show()

