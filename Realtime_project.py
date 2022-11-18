from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('DataFrame1').getOrCreate()
simpleData = [("James",34),("Ann",34),
              ("Michael",33),("Scott",53),
              ("Robert",37),("Chad",27)]


columns = ["firstname","age"]
df = spark.createDataFrame(data = simpleData,schema = columns)
print(df.take(2))
