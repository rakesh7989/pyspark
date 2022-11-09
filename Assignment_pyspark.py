# # Reading a Json file
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.master('local[*]').appName('json load').getOrCreate()
#
# df = spark.read.format('json').option('multiline',True).load("D:\\Pyspark_practice _DataSets\\todos.json")
# df.printSchema()
# df.show()

#####################

# Load parquet File
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.master('local[*]').appName('parquet load').getOrCreate()
#
# df1 = spark.read.format('parquet').load("D:\\Pyspark_practice _DataSets\\userdata1.parquet")
#
# df1.show()
############################

# Load Avro
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.master('local[*]').appName('Avro load').getOrCreate()
#
# df2 = spark.read.format('Avro').load("D:\\Pyspark_practice _DataSets\\twitter.avro")
#
# df2.show()
#
