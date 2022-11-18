from pyspark.sql import SparkSession

from pyspark.sql.functions import *
spark = SparkSession.builder.master('local[*]').appName('DataFrame').getOrCreate()
df_spark = spark.read.csv("D:\Pyspark_Assignment 1\Dataset_assign.csv",header=True,inferSchema=True)

df_spark.show()


# a : Convert the Issue Date with the timestamp format.
df=df_spark.withColumn('New_unixtime',from_unixtime(col('Issue Date ')/1000))
df.show(truncate=False)


b :Convert timestamp to date type
df1=df.withColumn("datetype",to_date("New_unixtime"))
df1.show(truncate=False)


C : Remove the starting extra space in Brand column for LG and Voltas fields
from pyspark.sql.functions import trim
df2=df1.withColumn('new_brand',trim(df1.Brand))
df2.show(truncate=False)

d:Replace empty string with None value
from pyspark.sql.functions import col,when
df3=df.withColumn("Country",when(col("Country")=='null',"").otherwise(col("Country")))
df3.show(truncate=False)





2 : Question

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
spark = SparkSession.builder.master('local[*]').appName('DataFrame').getOrCreate()
df = spark.read.csv("D:\Pyspark_Assignment 1\Product_Transactions.csv",header=True,inferSchema=True)

df.show()


# a.Change the camel case columns to snake case

# df1=df.withColumnRenamed('Source Id','Source_Id')\
#       .withColumnRenamed('Transaction Number','Transaction_Number')\
#       .withColumnRenamed('Mobile Number','Mobile_Number')\
#       .withColumnRenamed('Start Time','Start_Time')\
#       .withColumnRenamed('Product Number','Product_Number')
#
# df1.show()
#
#b.Add another column as start_time_ms and convert the values of StartTime to milliseconds.
df2 = df1.withColumn("start_time_ms", unix_timestamp(df1.Start_Time))
df2.show(truncate=False)

############# or ################
# # Renaming a column camel case to snake case
# from functools import reduce
# cols = df.columns
# df1 = reduce(lambda new_df, i : new_df.withColumnRenamed(i, i.replace(" ","_")),cols,df)
# df1.show()


# 3.Combine both the tables based on the Product Number
from pyspark.sql import SparkSession

from pyspark.sql.functions import *
spark = SparkSession.builder.master('local[*]').appName('DataFrame').getOrCreate()
df_spark = spark.read.csv("D:\Pyspark_Assignment 1\Dataset_assign.csv",header=True,inferSchema=True)


df1=df_spark.withColumnRenamed('Product number','Product_Number')
df1.show()





from pyspark.sql import SparkSession

from pyspark.sql.functions import *
spark = SparkSession.builder.master('local[*]').appName('DataFrame').getOrCreate()
df = spark.read.csv("D:\Pyspark_Assignment 1\Product_Transactions.csv",header=True,inferSchema=True)

df2=df.withColumnRenamed('Product Number','Product_Number')

df2.show()

# Join Two Tables based on Product Number.
df3 = df1.join(df2,df1.Product_Number==df2.Product_Number,"inner")
df3.show()

