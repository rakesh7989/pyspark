import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark1Assignment').getOrCreate()

def user(spark):
    users=spark.read.option('header',True).csv(r"D:\Spark_Assignment_Dataset\users.csv")
    return users

result=user(spark)
result.printSchema()
result.show(truncate=False)


#####################

def transact(spark):
    transaction=spark.read.option('header',True).csv(r"D:\Spark_Assignment_Dataset\transactions.csv")
    return transaction

T=transact(spark)
T.printSchema()
T.show(truncate=False)


# Join Two Tables based on userid .
comb = result.join(T,result.User_ID==T.UserID,"outer")
comb.show()


#a: Count of unique locations where each product is sold.

def unique_loc(comb):
    return comb.groupBy('Location','Product_Description ').count()

u=unique_loc(comb)
u.show()

# b: Find out products bought by each user.
def prod_bght(comb):
    return comb.groupBy('Product_Description ','User_ID').count()
ans=prod_bght(comb)
ans.show()

# c: Total spending done by each user on each product.

def tol_spen_us(comb):
    return comb.groupBy('Product_Description ','Price').count()

Tol=tol_spen_us(comb)
Tol.show()


######################

# def nav_lan(comb):
#     return comb.groupBy('NativeLanguage').count()
#
# lan=nav_lan(comb)
# lan.show()
























