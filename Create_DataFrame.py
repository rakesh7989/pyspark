
from pyspark.sql import SparkSession
if __name__ == "__main__":
    print("Application Started...")

    spark = SparkSession.builder.master('local[*]').appName('DataFrame').getOrCreate()
#Gets an existing SparkSession or, if there is no existing one,
# creates a new one based on the options set in this builder.

    df_spark = spark.read.csv("D:\Pyspark_practice _DataSets\Emp_pyspark3.csv",header=True,inferSchema=True)



    # Columns type  like dtype
    df_spark.printSchema()

    df_spark.show()


    # To the Selective columns to get
    df1 = df_spark.select('Name ', 'Salary')
    df1.show()

    # using the filter function checking the min salary emp in the table using above describe table
    df2 = df_spark.filter(df_spark.Salary == '30000')
    df2.show()
    # here we are trying to the max salary of an Emp
    df3 = df_spark.filter(df_spark.Salary == '75000')
    df3.show()
    # Salary of the Emp less_than_or_equal to 50000
    df4 = df_spark.filter("Salary<=50000")
    df4.show()

    # Using &
    df5 = df_spark.filter((df_spark['Salary'] <= 50000) &
                    (df_spark['Salary'] >= 30000))
    df5.show()
    # Using Or
    df6 = df_spark.filter((df_spark['Salary'] <= 50000) |
                    (df_spark['Salary'] >= 30000))
    df6.show()

    # using ~ Not
    df7 = df_spark.filter(~(df_spark['Salary'] <= 50000))
    df7.show()

    # Groupby functionality work with Aggregate function
    # First we need to intialize goupby the Aggregate function also we can use Aggregate function Directly

    df8 = df_spark.groupBy('Dept').sum()
    df8.show()

    # To check Dept wise emp count
    df9 = df_spark.groupBy('Dept').count()
    df9.show()

    # Max salary of the Emp
    df10 = df_spark.agg({'Salary': 'max'})
    df10.show()

    # Min salary of the Emp
    df11 = df_spark.agg({'Salary': 'min'})
    df11.show()

    # Adding Columns in the Data Frame
    df12 = df_spark = df_spark.withColumn('Exp_After_two_years', df_spark.Experince+2)
    df12.show()

    # using Filter()
    df13 = df_spark.filter(df_spark['Exp_After_two_years'] > 9)
    df13.show()

    # Drop the cloumn
    df14 = df_spark.drop('Exp_After_two_years')
    df14.show()


    # Rename the column_name (multiple)

    df15 = df_spark.withColumnRenamed("Name ","Name_New").withColumnRenamed("Salary","Emp_sal")
    df15.show()

    #spark.stop()









