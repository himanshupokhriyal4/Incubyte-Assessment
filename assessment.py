from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import date, timedelta,datetime
from pyspark.sql.functions import json_tuple, current_timestamp, monotonically_increasing_id, substring_index,col
from pyspark.sql.functions import *



spark = SparkSession \
    .builder \
    .appName('incubyte') \
    .master('local[*]') \
    .enableHiveSupport() \
    .config("spark.driver.extraClassPath", "/home/himanshu/Desktop/jars/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
today = date.today()

# Reading Medical Data and Converting into Spark Dataframe

df=spark.read.option("header","true")\
    .option("inferSchema","true")\
    .csv("/home/himanshu/Desktop/Incubyte/Data/")
df.createOrReplaceTempView("medical_data")

# Getting Distinct countries Available in Medical Data
df1=spark.sql("""select distinct(Country) from medical_data""")
df1.printSchema()

# Getting distinct countries into list

li=[str(row.Country) for row in df1.collect()]

# Putting data into seperate tables according to their country names
for ele in li:
    print(ele)
    df_country=spark.sql("""select * from medical_data where Country='{}' """.format(ele))
    table_name="Table_"+ele
    print(table_name)
    df_country.write.format('jdbc').options(
        url='jdbc:mysql://localhost:3306/medical_data',
        driver='com.mysql.jdbc.Driver',
        dbtable=table_name,
        user='incubyte',
        password='Admin@123').mode('append').save()