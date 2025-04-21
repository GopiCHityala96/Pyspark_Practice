# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("Tom").getOrCreate()
#
#
# data = [
#     (1, "Alice", 25, "New York", 50000.0),
#     (2, "Bob", 30, "San Francisco", 60000.0),
#     (3, "Charlie", 35, "Los Angeles", 70000.0),
#     (4, "David", 40, "Chicago", 80000.0),
#     (5, "Eva", 45, "Houston", 90000.0),
#     (6, "Frank", 50, "Phoenix", 100000.0),
#     (7, "Grace", 55, "Philadelphia", 110000.0),
#     (8, "Hank", 60, "San Antonio", 120000.0),
#     (9, "Ivy", 65, "San Diego", 130000.0),
#     (10, "Jack", 70, "Dallas", 140000.0)
# ]
#
# columns = [("id","C_name","age","country","salary")]
#
# df = spark.createDataFrame(data,columns)
# df.show()

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from setuptools.command.alias import alias

# Initialize Spark Session
spark = SparkSession.builder.appName("Tom").getOrCreate()

# Sample Data
data = [
    (1, "Alice", 25, "New York", 50000.0),
    (2, "Bob", 30, "San Francisco", 60000.0),
    (3, "Charlie", 35, "Los Angeles", 70000.0),
    (4, "David", 40, "Chicago", 80000.0),
    (5, "Eva", 45, "Houston", 90000.0),
    (6, "Frank", 50, "Phoenix", 100000.0),
    (7, "Grace", 55, "Philadelphia", 110000.0),
    (8, "Hank", 60, "San Antonio", 120000.0),
    (9, "Ivy", 65, "San Diego", 130000.0),
    (10, "Jack", 70, "Dallas", 140000.0)
]

# Corrected Column Names (List of Strings)
columns = ["id", "C_name", "age", "country", "salary"]

# Creating DataFrame
df = spark.createDataFrame(data, columns)

# Display DataFrame
df.show()

# df = df.select(col("id"),col("C_name"))
# df.show()

df = df.filter("age < 30 ")
df.show()
df = df.select(f.avg("salary"))
df = df.select(f.count("*").alias("row_count"))
df.show()

df = df.groupBy(df["age"]).count()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("first program").getOrCreate()
df = df.select(col("id"),col("name"))
df.collect()