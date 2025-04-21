# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
#
#
#
# spark = SparkSession.builder().appname("methods").getorcreatre()
#
#
# schema = StructType([
#     StructField("EmployeeID", IntegerType(), True),
#     StructField("Name", StringType(), True),
#     StructField("Department", StringType(), True),
#     StructField("Salary", DoubleType(), True),
#     StructField("Experience", IntegerType(), True)
# ])
#
# data = [
#     (101, "Alice", "HR", 60000, 5),
#     (102, "Bob", "IT", 80000, 7),
#     (103, "Charlie", "Finance", 75000, 6),
#     (104, "David", "IT", 90000, 8),
#     (105, "Eve", "HR", 65000, 5)
# ]
#
# # Create DataFrame
# df = spark.createDataFrame(data, schema=schema)
#
#
# # Print Schema
# df.printSchema()
#
#
# # # Select Specific Columns
# # df.select("Name", "Salary").show()
# #
# # # Filter Employees with Salary > 70000
# # df.filter(df.Salary > 70000).show()
# #
# # # Group by Department and Count Employees
# # df.groupBy("Department").count().show()
# #
# # # Sort Employees by Salary (Descending)
# # df.orderBy(df.Salary.desc()).show()
# #
# # # Add a New Column (Bonus = 10% of Salary)
# # df.withColumn("Bonus", df.Salary * 0.1).show()



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SecondHighestSalary").getOrCreate()

# Sample Data
data = [
    (1, 'Alice', 5000),
    (2, 'Bob', 7000),
    (3, 'Charlie', 6000),
    (4, 'David', 7000),
    (5, 'Eve', 8000)
]

# Creating DataFrame
columns = ["EmpID", "EmpName", "Salary"]
df = spark.createDataFrame(data, columns)

# Window Function for Ranking
windowSpec = Window.orderBy(F.desc("Salary"))
df_with_rank = df.withColumn("rank", F.dense_rank().over(windowSpec))

# Filter for Second Highest Salary
second_highest_salary = df_with_rank.filter(F.col("rank") == 2)

second_highest_salary.show()

