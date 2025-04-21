# from pyspark.sql import SparkSession
#
# # Initialize Spark session
# spark = SparkSession.builder.appName("DropNulls").getOrCreate()
#
# # Sample employee data with duplicates, NULLs, and extra columns
# data = [
#     (1, "Alice", "HR", 5000, 25, "New York"),
#     (2, "Bob", None, 7000, None, "Los Angeles"),   # Null in department and age
#     (3, "Charlie", "IT", 6000, 30, None),  # Null in city
#     (4, None, "Finance", 8000, 40, "Chicago"),  # Null in name
#     (5, "Eve", "HR", 5000, 28, "Houston"),
#     (5, "Eve", "HR", 5000, 28, "Houston"),  # Duplicate row
#     (6, None, None, None, None, None),  # All null values
#     (7, "David", "IT", 6000, 30, "Seattle"),
#     (7, "David", "IT", 6000, 30, "Seattle"),  # Duplicate row
#     (8, "Frank", None, None, 35, "Miami"),  # Null in department and salary
# ]
#
# columns = ["emp_id", "name", "department", "salary", "age", "city"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
#
# # Show original DataFrame
# df.show()
# # df.distinct().show()
# #
# # df.dropna().show()
#
# df_filled = df.fillna({"department": "Unknown", "salary": 0})
# df_filled.show()
#
# df.na.fill({"name": "Gopi","age":18}).show()
#
#
# from pyspark.sql.functions import col
# #
# # df_updated = df.withColumn("salary_increment", (df.salary * 1.10))
# # df_updated.show()
#
# from pyspark.sql.functions import when
#
# df_update = df.withColumn("department",when(col("department").isNull(),"unkonw").otherwise(col("department")))
# df_update.show()
#
#
