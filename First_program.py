# # # from pyspark.sql import SparkSession
# # #
# # # spark = SparkSession.builder.appName("sample_program").getOrCreate()
# # #
# # # # Step 3: Create Data
# # # data = [
# # #     (1, "Alice", 25, "New York", 50000.0),
# # #     (2, "Bob", 30, "San Francisco", 60000.0),
# # #     (3, "Charlie", 35, "Los Angeles", 70000.0),
# # #     (4, "David", 40, "Chicago", 80000.0),
# # #     (5, "Eva", 45, "Houston", 90000.0),
# # #     (6, "Frank", 50, "Phoenix", 100000.0),
# # #     (7, "Grace", 55, "Philadelphia", 110000.0),
# # #     (8, "Hank", 60, "San Antonio", 120000.0),
# # #     (9, "Ivy", 65, "San Diego", 130000.0),
# # #     (10, "Jack", 70, "Dallas", 140000.0)
# # # ]
# # #
# # # columns = ["id","name","age","country","salary"]
# # #
# # # df = spark.createDataFrame(data,columns)
# # #
# # # df.show()
# # from numpy.ma.core import true_divide
# # from pyspark.sql import SparkSession
# #
# # # Step 1: Create SparkSession
# # spark = SparkSession.builder.appName("sample_program").getOrCreate()
# #
# # # Step 2: Create Data
# # data = [
# #     (1, "Alice", 25, "New York", 50000.0),
# #     (2, "Bob", 30, "San Francisco", 60000.0),
# #     (3, "Charlie", 35, "Los Angeles", 70000.0),
# #     (4, "David", 40, "Chicago", 80000.0),
# #     (5, "Eva", 45, "Houston", 90000.0),
# #     (6, "Frank", 50, "Phoenix", 100000.0),
# #     (7, "Grace", 55, "Philadelphia", 110000.0),
# #     (8, "Hank", 60, "San Antonio", 120000.0),
# #     (9, "Ivy", 65, "San Diego", 130000.0),
# #     (10, "Jack", 70, "Dallas", 140000.0)
# # ]
# #
# # # Step 3: Define Correct Column Names
# # columns = ["id", "name", "age", "city", "salary"]
# #
# # # Step 4: Create DataFrame
# # df = spark.createDataFrame(data, columns)
# #
# # # Step 5: Show DataFrame
# # df.show()
# #
# #
# # from pyspark.sql.functions import col, count
# #
# # df1 = df.select("id","name")
# # df1.show()
# #
# # df2= df.filter("age >30")
# #
# # df2.show()
# #
# # from pyspark.sql import SparkSession
# # from pyspark.sql import functions as f
# #
# # df4 = df.select(f.max("salary"))
# #
# # df4.show()
# #
# # df5 = df.groupBy("salary").agg(count("*")).alias ("count1")
# #
# # df5.show()
# #
# # df5 = df.groupBy("age").agg(count("*")).alias("count2")
# # df5.show()
# # #
# # # df6 = df.orderBy("salary" ascending = true)
# # #
# # # df6.show()
# # #
# # # df6 = df.orderBy("salary",ascending = true))
# # #
# # # # df6.show()
# #
# # df6 = df.orderBy("salary", ascending=True)
# #
# # df6.show()
# #
# # df7 = df.orderBy("salary",decending=True)
# # df7.show()
# #
# # from pyspark.sql.functions import desc
# #
# # df7 = df.orderBy("salary",desending =True)
# # df7.show()
# #
# # # df7 = df.orderBy("salary")
# #
# # df7.show()
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
#
# # Initialize Spark session
# spark = SparkSession.builder.appName("EmployeeData").getOrCreate()
#
# # Sample employee data with NULL values and duplicates
# data = [
#     (1, "Alice", "HR", 5000),
#     (2, "Bob", None, 7000),  # Null in department
#     (3, "Charlie", "IT", 6000),
#     (4, None, "Finance", 8000),  # Null in name
#     (5, "Eve", "HR", 5000),
#     (5, "Eve", "HR", 5000),  # Duplicate row
#     (6, None, None, None)  # Row with all NULL values
# ]
#
# # Define schema
# columns = ["emp_id", "name", "department", "salary"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
#
# # Show original DataFrame
# df.show()
#
# df.dropDuplicates(['name','department','salary']).show()
# # df_no_duplicates = df.dropDuplicates(["name", "department", "salary"])
# # df.dropna['name','department','salary']).show()
# df_no_nulls = df.dropna(subset=["name", "department", "salary"])
