# # # from pyspark.sql import SparkSession
# # #
# # # spark = SparkSession.builder.appName("MyApp").getOrCreate()
# # # sc = spark.sparkContext  # Access SparkContext from SparkSession
# # #
# # # colunms = ["ID","NAME","AGE","NUMBER","MARKS"]
# # #
# # # data = [(1,"RAKESH",20,123355,50),
# # #         (2, "SURESH", 30, 123666, 60),
# # #         (3,"RAMESH",40,123777,70),
# # #         (4,"",50,123888,80)]
# # # df = spark.createDataFrame(data,colunms)
# # # df.show()
# # #
# # # from pyspark.sql import SparkSession
# # # spark = SparkSession.builder.appName("Gopi").getOrCreate()
# # #
# # # data = [(111, "Rama Krishna", "HR", 142009),
# # #        (222, "jila Bangaram", "HR", 142008),
# # #         (333, "Prashanth", "HR", 142007)
# # #         ]
# # # columns =["ID", "EMP_NAME", "DEPT", "HALL NO"]
# # #
# # # df = spark.createDataFrame(data,columns)
# # #
# # # df.collect()
# # #
# # #
# # #
# # # import pandas as pd
# # # from pyspark.sql import SparkSession
# # #
# # # # Initialize Spark session
# # # spark = SparkSession.builder.master("local").appName("PandasToSpark").getOrCreate()
# # #
# # # # Create a pandas DataFrame
# # # data = {
# # #     "Name": ["John", "Anna", "Peter", "Linda"],
# # #     "Age": [28, 24, 35, 40],
# # #     "City": ["New York", "Paris", "Berlin", "London"]
# # # }
# # # pandas_df = pd.DataFrame(data)
# # #
# # # # Show the pandas DataFrame
# # # print("Pandas DataFrame:")
# # # print(pandas_df)
# # #
# # # # Convert pandas DataFrame to PySpark DataFrame
# # # spark_df = spark.createDataFrame(pandas_df)
# # #
# # # # Show the PySpark DataFrame
# # #from nested_list_flatten_testcase import columns
# #
# # # print("PySpark DataFrame:")
# # # spark_df.show()
# #
# # # import pandas as pd
# # # df = pd.read_excel('C:/Users/gopi/Desktop/Gopi.Excel.xlsx')
# # # print(df)
# #
# # # import pandas as pd
# # # df = pd.read_excel('C:/Users/gopi/Desktop/Gopi.Excel.xlsx')
# #
# # # print(df)
# # #
# # #
# # #
# # #
# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
# # #
# # # # Corrected SparkSession initialization
# # # spark = SparkSession.builder.appName("Pyspark_methods").getOrCreate()
# # #
# # # # Define schema
# # # schema = StructType([
# # #     StructField("id", IntegerType(), True),
# # #     StructField("name", StringType(), True),
# # #     StructField("age", IntegerType(), True),
# # #     StructField("city", StringType(), True),
# # #     StructField("salary", FloatType(), True)
# # # ])
# # #
# # # # # Sample data
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
# # # # Create DataFrame
# # # df = spark.createDataFrame(data, schema)
# # #
# # # # Show DataFrame
# # # df.show()
# #
# #
# # from pyspark.sql.functions import col
# # df.select(col("id"),col("name"))
#
#
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
#
