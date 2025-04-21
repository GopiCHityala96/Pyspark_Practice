from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.appName("SCD2_Scenario").getOrCreate()

customer_dim_schema = StructType([
    StructField("Customer_SK", IntegerType(), False),
    StructField("Customer_ID", StringType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Date_Of_Birth", DateType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("Status_Code", StringType(), True),
    StructField("Geography_SK", IntegerType(), True),
    StructField("Join_Date", DateType(), True),
    StructField("Loyalty_Tier", StringType(), True),
    StructField("Effective_Date", DateType(), True),
    StructField("Expiry_Date", DateType(), True),
    StructField("Current_Flag", StringType(), True)
])

customer_dim_data = [
    (101, "C001", "John", "Doe", "M", datetime(1985, 5, 1), "john.doe@example.com", "1234567890", "A", 1, datetime(2020, 1, 1), "Gold", datetime(2020, 1, 1), datetime(2021, 5, 10), "N"),
    (102, "C001", "John", "Doe", "M", datetime(1985, 5, 1), "john.doe@example.com", "1234567890", "I", 1, datetime(2020, 1, 1), "Silver", datetime(2021, 5, 11), datetime(9999, 12, 31), "Y")
]

customer_dim_df = spark.createDataFrame(data=customer_dim_data, schema=customer_dim_schema)
