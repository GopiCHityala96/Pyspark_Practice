

from pyspark.sql import SparkSession

from pyspark.sql.functions import lit

ram = SparkSession.builder.appName("gopi").getOrCreate()

df = ram.read.csv(r"C:\Users\gopi\Downloads\MOCK_DATA (1).csv", header=True, inferSchema=True)
df.show()
df.printSchema()

df.select("id").show()

columns = ["id", "first_name", "last_name", "email", "gender", "ip_address"]

df = df.withColumn("salary",lit(10000))

df.show()


