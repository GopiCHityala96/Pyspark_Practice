# from pyspark.sql import SparkSession
#
# # Create a SparkSession
# spark = SparkSession.builder.appName("PartitioningExample").getOrCreate()
#
# # Sample data and DataFrame creation
# data = [
#     ("Ali", 21), ("Bob", 30), ("Catherine", 28),
#     ("David", 40), ("Eve", 35), ("Drago", 50),
#     ("Zlatan", 29), ("Hector", 33), ("Linus", 38)
# ]
# columns = ["Name", "Age"]
# df = spark.createDataFrame(data, columns)
#
# print("Default number of partitions:", df.rdd.getNumPartitions())
#
# df_repartitioned = df.repartition(6)
# print("Number of partitions after repartition:", df_repartitioned.rdd.getNumPartitions())
#
# # A simple action (count) to trigger the shuffle.
# count_repartitioned = df_repartitioned.count()
# print("Row count after repartition:", count_repartitioned)
#
#
# df_coalesced = df_repartitioned.coalesce(2)
# print("Number of partitions after coalesce:", df_coalesced.rdd.getNumPartitions())
#
# # Again, trigger an action to force the computation.
# count_coalesced = df_coalesced.count()
# print("Row count after coalesce:", count_coalesced)
#
# # Let's say we filter the original DataFrame, which might result in
# # many nearly empty partitions. We can use coalesce() to combine them.
# df_filtered = df.filter("Age >= 30")
# print("Partitions of filtered DataFrame:", df_filtered.rdd.getNumPartitions())
#
# # If we know that the filtered data is much smaller, we might want to reduce partitions.
# df_filtered_coalesced = df_filtered.coalesce(1)
# print("Partitions after coalescing filtered DataFrame:", df_filtered_coalesced.rdd.getNumPartitions())
# print("Row count in filtered DataFrame:", df_filtered_coalesced.count())
#
# Stop the SparkSession when done
# spark.stop()

# spark = df.orderBy(df["salary"].desc()).show()

# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("Example").getOrCreate()
#
# # Sample DataFrame
# data = [("Alice", 25), ("Bob", 30)]
# columns = ["name", "age"]
# df1 = spark.createDataFrame(data, columns)
#
# # Select specific columns
# df1.select("name", "age").show()
#
#
from pyspark.sql import SparkSession
Spark = SparkSession.builder.appName("data List").getOrCreate()

data = [("alice",50,"Hr"),
         ("rama",60,"It")]
columns = ["name","age"]

df1 = Spark.createDataFrame(data,columns)
df1.show()

df1 = df1.select("name","age").show()

df1.filter("age > 1")

df1.select(f.avg("column1"))

df1.groupBy("column2").count()


