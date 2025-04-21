from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# Initialize SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Initialize SQLContext (if needed)
sqlContext = SQLContext(spark)

# Sample Data
data = [(1, 'Alice', 5000), (2, 'Bob', 7000),
    (3, 'Charlie', 6000),
    (4, 'David', 7000),
    (5, 'Eve', 8000)]
columns = ["EmpID", "EmpName", "Salary"]
df = spark.createDataFrame(data, columns)

df.show()


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("data").getOrCreate()

data = [("gopi",44,"it"),
        ("gopi3",58,"mt"),
        ("gopi5",78,"pt")
        ]
columns = ["name","id","proficent"]

df = spark.createDataFrame(data,columns)
df.show()

