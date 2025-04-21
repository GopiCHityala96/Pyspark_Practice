# # input_list = [1,2,[3,4,[5,6]]]
# #
# # #output = [1,2,3,4,5,6]
# # result = []
# #
# # for i in input_list:
# #     if isinstance(i,list):
# #         result.extend(i)
# #     else:
# #         result.append(i)
# # print(result)
# #
# # s = "maram surundar reddy hadoop data engineer"
# #
# # char_freq = {}
# #
# # for c in s:
# #     if c in char_freq:
# #         char_freq[c] += 1
# #     else:
# #         char_freq[c] = 1
# #
# # print(char_freq)s = "maram surundar reddy hadoop data engineer"
# #
# # char_freq = {}
# # #
# # # for c in s:
# # #     if c in char_freq:
# # #         char_freq[c] += 1
# # #     else:
# # #         char_freq[c] = 1
# # #
# # # print(char_freq)
# #
# # # s = "maram surundar reddy hadoop data engineer"
# # #
# # # char_freq = {}
# # #
# # # for c in s:
# # #     if c in char_freq:
# # #         char_freq[c] += 1
# # #     else:
# # #         char_freq[c] = 1
# # #
# # # print(char_freq)
# # #
# # #
# # #
# # # def is_prime_simple(n):
# # #     if n < 2:
# # #         return False  # Numbers less than 2 are not prime
# # #     for i in range(2, n):  # Check divisibility from 2 to (n-1)
# # #         if n % i == 0:
# # #             return False  # If divisible, it's not prime
# # #     return True
# # # print(is_prime_simple(13))
# #
# # from pyspark.sql import SparkSession
# # spark = SparkSession.builder.appName('Data Frame').getOrCreate()
# #
# # emptyRDD = spark.sparkContext.emptyRDD()
# # print(emptyRDD)
# #
# # from pyspark.sql.types import StructType,StructField,StringType
# #
# # schema = StructType([
# #     StructField('firstname',StringType(),True)
# #     StructField('middlename',StringType,True)
# #     StructField('lastname',StringType(),True)
# #     ])
# #
# # from pysprak.sql.types import StructType,StructField,StringType
# #
# # schema = StructType([
# #     StructField('firstname',StringType(),True)
# #     StructField('middlename',StringType(),True)
# #     StructField('lastname',StringType(),True)
# # ])
# from pandas.core.interchange.dataframe_protocol import DataFrame
# # df = spark.CreateDataFrame([(1,'Ravi'),(2,'Gopi')],['ID','NAME'])
# # df.show()
# #
# # for i in range(5):
# # if i == 3:
# # break  # Stops the loop when i is 3
# # print(i)
# # for i in range(6)
# #     if i = 4
# #     breck #stops the loop then i is 5
# #     print(i)
#
#
# from puspark.sql import SparkSession
#
# from pyspark_methods import columns
#
# spark = SparkSession.builder.appName.('DataFrame').getOrCreate()
#
#
#
#
#
#
#
#
#
#
#
#
#
#
from ast import literal_eval

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("data frame").getOrCreate()

data = [("gopi",55,"doctor"),
        ("gopi1",56,"patient"),
        ("gopi2",57,"super doctor")
        ]
columns = ["NAME","AGE","PROFESSION"]

df = spark.createDataFrame(data,columns)
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


from pyspark.sql import SparkSession
from pysparl.sql funtion
ram = SparkSession.builder.appName("gopi").getOrCreate()

df = ram.read.csv(r"C:\Users\gopi\Downloads\MOCK_DATA (1).csv", header=True, inferSchema=True)
df.show()
df.printSchema()

df.select("id").show()

df = df.withColumn("salary int ",10000)

df.show()