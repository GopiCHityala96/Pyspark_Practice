# # #
# # # num1 = input("please enter number")
# # # num2 = input("please enter number2")
# # # print(num1,num2)
# # # print(num1+num2)
# # #
# # # for i in range(1,100,2):
# # #     print(i)
# # #
# # # for i in range(2,100,2):
# # #     print(i)
# # from venv import create
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import concat_ws
# from setuptools.command.alias import alias
#
# spark =  SparkSession.builder.appName("data").getOrCreate()
#
# df =  spark.read.csv("C://Users//gopi//Downloads//MOCK_DATA (1).csv",header = 'true')
# df.show(truncate = True)
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import concat_ws
#
# spark = SparkSession.builder.appName("concattofullname").getOrCreate()
#
# data = [("Salvatore","Larder")]
# columns = ["first_name","last_name"]
#
# df = spark.createDataFrame(data,columns)
#
# df_with_full_name = df.withColumn("full_name",concat_ws(" ", df.first_name,df.last_name).alias("full_name"))
#
# # df.select("id",).show()
# df_with_full_name.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

spark = SparkSession.builder.appName("FullNameSelectColumns").getOrCreate()

df = spark.read.option("header", True).csv("C:/Users/gopi/Downloads/MOCK_DATA (1).csv")

df_selected = df.select(
    "id",
    concat_ws(" ", df.first_name, df.last_name).alias("fullname"),
    "email",
    "gender",
    "ip_address"
)
df_selected.show(truncate=False)
