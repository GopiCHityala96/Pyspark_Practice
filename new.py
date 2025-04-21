from pyspark.sql import SparkSession

don = SparkSession.builder.appName("reading parquet").getOrCreate()

df = don.read.parquet("C:/Users/gopi/Downloads/run-1741083004729-part-block-0-r-00000-uncompressed.parquet")

df.show()

