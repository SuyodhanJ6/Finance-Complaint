from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SparkTest").getOrCreate()

print(spark.version)