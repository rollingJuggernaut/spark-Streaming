'''
This python script reads parquet file, process it and writes back to parquet file
'''

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create Spark Session
# here 3.0.3 is the spark version;refer using https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
spark = SparkSession \
    .builder \
    .appName("Spark Structured Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3") \
    .master("local[*]") \
    .getOrCreate()

# Set Spark logging level to ERROR to avoid various other logs on console.
spark.sparkContext.setLogLevel("ERROR")

print("Reading data from initial parquet file and generating temp file")

df = spark.read.parquet("D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/initial_file") \
    .write.mode("overwrite") \
    .format("parquet") \
    .save("D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/tmp_file")

#windowSpec  = Window.partitionBy("CustomerID").orderBy("timestampnew".desc())
windowSpec  = Window.partitionBy("CustomerID").orderBy("timestampnew")
print("temp file and the initial file are merged into a final file to be used in querying systems")

df1 = spark.read.parquet("D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/tmp_file", "D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/initial_file") \
    .withColumn("rec_number",row_number().over(windowSpec)) \
    .filter(col("rec_number")==1) \
    .write.mode("overwrite") \
    .format("parquet") \
    .save("D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/final_file")

print("display data fom parquet files")

df2 = spark.read.parquet("D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/final_file") \
    .createOrReplaceTempView("ParquetTable")
spark.sql("select TotalCreditLimit) from ParquetTable").show(1000)
