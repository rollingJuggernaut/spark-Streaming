'''
This pyspark script reads parquet file and displays its content 
'''

from pyspark.sql import SparkSession, DataFrame

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

print("Reading data from initial parquet file")

df2 = spark.read.parquet("D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/initial_file") \
    .createOrReplaceTempView("ParquetTable")
spark.sql("select * from ParquetTable").show(1000)
