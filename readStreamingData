'''
This python script reads streaming data from Apache kafka topic
Writes this data to parquet file
'''

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

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

kafka_topic_name = "demo-credit-transactions_9"
kafka_bootstrap_servers = "VGSGBLRAC03:9092"

#schema_String = "CustomerID STRING, CreditCardNo STRING, TransactionLocation STRING, TransactionAmount INT, TransactionCurrency STRING, MerchantName STRING, NumberofPasswordTries INT, TotalCreditLimit INT, CreditCardCurrency STRING"

print("Reading data from kafka topic")

# Create Streaming DataFrame by reading data from File Source (kafka topic).
df = spark\
    .readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)", "timestamp") \
    .selectExpr("split(value,',')[0] as CustID","split(value,',')[1] as CreditCardNo",
                "split(value,',')[2] as TransactionLocation","split(value,',')[3] as TransactionAmount",
                "split(value,',')[4] as TransactionCurrency","split(value,',')[5] as MerchantName",
                "split(value,',')[6] as NumberofPasswordTries","split(value,',')[7] as TotalCreditLimit",
                "split(value,',')[8] as CreditCardCny", "timestamp") \
    .select(substring("CustID",3,10).alias('CustomerID'),"CreditCardNo","TransactionLocation","TransactionAmount",
            "TransactionCurrency","MerchantName","NumberofPasswordTries","TotalCreditLimit",
            substring("CreditCardCny",1,3).alias("CreditCardCurrency"),col("timestamp").cast('timestamp').alias("timestampnew")) \
    .writeStream \
    .format("parquet") \
    .option("path", "D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/initial_file") \
    .option("checkpointLocation", "D:/Datasets/BusinessScenario-Dataset/BusinessScenario-Dataset/checkpoint") \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

df.awaitTermination()
