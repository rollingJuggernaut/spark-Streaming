'''
This python script reads dataset in .csv format and loads this into Apache kafka topic
Simple kafka producer
'''

from kafka import KafkaProducer
from json import dumps, loads

import time
import logging
import csv

logging.basicConfig(level=logging.INFO)

kafka_topic = 'demo-credit-transactions_9'

#record_count
count=0

producer = KafkaProducer(bootstrap_servers='VGSGBLRAC03:9092',
                         value_serializer=lambda K:dumps(K).encode('utf-8'))

print("Starting Kafka Producer Application")

with open("D:\Datasets\BusinessScenario-Dataset\BusinessScenario-Dataset\TranasctionLogs_100.csv") as file:
    reader = csv.reader(file, delimiter = '\t')
    for messages in reader:
        count=count+1
        producer.send(kafka_topic, messages)
        producer.flush()
        print("Producer record count -->", count, ";Data -->", messages)
        time.sleep(2)

print("Kafka Producer Application Completed. ", count ," records inserted to topic")
