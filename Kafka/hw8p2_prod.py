#  Illustrates the work of kafka's producer and consumer
#  @author Serguey Khovansky
#  @version: March 2016
# The problem:
#      Construct a producer and a consumer object. Let producer generate one random number 
#      between 0 and 10 every second.Let both producer and consumer run  until you kill 
#      them. Demonstrate that  your consumer is receiving messages by printing both the stream
#      of numbers generated on the producer and the stream of numbers fetched by the consumer.
#  To run: python hw8p2.py
from kafka import  KafkaProducer
import random
import time

#Producer for Kafka, the consumer is another program
class Producer(): 
    def run(self):
#Connect to port 9092
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        print('inside Producer')
#Generate an int random number in [0,10] and send it to topic 'spark_topic'        
        while True:
            myrnd=str(random.randint(0,10))
            print('Producer generates: ' + myrnd)
            producer.send('spark_topic',myrnd)
            time.sleep(1) 


print('start producer')
oproducer = Producer()
#Start   producer, which run indefinitely
oproducer.run()
print('end') 
