#  Illustrates the work of kafka's producer and consumer
#  @author Serguey Khovansky
#  @version: March 2016
# The problem:
#      Construct a producer and a consumer object. Let producer generate one random number 
#      between 0 and 10 every second.Let both producer and consumer run  until you kill 
#      them. Demonstrate that  your consumer is receiving messages by printing both the stream
#      of numbers generated on the producer and the stream of numbers fetched by the consumer.
#  To run: python hw8p2.py
from kafka import KafkaConsumer

#This is a Consumder for Kafka, the producer is a stand alone program
class Consumer():
    def run(self):
#Connect to port 9092
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        consumer.subscribe(['spark_topic'])
        for message in consumer:
            print('Consumer fetches: ' +message.value)

print('start consumer')
oconsumer = Consumer()
#Start consumer, which run indefinitely
oconsumer.run()

print('end') 
