from confluent_kafka import Consumer # could also use kafka-python
import asyncio
import json

######################################################################
# Custom Kafka consumer to test the Kafka producer for the police data
# 
# Run with: python consumer_server.py
#
######################################################################

async def myconsumer(topic_name):
    ''' Kafka consumer using confluent_kafka as in Course Part 1'''
    consumer = Consumer({
        'bootstrap.servers': 'PLAINTEXT://localhost:9092', # align with kafka_server.py
        'group.id': '0',   # any string will do
        'auto.offset.reset': 'earliest'
    })
    
    consumer.subscribe([topic_name]) # single topic to subscribe
    
    while True:
        messages = consumer.consume()  # can also use poll
        for message in messages:
            if message is None:
                print("myconsumer: Message is None, no message found")
            elif message.error() is not None:
                print(f"myconsumer: Error: {message.error()}")
            else:
                # print(f"{message.value()}\n")
                # deserialise byte stream with json content into dictionary
                print(f"{json.loads( message.value().decode('utf-8') ) }\n")
        await asyncio.sleep(1.0)
                
def launch_consumer_coroutine():
    ''' Launch the coroutine using asyncio '''
    try:
        asyncio.run(myconsumer('sf.police.calls'))
        
    except KeyboardInterrupt as e:
        print("Terminating myconsumer.")
        
if __name__ == '__main__':
    launch_consumer_coroutine()
