from confluent_kafka import Consumer, KafkaException, KafkaError
import json

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'web-scraping-consumer', 
    'auto.offset.reset': 'earliest'  
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

topic = 'Movies_With_Bad_Words'

#take data the topic
consumer.subscribe([topic])

def proccess_message(message):
    movie = json.loads(message.value().decode('utf-8'))
    print(movie)


def main():
    try:
        while True:
            msg = consumer.poll(1.0)  # Wait for a message for 1 second
            if msg is None:
                continue  # No message received
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition} @ {msg.offset}")
                else:
                    raise KafkaException(msg.error())
            else:
                proccess_message(msg)
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
