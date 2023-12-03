from confluent_kafka import Consumer, KafkaException
import json
import os
from src.build_config import read_ccloud_config
from dotenv import load_dotenv

load_dotenv()

conf = read_ccloud_config("client.properties")
conf["group.id"] = "aggregation_service"
conf["auto.offset.reset"] = "earliest"

# from dotenv import load_dotenv
# load_dotenv()

# Kafka Consumer Configuration
# conf = {
#     "bootstrap.servers": os.getenv("bootstrap.servers"),
#     "security.protocol": os.getenv("security.protocol"),
#     "sasl.mechanisms": os.getenv("sasl.mechanisms"),
#     "sasl.username": os.getenv("sasl.username"),
#     "sasl.password": os.getenv("sasl.password"),
#     "group.id": "aggregation_service",  # Set your consumer group
#     "auto.offset.reset": "earliest",
# }

consumer = Consumer(conf)
topic = os.getenv("USER_INFO_TOPIC_NAME")
print(f"Consumer subscribing to topic: {topic}")

try:
    consumer.subscribe([topic])
    print(f"Consumer subscribed to topic: {topic}")

    # Data Aggregation Variables
    user_count = 0
    user_emails = set()

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue  # Or handle the error as per your requirement
            # if msg.error().code() == KafkaException._PARTITION_EOF:
            # End of partition event
            # print(
            #     "%% %s [%d] reached end at offset %d\n"
            #     % (msg.topic(), msg.partition(), msg.offset())
            # )
            # elif msg.error():
            #     raise KafkaException(msg.error())
        else:
            user_data = json.loads(msg.value().decode("utf-8"))
            print(f"User Data Read:\n{user_data}")
            user_count += 1
            user_emails.add(user_data["email"])

            print(f"user_emails current state:\n{user_emails}")

            # Example Aggregation: Print Total Users and Unique Emails
            print(f"Total Users: {user_count}, Unique Emails: {len(user_emails)}")
except KafkaException as e:
    # Handle Kafka exceptions
    print(f"Kafka exception: {e}")

finally:
    # Close down consumer to commit final offsets.
    consumer.close()
