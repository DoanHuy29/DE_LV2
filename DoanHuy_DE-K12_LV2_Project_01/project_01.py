from confluent_kafka import Producer, Consumer, KafkaException
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import json
import threading
import logging

# Set up log
logging.basicConfig(filename="log.txt", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Connect MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["project_01_LV2"]
collection = db["product"]

# Transfer data from server Kafka to local Kafka
def takeData():
    # Config consumer
    consumer_conf = {
        "bootstrap.servers": "server_external_ports",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "server_username",
        "sasl.password": "server_password",
        "group.id": "my_consumer_group2",
        "auto.offset.reset": "earliest"
    }
    # Config local producer
    producer_conf = {
        "bootstrap.servers": "localhost:9094",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "local_username",
        "sasl.password": "local_password"
    }

    consumer = Consumer(consumer_conf)
    topic_source = "product_view"
    consumer.subscribe([topic_source])

    producer = Producer(producer_conf)
    topic_local = "product"

    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Error: {msg.error()}")
            continue
        producer.produce(topic_local, value=msg.value())
        producer.flush()
    consumer.close()

# Import data from local Kafka to MongoDB
def importDataToMongoDB():
    # Config local consumer
    local_consumer_conf = {
        "bootstrap.servers": "localhost:9094",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "local_username",
        "sasl.password": "local_password",
        "group.id": "consumer_group",
        "auto.offset.reset": "earliest"
    }

    consumer = Consumer(local_consumer_conf)
    topic_local = "product"
    consumer.subscribe([topic_local])

    while True:
        try:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            data = json.loads(msg.value().decode("utf-8"))
            collection.insert_one(data)
            logger.info("Success")
        except json.JSONDecodeError:
            logger.error("Error decoding Json")
        except PyMongoError as e:
            logger.error(f"MongoDB error: {e}")
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")

    consumer.close()

if __name__ == "__main__":
    thread1 = threading.Thread(target=takeData)
    thread2 = threading.Thread(target=importDataToMongoDB)
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()