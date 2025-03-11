from confluent_kafka import Consumer, KafkaException, KafkaError
import logging
import os
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service.kafka.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flows")

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'netflow-processor-group',
    'auto.offset.reset': 'earliest',  # Start from the beginning if no offset exists
}

def consume_kafka():
    """Consumes NetFlow data from Kafka, counts records, and logs every 10 minutes."""
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    logger.info(f"Processor started. Consuming messages from topic '{KAFKA_TOPIC}'")

    record_count = 0
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll Kafka for new messages
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break

            # Process NetFlow data
            record_count += 1  # Increment flow count

            # Log every 10 minutes
            elapsed_time = time.time() - start_time
            if elapsed_time >= 600:  # 600 seconds = 10 minutes
                logger.info(f"Processed {record_count} NetFlow records in the last 10 minutes.")
                record_count = 0  # Reset counter
                start_time = time.time()  # Reset timer

    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_kafka()
