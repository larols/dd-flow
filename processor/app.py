from confluent_kafka import Consumer, KafkaException, KafkaError
import logging, os, time, json
from ddtrace import patch_all, tracer

# Auto-instrument supported libs, including confluent-kafka & logging
patch_all()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("netflow-processor")

# Kafka config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service.kafka.svc.cluster.local:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "flows")
KAFKA_GROUP  = os.getenv("KAFKA_GROUP", "netflow-processor-group")
AUTO_OFFSET  = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
CLIENT_ID    = os.getenv("KAFKA_CLIENT_ID", "netflow-processor")

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": KAFKA_GROUP,
    "auto.offset.reset": AUTO_OFFSET,
    "client.id": CLIENT_ID,
}

def consume_kafka():
    """Consumes NetFlow data from Kafka, counts records, and logs every 10 minutes."""
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    logger.info(f"Processor started. Consuming messages from topic '{KAFKA_TOPIC}' on '{KAFKA_BROKER}'")

    record_count = 0
    start_time = time.time()

    try:
      while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error(f"Kafka error: {msg.error()}")
            break

        # Optional: parse JSON so we have a "work" span that DSM can visualize end-to-end
        with tracer.trace("flow.process", resource=KAFKA_TOPIC, service=os.getenv("DD_SERVICE", "netflow-processor")):
            try:
                _ = json.loads(msg.value())
            except Exception:
                # flows may be large or not strictly JSON – parsing is optional
                pass

        record_count += 1

        # Log every 10 minutes
        if (time.time() - start_time) >= 600:
            logger.info(f"Processed {record_count} NetFlow records in the last 10 minutes.")
            record_count = 0
            start_time = time.time()

    except KeyboardInterrupt:
        logger.info("Shutting down consumer (KeyboardInterrupt).")
    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_kafka()
