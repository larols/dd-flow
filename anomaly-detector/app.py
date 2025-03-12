from confluent_kafka import Consumer, Producer, KafkaException
import joblib
import json
import os
import numpy as np
import pandas as pd
import logging
import threading
import time
from sklearn.ensemble import IsolationForest

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service.kafka.svc.cluster.local:9092")
KAFKA_TOPIC = "flows"
ANOMALY_TOPIC = "anomalies"

# Model storage
MODEL_PATH = "isolation_forest_model.pkl"

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'netflow-anomaly-group',
    'auto.offset.reset': 'earliest',
}

# Kafka Producer Configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe([KAFKA_TOPIC])

# Lock to ensure safe model updating
model_lock = threading.Lock()

def train_model():
    """ Periodically trains an Isolation Forest model on NetFlow data every 24 hours. """
    while True:
        logger.info("Retraining Isolation Forest model with new NetFlow data...")

        messages = []
        for _ in range(10000):  # Train on last 10,000 records
            msg = consumer.poll(1.0)
            if msg is None or not msg.value():
                logger.warning("Received an empty message from Kafka. Skipping...")
                continue
            
            try:
                netflow_data = json.loads(msg.value().decode('utf-8', errors='ignore'))
                messages.append(netflow_data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from Kafka: {e}")
                continue

        if messages:
            df = pd.DataFrame(messages)
            features = ["bytes", "packets", "src_port", "dst_port", "proto"]

            new_model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
            new_model.fit(df[features])

            with model_lock:
                joblib.dump(new_model, MODEL_PATH)

            logger.info("Model retrained and updated successfully!")
        else:
            logger.warning("No new data available for training.")

        time.sleep(86400)  # Sleep for 24 hours before retraining


def detect_anomalies():
    """ Consumes NetFlow data and detects anomalies using the latest Isolation Forest model. """
    global consumer
    logger.info(f"Anomaly Detector started. Consuming from topic '{KAFKA_TOPIC}'")

    # Load initial model or create a placeholder if missing
    if not os.path.exists(MODEL_PATH):
        logger.warning("⚠️ No trained model found. Creating a temporary model to prevent crashes...")
        placeholder_model = IsolationForest(n_estimators=10, contamination=0.05, random_state=42)
        joblib.dump(placeholder_model, MODEL_PATH)
        logger.info("✅ Placeholder model created.")

    with model_lock:
        model = joblib.load(MODEL_PATH)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or not msg.value():
                logger.warning("Received an empty message from Kafka. Skipping...")
                continue

            try:
                netflow_data = json.loads(msg.value().decode('utf-8', errors='ignore'))
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from Kafka: {e}")
                continue

            feature_vector = np.array([
                netflow_data["bytes"],
                netflow_data["packets"],
                netflow_data["src_port"],
                netflow_data["dst_port"],
                netflow_data["proto"]
            ]).reshape(1, -1)

            with model_lock:
                prediction = model.predict(feature_vector)

            if prediction[0] == -1:
                logger.warning(f"Anomaly Detected: {netflow_data}")
                producer.produce(ANOMALY_TOPIC, json.dumps(netflow_data))
            else:
                logger.info("Normal Traffic")

    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    # Start both threads
    training_thread = threading.Thread(target=train_model, daemon=True)
    detection_thread = threading.Thread(target=detect_anomalies, daemon=True)

    training_thread.start()
    detection_thread.start()

    training_thread.join()
    detection_thread.join()
