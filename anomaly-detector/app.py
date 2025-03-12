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
from sklearn.preprocessing import LabelEncoder

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service.kafka.svc.cluster.local:9092")
KAFKA_TOPIC = "flows"
ANOMALY_TOPIC = "anomalies"

# Model storage
MODEL_PATH = "isolation_forest_model.pkl"

# Kafka Producer Configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER
}

producer = Producer(producer_conf)

# Lock to ensure safe model updating
model_lock = threading.Lock()

def train_model():
    """ Periodically trains an Isolation Forest model on NetFlow data every 24 hours. """
    while True:
        logger.info("Retraining Isolation Forest model with new NetFlow data...")

        consumer_train = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'netflow-train-group',
            'auto.offset.reset': 'earliest',
        })
        consumer_train.subscribe([KAFKA_TOPIC])

        messages = []
        for _ in range(10000):  # Train on last 10,000 records
            msg = consumer_train.poll(1.0)
            if msg is None or not msg.value():
                logger.warning("Received an empty message from Kafka. Skipping...")
                continue

            try:
                netflow_data = json.loads(msg.value().decode('utf-8', errors='ignore'))
                messages.append(netflow_data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from Kafka: {e}")
                continue

        consumer_train.close()

        if messages:
            try:
                df = pd.DataFrame(messages)
                features = ["bytes", "packets", "src_port", "dst_port", "proto"]

                # Label encode the 'proto' column
                label_encoder = LabelEncoder()
                df['proto'] = label_encoder.fit_transform(df['proto'])

                new_model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
                new_model.fit(df[features])

                with model_lock:
                    joblib.dump((new_model, label_encoder), MODEL_PATH)

                logger.info("Model retrained and updated successfully!")
            except ValueError as e:
                logger.error(f"Error during model training: {e}")
        else:
            logger.warning("No new data available for training.")

        time.sleep(86400)  # Sleep for 24 hours before retraining

def detect_anomalies():
    """ Consumes NetFlow data and detects anomalies using the latest Isolation Forest model. """
    consumer_detect = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'netflow-detect-group',
        'auto.offset.reset': 'earliest',
    })
    consumer_detect.subscribe([KAFKA_TOPIC])

    logger.info(f"Anomaly Detector started. Consuming from topic '{KAFKA_TOPIC}'")

    # Wait for the model to be trained
    while not os.path.exists(MODEL_PATH):
        logger.info("Waiting for model to be trained...")
        time.sleep(10)  # Wait for 10 seconds before checking again

    try:
        while True:
            # rest of the detection code
            # ...
    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
    finally:
        consumer_detect.close()
        producer.flush()

if __name__ == "__main__":
    # Start both threads
    training_thread = threading.Thread(target=train_model, daemon=True)
    detection_thread = threading.Thread(target=detect_anomalies, daemon=True)

    training_thread.start()
    detection_thread.start()
    while True:
        time.sleep(1)