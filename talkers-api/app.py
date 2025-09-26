from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from ddtrace import patch_all
import logging, os, json, threading, time

# Auto-instrument
patch_all()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("talkers-api")

# ---- Kafka config ----
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service.kafka.svc.cluster.local:9092")
IN_TOPIC     = os.getenv("KAFKA_IN_TOPIC", "top-talkers-10m")
GROUP_ID     = os.getenv("KAFKA_GROUP", "talkers-api-group")
AUTO_OFFSET  = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
CLIENT_ID    = os.getenv("KAFKA_CLIENT_ID", "talkers-api")

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": AUTO_OFFSET,
    "client.id": CLIENT_ID,
    "session.timeout.ms": 10000,
    "heartbeat.interval.ms": 3000,
    "socket.keepalive.enable": True,
}

# ---- Globals ----
latest_payload = {}
_lock = threading.Lock()

# ---- Background consumer thread ----
def consume_kafka():
    global latest_payload
    consumer = Consumer(consumer_conf)
    consumer.subscribe([IN_TOPIC])

    logger.info(f"talkers-api consuming from '{IN_TOPIC}' on '{KAFKA_BROKER}'")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.warning(f"Kafka error: {msg.error()}")
                time.sleep(1)
                continue

            try:
                data = json.loads(msg.value())
                with _lock:
                    latest_payload = data
                logger.info(f"Updated latest top-talkers window: {data['window']}")
            except Exception as e:
                logger.error(f"Failed to parse message: {e}")
    except Exception as e:
        logger.exception(f"Fatal error in consumer: {e}")
    finally:
        consumer.close()

# Start background thread - 
threading.Thread(target=consume_kafka, daemon=True).start()

# ---- FastAPI app ----
app = FastAPI(title="Talkers API", version="1.0.1")

@app.get("/api/latest")
def get_latest():
    with _lock:
        if latest_payload:
            return JSONResponse(content=latest_payload)
        return JSONResponse(content={"error": "No data yet"}, status_code=404)

