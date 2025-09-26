from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import logging, os, time, json, ipaddress, collections, hashlib
from ddtrace import patch_all, tracer

patch_all()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("netflow-processor")

# ---- Kafka config ----
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service.kafka.svc.cluster.local:9092")
IN_TOPIC     = os.getenv("KAFKA_TOPIC", "flows")
OUT_TOPIC    = os.getenv("KAFKA_OUT_TOPIC", "top-talkers-10m")
GROUP_ID     = os.getenv("KAFKA_GROUP", "netflow-processor-group")
AUTO_OFFSET  = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
CLIENT_ID    = os.getenv("KAFKA_CLIENT_ID", "netflow-processor")
WINDOW_SEC   = int(os.getenv("ANALYSIS_WINDOW_SECONDS", "600"))  # 10m

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": AUTO_OFFSET,
    "client.id": CLIENT_ID + ".consumer",
}

producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": CLIENT_ID + ".producer",
    # tune as you like:
    "acks": "all",
    "compression.type": os.getenv("KAFKA_COMPRESSION", "lz4"),
    "linger.ms": int(os.getenv("KAFKA_LINGER_MS", "50")),
}

# ---- helpers ----

_RFC1918_V4 = [
    ipaddress.IPv4Network("10.0.0.0/8"),
    ipaddress.IPv4Network("172.16.0.0/12"),
    ipaddress.IPv4Network("192.168.0.0/16"),
]

def is_external(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        if isinstance(ip, ipaddress.IPv4Address):
            return not any(ip in net for net in _RFC1918_V4)
        return True  # treat IPv6 as external; adjust if you want to exclude fc00::/7
    except Exception:
        return False

def human_bytes(n: int) -> str:
    units = ["B","KB","MB","GB","TB","PB"]
    val = float(n); i = 0
    while val >= 1024 and i < len(units)-1:
        val /= 1024.0; i += 1
    return f"{val:.1f} {units[i]}" if i else f"{int(val)} {units[i]}"

def bytes_field(rec) -> int:
    try:
        return int(rec.get("bytes", 0))
    except Exception:
        return 0

def make_key(win_start_s: int) -> bytes:
    # partition key per window, so all aggregates for a given 10-min window go to same partition
    return str(win_start_s).encode()

def flush_and_publish(p: Producer, win_start_s, win_end_s, msg_count, bytes_to_ext, bytes_from_ext, ext_ip_counter):
    # build top 10 by occurrences (you can switch to bytes easily)
    top10 = ext_ip_counter.most_common(10)

    payload = {
        "window": {"start_s": win_start_s, "end_s": win_end_s, "length_s": win_end_s - win_start_s},
        "records": msg_count,
        "bytes": {
            "to_external": bytes_to_ext,
            "from_external": bytes_from_ext,
            "to_external_h": human_bytes(bytes_to_ext),
            "from_external_h": human_bytes(bytes_from_ext),
        },
        "top_external_ips_by_count": [{"ip": ip, "count": c} for ip, c in top10],
        "service": os.getenv("DD_SERVICE", "netflow-processor"),
        "version": os.getenv("DD_VERSION", "1.0.0"),
    }

    # DSM-friendly trace around the publish
    with tracer.trace("flow.publish", resource=OUT_TOPIC, service=os.getenv("DD_SERVICE", "netflow-processor")) as span:
        span.set_tag("out.topic", OUT_TOPIC)
        span.set_tag("window.start_s", win_start_s)
        span.set_tag("window.records", msg_count)
        span.set_tag("bytes.to_external", bytes_to_ext)
        span.set_tag("bytes.from_external", bytes_from_ext)

        key = make_key(win_start_s)
        p.produce(OUT_TOPIC, key=key, value=json.dumps(payload).encode("utf-8"))
        p.flush(5)

    # human log summary
    logger.info(
        "Processed %d records in last %d s. To ext: %s, From ext: %s. Published aggregate to topic '%s'. Top10: %s",
        msg_count, payload["window"]["length_s"],
        payload["bytes"]["to_external_h"], payload["bytes"]["from_external_h"],
        OUT_TOPIC,
        top10 if top10 else "None",
    )

# ---- main loop ----

def consume_kafka():
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    consumer.subscribe([IN_TOPIC])

    logger.info(f"Processor started. Consuming '{IN_TOPIC}' → producing '{OUT_TOPIC}' on '{KAFKA_BROKER}'")

    win_start = int(time.time())
    msg_count = 0
    bytes_to_ext = 0
    bytes_from_ext = 0
    ext_ip_counter = collections.Counter()

    try:
        while True:
            now = int(time.time())
            if (now - win_start) >= WINDOW_SEC:
                flush_and_publish(producer, win_start, now, msg_count, bytes_to_ext, bytes_from_ext, ext_ip_counter)
                # reset window
                win_start = now
                msg_count = 0
                bytes_to_ext = 0
                bytes_from_ext = 0
                ext_ip_counter.clear()

            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                break

            with tracer.trace("flow.process", resource=IN_TOPIC, service=os.getenv("DD_SERVICE", "netflow-processor")):
                try:
                    rec = json.loads(msg.value())
                except Exception:
                    continue

                b = bytes_field(rec)
                src = rec.get("src_addr")
                dst = rec.get("dst_addr")

                if dst and is_external(dst):
                    bytes_to_ext += b
                    ext_ip_counter[dst] += 1
                if src and is_external(src):
                    bytes_from_ext += b
                    ext_ip_counter[src] += 1

            msg_count += 1

    except KeyboardInterrupt:
        logger.info("Shutting down consumer (KeyboardInterrupt).")
    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
    finally:
        consumer.close()
