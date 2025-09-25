from confluent_kafka import Consumer, KafkaException, KafkaError
import logging, os, time, json, ipaddress, collections
from ddtrace import patch_all, tracer

patch_all()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("netflow-processor")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service.kafka.svc.cluster.local:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "flows")
KAFKA_GROUP  = os.getenv("KAFKA_GROUP", "netflow-processor-group")
AUTO_OFFSET  = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
CLIENT_ID    = os.getenv("KAFKA_CLIENT_ID", "netflow-processor")
WINDOW_SEC   = int(os.getenv("ANALYSIS_WINDOW_SECONDS", "600"))  # 10 min

consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": KAFKA_GROUP,
    "auto.offset.reset": AUTO_OFFSET,
    "client.id": CLIENT_ID,
}

# ---- helpers ----

# RFC1918-only “local” as you specified: 10/8, 172.16/12, 192.168/16
_RFC1918_V4 = [
    ipaddress.IPv4Network("10.0.0.0/8"),
    ipaddress.IPv4Network("172.16.0.0/12"),
    ipaddress.IPv4Network("192.168.0.0/16"),
]

def is_external(ip_str: str) -> bool:
    """True if IP is NOT RFC1918 IPv4; IPv6 is treated as external."""
    try:
        ip = ipaddress.ip_address(ip_str)
        if isinstance(ip, ipaddress.IPv4Address):
            return not any(ip in net for net in _RFC1918_V4)
        # IPv6: count as external (adjust if you want to treat ULA fc00::/7 as local)
        return True
    except Exception:
        return False

def human_bytes(n: int) -> str:
    units = ["B","KB","MB","GB","TB","PB"]
    val = float(n); i = 0
    while val >= 1024 and i < len(units)-1:
        val /= 1024.0; i += 1
    return f"{val:.1f} {units[i]}" if i else f"{int(val)} {units[i]}"

def bytes_field(record) -> int:
    try:
        return int(record.get("bytes", 0))
    except Exception:
        return 0

def flush_window(start_ts, msg_count, bytes_to_ext, bytes_from_ext, ext_ip_counter):
    now = time.time()
    window_len = int(now - start_ts)
    top10_external = ext_ip_counter.most_common(10)

    # APM span
    with tracer.trace("flow.analysis", resource="external-10m", service=os.getenv("DD_SERVICE", "netflow-processor")) as span:
        span.set_tag("window.seconds", window_len)
        span.set_tag("window.records", msg_count)
        span.set_tag("bytes.to_external", bytes_to_ext)
        span.set_tag("bytes.from_external", bytes_from_ext)
        if top10_external:
            span.set_tag("top10.external_ips", json.dumps(top10_external))

    logger.info(
        "Processed %d NetFlow records in the last %d seconds. "
        "Bytes to external: %s, Bytes from external: %s, Top10 external IPs: %s",
        msg_count, window_len,
        human_bytes(bytes_to_ext), human_bytes(bytes_from_ext),
        top10_external if top10_external else "None"
    )

# ---- main ----

def consume_kafka():
    """Every 10 min: counts, bytes to/from external (non-RFC1918), Top 10 external IPs."""
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Processor started. Consuming '{KAFKA_TOPIC}' on '{KAFKA_BROKER}'")

    start_ts = time.time()
    msg_count = 0
    bytes_to_ext = 0     # dst is external
    bytes_from_ext = 0   # src is external
    ext_ip_counter = collections.Counter()

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if (now - start_ts) >= WINDOW_SEC:
                flush_window(start_ts, msg_count, bytes_to_ext, bytes_from_ext, ext_ip_counter)
                start_ts = now
                msg_count = 0
                bytes_to_ext = 0
                bytes_from_ext = 0
                ext_ip_counter.clear()

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                break

            with tracer.trace("flow.process", resource=KAFKA_TOPIC, service=os.getenv("DD_SERVICE", "netflow-processor")):
                try:
                    rec = json.loads(msg.value())
                except Exception:
                    continue

                b = bytes_field(rec)
                src = rec.get("src_addr")
                dst = rec.get("dst_addr")

                # bytes to/from EXTERNAL only
                if dst and is_external(dst):
                    bytes_to_ext += b
                    ext_ip_counter[dst] += 1  # count external IP appearance
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

if __name__ == "__main__":
    consume_kafka()
