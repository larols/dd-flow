from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import logging, os, time, json, ipaddress, collections, signal, threading
from ddtrace import patch_all, tracer

# If you launch with `ddtrace-run`, you can comment the next line out.
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
    # stability tweaks
    "session.timeout.ms": 10000,
    "heartbeat.interval.ms": 3000,
    "metadata.max.age.ms": 180000,
    "socket.keepalive.enable": True,
}

producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": CLIENT_ID + ".producer",
    "acks": "all",
    "compression.type": os.getenv("KAFKA_COMPRESSION", "lz4"),
    "linger.ms": int(os.getenv("KAFKA_LINGER_MS", "50")),
    "enable.idempotence": True,  # safe producer
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
        return True  # treat IPv6 as external unless you want to exclude fc00::/7
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
    # Partition key per window to cluster same-window aggregates
    return str(win_start_s).encode()

def flush_and_publish(p: Producer, win_start_s, win_end_s, msg_count, bytes_to_ext, bytes_from_ext, ext_ip_counter):
    # Build top 10 by occurrence (switch to bytes if desired)
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

    # Only publish if there was activity in the window
    if msg_count > 0:
        with tracer.trace("flow.publish", resource=OUT_TOPIC, service=os.getenv("DD_SERVICE", "netflow-processor")) as span:
            span.set_tag("out.topic", OUT_TOPIC)
            span.set_tag("window.start_s", win_start_s)
            span.set_tag("window.records", msg_count)
            span.set_tag("bytes.to_external", bytes_to_ext)
            span.set_tag("bytes.from_external", bytes_from_ext)

            try:
                p.produce(OUT_TOPIC, key=make_key(win_start_s), value=json.dumps(payload).encode("utf-8"))
                # Drive delivery callbacks & network I/O without blocking too long
                p.poll(0)
                p.flush(5)
            except BufferError as e:
                logger.warning(f"Producer buffer full, flushing and retrying: {e}")
                p.flush(5)
                p.produce(OUT_TOPIC, key=make_key(win_start_s), value=json.dumps(payload).encode("utf-8"))
                p.flush(5)
            except Exception as e:
                logger.exception(f"Failed to publish aggregate: {e}")

    logger.info(
        "Processed %d records in last %d s. To ext: %s, From ext: %s. Published aggregate to topic '%s'. Top10: %s",
        msg_count, payload["window"]["length_s"],
        payload["bytes"]["to_external_h"], payload["bytes"]["from_external_h"],
        OUT_TOPIC,
        top10 if top10 else "None",
    )

# ---- graceful shutdown ----
_shutdown = threading.Event()

def _handle_sigterm(*_):
    logger.info("Received termination signal; will flush and exit…")
    _shutdown.set()

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# ---- main run loop with resilience ----

def run_once():
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
        while not _shutdown.is_set():
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
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    continue

                # Treat common transient errors as retryable
                if code in (
                    KafkaError._ALL_BROKERS_DOWN,
                    KafkaError._TRANSPORT,
                    KafkaError._TIMED_OUT,
                    KafkaError._REVOKE_PARTITIONS,
                    KafkaError._WAIT_COORD,
                    KafkaError._UNKNOWN_PARTITION,
                    KafkaError._LEADER_NOT_AVAILABLE,
                    KafkaError._STATE,
                ):
                    logger.warning(f"Kafka transient error: {msg.error()}, retrying…")
                    time.sleep(2)
                    continue

                # Unexpected error: log and keep going (don't exit)
                logger.error(f"Kafka error: {msg.error()}, continuing…")
                time.sleep(2)
                continue

            with tracer.trace("flow.process", resource=IN_TOPIC, service=os.getenv("DD_SERVICE", "netflow-processor")):
                try:
                    rec = json.loads(msg.value())
                except Exception:
                    # skip non-JSON or malformed messages
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

    except Exception as e:
        logger.exception(f"Fatal error in processing loop: {e}")
        # Let finally close resources; outer loop will restart us
    finally:
        # Flush current window on shutdown or fatal exception
        try:
            now = int(time.time())
            flush_and_publish(producer, win_start, now, msg_count, bytes_to_ext, bytes_from_ext, ext_ip_counter)
        except Exception as e:
            logger.warning(f"Failed to flush final window: {e}")
        try:
            consumer.close()
        except Exception:
            pass
        try:
            producer.flush(5)
        except Exception:
            pass

def main():
    backoff = 1
    while not _shutdown.is_set():
        run_once()
        if _shutdown.is_set():
            break
        logger.warning(f"Consumer loop exited; restarting in {backoff}s…")
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)

if __name__ == "__main__":
    main()
