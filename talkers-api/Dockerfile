FROM python:3.11-slim

# System deps needed by confluent-kafka (librdkafka) + TLS
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 libssl3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# App deps
# If you keep a requirements.txt, replace the RUN below with:
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir \
    fastapi==0.115.* \
    uvicorn[standard]==0.30.* \
    confluent-kafka==2.* \
    ddtrace==2.*

# Copy app
COPY app.py /app/app.py

# Default envs (can be overridden by k8s)
ENV PYTHONUNBUFFERED=1 \
    KAFKA_BROKER="kafka-service.kafka.svc.cluster.local:9092" \
    KAFKA_IN_TOPIC="top-talkers-10m" \
    KAFKA_GROUP="talkers-api-group" \
    KAFKA_AUTO_OFFSET_RESET="latest" \
    KAFKA_CLIENT_ID="talkers-api"

# Expose HTTP
EXPOSE 8000

# Run with ddtrace for DSM/APM visibility
CMD ["ddtrace-run", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
