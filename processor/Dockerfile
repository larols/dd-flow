FROM python:3.9.21-slim

# System deps needed by confluent-kafka (librdkafka) and TLS/SASL
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-2 libssl3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# If you already have a requirements.txt, keep using it; otherwise this is fine too.
COPY requirements.txt .
# Make sure these are in requirements.txt:
#   ddtrace==2.*
#   confluent-kafka==2.*
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# No need to EXPOSE 5000 (this is a worker, not an HTTP server)

# Launch through ddtrace-run so confluent-kafka is auto-instrumented
CMD ["ddtrace-run", "python", "app.py"]