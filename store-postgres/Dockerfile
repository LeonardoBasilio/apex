# Runs apex_store's continuous Kafka -> Postgres consumer loop
# (scripts/run_consumer.py). Built from this directory alone — this lane
# has no dependency on stream-kafka's package, only on the wire format its
# producer writes (CONTRACT.md's job: the shape, not the code, is shared).
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY ddl ./ddl
RUN pip install --no-cache-dir -e ".[postgres,kafka]"

COPY scripts ./scripts

CMD ["python", "scripts/run_consumer.py"]
