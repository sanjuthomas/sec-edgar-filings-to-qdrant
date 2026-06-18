FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir .

ENV EDGAR_DATA_DIR=/Volumes/Transcend/edgar
ENV QDRANT_URL=http://qdrant:6333
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

CMD ["edgar-etl", "consume"]
