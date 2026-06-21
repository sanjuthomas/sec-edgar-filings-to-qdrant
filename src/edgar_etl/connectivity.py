"""Startup connectivity checks for Qdrant, MongoDB, and Kafka."""

from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from pymongo import MongoClient
from qdrant_client import QdrantClient

from edgar_etl.config import Settings
from edgar_etl.qdrant_search import BM25_VECTOR_NAME, is_bm25_ready


@dataclass
class ServiceStatus:
    name: str
    ok: bool
    detail: str


def check_qdrant(settings: Settings) -> ServiceStatus:
    try:
        client = QdrantClient(url=settings.qdrant_url, timeout=5)
        if not client.collection_exists(settings.qdrant_collection):
            return ServiceStatus(
                "qdrant",
                False,
                f"collection {settings.qdrant_collection!r} not found "
                "(run edgar-etl init-collection)",
            )
        info = client.get_collection(settings.qdrant_collection)
        count = info.points_count or 0
        bm25 = is_bm25_ready(client, settings.qdrant_collection)
        detail = f"connected, {count} points, BM25 {'ready' if bm25 else 'missing'}"
        return ServiceStatus("qdrant", True, detail)
    except Exception as exc:
        return ServiceStatus("qdrant", False, str(exc))


def check_qdrant_bm25(settings: Settings) -> ServiceStatus:
    try:
        client = QdrantClient(url=settings.qdrant_url, timeout=5)
        if not client.collection_exists(settings.qdrant_collection):
            return ServiceStatus(
                "qdrant_bm25",
                False,
                f"collection {settings.qdrant_collection!r} not found",
            )
        if not is_bm25_ready(client, settings.qdrant_collection):
            return ServiceStatus(
                "qdrant_bm25",
                False,
                f"sparse vector {BM25_VECTOR_NAME!r} missing "
                "(recreate collection with edgar-etl init-collection)",
            )
        count = client.get_collection(settings.qdrant_collection).points_count or 0
        return ServiceStatus(
            "qdrant_bm25",
            True,
            f"BM25 sparse vector ready, {count} indexed chunks",
        )
    except Exception as exc:
        return ServiceStatus("qdrant_bm25", False, str(exc))


def check_mongo(settings: Settings) -> ServiceStatus:
    if not settings.mongo_uri:
        return ServiceStatus("mongodb", False, "MONGO_URI not configured")
    try:
        client = MongoClient(
            settings.mongo_uri,
            serverSelectionTimeoutMS=settings.mongo_timeout_ms,
        )
        client.admin.command("ping")
        client.close()
        return ServiceStatus("mongodb", True, "connected")
    except Exception as exc:
        return ServiceStatus("mongodb", False, str(exc))


def check_kafka(settings: Settings) -> ServiceStatus:
    try:
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        metadata = admin.list_topics(timeout=settings.mongo_timeout_ms / 1000)
        topic_names = set(metadata.topics)
        if settings.kafka_topic in topic_names:
            detail = f"connected, topic {settings.kafka_topic!r} found"
        else:
            detail = (
                f"connected, topic {settings.kafka_topic!r} not found "
                f"({len(topic_names)} topics visible)"
            )
        return ServiceStatus("kafka", True, detail)
    except Exception as exc:
        return ServiceStatus("kafka", False, str(exc))


def check_kafka_consumer_group(settings: Settings) -> ServiceStatus:
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_group_id,
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([settings.kafka_topic])
        consumer.poll(0)
        consumer.close()
        return ServiceStatus(
            "kafka_consumer",
            True,
            f"group {settings.kafka_group_id!r} subscribed to {settings.kafka_topic!r}",
        )
    except Exception as exc:
        return ServiceStatus("kafka_consumer", False, str(exc))


def check_all(settings: Settings) -> list[ServiceStatus]:
    return [
        check_qdrant(settings),
        check_qdrant_bm25(settings),
        check_mongo(settings),
        check_kafka(settings),
        check_kafka_consumer_group(settings),
    ]
