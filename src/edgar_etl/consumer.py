import signal
import sys

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from edgar_etl.config import Settings
from edgar_etl.errors import FilingNotIndexableError, NonContentProcessingError
from edgar_etl.pipeline import configure_logging, parse_event, process_filing_event
from edgar_etl.store import FilingStore

logger = structlog.get_logger(__name__)

_running = True


def _shutdown_handler(signum: int, frame: object) -> None:
    global _running
    logger.info("shutdown signal received", signal=signum)
    _running = False


def run_consumer(
    settings: Settings | None = None,
    *,
    group_id: str | None = None,
    skip_if_processed: bool = True,
) -> None:
    settings = settings or Settings()
    configure_logging(settings.log_level)

    effective_group_id = group_id or settings.kafka_group_id

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": effective_group_id,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            "session.timeout.ms": settings.kafka_session_timeout_ms,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.kafka_topic])
    store = FilingStore(settings.qdrant_url, settings.qdrant_collection)

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    logger.info(
        "kafka consumer started",
        topic=settings.kafka_topic,
        group_id=effective_group_id,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        session_timeout_ms=settings.kafka_session_timeout_ms,
        skip_if_processed=skip_if_processed,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        qdrant_url=settings.qdrant_url,
        collection=settings.qdrant_collection,
    )

    try:
        while _running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            event = None
            try:
                event = parse_event(message.value())
                process_filing_event(
                    event,
                    settings,
                    store=store,
                    skip_if_processed=skip_if_processed,
                )
                consumer.commit(message=message, asynchronous=False)
            except (FilingNotIndexableError, NonContentProcessingError) as exc:
                logger.warning(
                    "skipping filing without indexing",
                    error=str(exc),
                    accession_number=getattr(event, "accession_number", None),
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                logger.exception(
                    "failed to process message",
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
    finally:
        consumer.close()
        logger.info("kafka consumer stopped")


def main() -> None:
    try:
        run_consumer()
    except KeyboardInterrupt:
        sys.exit(0)
