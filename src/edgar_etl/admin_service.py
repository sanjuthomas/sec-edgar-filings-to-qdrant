"""Admin operations: truncate, ticker load, connectivity."""

from __future__ import annotations

from dataclasses import dataclass

import structlog

from edgar_etl.config import Settings
from edgar_etl.connectivity import ServiceStatus, check_all
from edgar_etl.embedding_runtime import BACKEND_LABELS, get_embedding_backend, set_embedding_backend
from edgar_etl.errors import FilingNotIndexableError, NonContentProcessingError
from edgar_etl.models import FilingDownloadedEvent
from edgar_etl.mongo import MongoFilingStore
from edgar_etl.pipeline import process_filing_event
from edgar_etl.qdrant_search import (
    BM25_MODEL,
    BM25_VECTOR_NAME,
    DENSE_VECTOR_NAME,
    TEXT_INDEX_FIELD,
    is_bm25_ready,
)
from edgar_etl.store import FilingStore

logger = structlog.get_logger(__name__)
SUPPORTED_FORM_SELECTIONS = frozenset({"10-K", "10-Q", "8-K"})
TRUNCATABLE_TARGETS = frozenset({"filing_chunks"})


def _normalize_forms(forms: list[str] | None) -> list[str] | None:
    if forms is None:
        return None
    normalized: list[str] = []
    seen: set[str] = set()
    for form in forms:
        value = form.strip().upper().replace(" ", "")
        canonical = value.replace("10Q", "10-Q").replace("10K", "10-K").replace("8K", "8-K")
        if canonical not in SUPPORTED_FORM_SELECTIONS:
            raise ValueError(f"unsupported form selection: {form}")
        if canonical not in seen:
            normalized.append(canonical)
            seen.add(canonical)
    if not normalized:
        raise ValueError("at least one filing form must be selected")
    return normalized


@dataclass
class TickerLoadResult:
    ticker: str
    found: int
    processed: int
    skipped: int
    failed: int
    total_chunks: int
    errors: list[str]


def truncate_collection(settings: Settings, target: str) -> str:
    if target not in TRUNCATABLE_TARGETS:
        raise ValueError(f"unsupported truncate target: {target}")
    store = FilingStore(settings.qdrant_url, settings.qdrant_collection)
    store.truncate_collection(settings.embedding_dimension)
    logger.info("collection truncated", collection=settings.qdrant_collection)
    return (
        f"Truncated collection {settings.qdrant_collection} "
        "(removed all dense vectors, BM25 sparse vectors, and payload data)"
    )


def load_ticker(
    settings: Settings,
    ticker: str,
    *,
    forms: list[str] | None = None,
) -> TickerLoadResult:
    normalized = ticker.strip().upper()
    if not normalized:
        raise ValueError("ticker is required")

    mongo_store = MongoFilingStore(settings)
    filing_store = FilingStore(settings.qdrant_url, settings.qdrant_collection)
    selected_forms = _normalize_forms(forms)
    allowed_forms = selected_forms or settings.allowed_forms

    try:
        docs = mongo_store.list_filings_by_ticker(
            normalized,
            allowed_forms=allowed_forms,
        )
    finally:
        mongo_store.close()

    result = TickerLoadResult(
        ticker=normalized,
        found=len(docs),
        processed=0,
        skipped=0,
        failed=0,
        total_chunks=0,
        errors=[],
    )

    for doc in docs:
        accession_number = doc.get("accession_number", "?")
        try:
            event = FilingDownloadedEvent.model_validate(
                {
                    "event_type": "filing.downloaded",
                    "schema_version": 1,
                    **doc,
                }
            )
            chunk_count = process_filing_event(
                event,
                settings,
                store=filing_store,
                skip_if_processed=False,
            )
            if chunk_count == 0:
                result.skipped += 1
            else:
                result.processed += 1
                result.total_chunks += chunk_count
        except (FilingNotIndexableError, NonContentProcessingError) as exc:
            result.failed += 1
            result.errors.append(f"{accession_number}: {exc}")
            logger.warning(
                "ticker load skipped filing",
                ticker=normalized,
                accession_number=accession_number,
                error=str(exc),
            )
        except Exception as exc:
            result.failed += 1
            result.errors.append(f"{accession_number}: {exc}")
            logger.exception(
                "ticker load failed",
                ticker=normalized,
                accession_number=accession_number,
            )

    logger.info(
        "ticker load finished",
        ticker=normalized,
        found=result.found,
        processed=result.processed,
        skipped=result.skipped,
        failed=result.failed,
        total_chunks=result.total_chunks,
    )
    return result


def get_connectivity(settings: Settings) -> list[ServiceStatus]:
    return check_all(settings)


@dataclass
class EmbeddingConfigInfo:
    backend: str
    backend_label: str
    model: str
    device: str
    dimensions: int
    max_seq_length: int
    batch_size: int
    similarity: str
    library: str
    query_prompt: str | None
    ollama_base_url: str | None = None


@dataclass
class SchemaColumnInfo:
    name: str
    type: str
    nullable: bool
    notes: str | None = None


@dataclass
class SchemaTableInfo:
    name: str
    columns: list[SchemaColumnInfo]
    relationships: list[str]


@dataclass
class PipelineStepInfo:
    title: str
    description: str


@dataclass
class QdrantSearchConfigInfo:
    engine: str
    collection: str
    dense_vector: str
    sparse_vector: str
    bm25_model: str
    text_index_field: str
    bm25_ready: bool
    indexed_chunks: int
    rank_metric: str
    example_query: str
    docker_image: str


def get_embedding_config(settings: Settings) -> EmbeddingConfigInfo:
    backend = get_embedding_backend(settings)
    dims = settings.embedding_dimension or _infer_dimensions(settings.embedding_model)
    common = {
        "backend": backend,
        "backend_label": BACKEND_LABELS[backend],
        "dimensions": dims,
        "max_seq_length": settings.embedding_max_seq_length,
        "batch_size": settings.embedding_batch_size,
        "similarity": "cosine",
    }
    if backend == "ollama":
        return EmbeddingConfigInfo(
            **common,
            model=settings.ollama_embedding_model,
            device="host GPU via Ollama",
            library="Ollama /api/embed",
            query_prompt=None,
            ollama_base_url=settings.ollama_base_url,
        )
    return EmbeddingConfigInfo(
        **common,
        model=settings.embedding_model,
        device=settings.embedding_device,
        library="sentence-transformers (in-process)",
        query_prompt="query" if "bge-m3" in settings.embedding_model.lower() else None,
        ollama_base_url=None,
    )


def update_embedding_backend(settings: Settings, backend: str) -> EmbeddingConfigInfo:
    set_embedding_backend(settings, backend)  # type: ignore[arg-type]
    return get_embedding_config(settings)


def get_qdrant_search_config(settings: Settings) -> QdrantSearchConfigInfo:
    store = FilingStore(settings.qdrant_url, settings.qdrant_collection)
    bm25_ready = store.bm25_ready()
    chunk_count = store.count_points() if bm25_ready else 0

    return QdrantSearchConfigInfo(
        engine="Qdrant",
        collection=settings.qdrant_collection,
        dense_vector=DENSE_VECTOR_NAME,
        sparse_vector=BM25_VECTOR_NAME,
        bm25_model=BM25_MODEL,
        text_index_field=TEXT_INDEX_FIELD,
        bm25_ready=bm25_ready,
        indexed_chunks=chunk_count,
        rank_metric="BM25 score (sparse vector query)",
        example_query=(
            f"client.query_points(collection_name='{settings.qdrant_collection}', "
            f"query=Document(text='revenue growth', model='{BM25_MODEL}'), "
            f"using='{BM25_VECTOR_NAME}', limit=10)"
        ),
        docker_image="qdrant/qdrant:v1.18.2",
    )


def get_collection_schema(settings: Settings) -> list[SchemaTableInfo]:
    store = FilingStore(settings.qdrant_url, settings.qdrant_collection)
    dense_size = settings.embedding_dimension
    if store._client.collection_exists(settings.qdrant_collection):
        info = store._client.get_collection(settings.qdrant_collection)
        vectors = info.config.params.vectors
        if isinstance(vectors, dict) and DENSE_VECTOR_NAME in vectors:
            dense_size = vectors[DENSE_VECTOR_NAME].size

    payload_fields = [
        ("content", "text", "Plain-text chunk; full-text indexed for lexical filters."),
        ("accession_number", "keyword", "SEC accession number; facet for filing counts."),
        ("chunk_index", "integer", "Zero-based chunk order within a filing."),
        ("ticker", "keyword", "Stock ticker; filterable in search."),
        ("company_name", "text", "Company name from Kafka/Mongo metadata."),
        ("form", "keyword", "SEC form type (10-K, 10-Q, etc.)."),
        ("filing_date", "datetime", "Filing date from metadata."),
        ("section", "text", "ITEM header when detected in the filing."),
        ("local_path", "text", "Path to the source .htm file on disk."),
        ("document_url", "text", "SEC EDGAR document URL."),
        ("processed_at", "datetime", "UTC timestamp when the filing was indexed."),
        ("chunk_count", "integer", "Total chunks in the parent filing."),
    ]

    return [
        SchemaTableInfo(
            name=settings.qdrant_collection,
            columns=[
                SchemaColumnInfo(
                    name=DENSE_VECTOR_NAME,
                    type=f"vector({dense_size})",
                    nullable=False,
                    notes="Dense embedding from the active backend; cosine similarity search.",
                ),
                SchemaColumnInfo(
                    name=BM25_VECTOR_NAME,
                    type="sparse_vector",
                    nullable=False,
                    notes=f"Server-side BM25 inference via {BM25_MODEL} on chunk content.",
                ),
                *[
                    SchemaColumnInfo(
                        name=name,
                        type=field_type,
                        nullable=True,
                        notes=notes,
                    )
                    for name, field_type, notes in payload_fields
                ],
            ],
            relationships=[
                "One Qdrant point per text chunk (UUID id derived from accession_number + chunk_index).",
                f"Named dense vector {DENSE_VECTOR_NAME!r} for semantic search.",
                f"Named sparse vector {BM25_VECTOR_NAME!r} for keyword/BM25 search.",
                f"Payload field {TEXT_INDEX_FIELD!r} has a full-text index for lexical filters.",
            ],
        )
    ]


def get_pipeline_overview() -> list[PipelineStepInfo]:
    return [
        PipelineStepInfo(
            title="1. Filing metadata",
            description=(
                "MongoDB stores filing metadata (ticker, form, accession_number, local_path). "
                "Kafka events or the Load ticker action trigger indexing."
            ),
        ),
        PipelineStepInfo(
            title="2. Read & chunk",
            description=(
                "The ETL reads the local .htm file, extracts text, and splits it into "
                "overlapping chunks (CHUNK_SIZE / CHUNK_OVERLAP)."
            ),
        ),
        PipelineStepInfo(
            title="3. Embed",
            description=(
                "Each chunk is passed through the active embedding backend (embedded BGE-M3 "
                "or Ollama BGE-M3). Search queries use the same backend so vectors live in "
                "the same semantic space."
            ),
        ),
        PipelineStepInfo(
            title="4. Store in Qdrant",
            description=(
                "Each point stores dense vectors, BM25 sparse vectors, and payload fields "
                "in the filing_chunks collection. Both indexes are written on every upsert."
            ),
        ),
        PipelineStepInfo(
            title="5. Search",
            description=(
                "Semantic search queries the dense vector; keyword search queries the "
                "content-bm25 sparse vector. Both modes are available in the search UI."
            ),
        ),
    ]


def _infer_dimensions(model_name: str) -> int:
    lowered = model_name.lower()
    if "bge-m3" in lowered:
        return 1024
    if "bge-small" in lowered:
        return 384
    if "bge-base" in lowered:
        return 768
    if "bge-large" in lowered:
        return 1024
    return 0
