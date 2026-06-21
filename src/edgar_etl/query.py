from dataclasses import dataclass
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http import models

from edgar_etl.config import Settings
from edgar_etl.embed import embed_texts, query_prompt_name
from edgar_etl.qdrant_search import BM25_MODEL, BM25_VECTOR_NAME, DENSE_VECTOR_NAME, is_bm25_ready


@dataclass
class SearchResult:
    content: str
    score: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


@dataclass
class TextSearchResult:
    content: str
    rank: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


def _build_filter(
    *,
    ticker: str | None,
    form: str | None,
) -> models.Filter | None:
    must_conditions: list[models.FieldCondition] = []
    if ticker:
        must_conditions.append(
            models.FieldCondition(
                key="ticker",
                match=models.MatchValue(value=ticker.upper()),
            )
        )
    if form:
        must_conditions.append(
            models.FieldCondition(
                key="form",
                match=models.MatchValue(value=form.upper()),
            )
        )
    return models.Filter(must=must_conditions) if must_conditions else None


def _result_from_hit(hit: models.ScoredPoint) -> tuple[str, int, dict[str, Any], str]:
    payload = hit.payload or {}
    metadata = {
        key: value
        for key, value in payload.items()
        if key not in {"content", "accession_number", "chunk_index"}
    }
    return (
        str(payload.get("content", "")),
        int(payload.get("chunk_index", 0)),
        metadata,
        str(payload.get("accession_number", "")),
    )


def search_filings_text(
    query: str,
    settings: Settings,
    *,
    top_k: int = 5,
    ticker: str | None = None,
    form: str | None = None,
) -> list[TextSearchResult]:
    client = QdrantClient(url=settings.qdrant_url)
    if not is_bm25_ready(client, settings.qdrant_collection):
        raise RuntimeError(
            "Qdrant BM25 sparse vector is not ready "
            "(recreate the collection with edgar-etl init-collection)"
        )

    hits = client.query_points(
        collection_name=settings.qdrant_collection,
        query=models.Document(text=query, model=BM25_MODEL),
        using=BM25_VECTOR_NAME,
        query_filter=_build_filter(ticker=ticker, form=form),
        limit=top_k,
        with_payload=True,
    ).points

    results: list[TextSearchResult] = []
    for hit in hits:
        content, chunk_index, metadata, accession_number = _result_from_hit(hit)
        results.append(
            TextSearchResult(
                content=content,
                rank=float(hit.score or 0.0),
                accession_number=accession_number,
                chunk_index=chunk_index,
                metadata=metadata,
            )
        )
    return results


def search_filings(
    question: str,
    settings: Settings,
    *,
    top_k: int = 5,
    ticker: str | None = None,
    form: str | None = None,
) -> list[SearchResult]:
    query_vector = embed_texts(
        [question],
        model_name=settings.embedding_model,
        batch_size=1,
        device=settings.embedding_device,
        max_seq_length=settings.embedding_max_seq_length,
        prompt_name=query_prompt_name(settings),
        settings=settings,
    )[0]

    client = QdrantClient(url=settings.qdrant_url)
    hits = client.query_points(
        collection_name=settings.qdrant_collection,
        query=query_vector,
        using=DENSE_VECTOR_NAME,
        query_filter=_build_filter(ticker=ticker, form=form),
        limit=top_k,
        with_payload=True,
    ).points

    results: list[SearchResult] = []
    for hit in hits:
        content, chunk_index, metadata, accession_number = _result_from_hit(hit)
        results.append(
            SearchResult(
                content=content,
                score=float(hit.score or 0.0),
                accession_number=accession_number,
                chunk_index=chunk_index,
                metadata=metadata,
            )
        )
    return results


def format_text_results(results: list[TextSearchResult]) -> str:
    if not results:
        return "No matching chunks found."

    parts: list[str] = []
    for index, result in enumerate(results, start=1):
        meta = result.metadata
        header = (
            f"[{index}] {meta.get('ticker', '?')} {meta.get('form', '?')} "
            f"({result.accession_number}, chunk {result.chunk_index}) "
            f"rank={result.rank:.4f}"
        )
        if meta.get("section"):
            header += f" | {meta['section']}"
        parts.append(header)
        parts.append(result.content.strip())
        parts.append("")

    return "\n".join(parts).rstrip()


def format_results(results: list[SearchResult]) -> str:
    if not results:
        return "No matching chunks found."

    parts: list[str] = []
    for index, result in enumerate(results, start=1):
        meta = result.metadata
        header = (
            f"[{index}] {meta.get('ticker', '?')} {meta.get('form', '?')} "
            f"({result.accession_number}, chunk {result.chunk_index}) "
            f"score={result.score:.4f}"
        )
        if meta.get("section"):
            header += f" | {meta['section']}"
        parts.append(header)
        parts.append(result.content.strip())
        parts.append("")

    return "\n".join(parts).rstrip()
