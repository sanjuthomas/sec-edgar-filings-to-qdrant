from dataclasses import dataclass
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.http import models

from edgar_etl.config import Settings
from edgar_etl.embed import embed_texts


@dataclass
class SearchResult:
    content: str
    score: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


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
    )[0]

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

    query_filter = models.Filter(must=must_conditions) if must_conditions else None

    client = QdrantClient(url=settings.qdrant_url)
    hits = client.query_points(
        collection_name=settings.qdrant_collection,
        query=query_vector,
        query_filter=query_filter,
        limit=top_k,
        with_payload=True,
    ).points

    results: list[SearchResult] = []
    for hit in hits:
        payload = hit.payload or {}
        results.append(
            SearchResult(
                content=str(payload.get("content", "")),
                score=float(hit.score or 0.0),
                accession_number=str(payload.get("accession_number", "")),
                chunk_index=int(payload.get("chunk_index", 0)),
                metadata={
                    key: value
                    for key, value in payload.items()
                    if key not in {"content", "accession_number", "chunk_index"}
                },
            )
        )
    return results


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
