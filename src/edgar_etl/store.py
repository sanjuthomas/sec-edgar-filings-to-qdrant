import uuid
from datetime import datetime, timezone

from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.exceptions import UnexpectedResponse

from edgar_etl.models import FilingDownloadedEvent, TextChunk
from edgar_etl.qdrant_search import (
    BM25_MODEL,
    BM25_VECTOR_NAME,
    DENSE_VECTOR_NAME,
    TEXT_INDEX_FIELD,
    is_bm25_ready,
)

PAYLOAD_INDEX_FIELDS = ("accession_number", "ticker", "form")


def point_id_for_chunk(accession_number: str, chunk_index: int) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{accession_number}:{chunk_index}",
        )
    )


class FilingStore:
    def __init__(self, qdrant_url: str, collection_name: str) -> None:
        self._client = QdrantClient(url=qdrant_url)
        self._collection_name = collection_name

    def init_collection(self, vector_size: int) -> None:
        if not self._client.collection_exists(self._collection_name):
            self._client.create_collection(
                collection_name=self._collection_name,
                vectors_config={
                    DENSE_VECTOR_NAME: models.VectorParams(
                        size=vector_size,
                        distance=models.Distance.COSINE,
                    ),
                },
                sparse_vectors_config={
                    BM25_VECTOR_NAME: models.SparseVectorParams(
                        modifier=models.Modifier.IDF,
                    ),
                },
            )
        self._ensure_payload_indexes()

    def _ensure_payload_indexes(self) -> None:
        for field in PAYLOAD_INDEX_FIELDS:
            self._create_payload_index(
                field_name=field,
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
        self._create_payload_index(
            field_name=TEXT_INDEX_FIELD,
            field_schema=models.TextIndexParams(
                type=models.TextIndexType.TEXT,
                tokenizer=models.TokenizerType.WORD,
                lowercase=True,
            ),
        )

    def _create_payload_index(
        self,
        *,
        field_name: str,
        field_schema: models.PayloadSchemaType | models.TextIndexParams,
    ) -> None:
        try:
            self._client.create_payload_index(
                collection_name=self._collection_name,
                field_name=field_name,
                field_schema=field_schema,
            )
        except UnexpectedResponse as exc:
            message = str(exc).lower()
            if "already exists" not in message and "exist" not in message:
                raise

    def bm25_ready(self) -> bool:
        return is_bm25_ready(self._client, self._collection_name)

    def is_processed(self, accession_number: str) -> bool:
        points, _ = self._client.scroll(
            collection_name=self._collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="accession_number",
                        match=models.MatchValue(value=accession_number),
                    )
                ]
            ),
            limit=1,
            with_payload=False,
            with_vectors=False,
        )
        return len(points) > 0

    def upsert_filing(
        self,
        event: FilingDownloadedEvent,
        chunks: list[TextChunk],
        embeddings: list[list[float]],
    ) -> int:
        if len(chunks) != len(embeddings):
            raise ValueError("chunks and embeddings length mismatch")

        processed_at = datetime.now(timezone.utc).isoformat()
        self._client.delete(
            collection_name=self._collection_name,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="accession_number",
                            match=models.MatchValue(value=event.accession_number),
                        )
                    ]
                )
            ),
        )

        points: list[models.PointStruct] = []
        for chunk, embedding in zip(chunks, embeddings, strict=True):
            payload = {
                **chunk.metadata,
                "accession_number": event.accession_number,
                "chunk_index": chunk.chunk_index,
                "content": chunk.content,
                "ticker": event.ticker,
                "company_name": event.company_name,
                "form": event.form,
                "filing_date": event.filing_date.isoformat(),
                "local_path": event.local_path,
                "document_url": event.document_url,
                "downloaded_at": event.downloaded_at.isoformat(),
                "processed_at": processed_at,
                "chunk_count": len(chunks),
            }
            if chunk.section:
                payload["section"] = chunk.section

            points.append(
                models.PointStruct(
                    id=point_id_for_chunk(event.accession_number, chunk.chunk_index),
                    vector={
                        DENSE_VECTOR_NAME: embedding,
                        BM25_VECTOR_NAME: models.Document(
                            text=chunk.content,
                            model=BM25_MODEL,
                        ),
                    },
                    payload=payload,
                )
            )

        self._client.upsert(
            collection_name=self._collection_name,
            points=points,
        )
        return len(chunks)

    def truncate_collection(self, vector_size: int) -> None:
        if self._client.collection_exists(self._collection_name):
            self._client.delete_collection(self._collection_name)
        self.init_collection(vector_size)

    def count_points(self) -> int:
        info = self._client.get_collection(self._collection_name)
        return info.points_count or 0

    def collection_stats(self) -> tuple[int, int]:
        chunk_count = self.count_points()
        if chunk_count == 0:
            return 0, 0

        facet = self._client.facet(
            collection_name=self._collection_name,
            key="accession_number",
            limit=100_000,
        )
        filing_count = len(facet.hits)
        return filing_count, chunk_count
