"""Qdrant full-text / BM25 helpers for filing_chunks keyword search."""

from __future__ import annotations

from qdrant_client import QdrantClient

DENSE_VECTOR_NAME = "dense"
BM25_VECTOR_NAME = "content-bm25"
BM25_MODEL = "Qdrant/bm25"
TEXT_INDEX_FIELD = "content"


def is_bm25_ready(client: QdrantClient, collection_name: str) -> bool:
    if not client.collection_exists(collection_name):
        return False

    info = client.get_collection(collection_name)
    sparse_vectors = info.config.params.sparse_vectors
    if sparse_vectors is None:
        return False

    sparse_map = sparse_vectors if isinstance(sparse_vectors, dict) else {}
    return BM25_VECTOR_NAME in sparse_map
