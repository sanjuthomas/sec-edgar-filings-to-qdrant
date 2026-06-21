from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from edgar_etl.api import create_app
from edgar_etl.config import Settings
from edgar_etl.query import SearchResult, TextSearchResult


@pytest.fixture
def client() -> TestClient:
    with patch("edgar_etl.api.get_embedding_model"):
        app = create_app(Settings(qdrant_url="http://invalid:6333"))
        with TestClient(app) as test_client:
            yield test_client


def test_index_returns_html(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "SEC EDGAR Semantic Search" in response.text
    assert 'id="search-form"' in response.text
    assert "Qdrant" in response.text
    assert 'id="mode"' in response.text
    assert 'href="http://localhost:8001/"' in response.text


@patch("edgar_etl.api.get_embedding_backend", return_value="embedded")
@patch("edgar_etl.api.FilingStore")
def test_stats(mock_store_cls, _mock_backend, client: TestClient) -> None:
    mock_store_cls.return_value.collection_stats.return_value = (42, 1000)
    mock_store_cls.return_value.bm25_ready.return_value = True

    response = client.get("/api/stats")
    assert response.status_code == 200
    assert response.json() == {
        "filing_count": 42,
        "chunk_count": 1000,
        "bm25_ready": True,
        "embedding_backend": "embedded",
        "embedding_model": "BAAI/bge-m3",
    }


@patch("edgar_etl.api.search_filings")
def test_search_default_top_k(mock_search, client: TestClient) -> None:
    mock_search.return_value = [
        SearchResult(
            content="Revenue increased 12%.",
            score=0.69,
            accession_number="0001104659-26-063184",
            chunk_index=2,
            metadata={"ticker": "AEE", "form": "10-Q"},
        )
    ]

    response = client.get("/api/search", params={"q": "revenue growth"})
    assert response.status_code == 200
    data = response.json()
    assert data["query"] == "revenue growth"
    assert data["mode"] == "semantic"
    assert data["top_k"] == 10
    assert data["count"] == 1
    assert data["results"][0]["similarity"] == 0.69
    assert data["results"][0]["distance"] == 0.31
    mock_search.assert_called_once()
    assert mock_search.call_args.kwargs["top_k"] == 10


@patch("edgar_etl.api.search_filings")
def test_search_custom_top_k_and_filters(mock_search, client: TestClient) -> None:
    mock_search.return_value = []

    response = client.get(
        "/api/search",
        params={"q": "directors", "top_k": 25, "ticker": "aee", "form": "8-k"},
    )
    assert response.status_code == 200
    assert response.json()["top_k"] == 25
    mock_search.assert_called_once()
    kwargs = mock_search.call_args.kwargs
    assert kwargs["top_k"] == 25
    assert kwargs["ticker"] == "aee"
    assert kwargs["form"] == "8-k"


@patch("edgar_etl.api.search_filings_text")
def test_search_keyword_mode(mock_search_text, client: TestClient) -> None:
    mock_search_text.return_value = [
        TextSearchResult(
            content="Director election results.",
            rank=12.5,
            accession_number="0001104659-26-063184",
            chunk_index=0,
            metadata={"ticker": "AEE", "form": "8-K"},
        )
    ]

    response = client.get(
        "/api/search",
        params={"q": "director election", "mode": "keyword"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["mode"] == "keyword"
    assert data["results"][0]["rank"] == 12.5
    assert "similarity" not in data["results"][0]
    mock_search_text.assert_called_once()


def test_search_rejects_invalid_mode(client: TestClient) -> None:
    response = client.get("/api/search", params={"q": "test", "mode": "invalid"})
    assert response.status_code == 400


def test_search_rejects_empty_query(client: TestClient) -> None:
    response = client.get("/api/search", params={"q": "   "})
    assert response.status_code == 400
