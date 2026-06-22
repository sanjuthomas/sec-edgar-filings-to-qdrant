from unittest.mock import ANY, patch

import pytest
from fastapi.testclient import TestClient

from edgar_etl.admin_api import create_admin_app
from edgar_etl.config import Settings
from edgar_etl.kafka_manager import KafkaConsumerManager


@pytest.fixture
def admin_client() -> TestClient:
    settings = Settings(qdrant_url="http://invalid:6333")
    manager = KafkaConsumerManager(settings)
    app = create_admin_app(settings, kafka_manager=manager)
    with TestClient(app) as client:
        client.app.state.test_manager = manager
        yield client


def test_admin_index_returns_html(admin_client: TestClient) -> None:
    response = admin_client.get("/")
    assert response.status_code == 200
    assert "SEC EDGAR ETL Admin" in response.text
    assert 'id="truncate-btn"' in response.text
    assert "Qdrant — BM25 keyword search" in response.text
    assert 'id="load-embedding-backend"' in response.text
    assert "Select backend" in response.text
    assert 'id="embedding-backend-card"' not in response.text


@patch("edgar_etl.admin_api.get_qdrant_search_config")
@patch("edgar_etl.admin_api.get_collection_schema")
@patch("edgar_etl.admin_api.get_connectivity")
@patch("edgar_etl.admin_api.FilingStore")
def test_admin_status(
    mock_store_cls,
    mock_connectivity,
    mock_schema,
    mock_qdrant_search,
    admin_client: TestClient,
) -> None:
    from edgar_etl.admin_service import QdrantSearchConfigInfo, SchemaColumnInfo, SchemaTableInfo
    from edgar_etl.connectivity import ServiceStatus

    mock_store_cls.return_value.collection_stats.return_value = (5, 100)
    mock_store_cls.return_value.bm25_ready.return_value = True

    mock_qdrant_search.return_value = QdrantSearchConfigInfo(
        engine="Qdrant",
        collection="filing_chunks",
        dense_vector="dense",
        sparse_vector="content-bm25",
        bm25_model="Qdrant/bm25",
        text_index_field="content",
        bm25_ready=True,
        indexed_chunks=100,
        rank_metric="BM25 score",
        example_query="client.query_points(...)",
        docker_image="qdrant/qdrant:v1.18.2",
    )

    mock_connectivity.return_value = [
        ServiceStatus("qdrant", True, "connected"),
        ServiceStatus("qdrant_bm25", True, "BM25 ready"),
        ServiceStatus("mongodb", True, "connected"),
        ServiceStatus("kafka", True, "connected"),
        ServiceStatus("kafka_consumer", True, "ready"),
    ]
    mock_schema.return_value = [
        SchemaTableInfo(
            name="filing_chunks",
            columns=[
                SchemaColumnInfo(
                    name="dense",
                    type="vector(1024)",
                    nullable=False,
                    notes="Dense vector",
                )
            ],
            relationships=["One point per chunk"],
        )
    ]

    response = admin_client.get("/api/admin/status")
    assert response.status_code == 200
    data = response.json()
    assert data["filing_count"] == 5
    assert data["chunk_count"] == 100
    assert data["bm25_indexed_chunks"] == 100
    assert data["bm25_ready"] is True
    assert data["kafka"]["state"] == "stopped"
    assert data["embedding"]["model"] == "BAAI/bge-m3"
    assert data["embedding"]["backend"] == "embedded"
    assert data["qdrant_search"]["engine"] == "Qdrant"
    assert data["qdrant_search"]["sparse_vector"] == "content-bm25"
    assert data["collection_schema"][0]["name"] == "filing_chunks"
    assert len(data["pipeline"]) >= 3


@patch("edgar_etl.admin_api.update_embedding_backend")
def test_admin_set_embedding_backend(mock_update, admin_client: TestClient) -> None:
    from edgar_etl.admin_service import EmbeddingConfigInfo

    mock_update.return_value = EmbeddingConfigInfo(
        backend="ollama",
        backend_label="Ollama — BGE-M3",
        model="bge-m3",
        device="host GPU via Ollama",
        dimensions=1024,
        max_seq_length=512,
        batch_size=16,
        similarity="cosine",
        library="Ollama /api/embed",
        query_prompt=None,
        ollama_base_url="http://host.docker.internal:11434",
    )

    response = admin_client.post(
        "/api/admin/embedding-backend",
        json={"backend": "ollama"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["embedding"]["backend"] == "ollama"
    assert "Ollama" in data["message"]
    mock_update.assert_called_once_with(ANY, "ollama")


@patch("edgar_etl.admin_api.truncate_collection", return_value="Truncated collection filing_chunks")
def test_admin_truncate(mock_truncate, admin_client: TestClient) -> None:
    response = admin_client.post(
        "/api/admin/truncate",
        json={"table": "filing_chunks"},
    )
    assert response.status_code == 200
    assert response.json()["table"] == "filing_chunks"
    mock_truncate.assert_called_once()


@patch("edgar_etl.admin_api.load_ticker")
@patch("edgar_etl.admin_api.update_embedding_backend")
def test_admin_load_ticker(mock_update_backend, mock_load, admin_client: TestClient) -> None:
    from edgar_etl.admin_service import TickerLoadResult

    mock_load.return_value = TickerLoadResult(
        ticker="KKR",
        found=2,
        processed=2,
        skipped=0,
        failed=0,
        total_chunks=400,
        errors=[],
    )

    response = admin_client.post(
        "/api/admin/load-ticker",
        json={"ticker": "kkr", "backend": "embedded"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ticker"] == "KKR"
    assert data["total_chunks"] == 400
    mock_update_backend.assert_called_once_with(ANY, "embedded")
    mock_load.assert_called_once_with(ANY, "kkr", forms=None)


def test_admin_kafka_start_and_stop(admin_client: TestClient) -> None:
    manager: KafkaConsumerManager = admin_client.app.state.test_manager

    with patch.object(manager, "start") as mock_start, patch.object(
        manager,
        "status",
        return_value={
            "state": "running",
            "offset_mode": "earliest",
            "topic": "filings",
            "group_id": "edgar-qdrant-etl",
            "last_error": None,
        },
    ):
        response = admin_client.post(
            "/api/admin/kafka/start",
            json={"offset": "earliest"},
        )
        assert response.status_code == 200
        mock_start.assert_called_once_with("earliest")

    with patch.object(manager, "stop") as mock_stop, patch.object(
        manager,
        "status",
        return_value={
            "state": "stopped",
            "offset_mode": None,
            "topic": "filings",
            "group_id": "edgar-qdrant-etl",
            "last_error": None,
        },
    ):
        response = admin_client.post("/api/admin/kafka/stop")
        assert response.status_code == 200
        mock_stop.assert_called_once()
