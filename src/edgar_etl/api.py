from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from qdrant_client.http.exceptions import UnexpectedResponse

from edgar_etl.config import Settings
from edgar_etl.embed import get_embedding_model, should_preload_embedding_model
from edgar_etl.embedding_runtime import get_embedding_backend
from edgar_etl.query import SearchResult, TextSearchResult, search_filings, search_filings_text
from edgar_etl.store import FilingStore

_STATIC_DIR = Path(__file__).resolve().parent / "static"


class SearchResultOut(BaseModel):
    content: str
    distance: float
    similarity: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


class TextSearchResultOut(BaseModel):
    content: str
    rank: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


class SearchResponse(BaseModel):
    query: str
    mode: str
    top_k: int
    count: int
    results: list[SearchResultOut | TextSearchResultOut]


class StatsResponse(BaseModel):
    filing_count: int
    chunk_count: int
    bm25_ready: bool
    embedding_backend: str
    embedding_model: str


def _to_result_out(result: SearchResult) -> SearchResultOut:
    similarity = round(result.score, 4)
    return SearchResultOut(
        content=result.content,
        distance=round(1.0 - result.score, 4),
        similarity=similarity,
        accession_number=result.accession_number,
        chunk_index=result.chunk_index,
        metadata=result.metadata,
    )


def _to_text_result_out(result: TextSearchResult) -> TextSearchResultOut:
    return TextSearchResultOut(
        content=result.content,
        rank=round(result.rank, 4),
        accession_number=result.accession_number,
        chunk_index=result.chunk_index,
        metadata=result.metadata,
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if should_preload_embedding_model(app_settings):
            get_embedding_model(
                app_settings.embedding_model,
                app_settings.embedding_device,
                app_settings.embedding_max_seq_length,
            )
        yield

    app = FastAPI(
        title="SEC EDGAR Semantic Search",
        description="Verify Qdrant data with semantic and keyword search over filing chunks.",
        lifespan=lifespan,
    )

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return (_STATIC_DIR / "index.html").read_text(encoding="utf-8")

    @app.get("/api/stats", response_model=StatsResponse)
    def stats() -> StatsResponse:
        try:
            store = FilingStore(app_settings.qdrant_url, app_settings.qdrant_collection)
            filing_count, chunk_count = store.collection_stats()
            bm25_ready = store.bm25_ready()
        except UnexpectedResponse as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Qdrant unavailable: {exc}",
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Qdrant unavailable: {exc}",
            ) from exc
        return StatsResponse(
            filing_count=filing_count,
            chunk_count=chunk_count,
            bm25_ready=bm25_ready,
            embedding_backend=get_embedding_backend(app_settings),
            embedding_model=(
                app_settings.ollama_embedding_model
                if get_embedding_backend(app_settings) == "ollama"
                else app_settings.embedding_model
            ),
        )

    @app.get("/api/search", response_model=SearchResponse)
    def search(
        q: str = Query(..., min_length=1, description="Search term or question"),
        top_k: int = Query(10, ge=1, le=100, description="Number of chunks to return"),
        ticker: str | None = Query(None, description="Filter by ticker, e.g. AEE"),
        form: str | None = Query(None, description="Filter by form, e.g. 10-Q"),
        mode: str = Query(
            "semantic",
            description="Search mode: semantic (dense vectors) or keyword (BM25)",
        ),
    ) -> SearchResponse:
        query = q.strip()
        if not query:
            raise HTTPException(status_code=400, detail="Query cannot be empty")

        search_mode = mode.strip().lower()
        if search_mode not in {"semantic", "keyword"}:
            raise HTTPException(
                status_code=400,
                detail="mode must be 'semantic' or 'keyword'",
            )

        try:
            if search_mode == "keyword":
                text_results = search_filings_text(
                    query,
                    app_settings,
                    top_k=top_k,
                    ticker=ticker,
                    form=form,
                )
                return SearchResponse(
                    query=query,
                    mode=search_mode,
                    top_k=top_k,
                    count=len(text_results),
                    results=[_to_text_result_out(result) for result in text_results],
                )

            results = search_filings(
                query,
                app_settings,
                top_k=top_k,
                ticker=ticker,
                form=form,
            )
        except UnexpectedResponse as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Qdrant unavailable: {exc}",
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Search failed: {exc}",
            ) from exc

        return SearchResponse(
            query=query,
            mode=search_mode,
            top_k=top_k,
            count=len(results),
            results=[_to_result_out(result) for result in results],
        )

    return app
