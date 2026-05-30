from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.endpoints import health
from storage.db import init_db
from storage.db_manager import db_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield
    db_manager.shutdown()


app = FastAPI(
    title="Renzo Job Intelligence API",
    version="0.1.0",
    description="Personal job intelligence engine — REST API layer",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/v1", tags=["health"])
