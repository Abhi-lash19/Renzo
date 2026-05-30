from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.endpoints import auth, feedback, health, jobs, profile
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
    allow_origins=["*"],  # TODO: restrict to allowed origins in Phase 9 (SaaS layer)
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/v1", tags=["health"])
app.include_router(jobs.router, prefix="/v1", tags=["jobs"])
app.include_router(feedback.router, prefix="/v1", tags=["feedback"])
app.include_router(auth.router, prefix="/v1")
app.include_router(profile.router, prefix="/v1")
