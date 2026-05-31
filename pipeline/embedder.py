"""
pipeline/embedder.py — Embedding provider abstraction and text construction.

EmbeddingProvider Protocol: BGEEmbeddingProvider (production, lazy-loaded) and
MockEmbeddingProvider (deterministic, no torch dependency, used in tests).

All embeddings are 1024-dimensional float lists.
"""
from __future__ import annotations

import hashlib
import math
from typing import TYPE_CHECKING, List, Protocol, runtime_checkable

from config.settings import settings
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)

_MAX_JOB_DESC_CHARS = 1500
_MAX_EXPERIENCE_ITEMS = 5


@runtime_checkable
class EmbeddingProvider(Protocol):
    """Structural interface for all embedding backends."""
    dimensions: int

    def embed(self, text: str) -> List[float]: ...
    def embed_batch(self, texts: List[str]) -> List[List[float]]: ...


class MockEmbeddingProvider:
    """
    Deterministic embedding provider for tests.
    Uses SHA-256 hash of text to seed reproducible 1024-dim float vectors.
    Empty text → zero vector. Same text always → same vector.
    """
    dimensions: int = 1024

    def embed(self, text: str) -> List[float]:
        if not text or not text.strip():
            return [0.0] * self.dimensions
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        values: List[float] = []
        for i in range(self.dimensions):
            byte_val = digest[i % len(digest)]
            mixed = (byte_val ^ (i * 37 & 0xFF)) / 255.0
            values.append(math.sin(mixed * math.pi))
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        return [self.embed(t) for t in texts]


class BGEEmbeddingProvider:
    """
    Production embedding provider using BAAI/bge-large-en-v1.5 via sentence-transformers.
    Lazy-loads the model on first use (~2s). Model cached in memory for process lifetime.
    Requires: pip install sentence-transformers torch
    """
    dimensions: int = 1024

    def __init__(self, model_name: str | None = None) -> None:
        self._model_name = model_name or settings.EMBEDDING_MODEL
        self._model = None

    def _get_model(self):
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                logger.info(
                    f"[EMBEDDER] Loading model: {self._model_name}",
                    extra={"component": "EMBEDDER", "event": "model_load",
                           "meta": {"model": self._model_name}}
                )
                self._model = SentenceTransformer(self._model_name)
                logger.info("[EMBEDDER] Model loaded.",
                            extra={"component": "EMBEDDER", "event": "model_ready"})
            except ImportError as e:
                raise RuntimeError(
                    f"sentence-transformers not installed. "
                    f"Run: pip install sentence-transformers torch\nOriginal: {e}"
                ) from e
        return self._model

    def embed(self, text: str) -> List[float]:
        if not text or not text.strip():
            return [0.0] * self.dimensions
        return self._get_model().encode(text, normalize_embeddings=True).tolist()

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        return [
            e.tolist()
            for e in self._get_model().encode(
                texts,
                batch_size=settings.EMBEDDING_BATCH_SIZE,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
        ]


_provider_singleton: EmbeddingProvider | None = None


def get_embedding_provider() -> EmbeddingProvider:
    """Return the configured embedding provider (singleton per process)."""
    global _provider_singleton
    if _provider_singleton is None:
        import config.settings as _settings_module
        _current_settings = _settings_module.settings
        if _current_settings.EMBEDDING_PROVIDER == "mock":
            _provider_singleton = MockEmbeddingProvider()
        else:
            _provider_singleton = BGEEmbeddingProvider()
    return _provider_singleton


def reset_provider() -> None:
    """Reset the singleton (used in tests to switch providers)."""
    global _provider_singleton
    _provider_singleton = None


def build_job_text(job) -> str:
    """Construct canonical text for embedding a job (title + company + description)."""
    title = getattr(job, "title", "") or ""
    company = getattr(job, "company", "") or ""
    description = (getattr(job, "description", "") or "")[:_MAX_JOB_DESC_CHARS]
    return f"{title} at {company}. {description}".strip()


def build_profile_text(profile: dict) -> str:
    """Construct canonical text for embedding a user profile (skills + experience + role)."""
    if not profile:
        return ""
    all_skills = profile.get("all_skills") or profile.get("core_skills", [])
    experience = (profile.get("experience") or [])[:_MAX_EXPERIENCE_ITEMS]
    role = profile.get("role", "")

    parts = []
    if all_skills:
        parts.append(f"Skills: {', '.join(all_skills[:50])}")
    if experience:
        parts.append(f"Experience: {'. '.join(experience)}")
    if role:
        parts.append(f"Role: {role}")
    return ". ".join(parts)


def embed_text(text: str, provider: EmbeddingProvider) -> List[float]:
    """Convenience wrapper: embed a single text with the given provider."""
    return provider.embed(text)
