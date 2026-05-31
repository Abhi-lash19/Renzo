"""
intelligence/skill_adjacency.py — Skill adjacency graph for transferable skills.

SKILL_GRAPH maps each canonical skill to its adjacent skills with a confidence weight.
Direction: PROFILE→JOB — "knowing A makes you relevant for a job requiring B with confidence X."

Confidence scale:
  0.9–1.0: near-identical (different name, same concept)
  0.7–0.9: same domain, highly overlapping (FastAPI→Flask)
  0.5–0.7: transferable with moderate learning curve (AWS→GCP)
  < 0.5:   not included (gap is too large)

All keys and values MUST be lowercase (same normalization as normalize_skill()).
"""
from __future__ import annotations

from typing import List, Tuple

SKILL_GRAPH: dict[str, dict[str, float]] = {
    # ── Python web frameworks ────────────────────────────────────────────────
    "fastapi": {"flask": 0.9, "starlette": 0.95, "django": 0.75, "express": 0.55, "nestjs": 0.5},
    "django": {"flask": 0.85, "fastapi": 0.8, "rails": 0.5},
    "flask": {"fastapi": 0.85, "django": 0.8, "express": 0.55},
    "starlette": {"fastapi": 0.95, "flask": 0.8, "django": 0.65},
    # ── Node / JS frameworks ─────────────────────────────────────────────────
    "express": {"nestjs": 0.8, "fastapi": 0.55, "flask": 0.55, "nodejs": 0.9},
    "nestjs": {"express": 0.8, "fastapi": 0.5, "nodejs": 0.9, "typescript": 0.85},
    # ── Languages ────────────────────────────────────────────────────────────
    "python": {"golang": 0.5},
    "golang": {"python": 0.5, "rust": 0.5},
    "nodejs": {"express": 0.92, "nestjs": 0.85, "typescript": 0.85, "javascript": 0.95},
    "typescript": {"javascript": 0.95, "nodejs": 0.8},
    "javascript": {"typescript": 0.9, "nodejs": 0.75},
    "ruby": {"rails": 0.92, "python": 0.5},
    "rails": {"django": 0.55, "ruby": 0.92, "laravel": 0.5},
    "laravel": {"rails": 0.55, "django": 0.5, "php": 0.9},
    "php": {"laravel": 0.85},
    # ── Cloud providers ──────────────────────────────────────────────────────
    "aws": {"gcp": 0.75, "azure": 0.7, "cloud": 0.9},
    "gcp": {"aws": 0.75, "azure": 0.7, "cloud": 0.9},
    "azure": {"aws": 0.7, "gcp": 0.7, "cloud": 0.9},
    # ── AWS services ─────────────────────────────────────────────────────────
    "lambda": {"cloud functions": 0.85, "azure functions": 0.8, "serverless": 0.9},
    "s3": {"gcs": 0.85, "azure blob storage": 0.8, "object storage": 0.9},
    "ec2": {"gce": 0.8, "azure vm": 0.75, "compute engine": 0.75},
    "sqs": {"sns": 0.8, "kafka": 0.65, "rabbitmq": 0.6, "pub sub": 0.7, "azure service bus": 0.7},
    "sns": {"sqs": 0.8, "pub sub": 0.7, "kafka": 0.6},
    "eks": {"kubernetes": 0.9, "gke": 0.85, "aks": 0.85, "ecs": 0.7},
    "ecs": {"kubernetes": 0.7, "eks": 0.7, "docker": 0.8},
    "dynamodb": {"mongodb": 0.65, "nosql": 0.9, "documentdb": 0.75, "cosmosdb": 0.65},
    "cloudwatch": {"datadog": 0.7, "prometheus": 0.65, "grafana": 0.6, "monitoring": 0.9},
    "kinesis": {"kafka": 0.7, "sqs": 0.65, "event streaming": 0.85},
    "elasticache": {"redis": 0.85, "memcached": 0.75},
    "aurora": {"postgresql": 0.85, "mysql": 0.8, "sql": 0.85},
    # ── GCP services ─────────────────────────────────────────────────────────
    "gke": {"kubernetes": 0.9, "eks": 0.85, "aks": 0.85},
    "gce": {"ec2": 0.8, "azure vm": 0.7},
    "gcs": {"s3": 0.85, "azure blob storage": 0.75},
    "cloud functions": {"lambda": 0.85, "azure functions": 0.8, "serverless": 0.9},
    "pub sub": {"kafka": 0.7, "sqs": 0.7, "sns": 0.65},
    # ── Azure services ───────────────────────────────────────────────────────
    "aks": {"kubernetes": 0.9, "eks": 0.85, "gke": 0.85},
    "azure functions": {"lambda": 0.8, "cloud functions": 0.8, "serverless": 0.9},
    "cosmosdb": {"dynamodb": 0.65, "mongodb": 0.65, "nosql": 0.9},
    # ── Containers & orchestration ───────────────────────────────────────────
    "docker": {"podman": 0.85, "kubernetes": 0.65, "containerd": 0.75, "ecs": 0.6},
    "kubernetes": {"eks": 0.9, "gke": 0.85, "aks": 0.85, "helm": 0.8, "ecs": 0.65, "openshift": 0.65, "docker": 0.7},
    "helm": {"kubernetes": 0.85, "kustomize": 0.75},
    "kustomize": {"helm": 0.75, "kubernetes": 0.7},
    "podman": {"docker": 0.85, "kubernetes": 0.6},
    "containerd": {"docker": 0.8, "kubernetes": 0.65},
    "openshift": {"kubernetes": 0.75, "eks": 0.65},
    # ── Relational databases ─────────────────────────────────────────────────
    "postgresql": {"mysql": 0.85, "mariadb": 0.85, "sql": 0.95, "sqlite": 0.8, "aurora": 0.8, "cockroachdb": 0.7},
    "mysql": {"postgresql": 0.85, "mariadb": 0.92, "sql": 0.95, "sqlite": 0.8, "aurora": 0.8},
    "mariadb": {"mysql": 0.92, "postgresql": 0.82, "sql": 0.9},
    "sql": {"postgresql": 0.85, "mysql": 0.85, "sqlite": 0.8},
    "sqlite": {"postgresql": 0.75, "mysql": 0.75, "sql": 0.85},
    "cockroachdb": {"postgresql": 0.8, "sql": 0.85},
    # ── NoSQL / document stores ──────────────────────────────────────────────
    "mongodb": {"dynamodb": 0.6, "couchdb": 0.7, "firestore": 0.65, "nosql": 0.9, "documentdb": 0.75},
    "documentdb": {"mongodb": 0.85, "dynamodb": 0.65, "nosql": 0.85},
    "firestore": {"mongodb": 0.65, "dynamodb": 0.6, "nosql": 0.85},
    "couchdb": {"mongodb": 0.7, "nosql": 0.85},
    # ── Caching ──────────────────────────────────────────────────────────────
    "redis": {"memcached": 0.75, "elasticache": 0.85, "valkey": 0.9},
    "memcached": {"redis": 0.75, "elasticache": 0.7},
    "valkey": {"redis": 0.9},
    # ── Search ───────────────────────────────────────────────────────────────
    "elasticsearch": {"opensearch": 0.9, "solr": 0.65, "search": 0.85},
    "opensearch": {"elasticsearch": 0.9, "solr": 0.6},
    "solr": {"elasticsearch": 0.65, "opensearch": 0.6},
    # ── Messaging / event streaming ──────────────────────────────────────────
    "kafka": {"rabbitmq": 0.65, "sqs": 0.65, "pub sub": 0.7, "kinesis": 0.7, "event streaming": 0.9, "activemq": 0.65},
    "rabbitmq": {"kafka": 0.65, "sqs": 0.6, "celery": 0.7, "messaging": 0.85, "activemq": 0.75},
    "activemq": {"rabbitmq": 0.75, "kafka": 0.65, "messaging": 0.85},
    "celery": {"rabbitmq": 0.7, "rq": 0.75, "task queue": 0.9},
    "rq": {"celery": 0.75, "task queue": 0.85},
    # ── IaC / DevOps ─────────────────────────────────────────────────────────
    "terraform": {"pulumi": 0.75, "cloudformation": 0.7, "ansible": 0.55, "iac": 0.95},
    "pulumi": {"terraform": 0.75, "cloudformation": 0.65, "iac": 0.9},
    "ansible": {"puppet": 0.65, "chef": 0.6, "saltstack": 0.6, "configuration management": 0.9},
    "puppet": {"ansible": 0.65, "chef": 0.7, "saltstack": 0.6},
    "chef": {"ansible": 0.65, "puppet": 0.7},
    "github actions": {"gitlab ci": 0.85, "jenkins": 0.75, "circleci": 0.8, "cicd": 0.95},
    "gitlab ci": {"github actions": 0.85, "jenkins": 0.75, "cicd": 0.95},
    "jenkins": {"github actions": 0.7, "gitlab ci": 0.7, "circleci": 0.7, "cicd": 0.9},
    "circleci": {"github actions": 0.8, "gitlab ci": 0.75, "cicd": 0.9},
    # ── API styles ───────────────────────────────────────────────────────────
    "rest": {"graphql": 0.6, "grpc": 0.55, "api": 0.9, "openapi": 0.75},
    "graphql": {"rest": 0.65, "api": 0.85, "apollo": 0.8},
    "grpc": {"rest": 0.6, "protobuf": 0.9, "api": 0.8},
    # ── Monitoring / observability ───────────────────────────────────────────
    "datadog": {"prometheus": 0.7, "grafana": 0.65, "cloudwatch": 0.65, "newrelic": 0.65, "monitoring": 0.9},
    "prometheus": {"grafana": 0.9, "alertmanager": 0.85, "monitoring": 0.9, "datadog": 0.6},
    "grafana": {"prometheus": 0.85, "kibana": 0.7, "datadog": 0.6, "observability": 0.85},
    "alertmanager": {"prometheus": 0.85, "monitoring": 0.85},
    "kibana": {"grafana": 0.7, "elasticsearch": 0.85},
    "newrelic": {"datadog": 0.7, "monitoring": 0.85},
    # ── Serverless ───────────────────────────────────────────────────────────
    "serverless": {"lambda": 0.9, "cloud functions": 0.85, "azure functions": 0.8, "faas": 0.9},
    # ── Architecture patterns ────────────────────────────────────────────────
    "microservices": {"service mesh": 0.7, "api gateway": 0.75, "event driven": 0.75, "distributed systems": 0.8},
    "event driven": {"microservices": 0.75, "kafka": 0.7, "messaging": 0.8},
    "distributed systems": {"microservices": 0.75, "kafka": 0.6},
    # ── Spring / Java ────────────────────────────────────────────────────────
    "spring boot": {"django": 0.5, "fastapi": 0.5, "express": 0.5},
}

DEFAULT_MIN_CONFIDENCE: float = 0.5


def get_adjacent_skills(
    skill: str,
    min_confidence: float = 0.0,
) -> List[Tuple[str, float]]:
    """
    Return all skills adjacent to the given skill, sorted by confidence descending.

    Args:
        skill:          Canonical skill string (normalized, lowercase).
        min_confidence: Only return adjacencies with confidence >= this value.

    Returns:
        List of (adjacent_skill, confidence) tuples sorted by confidence descending.
    """
    if not skill:
        return []
    adjacencies = SKILL_GRAPH.get(skill, {})
    results = [(adj, conf) for adj, conf in adjacencies.items() if conf >= min_confidence]
    return sorted(results, key=lambda x: (-x[1], x[0]))


def get_transferable_skills(
    profile_skills: List[str],
    job_required_skills: List[str],
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> List[Tuple[str, str, float]]:
    """
    Identify skills the user can transfer from their profile to a job's requirements.

    For each profile skill P, checks if any job-required skill J is adjacent in
    SKILL_GRAPH[P] with confidence >= min_confidence.

    Direction is PROFILE→JOB: knowing A makes you relevant for a job requiring B.

    Args:
        profile_skills:      Canonical skills the user has (from matched_skills).
        job_required_skills: Canonical skills the job requires (from missing_skills).
        min_confidence:      Minimum adjacency confidence to qualify as transferable.

    Returns:
        Sorted, deduplicated list of (profile_skill, job_skill, confidence) tuples.
    """
    if not profile_skills or not job_required_skills:
        return []

    job_skill_set = set(job_required_skills)
    seen: set[tuple[str, str]] = set()
    results: List[Tuple[str, str, float]] = []

    for profile_skill in profile_skills:
        adjacencies = SKILL_GRAPH.get(profile_skill, {})
        for job_skill, confidence in adjacencies.items():
            if (
                job_skill in job_skill_set
                and confidence >= min_confidence
                and (profile_skill, job_skill) not in seen
            ):
                seen.add((profile_skill, job_skill))
                results.append((profile_skill, job_skill, confidence))

    return sorted(results, key=lambda t: (t[0], t[1]))
