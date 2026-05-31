"""
evaluation/datasets/golden_profiles.py — Synthetic user profiles for evaluation.

Each profile is a dict in the same format as utils/profile_loader.load_profile().
"""
from typing import Dict, Any


def _make_profile(
    name: str,
    role: str,
    core_skills: list,
    secondary_skills: list,
    target_roles: list,
    preferred_keywords: list,
    exclude_keywords: list,
    cloud: list = None,
    devops: list = None,
) -> Dict[str, Any]:
    cloud = cloud or []
    devops = devops or []
    all_skills = list(dict.fromkeys(core_skills + secondary_skills + cloud + devops))
    weighted_skills = {s: 1.0 for s in core_skills}
    weighted_skills.update({s: 0.6 for s in secondary_skills if s not in weighted_skills})
    weighted_skills.update({s: 0.5 for s in preferred_keywords if s not in weighted_skills})
    return {
        "name": name,
        "role": role,
        "experience_level": "2-5 years",
        "core_skills": core_skills,
        "secondary_skills": secondary_skills,
        "all_skills": all_skills,
        "weighted_skills": weighted_skills,
        "cloud": cloud,
        "devops": devops,
        "preferred_roles": target_roles,
        "target_roles": target_roles,
        "preferred_keywords": preferred_keywords,
        "exclude_keywords": exclude_keywords,
        "bonus_keywords": ["startup", "remote", "saas", "cloud native"],
        "projects": ["api platform", "microservices", "automation"],
        "experience": ["backend development", "rest api", "cloud infrastructure"],
        "location": "remote",
        "remote_preferred": True,
        "is_empty": False,
        "source": "evaluation",
    }


GOLDEN_PROFILES: Dict[str, Dict[str, Any]] = {
    "junior-python-backend": _make_profile(
        name="Alex Chen",
        role="Backend Developer",
        core_skills=["python", "fastapi", "postgresql", "docker", "rest"],
        secondary_skills=["kubernetes", "redis", "aws", "git"],
        target_roles=["backend developer", "python developer", "software engineer", "backend engineer"],
        preferred_keywords=["backend", "api", "microservices", "cloud", "rest", "serverless"],
        exclude_keywords=["frontend", "react", "angular", "mobile", "ios", "android", "wordpress", "php"],
        cloud=["aws", "s3", "ec2"],
        devops=["docker", "kubernetes"],
    ),
    "aws-cloud-engineer": _make_profile(
        name="Jordan Kim",
        role="Cloud Engineer",
        core_skills=["aws", "terraform", "kubernetes", "docker", "python"],
        secondary_skills=["jenkins", "ansible", "postgresql", "prometheus", "grafana"],
        target_roles=["cloud engineer", "devops engineer", "platform engineer", "sre", "infrastructure engineer"],
        preferred_keywords=["cloud", "infrastructure", "devops", "kubernetes", "aws", "iac", "cicd"],
        exclude_keywords=["frontend", "mobile", "ios", "android", "sales", "marketing", "react"],
        cloud=["aws", "gcp", "azure", "eks", "ec2", "s3", "lambda"],
        devops=["terraform", "ansible", "jenkins", "kubernetes", "docker", "cicd"],
    ),
    "fullstack-developer": _make_profile(
        name="Sam Rivera",
        role="Full Stack Developer",
        core_skills=["python", "react", "postgresql", "nodejs", "rest"],
        secondary_skills=["docker", "aws", "graphql", "redis", "typescript"],
        target_roles=["full stack developer", "software engineer", "web developer", "fullstack engineer"],
        preferred_keywords=["backend", "frontend", "api", "web", "fullstack", "cloud"],
        exclude_keywords=["mobile", "ios", "android", "sales", "lead", "principal", "wordpress"],
        cloud=["aws", "s3"],
        devops=["docker", "cicd"],
    ),
}
