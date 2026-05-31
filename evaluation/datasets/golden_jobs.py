"""
evaluation/datasets/golden_jobs.py — Synthetic job postings for evaluation.

20 jobs covering: strongly relevant, stretch, and not-relevant for each profile.
Job descriptions use exact skill terms to produce predictable scoring behavior.
"""
from datetime import datetime, timezone
from pipeline.models import Job


def _make_job(job_id: str, title: str, company: str, description: str, is_remote: bool = True) -> Job:
    job = Job(
        job_id=f"eval_job_{job_id}",
        title=title,
        company=company,
        location="Remote" if is_remote else "San Francisco, CA",
        description=description,
        url=f"https://eval.example.com/jobs/{job_id}",
        source="evaluation",
        posted_at=datetime.now(timezone.utc),
        fetched_at=datetime.now(timezone.utc),
    )
    job.score = 0.0
    job.is_remote = is_remote
    return job


GOLDEN_JOBS: dict = {
    # Strong for junior-python-backend
    "j001": _make_job("j001", "Python Backend Engineer", "TechFlow",
        "We are looking for a Python backend engineer to join our team. "
        "You will build and maintain REST APIs using FastAPI and PostgreSQL. "
        "Experience with Docker, Kubernetes, and microservices architecture is required. "
        "You will work with AWS for cloud infrastructure and Redis for caching. "
        "Backend development experience with Python is essential. "
        "We build distributed systems and event-driven microservices. "
        "Remote-friendly startup with strong engineering culture."),
    "j002": _make_job("j002", "Backend Developer - Python/FastAPI", "CloudBase",
        "Backend developer needed for our core API platform. "
        "Stack: Python, FastAPI, PostgreSQL, Docker, REST API design. "
        "We use AWS for hosting and Redis for session management. "
        "You will design and build microservices, integrate with third-party APIs, "
        "and work on our cloud-native backend infrastructure. "
        "Strong Python skills and experience with backend development required. "
        "Competitive salary, fully remote."),
    "j003": _make_job("j003", "Software Engineer - Backend", "DataStream",
        "Join our backend team building high-scale Python services. "
        "We use FastAPI, PostgreSQL, and Docker extensively. "
        "Our stack includes AWS Lambda for serverless compute, SQS for messaging, "
        "and Redis for caching. Strong Python and REST API knowledge required. "
        "Experience with microservices and distributed systems preferred. "
        "Backend engineering role with opportunity to grow into cloud architecture."),
    "j004": _make_job("j004", "API Engineer", "NovaSoft",
        "Building the next generation of REST APIs with Python and FastAPI. "
        "PostgreSQL for persistence, Docker for containerization, Kubernetes for orchestration. "
        "AWS infrastructure, microservices architecture, backend Python development. "
        "We need someone who knows Python, REST, API design, and backend systems inside and out. "
        "Fully remote position, startup culture, equity included."),
    "j005": _make_job("j005", "Python Microservices Engineer", "ScaleHub",
        "Python developer for our microservices platform. "
        "Core stack: Python, FastAPI, PostgreSQL, Docker, Kubernetes, AWS. "
        "You will build event-driven microservices, REST APIs, and cloud-native backend services. "
        "Redis for caching, messaging with SQS. "
        "Backend focus, Python expertise required, cloud experience valued."),
    # Stretch for junior-python-backend
    "j006": _make_job("j006", "Flask Backend Developer", "WebCore",
        "Backend developer role using Flask and SQLAlchemy for REST APIs. "
        "We build Python and Flask web services for our platform. "
        "PostgreSQL database, Docker deployment, some AWS usage. "
        "Python backend experience required, FastAPI or Django a plus. "
        "Microservices architecture, REST API design, backend Python developer."),
    "j007": _make_job("j007", "Django REST Framework Developer", "AppForge",
        "Python developer needed for Django-based REST API platform. "
        "Experience with Django REST Framework, PostgreSQL, Docker required. "
        "AWS deployment, Python backend development, microservices patterns. "
        "FastAPI experience helpful but not required. Backend Python focus."),
    "j008": _make_job("j008", "Backend Engineer - Go/Python", "Nexus",
        "Backend engineer for our polyglot microservices platform. "
        "Primary stack: Golang and Python. REST APIs, PostgreSQL, Docker. "
        "AWS cloud infrastructure, Kubernetes orchestration, backend focus. "
        "Python or Go experience, microservices background preferred."),
    # Strong for aws-cloud-engineer
    "j009": _make_job("j009", "AWS Cloud Engineer", "CloudFirst",
        "Cloud engineer role focused on AWS infrastructure automation. "
        "Terraform for infrastructure as code, Kubernetes on EKS, Docker containers. "
        "Experience with EC2, S3, Lambda, VPC, IAM, CloudWatch required. "
        "Ansible for configuration management, Python scripting for automation. "
        "Platform engineering, devops, cloud infrastructure. "
        "Prometheus monitoring, Grafana dashboards, Jenkins CI/CD pipeline. "
        "AWS certifications valued, fully remote."),
    "j010": _make_job("j010", "DevOps Engineer - Terraform/AWS", "InfraCore",
        "DevOps engineer to own our cloud infrastructure on AWS. "
        "Terraform for infrastructure as code, Ansible for configuration management. "
        "AWS services: EC2, EKS, S3, Lambda, RDS, CloudWatch. "
        "Kubernetes orchestration, Docker containerization, Jenkins CI/CD. "
        "Python scripting, Prometheus monitoring, Grafana dashboards. "
        "Platform engineer, SRE background, cloud infrastructure automation."),
    "j011": _make_job("j011", "Platform Engineer - Kubernetes/AWS", "CoreInfra",
        "Platform engineer to build and maintain our Kubernetes platform on AWS. "
        "EKS cluster management, Helm charts, Terraform IaC, Docker containers. "
        "AWS infrastructure: EC2, S3, RDS, Lambda. CI/CD with Jenkins, Ansible automation. "
        "Prometheus monitoring, Grafana dashboards, Python scripts for automation. "
        "SRE practices, cloud native infrastructure, devops culture."),
    # Stretch for aws-cloud-engineer (GCP/Azure instead of AWS)
    "j012": _make_job("j012", "GCP Cloud Engineer", "CloudShift",
        "Cloud engineer for Google Cloud Platform infrastructure. "
        "GKE Kubernetes, Terraform infrastructure as code, Docker, Helm, Ansible. "
        "GCP services: Compute Engine, Cloud Storage, Cloud Functions, BigQuery. "
        "CI/CD pipelines, monitoring, devops practices, Python automation. "
        "AWS experience helpful, GCP or Azure cloud background welcome."),
    "j013": _make_job("j013", "Azure DevOps Engineer", "SkyPlatform",
        "DevOps engineer managing Azure cloud infrastructure. "
        "Azure Kubernetes Service, Terraform IaC, Docker, Jenkins CI/CD. "
        "Infrastructure as code, Ansible configuration management, monitoring. "
        "Python scripting, cloud automation, devops engineering practices. "
        "AWS or GCP experience transferable, infrastructure focus."),
    # Strong for fullstack-developer
    "j014": _make_job("j014", "Full Stack Engineer - Python/React", "Buildify",
        "Full stack developer joining our product engineering team. "
        "Backend: Python, FastAPI, PostgreSQL, REST API design. "
        "Frontend: React, TypeScript, modern JavaScript. "
        "Docker deployment, AWS hosting, GraphQL API, Redis caching. "
        "Full stack engineer with both backend and frontend experience. "
        "Node.js experience a plus. Web developer who ships complete features."),
    "j015": _make_job("j015", "Software Engineer - Full Stack", "Creatify",
        "Full stack software engineer for our web platform. "
        "Python backend with REST APIs and PostgreSQL. React frontend with TypeScript. "
        "Node.js for some services, Docker containers, AWS cloud, GraphQL APIs. "
        "Redis caching, full stack web development, modern web applications. "
        "Competitive compensation, remote work, strong engineering team."),
    # Not relevant (excluded tech / wrong domain)
    "j016": _make_job("j016", "React Frontend Developer", "UIDesign Co",
        "Frontend developer with React expertise needed. "
        "React, TypeScript, CSS, HTML5, webpack, frontend development. "
        "UI/UX design implementation, component libraries, Figma integration. "
        "No backend work — pure frontend JavaScript focus. "
        "React Native mobile development is a bonus.", is_remote=False),
    "j017": _make_job("j017", "iOS Mobile Developer", "MobileFirst",
        "iOS developer for our consumer mobile application. "
        "Swift, SwiftUI, Xcode, iOS development, Apple platform guidelines. "
        "Mobile UI design, Core Data, Push notifications, App Store deployment. "
        "Objective-C knowledge helpful. Mobile engineer background required. "
        "iOS app development, no web backend work."),
    "j018": _make_job("j018", "WordPress Developer", "MediaSite",
        "WordPress developer for our content management system. "
        "PHP, WordPress, WooCommerce, Elementor, MySQL database. "
        "Theme development, plugin customization, PHP scripting for WordPress. "
        "No Python or cloud experience needed. WordPress and PHP focus only. "
        "Part-time contractor, project-based WordPress website work."),
    "j019": _make_job("j019", "Sales Engineer - Enterprise SaaS", "EnterpriseX",
        "Sales engineer for our enterprise B2B SaaS product. "
        "Customer demos, technical sales, pre-sales engineering, Salesforce CRM. "
        "No software development required — explain technical concepts to stakeholders. "
        "Sales quota, commissions, enterprise account management role. "
        "Communication skills, sales experience, customer-facing position."),
    "j020": _make_job("j020", "Principal ML Research Scientist", "DeepAI",
        "Principal ML research scientist for our AI research team. "
        "PhD required in machine learning, NLP, computer vision, or reinforcement learning. "
        "Research publications, PyTorch, TensorFlow, CUDA, GPU clusters, distributed training. "
        "Research background, academic publications, deep learning expertise required. "
        "Principal-level position, research team leadership responsibilities."),
}
