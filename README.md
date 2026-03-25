# Renzo

A personal system I built to **automate and optimize my job search** using structured data, scoring, and intelligent insights.

---

## Why I Built This

Job hunting is broken.

* Too many irrelevant listings
* Missed opportunities due to late discovery
* No clarity on *what skills actually matter*
* Resume tweaking is repetitive and manual

So instead of applying blindly, I decided to **treat job search like an engineering problem**.

---

## What This System Does

Every run (in ~5 minutes), the system:

```
Fetch → Filter → Rank → Analyze → Recommend
```

### 1. Fetch Jobs

* Aggregates jobs from multiple sources (Indeed, APIs, remote boards)
* Focused on Backend / Python / AWS roles

### 2. Filter & Clean

* Removes irrelevant roles
* Deduplicates listings
* Keeps only **recent jobs (≤ 6 hours)**

### 3. Score & Rank

Each job is ranked using:

* Skill match
* Recency
* Role relevance
* Startup/product company boost

### 4. Intelligence Layer

For top jobs, the system:

* Identifies **missing skills per job**
* Generates **resume improvement suggestions**
* Recommends **relevant GitHub projects**
* Produces an **aggregated skill gap report**

### 5. Output

Generates structured outputs like:

```
/output/
  ├── top_jobs.json
  ├── job_report.txt
  ├── skill_gap_report.txt
  ├── resume_suggestions.txt
```

---

## Architecture Overview

The system is designed as a modular pipeline:

* Fetchers (plugin-based)
* Normalization layer
* Filtering & deduplication
* Scoring engine
* SQLite storage
* Intelligence modules

---

## Tech Stack

* Python (core engine)
* SQLite (storage)
* RSS + APIs (data sources)
* Rule-based NLP (initial version)
* Docker (planned)

---

## 📁 Project Structure

```
job-intelligence-engine/
├── fetchers/        # job sources (plugins)
├── pipeline/        # filtering, scoring, dedup
├── intelligence/    # skill gap, resume suggestions
├── storage/         # database logic
├── output/          # reports & exports
├── utils/           # helpers
```

---

## Goal

To build a **personal job intelligence system** that:

* Surfaces only high-quality opportunities
* Adapts to my skills
* Continuously improves my profile
* Reduces time-to-apply dramatically

---

## Disclaimer

This is a personal automation system.
All scraping and API usage follows rate limits and safe practices.

---

## Thought Behind This

Instead of:

> "Apply to more jobs"

I’m building:

> "Apply to the *right* jobs, faster, with better preparation"

---

⭐ If you find this interesting, feel free to explore or fork!
