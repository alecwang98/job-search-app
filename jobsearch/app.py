from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from email.parser import BytesParser
from email.policy import default as EMAIL_POLICY
from pathlib import Path
from typing import Any, Iterable
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "jobs.sqlite"
RESUME_DIR = DATA_DIR / "resumes"
ENV_PATH = ROOT / ".env"
HERMES_ENV_PATH = Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes")) / ".env"


def load_local_env(path: Path = ENV_PATH) -> None:
    """Load simple KEY=VALUE entries from a local .env file without overriding real environment variables."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


def load_environment_files(root_env: Path = ENV_PATH, hermes_env: Path = HERMES_ENV_PATH) -> None:
    """Load project .env first, then Hermes .env as a fallback for missing shared keys."""
    load_local_env(root_env)
    load_local_env(hermes_env)


def parse_multipart_file_field(content_type: str, body: bytes, field_name: str) -> dict[str, Any] | None:
    """Parse one multipart/form-data file field without the removed stdlib cgi module."""
    if not content_type or "multipart/form-data" not in content_type.lower():
        return None
    raw_message = (
        f"Content-Type: {content_type}\r\n"
        "MIME-Version: 1.0\r\n"
        "\r\n"
    ).encode("utf-8") + body
    message = BytesParser(policy=EMAIL_POLICY).parsebytes(raw_message)
    if not message.is_multipart():
        return None
    for part in message.iter_parts():
        if part.get_content_disposition() != "form-data":
            continue
        if part.get_param("name", header="content-disposition") != field_name:
            continue
        filename = part.get_filename()
        if not filename:
            return None
        return {
            "filename": filename,
            "content": part.get_payload(decode=True) or b"",
            "content_type": part.get_content_type(),
        }
    return None


load_environment_files()

_BACKGROUND_LOCK = threading.Lock()
_BACKGROUND_JOBS: dict[str, dict[str, Any]] = {}

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JobSearchPhase0/0.1; local personal job tracker)",
    "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
}


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def display_local_time(value: Any, timezone_name: str | None = None) -> str:
    """Format stored UTC-ish timestamps for local dashboard display without changing storage."""
    if value is None or value == "":
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    local_zone = ZoneInfo(timezone_name or os.environ.get("JOBSEARCH_DISPLAY_TIMEZONE") or "America/Los_Angeles")
    return parsed.astimezone(local_zone).strftime("%Y-%m-%d %H:%M %Z")


def reset_background_jobs_for_tests() -> None:
    with _BACKGROUND_LOCK:
        _BACKGROUND_JOBS.clear()


def background_job_snapshot(job_name: str) -> dict[str, Any] | None:
    with _BACKGROUND_LOCK:
        job = _BACKGROUND_JOBS.get(job_name)
        return dict(job) if job else None


def start_llm_profile_extraction_background(db_path: Path = DB_PATH, runner=None, force: bool = False) -> dict[str, Any]:
    """Start LLM profile extraction in a background thread and return immediately."""
    job_name = "llm_profile_extraction"
    with _BACKGROUND_LOCK:
        existing = _BACKGROUND_JOBS.get(job_name)
        if existing and existing.get("status") == "running":
            return dict(existing)
        if existing and existing.get("status") == "succeeded" and not force:
            return dict(existing)
        job = {
            "name": job_name,
            "status": "running",
            "message": "LLM profile extraction is running in the background.",
            "started_at": now_iso(),
            "finished_at": None,
        }
        _BACKGROUND_JOBS[job_name] = job

    def run() -> None:
        db = None
        try:
            db = Database(db_path)
            db.init()
            result = runner(db) if runner else db.extract_llm_profile_from_active_resumes()
            model = None
            if isinstance(result, dict):
                model = result.get("model_name")
            else:
                model = result["model_name"]
            message = f"LLM profile extraction complete{f' with {model}' if model else ''}."
            status = "succeeded"
        except Exception as exc:
            if db is not None:
                try:
                    db.conn.rollback()
                except Exception:
                    pass
            message = f"LLM profile extraction failed: {exc}"
            status = "failed"
        finally:
            if db is not None:
                try:
                    db.conn.close()
                except Exception:
                    pass
        with _BACKGROUND_LOCK:
            current = _BACKGROUND_JOBS.get(job_name, {})
            current.update({"status": status, "message": message, "finished_at": now_iso()})
            _BACKGROUND_JOBS[job_name] = current

    thread = threading.Thread(target=run, name="llm-profile-extraction", daemon=True)
    thread.start()
    return background_job_snapshot(job_name) or job


def start_llm_bulk_rating_background(db_path: Path = DB_PATH, job_ids: Iterable[int] = (), runner=None, force: bool = False) -> dict[str, Any]:
    """Start bulk LLM rating in a background thread and return immediately."""
    normalized_job_ids = list(dict.fromkeys(int(job_id) for job_id in job_ids))
    job_name = "llm_bulk_rating"
    with _BACKGROUND_LOCK:
        existing = _BACKGROUND_JOBS.get(job_name)
        if existing and existing.get("status") == "running":
            return dict(existing)
        if existing and existing.get("status") == "succeeded" and not force:
            return dict(existing)
        job = {
            "name": job_name,
            "status": "running",
            "message": f"LLM bulk rating is running in the background for {len(normalized_job_ids)} jobs.",
            "started_at": now_iso(),
            "finished_at": None,
            "requested": len(normalized_job_ids),
        }
        _BACKGROUND_JOBS[job_name] = job

    def run() -> None:
        db = None
        try:
            db = Database(db_path)
            db.init()

            def progress_callback(event: dict[str, Any]) -> None:
                with _BACKGROUND_LOCK:
                    current = _BACKGROUND_JOBS.get(job_name, {})
                    current.update({
                        "requested": event.get("requested", len(normalized_job_ids)),
                        "rated": event.get("rated", 0),
                        "failed": event.get("failed", 0),
                        "completed": event.get("completed", 0),
                        "last_job_id": event.get("last_job_id"),
                        "message": (
                            f"LLM bulk rating running: {event.get('completed', 0)} of "
                            f"{event.get('requested', len(normalized_job_ids))} completed; "
                            f"rated {event.get('rated', 0)}, failed {event.get('failed', 0)}."
                        ),
                    })
                    _BACKGROUND_JOBS[job_name] = current

            result = runner(db, normalized_job_ids) if runner else db.rate_jobs_with_llm(normalized_job_ids, force=force, progress_callback=progress_callback)
            message = f"Rated {result.get('rated', 0)} of {result.get('requested', len(normalized_job_ids))} jobs with LLM. Failed: {result.get('failed', 0)}."
            status = "succeeded" if result.get("failed", 0) == 0 else "failed"
        except Exception as exc:
            if db is not None:
                try:
                    db.conn.rollback()
                except Exception:
                    pass
            message = f"LLM bulk rating failed: {exc}"
            status = "failed"
        finally:
            if db is not None:
                try:
                    db.conn.close()
                except Exception:
                    pass
        with _BACKGROUND_LOCK:
            current = _BACKGROUND_JOBS.get(job_name, {})
            current.update({"status": status, "message": message, "finished_at": now_iso()})
            _BACKGROUND_JOBS[job_name] = current

    thread = threading.Thread(target=run, name="llm-bulk-rating", daemon=True)
    thread.start()
    return background_job_snapshot(job_name) or job


def start_llm_fast_rating_background(db_path: Path = DB_PATH, job_ids: Iterable[int] = (), runner=None, force: bool = False) -> dict[str, Any]:
    """Start bulk fast LLM pre-rating in a background thread and return immediately."""
    normalized_job_ids = list(dict.fromkeys(int(job_id) for job_id in job_ids))
    job_name = "llm_fast_rating"
    with _BACKGROUND_LOCK:
        existing = _BACKGROUND_JOBS.get(job_name)
        if existing and existing.get("status") == "running":
            return dict(existing)
        job = {
            "name": job_name,
            "status": "running",
            "message": f"Fast LLM rating is running in the background for {len(normalized_job_ids)} jobs.",
            "started_at": now_iso(),
            "finished_at": None,
            "requested": len(normalized_job_ids),
        }
        _BACKGROUND_JOBS[job_name] = job

    def run() -> None:
        db = None
        try:
            db = Database(db_path)
            db.init()
            result = runner(db, normalized_job_ids) if runner else db.fast_rate_jobs_with_llm(normalized_job_ids, force=force)
            message = f"Fast-rated {result.get('rated', 0)} of {result.get('requested', len(normalized_job_ids))} jobs. Failed: {result.get('failed', 0)}."
            status = "succeeded" if result.get("failed", 0) == 0 else "failed"
        except Exception as exc:
            if db is not None:
                try:
                    db.conn.rollback()
                except Exception:
                    pass
            message = f"Fast LLM rating failed: {exc}"
            status = "failed"
        finally:
            if db is not None:
                try:
                    db.conn.close()
                except Exception:
                    pass
        with _BACKGROUND_LOCK:
            current = _BACKGROUND_JOBS.get(job_name, {})
            current.update({"status": status, "message": message, "finished_at": now_iso()})
            _BACKGROUND_JOBS[job_name] = current

    thread = threading.Thread(target=run, name="llm-fast-rating", daemon=True)
    thread.start()
    return background_job_snapshot(job_name) or job


def strip_html(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", value)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p>", "\n\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def content_hash(*parts: str | None) -> str:
    joined = "\n".join(p or "" for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def classify_remote(location: str | None, title: str | None = None, description: str | None = None) -> str | None:
    blob = " ".join([location or "", title or "", description or ""]).lower()
    if "remote" in blob:
        return "remote"
    if "hybrid" in blob:
        return "hybrid"
    if location:
        return "onsite_or_unspecified"
    return None


def filter_job(job: "NormalizedJob") -> tuple[str, str]:
    return "new", "ready for rating"


def http_json(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> Any:
    data = None
    headers = dict(DEFAULT_HEADERS)
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=45) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


@dataclass
class NormalizedJob:
    company_name: str
    source: str
    source_job_id: str
    requisition_id: str | None
    title: str
    location: str | None
    remote_type: str | None
    department: str | None
    employment_type: str | None
    salary_min: float | None
    salary_max: float | None
    currency: str | None
    job_url: str
    apply_url: str
    description_raw_html: str | None
    description_text: str | None
    posted_at: str | None
    status: str = "new"
    filter_reason: str | None = None
    raw_json: dict[str, Any] | None = None


def _blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def raw_top_level_keys(raw_json: dict[str, Any] | None) -> list[str]:
    if not isinstance(raw_json, dict):
        return []
    keys = set(raw_json.keys())
    if isinstance(raw_json.get("listing"), dict):
        keys.update(f"listing.{k}" for k in raw_json["listing"].keys())
    if isinstance(raw_json.get("detail"), dict):
        keys.update(f"detail.{k}" for k in raw_json["detail"].keys())
    return sorted(keys)


def build_ingestion_audit(job: NormalizedJob) -> dict[str, Any]:
    required_fields = {
        "source_job_id": job.source_job_id,
        "title": job.title,
        "location": job.location,
        "job_url": job.job_url,
        "apply_url": job.apply_url,
        "description_text": job.description_text,
        "posted_at": job.posted_at,
    }
    optional_fields = {
        "requisition_id": job.requisition_id,
        "remote_type": job.remote_type,
        "department": job.department,
        "employment_type": job.employment_type,
        "salary_min": job.salary_min,
        "salary_max": job.salary_max,
        "currency": job.currency,
    }
    required_missing = [name for name, value in required_fields.items() if _blank(value) or (name == "title" and str(value).strip() == "Untitled")]
    optional_missing = [name for name, value in optional_fields.items() if _blank(value)]
    warnings: list[str] = []
    if "description_text" in required_missing:
        warnings.append("Missing description")
    if "location" in required_missing:
        warnings.append("Missing location")
    if "title" in required_missing:
        warnings.append("Missing title")
    detail_fetch_status = "not_applicable"
    raw_json = job.raw_json if isinstance(job.raw_json, dict) else {}
    detail = raw_json.get("detail") if isinstance(raw_json, dict) else None
    if isinstance(detail, dict):
        if detail.get("detail_fetch_error"):
            detail_fetch_status = "failed"
            warnings.append("Detail fetch failed")
        else:
            detail_fetch_status = "ok"
    location_status = "ok"
    if "location" in required_missing:
        location_status = "missing"
    elif job.location and (";" in job.location or "/" in job.location):
        location_status = "raw_multi_location"
    known_raw_keys = {
        "id", "internal_job_id", "requisition_id", "title", "location", "absolute_url", "content", "first_published", "updated_at",
        "departments", "offices", "metadata", "data_compliance", "company_name", "language", "application_deadline",
        "listing", "detail", "detail_info", "listing.title", "listing.externalPath", "listing.locationsText", "listing.postedOn",
        "listing.bulletFields", "detail.jobPostingInfo", "detail.detail_fetch_error",
    }
    extra_keys = [key for key in raw_top_level_keys(raw_json) if key not in known_raw_keys]
    return {
        "required_missing": required_missing,
        "optional_missing": optional_missing,
        "extra_keys": extra_keys,
        "warnings": warnings,
        "description_length": len(job.description_text or ""),
        "location_status": location_status,
        "detail_fetch_status": detail_fetch_status,
    }


LOCAL_PROFILE_STOPWORDS = {
    "about", "above", "across", "after", "again", "against", "also", "among", "because", "before", "being",
    "below", "between", "both", "built", "could", "during", "each", "from", "have", "into", "more",
    "most", "other", "over", "resume", "should", "than", "that", "their", "there", "these", "this", "through",
    "under", "using", "with", "within", "without", "work", "worked", "years",
}

RATING_WEIGHTS = {
    "core_job_function_match": 0.30,
    "experience_seniority_match": 0.20,
    "domain_match": 0.15,
    "technical_tool_match": 0.15,
    "resume_evidence_strength": 0.10,
    "gap_severity": 0.05,
    "strategic_career_value": 0.05,
}

PROFILE_PROMPT_VERSION = "llm-profile-v1"
RATING_PROMPT_VERSION = "llm-rating-v1"
FAST_RATING_PROMPT_VERSION = "llm-fast-rating-v5"
DEFAULT_LLM_MODEL = "gpt-4o-mini"
DEEPSEEK_V4_FLASH_OPENROUTER_MODEL = "deepseek/deepseek-v4-flash"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
REFERENCE_RUBRIC = """
Rate jobs by expected interview probability plus role fit, not title similarity.
Overall fit score uses 7 weighted areas: core job-function match 30%, experience/seniority 20%, domain 15%, technical/tool 15%, resume evidence 10%, gap severity 5%, strategic career value 5%.
Score scale:
- 9.0-9.5 Excellent / Apply ASAP: rare near-ideal fit, strong direct evidence, practical feasibility, and only minor gaps.
- 8.5-8.9 Strong fit: credible interview target with strong evidence and manageable tailoring gaps.
- 8.0-8.4 Good stretch: good adjacent fit with clear strengths but one or more meaningful gaps.
- 7.0-7.9 Medium stretch: plausible but needs significant tailoring or has notable seniority/domain gaps.
- 6.0-6.9 Reach: weak/uncertain fit; apply only if strategically interesting.
- Below 6.0 Skip or low priority: wrong function, major blocker, or poor evidence fit.
Use only evidence, target roles, skills, domains, proof points, gaps, and practical constraints contained in the current extracted profile JSON. Do not introduce candidate-specific examples, assumed industries, assumed companies, assumed tools, or hidden keyword lists outside that profile.
Separate skill fit from practical fit for constraints explicitly present in the job and/or extracted profile, such as eligibility, location/work authorization, language requirements, years/seniority mismatch, or function mismatch.
Use the full scale: do not cap strong credible interview targets below 8.5 merely because they have manageable gaps; reserve 9.0+ for rare near-ideal matches.
""".strip()


def configured_llm_model() -> str:
    return os.environ.get("JOBSEARCH_LLM_MODEL") or os.environ.get("OPENAI_MODEL") or DEFAULT_LLM_MODEL


def call_openai_compatible_json(
    system_prompt: str,
    user_prompt: str,
    model_name: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> dict[str, Any]:
    """Call an OpenAI-compatible chat-completions endpoint and return parsed JSON."""
    resolved_api_key = api_key or os.environ.get("JOBSEARCH_LLM_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not resolved_api_key:
        raise RuntimeError("Set JOBSEARCH_LLM_API_KEY or OPENAI_API_KEY before using LLM extraction/rating.")
    model = model_name or configured_llm_model()
    resolved_base_url = base_url or os.environ.get("JOBSEARCH_LLM_BASE_URL") or os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1"
    url = resolved_base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {resolved_api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"LLM request failed: HTTP {exc.code} {detail[:500]}") from exc
    content = data["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise RuntimeError("LLM response was not a JSON object.")
    return parsed


def deepseek_v4_flash_openrouter_client(system_prompt: str, user_prompt: str, model_name: str | None = None) -> dict[str, Any]:
    """Call DeepSeek v4 Flash through OpenRouter for fast-rating benchmarks."""
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("Set OPENROUTER_API_KEY before using --fast-mode openrouter-v4-flash.")
    return call_openai_compatible_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model_name=model_name or DEEPSEEK_V4_FLASH_OPENROUTER_MODEL,
        api_key=api_key,
        base_url=OPENROUTER_BASE_URL,
    )


def fast_llm_client_for_mode(mode: str):
    if mode == "default":
        return call_openai_compatible_json
    if mode == "openrouter-v4-flash":
        return deepseek_v4_flash_openrouter_client
    raise ValueError(f"Unknown fast LLM mode: {mode}")


def fast_model_name_for_mode(mode: str, model_name: str | None = None) -> str | None:
    if model_name:
        return model_name
    if mode == "openrouter-v4-flash":
        return DEEPSEEK_V4_FLASH_OPENROUTER_MODEL
    return None


def normalize_llm_profile(profile: dict[str, Any]) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "candidate_summary": "",
        "target_roles": [],
        "target_industries": [],
        "seniority": {},
        "core_strengths": [],
        "technical_skills": [],
        "domain_skills": [],
        "proof_points": [],
        "weaknesses_or_gaps": [],
        "practical_constraints": {},
        "resume_bullet_inventory": [],
    }
    normalized = {**defaults, **(profile or {})}
    return normalized


def rating_recommendation_for_score(score: float) -> str:
    if score >= 9.0:
        return "Apply ASAP"
    if score >= 8.5:
        return "Strong fit — worth applying"
    if score >= 8.0:
        return "Good stretch — apply if interested"
    if score >= 7.0:
        return "Medium stretch"
    if score >= 6.0:
        return "Reach"
    return "Skip or very low priority"


def normalize_llm_rating(rating: dict[str, Any]) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "overall_score": 0.0,
        "skill_fit_score": 0.0,
        "practical_fit_score": 0.0,
        "recommendation": "",
        "categories": {},
        "strongest_evidence": [],
        "main_gaps": [],
        "practical_notes": [],
        "resume_tailoring_notes": [],
        "interview_probability_reasoning": "",
        "apply_decision": "",
    }
    normalized = {**defaults, **(rating or {})}
    for key in ["overall_score", "skill_fit_score", "practical_fit_score"]:
        try:
            normalized[key] = float(normalized[key])
        except Exception:
            normalized[key] = 0.0
    normalized["overall_score"] = max(1.0, min(9.5, normalized["overall_score"]))
    normalized["skill_fit_score"] = max(0.0, min(9.5, normalized["skill_fit_score"]))
    normalized["practical_fit_score"] = max(0.0, min(9.5, normalized["practical_fit_score"]))
    band_recommendation = rating_recommendation_for_score(normalized["overall_score"])
    normalized["recommendation"] = band_recommendation
    normalized["apply_decision"] = band_recommendation
    return normalized


def fast_rating_bucket_label(bucket: str | None) -> str:
    return {
        "gte_7": ">=7 fast pass",
        "lt_7": "<7 fast skip",
        "needs_manual_review": "Needs manual review",
    }.get(str(bucket or ""), "Unrated")


def normalize_fast_rating(rating: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "bucket": str((rating or {}).get("bucket") or "needs_manual_review"),
        "confidence": str((rating or {}).get("confidence") or "low"),
        "reason_codes": (rating or {}).get("reason_codes") or [],
        "short_reason": str((rating or {}).get("short_reason") or ""),
    }
    if normalized["bucket"] not in {"gte_7", "lt_7", "needs_manual_review"}:
        normalized["bucket"] = "needs_manual_review"
    if normalized["confidence"] not in {"low", "medium", "high"}:
        normalized["confidence"] = "low"
    if not isinstance(normalized["reason_codes"], list):
        normalized["reason_codes"] = [str(normalized["reason_codes"])]
    normalized["reason_codes"] = [str(item) for item in normalized["reason_codes"][:8]]
    return normalized


def sanitize_filename(filename: str) -> str:
    name = Path(filename or "resume.txt").name.strip() or "resume.txt"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)[:160]


def extract_docx_text(path: Path) -> str:
    with zipfile.ZipFile(path) as zf:
        xml = zf.read("word/document.xml")
    root = ET.fromstring(xml)
    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    paragraphs: list[str] = []
    for para in root.iter(f"{ns}p"):
        chunks = [node.text or "" for node in para.iter(f"{ns}t")]
        text = "".join(chunks).strip()
        if text:
            paragraphs.append(text)
    return "\n".join(paragraphs)


def extract_pdf_text(path: Path) -> str:
    try:
        import fitz  # type: ignore
    except Exception as exc:
        raise RuntimeError("PDF extraction requires PyMuPDF (`pip install pymupdf`).") from exc
    doc = fitz.open(path)
    try:
        return "\n".join(page.get_text() for page in doc).strip()
    finally:
        doc.close()


def extract_resume_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md"}:
        return path.read_text(encoding="utf-8", errors="ignore").strip()
    if suffix == ".docx":
        return extract_docx_text(path).strip()
    if suffix == ".pdf":
        return extract_pdf_text(path).strip()
    raise ValueError(f"Unsupported resume file type: {suffix or 'unknown'}")


def _keyword_phrases_from_text(text: str, limit: int = 30) -> list[str]:
    """Extract generic, resume-derived keyword phrases without candidate-specific dictionaries."""
    tokens = [token.lower() for token in re.findall(r"[A-Za-z][A-Za-z0-9+.#-]{2,}", text)]
    counts: dict[str, int] = {}
    for token in tokens:
        if token in LOCAL_PROFILE_STOPWORDS:
            continue
        counts[token] = counts.get(token, 0) + 1
    phrases: dict[str, int] = dict(counts)
    for left, right in zip(tokens, tokens[1:]):
        if left in LOCAL_PROFILE_STOPWORDS or right in LOCAL_PROFILE_STOPWORDS:
            continue
        phrases[f"{left} {right}"] = phrases.get(f"{left} {right}", 0) + 2
    return sorted(phrases, key=lambda item: (-phrases[item], item))[:limit]


def build_profile_from_texts(resume_texts: list[str]) -> dict[str, Any]:
    """Build a generic local/debug profile only from resume text.

    This fallback intentionally avoids hardcoded candidate roles, domains, companies,
    technologies, proof-point names, or target directions. The production dashboard path
    uses LLM extraction; this helper exists for tests/debugging when no model call is made.
    """
    combined = "\n\n".join(text for text in resume_texts if text).strip()
    years = sorted(set(re.findall(r"\b(?:19|20)\d{2}\b|\b\d+\+?\s+years?\b", combined, flags=re.I)))[:20]
    numeric_years = [int(match) for match in re.findall(r"\b(\d+)\+?\s+years?\b", combined, flags=re.I)]
    baseline_years = max(numeric_years) if numeric_years else 0
    lines = [line.strip() for line in combined.splitlines() if line.strip()]
    keywords = _keyword_phrases_from_text(combined)
    return {
        "skills": keywords,
        "domains": [],
        "experience_signals": years,
        "baseline_years_experience": baseline_years,
        "proof_points": [],
        "target_directions": [],
        "highlights": lines[:12],
        "summary": " ".join(lines[:4])[:1000],
        "source_text_characters": len(combined),
    }


def _text_for_job(job: NormalizedJob) -> str:
    return "\n".join(
        str(part or "")
        for part in [job.title, job.department, job.location, job.description_text]
    ).lower()


def _flatten_profile_strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        flattened: list[str] = []
        for key, nested in value.items():
            if isinstance(key, str):
                flattened.append(key)
            flattened.extend(_flatten_profile_strings(nested))
        return flattened
    if isinstance(value, (list, tuple, set)):
        flattened = []
        for item in value:
            flattened.extend(_flatten_profile_strings(item))
        return flattened
    return [str(value)]


def _profile_terms(profile: dict[str, Any], *keys: str) -> list[str]:
    terms: list[str] = []
    for key in keys:
        terms.extend(_flatten_profile_strings(profile.get(key)))
    seen: set[str] = set()
    result: list[str] = []
    for term in terms:
        cleaned = re.sub(r"\s+", " ", str(term)).strip()
        if len(cleaned) < 3:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(cleaned)
    return result


def _matched_terms(terms: Iterable[str], text: str) -> list[str]:
    lowered = text.lower()
    return sorted({term for term in terms if term and term.lower() in lowered})


def _term_tokens(term: str) -> set[str]:
    return {
        token.lower()
        for token in re.findall(r"[A-Za-z][A-Za-z0-9+.#-]{2,}", term)
        if token.lower() not in LOCAL_PROFILE_STOPWORDS
    }


def _overlap_matches(candidate_terms: Iterable[str], job_text: str, min_overlap: int = 1) -> list[str]:
    job_tokens = _term_tokens(job_text)
    matches = []
    for term in candidate_terms:
        tokens = _term_tokens(term)
        if tokens and len(tokens & job_tokens) >= min_overlap:
            matches.append(term)
    return sorted(set(matches))


def _score_from_match_count(count: int, strong: int = 4, base: float = 2.0) -> float:
    if count <= 0:
        return base
    return min(9.5, base + (7.5 * min(count, strong) / strong))


def _years_required(text: str) -> int | None:
    matches = [int(match) for match in re.findall(r"\b(\d+)\+?\s+years?\b", text, flags=re.I)]
    return max(matches) if matches else None


def _profile_years(profile: dict[str, Any]) -> int:
    seniority = profile.get("seniority") if isinstance(profile.get("seniority"), dict) else {}
    candidates = [
        profile.get("baseline_years_experience"),
        seniority.get("years_experience") if isinstance(seniority, dict) else None,
    ]
    for candidate in candidates:
        try:
            if candidate is not None:
                return int(float(candidate))
        except Exception:
            continue
    return 0


def _rating_recommendation(score: float) -> str:
    return rating_recommendation_for_score(score)


def rate_job_fit(profile: dict[str, Any], job: NormalizedJob) -> dict[str, Any]:
    """Deterministic/debug fit rating derived only from the supplied profile JSON."""
    text = _text_for_job(job)
    target_terms = _profile_terms(profile, "target_roles", "target_directions")
    skill_terms = _profile_terms(profile, "technical_skills", "skills", "core_strengths")
    domain_terms = _profile_terms(profile, "target_industries", "domain_skills", "domains")
    proof_terms = _profile_terms(profile, "proof_points", "resume_bullet_inventory", "highlights")
    gap_terms = _profile_terms(profile, "weaknesses_or_gaps", "practical_constraints")

    matched_targets = _matched_terms(target_terms, text)
    matched_skills = _matched_terms(skill_terms, text)
    matched_domains = _matched_terms(domain_terms, text)
    evidence = _overlap_matches(proof_terms, text, min_overlap=2)
    gaps = _matched_terms(gap_terms, text)

    required_years = _years_required(text)
    baseline_years = _profile_years(profile)
    if required_years is None or baseline_years <= 0:
        exp_score = 8.0
    elif required_years <= baseline_years + 1:
        exp_score = 9.0
    elif required_years <= baseline_years + 3:
        exp_score = 6.0
        gaps.append(f"Requires {required_years}+ years vs extracted profile {baseline_years}+ years")
    else:
        exp_score = 3.5
        gaps.append(f"Requires {required_years}+ years vs extracted profile {baseline_years}+ years")

    core_score = max(
        _score_from_match_count(len(matched_targets), strong=2, base=2.0),
        _score_from_match_count(len(matched_skills) + len(matched_domains), strong=5, base=2.0),
    )
    domain_score = _score_from_match_count(len(matched_domains), strong=4, base=2.0)
    tech_score = _score_from_match_count(len(matched_skills), strong=5, base=2.0)
    evidence_score = _score_from_match_count(len(evidence), strong=3, base=1.5)
    gap_score = 9.0 if not gaps else max(1.0, 9.0 - 2.0 * len(set(gaps)))
    strategic_score = _score_from_match_count(len(matched_targets), strong=2, base=5.0)
    if not matched_targets and (matched_domains or matched_skills):
        strategic_score = 7.0

    categories = {
        "core_job_function_match": {"weight": RATING_WEIGHTS["core_job_function_match"], "score": round(core_score, 1), "matches": matched_targets or matched_skills[:5]},
        "experience_seniority_match": {"weight": RATING_WEIGHTS["experience_seniority_match"], "score": round(exp_score, 1), "required_years": required_years, "baseline_years": baseline_years},
        "domain_match": {"weight": RATING_WEIGHTS["domain_match"], "score": round(domain_score, 1), "matches": matched_domains},
        "technical_tool_match": {"weight": RATING_WEIGHTS["technical_tool_match"], "score": round(tech_score, 1), "matches": matched_skills},
        "resume_evidence_strength": {"weight": RATING_WEIGHTS["resume_evidence_strength"], "score": round(evidence_score, 1), "matches": evidence},
        "gap_severity": {"weight": RATING_WEIGHTS["gap_severity"], "score": round(gap_score, 1), "gaps": sorted(set(gaps))},
        "strategic_career_value": {"weight": RATING_WEIGHTS["strategic_career_value"], "score": round(strategic_score, 1), "matches": matched_targets},
    }
    skill_fit_score = sum(categories[name]["score"] * weight for name, weight in RATING_WEIGHTS.items())
    practical_penalty = 0.0 if not gaps else min(3.0, 0.75 * len(set(gaps)))
    overall_score = max(1.0, min(9.5, skill_fit_score - practical_penalty))

    return {
        "overall_score": round(overall_score, 1),
        "skill_fit_score": round(skill_fit_score, 1),
        "practical_fit_score": round(max(1.0, min(9.5, skill_fit_score - practical_penalty)), 1),
        "recommendation": _rating_recommendation(overall_score),
        "categories": categories,
        "evidence": evidence,
        "gaps": sorted(set(gaps)),
        "practical_notes": sorted(set(gaps)),
        "score_scale": "0-10",
    }


class Database:
    def __init__(self, path: Path = DB_PATH):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row

    def init(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                slug TEXT NOT NULL UNIQUE,
                ats_type TEXT NOT NULL,
                careers_url TEXT,
                source_api_url TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL REFERENCES companies(id),
                company_name TEXT NOT NULL,
                source TEXT NOT NULL,
                source_job_id TEXT NOT NULL,
                requisition_id TEXT,
                title TEXT NOT NULL,
                location TEXT,
                remote_type TEXT,
                department TEXT,
                employment_type TEXT,
                salary_min REAL,
                salary_max REAL,
                currency TEXT,
                job_url TEXT NOT NULL,
                apply_url TEXT NOT NULL,
                description_raw_html TEXT,
                description_text TEXT,
                posted_at TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                status TEXT NOT NULL,
                source_status TEXT NOT NULL DEFAULT 'newly_discovered',
                review_status TEXT NOT NULL DEFAULT 'unreviewed',
                is_hidden INTEGER NOT NULL DEFAULT 0,
                is_saved INTEGER NOT NULL DEFAULT 0,
                saved_at TEXT,
                reviewed_at TEXT,
                applied_at TEXT,
                filter_reason TEXT,
                content_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(company_id, source, source_job_id)
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_company_status ON jobs(company_id, status);
            CREATE INDEX IF NOT EXISTS idx_jobs_title ON jobs(title);

            CREATE TABLE IF NOT EXISTS job_fetch_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL REFERENCES companies(id),
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                jobs_found INTEGER NOT NULL DEFAULT 0,
                jobs_created INTEGER NOT NULL DEFAULT 0,
                jobs_updated INTEGER NOT NULL DEFAULT 0,
                jobs_expired INTEGER NOT NULL DEFAULT 0,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS job_raw_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id),
                source TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS resume_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_filename TEXT NOT NULL,
                stored_filename TEXT NOT NULL,
                stored_path TEXT NOT NULL,
                content_type TEXT,
                content_hash TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                extracted_text TEXT,
                extraction_error TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                uploaded_at TEXT NOT NULL,
                removed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS profile_extractions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_resume_ids_json TEXT NOT NULL,
                source_resume_hash TEXT NOT NULL,
                profile_hash TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                extractor_version TEXT NOT NULL,
                extraction_method TEXT NOT NULL DEFAULT 'local',
                model_name TEXT,
                prompt_version TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS job_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id),
                profile_extraction_id INTEGER NOT NULL REFERENCES profile_extractions(id),
                profile_hash TEXT NOT NULL,
                job_content_hash TEXT NOT NULL,
                rubric_version TEXT NOT NULL,
                rater_version TEXT NOT NULL,
                model_name TEXT NOT NULL,
                overall_score REAL NOT NULL,
                skill_fit_score REAL NOT NULL,
                practical_fit_score REAL NOT NULL,
                recommendation TEXT NOT NULL,
                rating_json TEXT NOT NULL,
                is_current INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS job_fast_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id),
                profile_extraction_id INTEGER NOT NULL REFERENCES profile_extractions(id),
                profile_hash TEXT NOT NULL,
                job_content_hash TEXT NOT NULL,
                rater_version TEXT NOT NULL,
                model_name TEXT NOT NULL,
                bucket TEXT NOT NULL,
                confidence TEXT NOT NULL,
                reason_codes_json TEXT NOT NULL,
                short_reason TEXT NOT NULL,
                rating_json TEXT NOT NULL,
                is_current INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS job_ingestion_audits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id),
                source TEXT NOT NULL,
                required_missing_json TEXT NOT NULL,
                optional_missing_json TEXT NOT NULL,
                extra_keys_json TEXT NOT NULL,
                warnings_json TEXT NOT NULL,
                description_length INTEGER NOT NULL DEFAULT 0,
                location_status TEXT NOT NULL,
                detail_fetch_status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        self.conn.commit()
        self.seed_companies()
        self.migrate_deprecated_statuses_to_new()
        self.migrate_review_state_columns()
        self.migrate_llm_columns()
        self.backfill_missing_ingestion_audits()

    def seed_companies(self) -> None:
        ts = now_iso()
        companies = [
            (
                "Databricks",
                "databricks",
                "greenhouse",
                "https://www.databricks.com/company/careers/open-positions",
                "https://api.greenhouse.io/v1/boards/databricks/jobs?content=true",
            ),
            (
                "NVIDIA",
                "nvidia",
                "workday",
                "https://www.nvidia.com/en-us/about-nvidia/careers/",
                "https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite/jobs",
            ),
        ]
        for row in companies:
            self.conn.execute(
                """
                INSERT INTO companies(name, slug, ats_type, careers_url, source_api_url, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    name=excluded.name,
                    ats_type=excluded.ats_type,
                    careers_url=excluded.careers_url,
                    source_api_url=excluded.source_api_url,
                    updated_at=excluded.updated_at
                """,
                (*row, ts, ts),
            )
        self.conn.commit()

    def companies(self, slug: str | None = None) -> list[sqlite3.Row]:
        if slug:
            return self.conn.execute("SELECT * FROM companies WHERE enabled=1 AND slug=? ORDER BY name", (slug,)).fetchall()
        return self.conn.execute("SELECT * FROM companies WHERE enabled=1 ORDER BY name").fetchall()

    def upload_resume_file(self, filename: str, content: bytes, content_type: str | None = None) -> int:
        safe_name = sanitize_filename(filename)
        digest = hashlib.sha256(content).hexdigest()
        ts = now_iso()
        resume_dir = self.path.parent / "resumes"
        resume_dir.mkdir(parents=True, exist_ok=True)
        cur = self.conn.execute(
            """
            INSERT INTO resume_files(original_filename, stored_filename, stored_path, content_type, content_hash, file_size, is_active, uploaded_at)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (safe_name, safe_name, "", content_type or "application/octet-stream", digest, len(content), ts),
        )
        assert cur.lastrowid is not None
        resume_id = int(cur.lastrowid)
        stored_filename = f"{resume_id}-{safe_name}"
        stored_path = resume_dir / stored_filename
        stored_path.write_bytes(content)
        self.conn.execute(
            "UPDATE resume_files SET stored_filename=?, stored_path=? WHERE id=?",
            (stored_filename, str(stored_path), resume_id),
        )
        self.conn.commit()
        return resume_id

    def active_resume_files(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM resume_files WHERE is_active=1 ORDER BY uploaded_at, id"
        ).fetchall()

    def latest_profile(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM profile_extractions ORDER BY created_at DESC, id DESC LIMIT 1"
        ).fetchone()

    def latest_active_llm_profile(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM profile_extractions WHERE is_active=1 AND extraction_method='llm' ORDER BY created_at DESC, id DESC LIMIT 1"
        ).fetchone()

    def latest_job_rating(self, job_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM job_ratings WHERE job_id=? AND is_current=1 ORDER BY created_at DESC, id DESC LIMIT 1",
            (job_id,),
        ).fetchone()

    def latest_job_fast_rating(self, job_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM job_fast_ratings WHERE job_id=? AND is_current=1 ORDER BY created_at DESC, id DESC LIMIT 1",
            (job_id,),
        ).fetchone()

    def active_resume_source_hash(self) -> str:
        rows = self.active_resume_files()
        payload = json.dumps(
            [{"id": row["id"], "content_hash": row["content_hash"]} for row in rows],
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def latest_profile_is_current(self) -> bool:
        latest = self.latest_profile()
        return bool(latest and latest["is_active"] and latest["source_resume_hash"] == self.active_resume_source_hash())

    def remove_resume_file(self, resume_id: int) -> bool:
        ts = now_iso()
        cur = self.conn.execute(
            "UPDATE resume_files SET is_active=0, removed_at=? WHERE id=? AND is_active=1",
            (ts, resume_id),
        )
        # Any profile snapshot that used this resume must no longer feed future ratings.
        pattern = f'%{resume_id}%'
        self.conn.execute(
            "UPDATE profile_extractions SET is_active=0 WHERE source_resume_ids_json LIKE ?",
            (pattern,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def extract_profile_from_active_resumes(self) -> sqlite3.Row:
        rows = self.active_resume_files()
        if not rows:
            raise ValueError("Upload at least one active resume before extracting a profile.")
        resume_texts: list[str] = []
        ts = now_iso()
        for row in rows:
            text = ""
            error = None
            try:
                text = extract_resume_text(Path(row["stored_path"]))
            except Exception as exc:
                error = str(exc)
            self.conn.execute(
                "UPDATE resume_files SET extracted_text=?, extraction_error=? WHERE id=?",
                (text or None, error, row["id"]),
            )
            if text:
                resume_texts.append(text)
        if not resume_texts:
            self.conn.commit()
            raise ValueError("Could not extract text from the active resume files.")
        profile = build_profile_from_texts(resume_texts)
        source_ids = [row["id"] for row in rows]
        source_hash = self.active_resume_source_hash()
        profile_json = json.dumps(profile, sort_keys=True)
        profile_hash = hashlib.sha256((source_hash + profile_json).encode("utf-8")).hexdigest()
        self.conn.execute("UPDATE profile_extractions SET is_active=0")
        cur = self.conn.execute(
            """
            INSERT INTO profile_extractions(source_resume_ids_json, source_resume_hash, profile_hash, profile_json, extractor_version, extraction_method, model_name, prompt_version, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, 'local', NULL, NULL, 1, ?)
            """,
            (json.dumps(source_ids), source_hash, profile_hash, profile_json, "local-keyword-v1", ts),
        )
        self.conn.commit()
        row = self.conn.execute("SELECT * FROM profile_extractions WHERE id=?", (cur.lastrowid,)).fetchone()
        assert row is not None
        return row

    def _extract_active_resume_texts(self) -> tuple[list[sqlite3.Row], list[str]]:
        rows = self.active_resume_files()
        if not rows:
            raise ValueError("Upload at least one active resume before extracting a profile.")
        resume_texts: list[str] = []
        for row in rows:
            text = ""
            error = None
            try:
                text = extract_resume_text(Path(row["stored_path"]))
            except Exception as exc:
                error = str(exc)
            self.conn.execute(
                "UPDATE resume_files SET extracted_text=?, extraction_error=? WHERE id=?",
                (text or None, error, row["id"]),
            )
            if text:
                resume_texts.append(f"Resume file: {row['original_filename']}\n{text}")
        if not resume_texts:
            self.conn.commit()
            raise ValueError("Could not extract text from the active resume files.")
        self.conn.commit()
        return rows, resume_texts

    def extract_llm_profile_from_active_resumes(
        self,
        llm_client=call_openai_compatible_json,
        model_name: str | None = None,
    ) -> sqlite3.Row:
        rows, resume_texts = self._extract_active_resume_texts()
        model = model_name or configured_llm_model()
        system_prompt = "Return only valid JSON. You are a precise career profile extraction engine."
        user_prompt = f"""
Build a comprehensive structured candidate profile from the resume text and reference rubric.
Use the reference to infer evidence relationships; do not invent facts not supported by the input.

REFERENCE 7-CATEGORY RUBRIC:
{REFERENCE_RUBRIC}

Required JSON keys: candidate_summary, target_roles, target_industries, seniority, core_strengths, technical_skills, domain_skills, proof_points, weaknesses_or_gaps, practical_constraints, resume_bullet_inventory.
Proof points should map evidence to the job categories it supports. Include concise resume bullet evidence where available.

RESUME TEXT:
{chr(10).join(resume_texts)}
""".strip()
        profile = normalize_llm_profile(llm_client(system_prompt=system_prompt, user_prompt=user_prompt, model_name=model))
        source_ids = [row["id"] for row in rows]
        source_hash = self.active_resume_source_hash()
        profile_json = json.dumps(profile, sort_keys=True, ensure_ascii=False)
        profile_hash = hashlib.sha256((source_hash + PROFILE_PROMPT_VERSION + model + profile_json).encode("utf-8")).hexdigest()
        ts = now_iso()
        self.conn.execute("UPDATE profile_extractions SET is_active=0")
        self.conn.execute("UPDATE job_ratings SET is_current=0")
        cur = self.conn.execute(
            """
            INSERT INTO profile_extractions(
                source_resume_ids_json, source_resume_hash, profile_hash, profile_json, extractor_version,
                extraction_method, model_name, prompt_version, is_active, created_at
            ) VALUES (?, ?, ?, ?, ?, 'llm', ?, ?, 1, ?)
            """,
            (json.dumps(source_ids), source_hash, profile_hash, profile_json, PROFILE_PROMPT_VERSION, model, PROFILE_PROMPT_VERSION, ts),
        )
        self.conn.commit()
        row = self.conn.execute("SELECT * FROM profile_extractions WHERE id=?", (cur.lastrowid,)).fetchone()
        assert row is not None
        return row

    def _job_row_to_normalized(self, row: sqlite3.Row) -> NormalizedJob:
        return NormalizedJob(
            company_name=row["company_name"], source=row["source"], source_job_id=row["source_job_id"],
            requisition_id=row["requisition_id"], title=row["title"], location=row["location"],
            remote_type=row["remote_type"], department=row["department"], employment_type=row["employment_type"],
            salary_min=row["salary_min"], salary_max=row["salary_max"], currency=row["currency"],
            job_url=row["job_url"], apply_url=row["apply_url"], description_raw_html=row["description_raw_html"],
            description_text=row["description_text"], posted_at=row["posted_at"], status=row["status"],
            filter_reason=row["filter_reason"],
        )

    def fast_rate_job_with_llm(
        self,
        job_id: int,
        llm_client=call_openai_compatible_json,
        model_name: str | None = None,
        force: bool = False,
        persist: bool = True,
    ) -> sqlite3.Row | dict[str, Any]:
        profile_row = self.latest_active_llm_profile()
        if profile_row is None:
            raise ValueError("Run LLM profile extraction before fast-rating jobs.")
        job_row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if job_row is None:
            raise ValueError("Job not found.")
        model = model_name or configured_llm_model()
        job_hash = content_hash(job_row["title"], job_row["location"], job_row["description_text"], job_row["job_url"])
        if not force:
            cached = self.conn.execute(
                """
                SELECT * FROM job_fast_ratings
                WHERE job_id=? AND profile_extraction_id=? AND profile_hash=? AND job_content_hash=?
                  AND rater_version=? AND model_name=? AND is_current=1
                ORDER BY created_at DESC, id DESC LIMIT 1
                """,
                (job_id, profile_row["id"], profile_row["profile_hash"], job_hash, FAST_RATING_PROMPT_VERSION, model),
            ).fetchone()
            if cached:
                return cached
        profile = json.loads(profile_row["profile_json"])
        job_payload = {
            "title": job_row["title"],
            "company": job_row["company_name"],
            "location": job_row["location"],
            "department": job_row["department"],
            "description": job_row["description_text"],
            "job_url": job_row["job_url"],
        }
        system_prompt = "Return only valid JSON. You are a fast, recall-heavy job-fit triage classifier."
        user_prompt = f"""
Decide whether this job is plausibly a 7/10 or better fit for the candidate. This is a recall-biased prefilter, not the final rating.

Use the CANDIDATE PROFILE JSON extracted from the resume as the source of fit signals. Do not rely on hidden hardcoded keyword lists. Compare the job to the profile's target roles, skills, domains, proof points, seniority, strengths, gaps, and constraints.

Prefer false positives over false negatives. If the role has plausible alignment and no clear blocker, choose gte_7 so it can receive deeper rating later. DeepSeek/cheap fast models can be over-strict, so use the safest recall-preserving bucket, not the neatest-looking rejection.

Bucket policy:
- choose gte_7 for plausible 7+ fits, direct matches to extracted target roles, adjacent roles that use extracted transferable skills/domains/proof points, or uncertain but promising jobs with transferable evidence from the current profile.
- choose needs_manual_review when the evidence is ambiguous, mixed, or too thin to safely reject.
- choose needs_manual_review, not lt_7, for stretch seniority, international/onsite locations, customer-facing variants, unfamiliar title wording, or adjacent data/platform/operations roles when the profile has any transferable bridge. For recall safety, even explicit practical concerns such as location, work authorization, clearance, language, seniority, or missing specialized subdomain experience should usually be needs_manual_review when there is a plausible bridge, because deep rating or the user can verify feasibility.
- choose lt_7 only when the role is clearly irrelevant/wrong-function with no transferable bridge, or when a non-negotiable requirement explicitly conflicts with the extracted profile and there is no meaningful role/skill/domain/proof-point overlap to justify deeper review.

Do not use lt_7 for merely imperfect matches, missing one preferred skill, partial domain mismatch, stretch seniority, unfamiliar title wording, international location, customer-facing framing, explicit-but-reviewable practical concerns, or roles that need tailoring but still use the candidate's extracted target roles, transferable skills, domains, proof points, or strengths. Those should be gte_7 when plausible, or needs_manual_review when genuinely uncertain.

Profile-agnostic decision examples:
- Job title/function matches an extracted target role, or the description uses multiple extracted skills/domains/proof points with no hard blocker => gte_7.
- Job is adjacent to an extracted target role and uses at least one extracted transferable skill/domain/proof point, but seniority, location, customer-facing scope, authorization, clearance, language, or requirements are unclear or potentially difficult => needs_manual_review, not lt_7.
- Job has no meaningful overlap with extracted target roles/skills/domains/proof points, or has an explicit non-negotiable conflict plus no transferable bridge in the profile => lt_7.

Precision is secondary; the deep rater will verify. The fast rater's job is to avoid missing plausible >=7 roles.

Return JSON with keys:
- bucket: one of gte_7, lt_7, needs_manual_review
- confidence: low, medium, or high
- reason_codes: short machine-readable strings
- short_reason: one concise sentence

CANDIDATE PROFILE JSON:
{json.dumps(profile, ensure_ascii=False)}

JOB JSON:
{json.dumps(job_payload, ensure_ascii=False)}
""".strip()
        rating = normalize_fast_rating(llm_client(system_prompt=system_prompt, user_prompt=user_prompt, model_name=model))
        rating_json = json.dumps(rating, sort_keys=True, ensure_ascii=False)
        ts = now_iso()
        if not persist:
            return {
                "job_id": job_id,
                "profile_extraction_id": profile_row["id"],
                "profile_hash": profile_row["profile_hash"],
                "job_content_hash": job_hash,
                "rater_version": FAST_RATING_PROMPT_VERSION,
                "model_name": model,
                "bucket": rating["bucket"],
                "confidence": rating["confidence"],
                "reason_codes_json": json.dumps(rating["reason_codes"], ensure_ascii=False),
                "short_reason": rating["short_reason"],
                "rating_json": rating_json,
                "is_current": 0,
                "created_at": ts,
            }
        self.conn.execute("UPDATE job_fast_ratings SET is_current=0 WHERE job_id=?", (job_id,))
        cur = self.conn.execute(
            """
            INSERT INTO job_fast_ratings(
                job_id, profile_extraction_id, profile_hash, job_content_hash, rater_version,
                model_name, bucket, confidence, reason_codes_json, short_reason, rating_json, is_current, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (
                job_id, profile_row["id"], profile_row["profile_hash"], job_hash, FAST_RATING_PROMPT_VERSION,
                model, rating["bucket"], rating["confidence"], json.dumps(rating["reason_codes"], ensure_ascii=False),
                rating["short_reason"], rating_json, ts,
            ),
        )
        self.conn.commit()
        row = self.conn.execute("SELECT * FROM job_fast_ratings WHERE id=?", (cur.lastrowid,)).fetchone()
        assert row is not None
        return row

    def fast_rate_jobs_with_llm(
        self,
        job_ids: Iterable[int],
        llm_client=call_openai_compatible_json,
        model_name: str | None = None,
        force: bool = False,
        progress_callback=None,
        persist: bool = True,
    ) -> dict[str, Any]:
        unique_job_ids = list(dict.fromkeys(int(job_id) for job_id in job_ids))
        summary: dict[str, Any] = {"requested": len(unique_job_ids), "rated": 0, "failed": 0, "completed": 0, "errors": [], "last_job_id": None, "bucket_counts": {}}
        for job_id in unique_job_ids:
            try:
                row = self.fast_rate_job_with_llm(job_id, llm_client=llm_client, model_name=model_name, force=force, persist=persist)
                summary["rated"] += 1
                bucket = str(row["bucket"])
                summary["bucket_counts"][bucket] = summary["bucket_counts"].get(bucket, 0) + 1
                status = "rated"
                error = None
            except Exception as exc:
                summary["failed"] += 1
                error = str(exc)
                summary["errors"].append({"job_id": job_id, "error": error})
                status = "failed"
            summary["completed"] += 1
            summary["last_job_id"] = job_id
            if progress_callback:
                progress_callback({**summary, "status": status, "job_id": job_id, "error": error})
        return summary

    def rate_job_with_llm(
        self,
        job_id: int,
        llm_client=call_openai_compatible_json,
        model_name: str | None = None,
        force: bool = False,
        persist: bool = True,
    ) -> sqlite3.Row | dict[str, Any]:
        profile_row = self.latest_active_llm_profile()
        if profile_row is None:
            raise ValueError("Run LLM profile extraction before rating jobs.")
        job_row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if job_row is None:
            raise ValueError("Job not found.")
        model = model_name or configured_llm_model()
        job_hash = content_hash(job_row["title"], job_row["location"], job_row["description_text"], job_row["job_url"])
        if not force:
            cached = self.conn.execute(
                """
                SELECT * FROM job_ratings
                WHERE job_id=? AND profile_extraction_id=? AND profile_hash=? AND job_content_hash=?
                  AND rubric_version=? AND rater_version=? AND model_name=? AND is_current=1
                ORDER BY created_at DESC, id DESC LIMIT 1
                """,
                (job_id, profile_row["id"], profile_row["profile_hash"], job_hash, RATING_PROMPT_VERSION, RATING_PROMPT_VERSION, model),
            ).fetchone()
            if cached:
                return cached
        profile = json.loads(profile_row["profile_json"])
        job_payload = {
            "title": job_row["title"],
            "company": job_row["company_name"],
            "location": job_row["location"],
            "department": job_row["department"],
            "description": job_row["description_text"],
            "job_url": job_row["job_url"],
        }
        system_prompt = "Return only valid JSON. You are a rigorous job-fit evaluator using the provided rubric."
        user_prompt = f"""
Rate this job for the candidate using expected interview probability plus role fit, not title similarity.
Use only the CANDIDATE PROFILE JSON below for candidate-specific roles, domains, skills, proof points, gaps, constraints, and evidence. Do not add hidden candidate assumptions, hardcoded examples, or external profile knowledge.

REFERENCE 7-CATEGORY RUBRIC:
{REFERENCE_RUBRIC}

Return JSON with keys: overall_score, skill_fit_score, practical_fit_score, recommendation, categories, strongest_evidence, main_gaps, practical_notes, resume_tailoring_notes, interview_probability_reasoning, apply_decision.
Each category must include score, reason, matched_evidence, and gaps when applicable.
Use category names aligned to the rubric when possible: core_job_function_match, experience_seniority_match, domain_match, technical_tool_match, resume_evidence_strength, gap_severity, strategic_career_value.
Make the recommendation consistent with the numeric score band. Use 8.5+ for strong credible interview targets with manageable gaps; do not require a perfect match for 8.5-8.9.

CANDIDATE PROFILE JSON:
{json.dumps(profile, ensure_ascii=False)}

JOB JSON:
{json.dumps(job_payload, ensure_ascii=False)}
""".strip()
        rating = normalize_llm_rating(llm_client(system_prompt=system_prompt, user_prompt=user_prompt, model_name=model))
        rating_json = json.dumps(rating, sort_keys=True, ensure_ascii=False)
        ts = now_iso()
        if not persist:
            return {
                "job_id": job_id,
                "profile_extraction_id": profile_row["id"],
                "profile_hash": profile_row["profile_hash"],
                "job_content_hash": job_hash,
                "rubric_version": RATING_PROMPT_VERSION,
                "rater_version": RATING_PROMPT_VERSION,
                "model_name": model,
                "overall_score": rating["overall_score"],
                "skill_fit_score": rating["skill_fit_score"],
                "practical_fit_score": rating["practical_fit_score"],
                "recommendation": rating["recommendation"],
                "rating_json": rating_json,
                "is_current": 0,
                "created_at": ts,
            }
        self.conn.execute("UPDATE job_ratings SET is_current=0 WHERE job_id=?", (job_id,))
        cur = self.conn.execute(
            """
            INSERT INTO job_ratings(
                job_id, profile_extraction_id, profile_hash, job_content_hash, rubric_version, rater_version,
                model_name, overall_score, skill_fit_score, practical_fit_score, recommendation, rating_json, is_current, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (
                job_id, profile_row["id"], profile_row["profile_hash"], job_hash, RATING_PROMPT_VERSION, RATING_PROMPT_VERSION,
                model, rating["overall_score"], rating["skill_fit_score"], rating["practical_fit_score"], rating["recommendation"], rating_json, ts,
            ),
        )
        self.conn.commit()
        row = self.conn.execute("SELECT * FROM job_ratings WHERE id=?", (cur.lastrowid,)).fetchone()
        assert row is not None
        return row

    def rate_jobs_with_llm(
        self,
        job_ids: Iterable[int],
        llm_client=call_openai_compatible_json,
        model_name: str | None = None,
        force: bool = False,
        max_workers: int | None = None,
        progress_callback=None,
    ) -> dict[str, Any]:
        unique_job_ids = list(dict.fromkeys(int(job_id) for job_id in job_ids))
        worker_count = max_workers if max_workers is not None else int(os.environ.get("JOBSEARCH_LLM_MAX_WORKERS", "4") or "4")
        worker_count = max(1, min(int(worker_count), max(1, len(unique_job_ids)) if unique_job_ids else 1))
        summary: dict[str, Any] = {
            "requested": len(unique_job_ids),
            "rated": 0,
            "failed": 0,
            "completed": 0,
            "errors": [],
            "max_workers": worker_count,
            "last_job_id": None,
        }
        lock = threading.Lock()

        def report(event: dict[str, Any]) -> None:
            if progress_callback:
                progress_callback(dict(event))

        def update(status: str, job_id: int, error: str | None = None) -> None:
            with lock:
                summary["completed"] += 1
                summary["last_job_id"] = job_id
                if status in {"rated", "cached"}:
                    summary["rated"] += 1
                else:
                    summary["failed"] += 1
                    summary["errors"].append({"job_id": job_id, "error": error or "unknown error"})
                event = {
                    "status": status,
                    "job_id": job_id,
                    "requested": summary["requested"],
                    "rated": summary["rated"],
                    "failed": summary["failed"],
                    "completed": summary["completed"],
                    "last_job_id": summary["last_job_id"],
                }
            report(event)

        def rate_one(job_id: int) -> tuple[int, str, str | None]:
            worker_db = self if worker_count == 1 else Database(self.path)
            try:
                before_id = None
                if not force:
                    latest = worker_db.latest_job_rating(job_id)
                    before_id = latest["id"] if latest else None
                row = worker_db.rate_job_with_llm(job_id, llm_client=llm_client, model_name=model_name, force=force)
                status = "cached" if before_id is not None and row["id"] == before_id else "rated"
                return job_id, status, None
            except Exception as exc:
                return job_id, "failed", str(exc)
            finally:
                if worker_db is not self:
                    try:
                        worker_db.conn.close()
                    except Exception:
                        pass

        if worker_count == 1:
            for job_id in unique_job_ids:
                job_id, status, error = rate_one(job_id)
                update(status, job_id, error)
            return summary

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="llm-rating") as executor:
            futures = [executor.submit(rate_one, job_id) for job_id in unique_job_ids]
            for future in concurrent.futures.as_completed(futures):
                job_id, status, error = future.result()
                update(status, job_id, error)
        return summary

    def migrate_deprecated_statuses_to_new(self) -> None:
        self.conn.execute(
            """
            UPDATE jobs
            SET status='new', filter_reason='ready for rating', updated_at=?
            WHERE status IN ('candidate', 'filtered_out')
            """,
            (now_iso(),),
        )
        self.conn.commit()

    def migrate_review_state_columns(self) -> None:
        existing = {row["name"] for row in self.conn.execute("PRAGMA table_info(jobs)")}
        column_defs = {
            "source_status": "TEXT NOT NULL DEFAULT 'newly_discovered'",
            "review_status": "TEXT NOT NULL DEFAULT 'unreviewed'",
            "is_hidden": "INTEGER NOT NULL DEFAULT 0",
            "is_saved": "INTEGER NOT NULL DEFAULT 0",
            "saved_at": "TEXT",
            "reviewed_at": "TEXT",
            "applied_at": "TEXT",
        }
        for name, ddl in column_defs.items():
            if name not in existing:
                self.conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {ddl}")
        ts = now_iso()
        self.conn.execute("UPDATE jobs SET source_status='expired' WHERE status='expired'")
        self.conn.execute("UPDATE jobs SET source_status='active' WHERE status='hidden' AND source_status='newly_discovered'")
        self.conn.execute("UPDATE jobs SET is_hidden=1 WHERE status='hidden'")
        self.conn.execute("UPDATE jobs SET review_status='unreviewed' WHERE review_status IS NULL OR review_status='' OR review_status NOT IN ('unreviewed','reviewed','applied')")
        self.conn.execute("UPDATE jobs SET is_saved=COALESCE(is_saved, 0)")
        self.conn.execute("UPDATE jobs SET is_hidden=COALESCE(is_hidden, 0), updated_at=? WHERE is_hidden IS NULL", (ts,))
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_source_status ON jobs(source_status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_review_status ON jobs(review_status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_hidden_saved ON jobs(is_hidden, is_saved)")
        self.conn.commit()

    def migrate_llm_columns(self) -> None:
        profile_columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(profile_extractions)")}
        profile_defs = {
            "extraction_method": "TEXT NOT NULL DEFAULT 'local'",
            "model_name": "TEXT",
            "prompt_version": "TEXT",
        }
        for name, ddl in profile_defs.items():
            if name not in profile_columns:
                self.conn.execute(f"ALTER TABLE profile_extractions ADD COLUMN {name} {ddl}")
        self.conn.execute("UPDATE profile_extractions SET extraction_method='local' WHERE extraction_method IS NULL OR extraction_method='' ")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_profile_extractions_method ON profile_extractions(extraction_method, is_active)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_job_ratings_current ON job_ratings(job_id, is_current)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_job_fast_ratings_current ON job_fast_ratings(job_id, is_current)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_job_ingestion_audits_job_id ON job_ingestion_audits(job_id, id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_job_raw_snapshots_job_id ON job_raw_snapshots(job_id, id)")
        self.conn.commit()

    def backfill_missing_ingestion_audits(self) -> None:
        rows = self.conn.execute(
            """
            SELECT j.*, rs.raw_json
            FROM jobs j
            LEFT JOIN job_raw_snapshots rs ON rs.id = (
                SELECT MAX(rs2.id) FROM job_raw_snapshots rs2 WHERE rs2.job_id = j.id
            )
            WHERE NOT EXISTS (SELECT 1 FROM job_ingestion_audits a WHERE a.job_id = j.id)
            """
        ).fetchall()
        if not rows:
            return
        ts = now_iso()
        for row in rows:
            raw_json = None
            if row["raw_json"]:
                try:
                    raw_json = json.loads(row["raw_json"])
                except Exception:
                    raw_json = None
            job = NormalizedJob(
                company_name=row["company_name"],
                source=row["source"],
                source_job_id=row["source_job_id"],
                requisition_id=row["requisition_id"],
                title=row["title"],
                location=row["location"],
                remote_type=row["remote_type"],
                department=row["department"],
                employment_type=row["employment_type"],
                salary_min=row["salary_min"],
                salary_max=row["salary_max"],
                currency=row["currency"],
                job_url=row["job_url"],
                apply_url=row["apply_url"],
                description_raw_html=row["description_raw_html"],
                description_text=row["description_text"],
                posted_at=row["posted_at"],
                status=row["status"],
                filter_reason=row["filter_reason"],
                raw_json=raw_json,
            )
            audit = build_ingestion_audit(job)
            self.conn.execute(
                """
                INSERT INTO job_ingestion_audits(
                    job_id, source, required_missing_json, optional_missing_json, extra_keys_json, warnings_json,
                    description_length, location_status, detail_fetch_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"], row["source"], json.dumps(audit["required_missing"], ensure_ascii=False),
                    json.dumps(audit["optional_missing"], ensure_ascii=False), json.dumps(audit["extra_keys"], ensure_ascii=False),
                    json.dumps(audit["warnings"], ensure_ascii=False), audit["description_length"], audit["location_status"],
                    audit["detail_fetch_status"], ts,
                ),
            )
        self.conn.commit()

    def start_run(self, company_id: int) -> int:
        cur = self.conn.execute(
            "INSERT INTO job_fetch_runs(company_id, started_at, status) VALUES (?, ?, 'running')",
            (company_id, now_iso()),
        )
        self.conn.commit()
        if cur.lastrowid is None:
            raise RuntimeError("failed to create fetch run")
        return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, found: int, created: int, updated: int, expired: int, error: str | None = None) -> None:
        self.conn.execute(
            """
            UPDATE job_fetch_runs
            SET finished_at=?, status=?, jobs_found=?, jobs_created=?, jobs_updated=?, jobs_expired=?, error_message=?
            WHERE id=?
            """,
            (now_iso(), status, found, created, updated, expired, error, run_id),
        )
        self.conn.commit()

    def latest_fetch_runs(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT r.*, c.name AS company_name, c.slug AS company_slug
            FROM job_fetch_runs r
            JOIN companies c ON c.id = r.company_id
            ORDER BY r.started_at DESC, r.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    def upsert_jobs(self, company: sqlite3.Row, jobs: Iterable[NormalizedJob], refresh_mode: str = "full") -> tuple[int, int, list[int]]:
        created = updated = 0
        seen_ids: list[int] = []
        ts = now_iso()
        for job in jobs:
            status, reason = filter_job(job)
            job.status = status
            job.filter_reason = reason
            chash = content_hash(job.title, job.location, job.description_text, job.job_url)
            existing = self.conn.execute(
                "SELECT id, content_hash, status, source_status, is_hidden FROM jobs WHERE company_id=? AND source=? AND source_job_id=?",
                (company["id"], job.source, job.source_job_id),
            ).fetchone()
            if existing is None:
                cur = self.conn.execute(
                    """
                    INSERT INTO jobs(
                        company_id, company_name, source, source_job_id, requisition_id, title, location, remote_type,
                        department, employment_type, salary_min, salary_max, currency, job_url, apply_url,
                        description_raw_html, description_text, posted_at, first_seen_at, last_seen_at, status,
                        source_status, review_status, is_hidden, is_saved, filter_reason, content_hash, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?)
                    """,
                    (
                        company["id"], job.company_name, job.source, job.source_job_id, job.requisition_id, job.title,
                        job.location, job.remote_type, job.department, job.employment_type, job.salary_min, job.salary_max,
                        job.currency, job.job_url, job.apply_url, job.description_raw_html, job.description_text, job.posted_at,
                        ts, ts, job.status, "newly_discovered", "unreviewed", job.filter_reason, chash, ts, ts,
                    ),
                )
                if cur.lastrowid is None:
                    raise RuntimeError("failed to insert job")
                job_id = int(cur.lastrowid)
                created += 1
            else:
                job_id = int(existing["id"])
                source_status = existing["source_status"]
                if refresh_mode == "full" and source_status == "newly_discovered":
                    source_status = "active"
                if source_status == "expired":
                    source_status = "active"
                preserved_status = "hidden" if existing["is_hidden"] else job.status
                self.conn.execute(
                    """
                    UPDATE jobs SET
                        company_name=?, requisition_id=?, title=?, location=?, remote_type=?, department=?, employment_type=?,
                        salary_min=?, salary_max=?, currency=?, job_url=?, apply_url=?, description_raw_html=?, description_text=?,
                        posted_at=?, last_seen_at=?, status=?, source_status=?, filter_reason=?, content_hash=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        job.company_name, job.requisition_id, job.title, job.location, job.remote_type, job.department,
                        job.employment_type, job.salary_min, job.salary_max, job.currency, job.job_url, job.apply_url,
                        job.description_raw_html, job.description_text, job.posted_at, ts, preserved_status, source_status,
                        job.filter_reason, chash, ts, job_id,
                    ),
                )
                updated += 1
            if job.raw_json:
                self.conn.execute(
                    "INSERT INTO job_raw_snapshots(job_id, source, raw_json, fetched_at) VALUES (?, ?, ?, ?)",
                    (job_id, job.source, json.dumps(job.raw_json, ensure_ascii=False), ts),
                )
            audit = build_ingestion_audit(job)
            self.conn.execute(
                """
                INSERT INTO job_ingestion_audits(
                    job_id, source, required_missing_json, optional_missing_json, extra_keys_json, warnings_json,
                    description_length, location_status, detail_fetch_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id, job.source, json.dumps(audit["required_missing"], ensure_ascii=False),
                    json.dumps(audit["optional_missing"], ensure_ascii=False), json.dumps(audit["extra_keys"], ensure_ascii=False),
                    json.dumps(audit["warnings"], ensure_ascii=False), audit["description_length"], audit["location_status"],
                    audit["detail_fetch_status"], ts,
                ),
            )
            seen_ids.append(job_id)
        self.conn.commit()
        return created, updated, seen_ids

    def expire_missing(self, company_id: int, seen_ids: list[int]) -> int:
        if not seen_ids:
            return 0
        placeholders = ",".join("?" for _ in seen_ids)
        params: list[Any] = [now_iso(), company_id, *seen_ids]
        cur = self.conn.execute(
            f"""
            UPDATE jobs
            SET status='expired', source_status='expired', updated_at=?
            WHERE company_id=? AND is_hidden != 1 AND id NOT IN ({placeholders})
            """,
            params,
        )
        self.conn.commit()
        return int(cur.rowcount)
    def hide_job(self, job_id: int) -> bool:
        cur = self.conn.execute(
            "UPDATE jobs SET status='hidden', is_hidden=1, updated_at=? WHERE id=?",
            (now_iso(), job_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def unhide_job(self, job_id: int) -> bool:
        cur = self.conn.execute(
            "UPDATE jobs SET status=CASE WHEN source_status='expired' THEN 'expired' ELSE 'new' END, is_hidden=0, filter_reason='ready for rating', updated_at=? WHERE id=? AND is_hidden=1",
            (now_iso(), job_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def unhide_all_jobs(self) -> int:
        cur = self.conn.execute(
            "UPDATE jobs SET status=CASE WHEN source_status='expired' THEN 'expired' ELSE 'new' END, is_hidden=0, filter_reason='ready for rating', updated_at=? WHERE is_hidden=1",
            (now_iso(),),
        )
        self.conn.commit()
        return int(cur.rowcount)

    def save_job(self, job_id: int) -> bool:
        cur = self.conn.execute("UPDATE jobs SET is_saved=1, saved_at=COALESCE(saved_at, ?), updated_at=? WHERE id=?", (now_iso(), now_iso(), job_id))
        self.conn.commit()
        return cur.rowcount > 0

    def unsave_job(self, job_id: int) -> bool:
        cur = self.conn.execute("UPDATE jobs SET is_saved=0, saved_at=NULL, updated_at=? WHERE id=?", (now_iso(), job_id))
        self.conn.commit()
        return cur.rowcount > 0

    def mark_reviewed(self, job_id: int) -> bool:
        cur = self.conn.execute("UPDATE jobs SET review_status='reviewed', reviewed_at=COALESCE(reviewed_at, ?), updated_at=? WHERE id=? AND review_status != 'applied'", (now_iso(), now_iso(), job_id))
        self.conn.commit()
        return cur.rowcount > 0

    def mark_applied(self, job_id: int) -> str | None:
        row = self.conn.execute("SELECT apply_url, job_url FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return None
        ts = now_iso()
        self.conn.execute("UPDATE jobs SET review_status='applied', applied_at=COALESCE(applied_at, ?), updated_at=? WHERE id=?", (ts, ts, job_id))
        self.conn.commit()
        return row["apply_url"] or row["job_url"]


VALID_REFRESH_MODES = {"full", "search"}


def matches_search_text(*values: Any, search_text: str) -> bool:
    query = search_text.strip().lower()
    if not query:
        return True
    haystack = " ".join(str(v or "") for v in values).lower()
    return query in haystack


def location_terms(location_filter: str = "") -> list[str]:
    return [part.strip().lower() for part in location_filter.split(",") if part.strip()]


def matches_location_text(location: Any, location_filter: str = "") -> bool:
    terms = location_terms(location_filter)
    if not terms:
        return True
    haystack = str(location or "").lower()
    return any(term in haystack for term in terms)


def should_expire_missing_after_refresh(mode: str, limit: int | None, search_text: str = "", location_filter: str = "") -> bool:
    return mode == "full"


class GreenhouseConnector:
    source = "greenhouse"

    def fetch_list(
        self,
        company: sqlite3.Row,
        limit: int | None = None,
        mode: str = "full",
        search_text: str = "",
        location_filter: str = "",
    ) -> list[dict[str, Any]]:
        payload = http_json(company["source_api_url"])
        raw_jobs = payload.get("jobs", [])
        if search_text:
            raw_jobs = [
                raw for raw in raw_jobs
                if matches_search_text(
                    raw.get("title"),
                    (raw.get("location") or {}).get("name"),
                    raw.get("content"),
                    search_text=search_text,
                )
            ]
        if location_filter:
            raw_jobs = [raw for raw in raw_jobs if matches_location_text((raw.get("location") or {}).get("name"), location_filter)]
        if limit:
            raw_jobs = raw_jobs[:limit]
        return raw_jobs

    def fetch_detail_if_needed(self, company: sqlite3.Row, raw: dict[str, Any]) -> dict[str, Any]:
        # Greenhouse `content=true` already includes full job descriptions in
        # the list response, so no additional detail call is needed.
        return raw

    def normalize(self, company: sqlite3.Row, raw: dict[str, Any]) -> NormalizedJob:
        title = raw.get("title") or "Untitled"
        location = (raw.get("location") or {}).get("name")
        departments = raw.get("departments") or []
        department = ", ".join(d.get("name", "") for d in departments if d.get("name")) or None
        desc_html = raw.get("content") or ""
        return NormalizedJob(
            company_name=company["name"],
            source=self.source,
            source_job_id=str(raw.get("id") or raw.get("internal_job_id") or raw.get("requisition_id") or title),
            requisition_id=str(raw.get("requisition_id") or raw.get("internal_job_id") or raw.get("id")),
            title=title,
            location=location,
            remote_type=classify_remote(location, title, desc_html),
            department=department,
            employment_type=None,
            salary_min=None,
            salary_max=None,
            currency=None,
            job_url=raw.get("absolute_url") or company["careers_url"],
            apply_url=raw.get("absolute_url") or company["careers_url"],
            description_raw_html=desc_html,
            description_text=strip_html(desc_html),
            posted_at=raw.get("first_published") or raw.get("updated_at"),
            raw_json=raw,
        )

    def fetch(self, company: sqlite3.Row, limit: int | None = None, mode: str = "full", search_text: str = "", location_filter: str = "") -> list[NormalizedJob]:
        raw_jobs = self.fetch_list(company, limit=limit, mode=mode, search_text=search_text, location_filter=location_filter)
        return [self.normalize(company, self.fetch_detail_if_needed(company, raw)) for raw in raw_jobs]


class NvidiaWorkdayConnector:
    source = "workday"
    detail_base = "https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite"
    public_base = "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite"

    def fetch_list(
        self,
        company: sqlite3.Row,
        limit: int | None = None,
        mode: str = "full",
        search_text: str = "",
        location_filter: str = "",
    ) -> list[dict[str, Any]]:
        # NVIDIA's Workday CXS endpoint currently returns HTTP 400 for page sizes
        # above 20, even though many Workday examples use 100. Keep this capped
        # and paginate for larger refreshes.
        page_size = 20
        offset = 0
        postings: list[dict[str, Any]] = []
        total: int | None = None
        while True:
            remaining = None if limit is None else max(limit - len(postings), 0)
            if remaining == 0:
                break
            if total is not None and offset >= total:
                break
            this_limit = min(page_size, remaining) if remaining else page_size
            data = http_json(
                company["source_api_url"],
                method="POST",
                payload={"appliedFacets": {}, "limit": this_limit, "offset": offset, "searchText": search_text},
            )
            response_total = int(data.get("total", 0) or 0)
            # Workday returns the real total on the first page, but NVIDIA's
            # endpoint may return total=0 on later pages. Preserve the first
            # positive total instead of overwriting it with zero.
            if total is None and response_total > 0:
                total = response_total
            page = data.get("jobPostings", [])
            if not page:
                break
            raw_page_len = len(page)
            if location_filter:
                page = [raw for raw in page if matches_location_text(raw.get("locationsText"), location_filter)]
            postings.extend(page)
            offset += raw_page_len
        return postings[:limit] if limit is not None else postings

    def fetch_detail_if_needed(self, company: sqlite3.Row, raw: dict[str, Any]) -> dict[str, Any]:
        external_path = raw.get("externalPath") or ""
        detail_url = self.detail_base + external_path
        try:
            detail = http_json(detail_url)
            detail_info = detail.get("jobPostingInfo", {}) if isinstance(detail, dict) else {}
            if not isinstance(detail_info, dict):
                detail_info = {}
        except Exception as exc:  # keep listing even if detail fetch fails
            detail = {"detail_fetch_error": str(exc)}
            detail_info = {}
        return {"listing": raw, "detail": detail, "detail_info": detail_info}

    def normalize(self, company: sqlite3.Row, enriched: dict[str, Any]) -> NormalizedJob:
        raw = enriched.get("listing", {})
        detail = enriched.get("detail", {})
        info = enriched.get("detail_info", {})
        if not isinstance(info, dict):
            info = {}
        external_path = raw.get("externalPath") or ""
        public_url = self.public_base + external_path
        desc_html = info.get("jobDescription") or ""
        posted_at = info.get("postedOn") or raw.get("postedOn")
        title = str(info.get("title") or raw.get("title") or "Untitled")
        location = info.get("location") or raw.get("locationsText")
        req = None
        bullet_fields = raw.get("bulletFields") or []
        if bullet_fields:
            req = str(bullet_fields[0])
        req = req or info.get("jobReqId") or info.get("id") or external_path.rsplit("_", 1)[-1]
        combined_raw = {"listing": raw, "detail": detail}
        return NormalizedJob(
            company_name=company["name"],
            source=self.source,
            source_job_id=str(req or external_path or title),
            requisition_id=str(req) if req else None,
            title=title,
            location=location,
            remote_type=classify_remote(location, title, desc_html),
            department=None,
            employment_type=None,
            salary_min=None,
            salary_max=None,
            currency=None,
            job_url=public_url,
            apply_url=public_url,
            description_raw_html=desc_html,
            description_text=strip_html(desc_html),
            posted_at=posted_at,
            raw_json=combined_raw,
        )

    def fetch(self, company: sqlite3.Row, limit: int | None = None, mode: str = "full", search_text: str = "", location_filter: str = "") -> list[NormalizedJob]:
        postings = self.fetch_list(company, limit=limit, mode=mode, search_text=search_text, location_filter=location_filter)
        return [self.normalize(company, self.fetch_detail_if_needed(company, raw)) for raw in postings]


def connector_for(company: sqlite3.Row):
    if company["ats_type"] == "greenhouse":
        return GreenhouseConnector()
    if company["ats_type"] == "workday":
        return NvidiaWorkdayConnector()
    raise ValueError(f"Unsupported ATS type: {company['ats_type']}")


def refresh(slug: str | None = None, limit: int | None = None, mode: str = "full", search_text: str = "", location_filter: str = "") -> list[dict[str, Any]]:
    if mode not in VALID_REFRESH_MODES:
        raise ValueError(f"Unsupported refresh mode: {mode}")
    if mode == "full":
        limit = None
        search_text = ""
        location_filter = ""
    db = Database()
    db.init()
    summaries = []
    companies = db.companies(slug)
    if not companies:
        raise SystemExit(f"No enabled company found for slug={slug!r}")
    for company in companies:
        run_id = db.start_run(company["id"])
        found = created = updated = expired = 0
        try:
            jobs = connector_for(company).fetch(company, limit=limit, mode=mode, search_text=search_text, location_filter=location_filter)
            found = len(jobs)
            created, updated, seen_ids = db.upsert_jobs(company, jobs, refresh_mode=mode)
            if should_expire_missing_after_refresh(mode=mode, limit=limit, search_text=search_text, location_filter=location_filter):
                expired = db.expire_missing(company["id"], seen_ids)
            else:
                expired = 0
            db.finish_run(run_id, "success", found, created, updated, expired)
            summaries.append({"company": company["name"], "status": "success", "found": found, "created": created, "updated": updated, "expired": expired})
        except Exception as exc:
            db.finish_run(run_id, "error", found, created, updated, expired, str(exc))
            summaries.append({"company": company["name"], "status": "error", "error": str(exc)})
    return summaries


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def tooltip(text: str) -> str:
    escaped = html.escape("" if text is None else str(text), quote=True).replace("&#x27;", "'")
    return f'<span class="tooltip" tabindex="0" data-tooltip="{escaped}" title="{escaped}">?</span>'


def render_refresh_status_section(db: Database | None = None, limit: int = 6) -> str:
    close_db = False
    if db is None:
        db = Database()
        db.init()
        close_db = True
    try:
        runs = db.latest_fetch_runs(limit=limit)
    finally:
        if close_db:
            db.conn.close()
    if not runs:
        return """
        <section class=\"refresh-status\" aria-label=\"Daily full refresh status\">
          <h2>Daily full refresh status</h2>
          <p class=\"hint\">Scheduled for 12:00 AM daily. No refresh runs recorded yet.</p>
        </section>
        """
    items = []
    for run in runs:
        status = run["status"]
        status_label = "✅ success" if status == "success" else ("❌ failed" if status == "error" else f"⏳ {status}")
        finished = display_local_time(run["finished_at"]) if run["finished_at"] else "still running"
        started = display_local_time(run["started_at"])
        error_html = f'<p class=\"error\">Error: {esc(run["error_message"])}</p>' if run["error_message"] else ""
        items.append(
            f"""
            <li class=\"refresh-run status-{esc(status)}\">
              <strong>{esc(run['company_name'])}</strong>: {status_label}
              <span class=\"meta\">started {esc(started)}; finished {esc(finished)}</span>
              <span class=\"meta\">found={esc(run['jobs_found'])} created={esc(run['jobs_created'])} updated={esc(run['jobs_updated'])} expired={esc(run['jobs_expired'])}</span>
              {error_html}
            </li>
            """
        )
    return f"""
    <section class=\"refresh-status\" aria-label=\"Daily full refresh status\">
      <h2>Daily full refresh status</h2>
      <p class=\"hint\">Scheduled for 12:00 AM daily via Hermes cron. Latest refresh outcomes are shown below.</p>
      <ul>{''.join(items)}</ul>
    </section>
    """


SORT_OPTIONS: dict[str, tuple[str, str]] = {
    "default": ("Recommended/default", "CASE j.source_status WHEN 'newly_discovered' THEN 0 WHEN 'active' THEN 1 WHEN 'expired' THEN 2 ELSE 3 END, CASE j.review_status WHEN 'unreviewed' THEN 0 WHEN 'reviewed' THEN 1 WHEN 'applied' THEN 2 ELSE 3 END, j.last_seen_at DESC, j.company_name, j.title"),
    "newest": ("Newest first", "j.first_seen_at DESC, j.last_seen_at DESC, j.company_name, j.title"),
    "oldest": ("Oldest first", "j.first_seen_at ASC, j.company_name, j.title"),
    "last_seen_desc": ("Last seen recently", "j.last_seen_at DESC, j.company_name, j.title"),
    "rating_desc": ("Highest LLM fit score", "COALESCE(jr.overall_score, -1) DESC, j.last_seen_at DESC, j.company_name, j.title"),
    "company_asc": ("Company A-Z", "j.company_name ASC, j.title ASC"),
    "title_asc": ("Title A-Z", "j.title ASC, j.company_name ASC"),
}


def order_by_sql(sort_key: str) -> str:
    return SORT_OPTIONS.get(sort_key, SORT_OPTIONS["default"])[1]


def sort_options_html(selected: str) -> str:
    return "".join(
        f'<option value="{esc(value)}" {"selected" if selected == value else ""}>{esc(label)}</option>'
        for value, (label, _sql) in SORT_OPTIONS.items()
    )


def parse_positive_int(value: str | None, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        parsed = default
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def query_string(params: dict[str, list[str]], **updates: Any) -> str:
    merged: dict[str, str] = {k: v[0] for k, v in params.items() if v and v[0] != ""}
    for key, value in updates.items():
        if value is None or value == "":
            merged.pop(key, None)
        else:
            merged[key] = str(value)
    return urllib.parse.urlencode(merged)


def parse_refresh_form(params: dict[str, list[str]]) -> tuple[dict[str, Any], dict[str, str]]:
    mode = params.get("mode", ["full"])[0] or "full"
    if mode not in VALID_REFRESH_MODES:
        raise ValueError(f"Unsupported refresh mode: {mode}")
    company = params.get("company", [""])[0]
    if company not in {"", "databricks", "nvidia"}:
        raise ValueError(f"Unsupported company: {company}")
    raw_limit = params.get("limit", [""])[0].strip()
    limit = None
    if raw_limit:
        try:
            limit = int(raw_limit)
        except ValueError as exc:
            raise ValueError("Limit must be a positive integer") from exc
        if limit < 1:
            raise ValueError("Limit must be a positive integer")
    source_q = params.get("source_q", [""])[0].strip()
    source_location = params.get("source_location", [""])[0].strip()
    if mode == "full":
        limit = None
        source_location = ""
    search_text = source_q if mode == "search" else ""
    request = {"mode": mode, "search_text": search_text, "location_filter": source_location, "slug": company or None, "limit": limit}
    redirect_params: dict[str, str] = {}
    if company:
        redirect_params["company"] = company
    if source_location:
        redirect_params["location"] = source_location
    if mode == "search" and source_q:
        redirect_params["q"] = source_q
    return request, redirect_params


def rating_status_condition(rating_status: str) -> str | None:
    if rating_status == "unrated":
        return "jr.id IS NULL AND jfr.id IS NULL"
    if rating_status == "fast_rated":
        return "jr.id IS NULL AND jfr.id IS NOT NULL AND jfr.bucket != 'needs_manual_review'"
    if rating_status == "needs_manual_review":
        return "jr.id IS NULL AND jfr.id IS NOT NULL AND jfr.bucket = 'needs_manual_review'"
    if rating_status == "deep_rated":
        return "jr.id IS NOT NULL"
    return None


def current_fast_rating_bucket(db: Database, job_id: int) -> str | None:
    row = db.latest_job_fast_rating(job_id)
    return str(row["bucket"]) if row else None


def query_jobs_from_db(db: Database, params: dict[str, list[str]]) -> tuple[list[sqlite3.Row], int, int, int, int]:
    where = []
    values: list[Any] = []
    company = params.get("company", [""])[0]
    status = params.get("status", [""])[0]
    source_status = params.get("source_status", [status])[0]
    review_status = params.get("review_status", [""])[0]
    rating_status = params.get("rating_status", [""])[0]
    saved_mode = params.get("saved", [""])[0]
    hidden_mode = params.get("hidden", [""])[0] or "exclude"
    q = params.get("q", [""])[0].strip()
    location = params.get("location", [""])[0].strip()
    exclude_title = params.get("exclude_title", [""])[0].strip()
    sort_key = params.get("sort", ["default"])[0] or "default"
    page = parse_positive_int(params.get("page", ["1"])[0], 1)
    per_page = parse_positive_int(params.get("per_page", ["100"])[0], 100, maximum=500)

    if company:
        where.append("c.slug=?")
        values.append(company)
    if source_status:
        where.append("j.source_status=?")
        values.append(source_status)
    if review_status:
        where.append("j.review_status=?")
        values.append(review_status)
    rating_condition = rating_status_condition(rating_status)
    if rating_condition:
        where.append(rating_condition)
    if saved_mode == "saved":
        where.append("j.is_saved=1")
    elif saved_mode == "unsaved":
        where.append("j.is_saved=0")
    if hidden_mode == "only":
        where.append("j.is_hidden = 1")
    elif hidden_mode != "include":
        where.append("j.is_hidden = 0")
    if q:
        like = f"%{q}%"
        where.append("(j.title LIKE ? OR j.company_name LIKE ? OR j.location LIKE ? OR j.description_text LIKE ?)")
        values.extend([like, like, like, like])
    terms = location_terms(location)
    if terms:
        where.append("(" + " OR ".join("j.location LIKE ?" for _ in terms) + ")")
        values.extend(f"%{term}%" for term in terms)
    exclude_terms = location_terms(exclude_title)
    if exclude_terms:
        where.append("(" + " AND ".join("j.title NOT LIKE ?" for _ in exclude_terms) + ")")
        values.extend(f"%{term}%" for term in exclude_terms)

    audit_join = """
        LEFT JOIN job_ingestion_audits a ON a.id = (
            SELECT MAX(a2.id) FROM job_ingestion_audits a2 WHERE a2.job_id = j.id
        )
    """
    rating_join = """
        LEFT JOIN job_ratings jr ON jr.id = (
            SELECT MAX(jr2.id) FROM job_ratings jr2 WHERE jr2.job_id = j.id AND jr2.is_current=1
        )
    """
    fast_rating_join = """
        LEFT JOIN job_fast_ratings jfr ON jfr.id = (
            SELECT MAX(jfr2.id) FROM job_fast_ratings jfr2 WHERE jfr2.job_id = j.id AND jfr2.is_current=1
        )
    """
    base_from = f"FROM jobs j JOIN companies c ON c.id=j.company_id {audit_join} {rating_join} {fast_rating_join}"
    where_sql = " WHERE " + " AND ".join(where) if where else ""
    order_sql = order_by_sql(sort_key)
    total = int(db.conn.execute(f"SELECT COUNT(*) {base_from}{where_sql}", values).fetchone()[0])
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page

    sql = f"""
        SELECT j.*, c.slug AS company_slug,
               COALESCE(a.warnings_json, '[]') AS audit_warnings_json,
               COALESCE(a.required_missing_json, '[]') AS audit_required_missing_json,
               COALESCE(a.optional_missing_json, '[]') AS audit_optional_missing_json,
               COALESCE(a.extra_keys_json, '[]') AS audit_extra_keys_json,
               COALESCE(a.description_length, LENGTH(COALESCE(j.description_text, ''))) AS audit_description_length,
               COALESCE(a.location_status, 'unknown') AS audit_location_status,
               COALESCE(a.detail_fetch_status, 'unknown') AS audit_detail_fetch_status,
               jr.overall_score AS deep_overall_score,
               jr.recommendation AS deep_recommendation,
               jfr.bucket AS fast_rating_bucket,
               jfr.confidence AS fast_rating_confidence,
               jfr.short_reason AS fast_rating_reason
        {base_from}
        {where_sql}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
    """
    rows = db.conn.execute(sql, (*values, per_page, offset)).fetchall()
    return rows, total, page, per_page, total_pages


def query_jobs(params: dict[str, list[str]]) -> tuple[list[sqlite3.Row], int, int, int, int]:
    db = Database()
    db.init()
    return query_jobs_from_db(db, params)


def query_job_ids_from_db(db: Database, params: dict[str, list[str]], scope: str = "all") -> list[int]:
    """Return job IDs for the current filters; scope='page' applies current pagination."""
    if scope == "page":
        rows, _, _, _, _ = query_jobs_from_db(db, params)
        return [int(row["id"]) for row in rows]
    where = []
    values: list[Any] = []
    company = params.get("company", [""])[0]
    status = params.get("status", [""])[0]
    source_status = params.get("source_status", [status])[0]
    review_status = params.get("review_status", [""])[0]
    rating_status = params.get("rating_status", [""])[0]
    saved_mode = params.get("saved", [""])[0]
    hidden_mode = params.get("hidden", [""])[0] or "exclude"
    q = params.get("q", [""])[0].strip()
    location = params.get("location", [""])[0].strip()
    exclude_title = params.get("exclude_title", [""])[0].strip()
    sort_key = params.get("sort", ["default"])[0] or "default"

    if company:
        where.append("c.slug=?")
        values.append(company)
    if source_status:
        where.append("j.source_status=?")
        values.append(source_status)
    if review_status:
        where.append("j.review_status=?")
        values.append(review_status)
    rating_condition = rating_status_condition(rating_status)
    if rating_condition:
        where.append(rating_condition)
    if saved_mode == "saved":
        where.append("j.is_saved=1")
    elif saved_mode == "unsaved":
        where.append("j.is_saved=0")
    if hidden_mode == "only":
        where.append("j.is_hidden = 1")
    elif hidden_mode != "include":
        where.append("j.is_hidden = 0")
    if q:
        like = f"%{q}%"
        where.append("(j.title LIKE ? OR j.company_name LIKE ? OR j.location LIKE ? OR j.description_text LIKE ?)")
        values.extend([like, like, like, like])
    terms = location_terms(location)
    if terms:
        where.append("(" + " OR ".join("j.location LIKE ?" for _ in terms) + ")")
        values.extend(f"%{term}%" for term in terms)
    exclude_terms = location_terms(exclude_title)
    if exclude_terms:
        where.append("(" + " AND ".join("j.title NOT LIKE ?" for _ in exclude_terms) + ")")
        values.extend(f"%{term}%" for term in exclude_terms)

    where_sql = " WHERE " + " AND ".join(where) if where else ""
    rating_join = """
        LEFT JOIN job_ratings jr ON jr.id = (
            SELECT MAX(jr2.id) FROM job_ratings jr2 WHERE jr2.job_id = j.id AND jr2.is_current=1
        )
    """
    fast_rating_join = """
        LEFT JOIN job_fast_ratings jfr ON jfr.id = (
            SELECT MAX(jfr2.id) FROM job_fast_ratings jfr2 WHERE jfr2.job_id = j.id AND jfr2.is_current=1
        )
    """
    order_sql = order_by_sql(sort_key)
    rows = db.conn.execute(
        f"""
        SELECT j.id
        FROM jobs j JOIN companies c ON c.id=j.company_id {rating_join} {fast_rating_join}
        {where_sql}
        ORDER BY {order_sql}
        """,
        values,
    ).fetchall()
    return [int(row["id"]) for row in rows]


def get_job_from_db(db: Database, job_id: int) -> sqlite3.Row | None:
    audit_join = """
        LEFT JOIN job_ingestion_audits a ON a.id = (
            SELECT MAX(a2.id) FROM job_ingestion_audits a2 WHERE a2.job_id = j.id
        )
    """
    return db.conn.execute(
        f"""
        SELECT j.*, c.slug AS company_slug,
               COALESCE(a.warnings_json, '[]') AS audit_warnings_json,
               COALESCE(a.required_missing_json, '[]') AS audit_required_missing_json,
               COALESCE(a.optional_missing_json, '[]') AS audit_optional_missing_json,
               COALESCE(a.extra_keys_json, '[]') AS audit_extra_keys_json,
               COALESCE(a.description_length, LENGTH(COALESCE(j.description_text, ''))) AS audit_description_length,
               COALESCE(a.location_status, 'unknown') AS audit_location_status,
               COALESCE(a.detail_fetch_status, 'unknown') AS audit_detail_fetch_status,
               EXISTS(SELECT 1 FROM job_raw_snapshots rs WHERE rs.job_id = j.id) AS raw_snapshot_available
        FROM jobs j JOIN companies c ON c.id=j.company_id {audit_join}
        WHERE j.id=?
        """,
        (job_id,),
    ).fetchone()


def get_job(job_id: int) -> sqlite3.Row | None:
    db = Database()
    db.init()
    return get_job_from_db(db, job_id)


def json_list(value: Any) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def audit_badges(row: sqlite3.Row) -> str:
    warnings = json_list(row["audit_warnings_json"] if "audit_warnings_json" in row.keys() else "[]")
    if not warnings:
        return ""
    return '<p class="audit-badges">' + " ".join(f'<span class="audit-badge">⚠ {esc(w)}</span>' for w in warnings[:3]) + "</p>"


def render_audit_section(row: sqlite3.Row | None) -> str:
    if row is None:
        return ""
    warnings = json_list(row["audit_warnings_json"])
    required_missing = json_list(row["audit_required_missing_json"])
    optional_missing = json_list(row["audit_optional_missing_json"])
    extra_keys = json_list(row["audit_extra_keys_json"])
    raw_available = bool(row["raw_snapshot_available"]) if "raw_snapshot_available" in row.keys() else False
    def list_text(items: list[str]) -> str:
        return esc(", ".join(items) if items else "None")
    warnings_html = "".join(f"<li>⚠ {esc(w)}</li>" for w in warnings) or "<li>None</li>"
    return f"""
          <h2>Ingestion audit</h2>
          <dl>
            <dt>Warnings</dt><dd><ul>{warnings_html}</ul></dd>
            <dt>Required missing</dt><dd>{list_text(required_missing)}</dd>
            <dt>Optional missing</dt><dd>{list_text(optional_missing)}</dd>
            <dt>Extra raw keys</dt><dd>{list_text(extra_keys[:20])}</dd>
            <dt>Description length</dt><dd>{esc(row['audit_description_length'])}</dd>
            <dt>Location status</dt><dd>{esc(row['audit_location_status'])}</dd>
            <dt>Detail fetch status</dt><dd>{esc(row['audit_detail_fetch_status'])}</dd>
            <dt>Raw snapshot</dt><dd>{'Raw snapshot available' if raw_available else 'No raw snapshot saved'}</dd>
          </dl>
    """


def render_profile_section(db: Database | None = None) -> str:
    close_db = False
    if db is None:
        db = Database()
        db.init()
        close_db = True
    try:
        resumes = db.active_resume_files()
        latest = db.latest_profile()
        is_current = db.latest_profile_is_current()
        bg_job = background_job_snapshot("llm_profile_extraction")
        bg_status = f'<p class="flash">{esc(bg_job["message"])}</p>' if bg_job else ""
        profile_html = "<p class=\"hint\">No profile extracted yet. Upload resume files, then press Extract/update profile with LLM when you are ready to spend an LLM call.</p>"
        if latest:
            profile = json.loads(latest["profile_json"])
            status = "Current for active resumes" if is_current else "Stale — active resume files changed"
            method = latest["extraction_method"] if "extraction_method" in latest.keys() else "local"
            if method == "llm":
                target_roles = ", ".join(profile.get("target_roles", [])[:12]) or "No target roles extracted yet"
                industries = ", ".join(profile.get("target_industries", [])[:12]) or "No target industries extracted yet"
                strengths = ", ".join(profile.get("core_strengths", [])[:12]) or "No core strengths extracted yet"
                technical = ", ".join(profile.get("technical_skills", [])[:20]) or "No technical skills extracted yet"
                domains = ", ".join(profile.get("domain_skills", [])[:20]) or "No domain skills extracted yet"
                gaps = ", ".join(profile.get("weaknesses_or_gaps", [])[:10]) or "No major gaps extracted yet"
                profile_html = f"""
              <p><strong>Profile status:</strong> {esc(status)}</p>
              <p><strong>Candidate summary:</strong> {esc(profile.get('candidate_summary', ''))}</p>
              <p><strong>Target roles:</strong> {esc(target_roles)}</p>
              <p><strong>Target industries:</strong> {esc(industries)}</p>
              <p><strong>Core strengths:</strong> {esc(strengths)}</p>
              <p><strong>Technical skills:</strong> {esc(technical)}</p>
              <p><strong>Domain skills:</strong> {esc(domains)}</p>
              <p><strong>Weaknesses/gaps:</strong> {esc(gaps)}</p>
              <p class="hint">Extractor: {esc(latest['extractor_version'])} · Model: {esc(latest['model_name'])} · Prompt: {esc(latest['prompt_version'])} · Created {esc(display_local_time(latest['created_at']))}</p>
            """
            else:
                skills = ", ".join(profile.get("skills", [])[:20]) or "No skills detected yet"
                domains = ", ".join(profile.get("domains", [])[:12]) or "No domains detected yet"
                proof_points = ", ".join(profile.get("proof_points", [])[:12]) or "No proof points detected yet"
                targets = ", ".join(profile.get("target_directions", [])[:8]) or "No target directions set yet"
                experience_baseline = profile.get("baseline_years_experience", "unknown")
                profile_html = f"""
              <p><strong>Profile status:</strong> {esc(status)}</p>
              <p><strong>Skills:</strong> {esc(skills)}</p>
              <p><strong>Domains:</strong> {esc(domains)}</p>
              <p><strong>Proof points:</strong> {esc(proof_points)}</p>
              <p><strong>Target directions:</strong> {esc(targets)}</p>
              <p><strong>Experience baseline:</strong> {esc(experience_baseline)}+ years</p>
              <p class="hint">Extractor: {esc(latest['extractor_version'])} · Created {esc(display_local_time(latest['created_at']))}</p>
            """
        if resumes:
            resume_items = "".join(
                f"""
                <li>
                  {esc(row['original_filename'])} <span class="hint">({row['file_size']} bytes, uploaded {esc(display_local_time(row['uploaded_at']))})</span>
                  {'<span class="audit-badge">text extracted</span>' if row['extracted_text'] else ''}
                  {f'<span class="audit-badge">⚠ {esc(row["extraction_error"])}</span>' if row['extraction_error'] else ''}
                  <form class="inline" method="post" action="/resumes/{row['id']}/remove">
                    <button type="submit">Remove</button>
                  </form>
                </li>
                """
                for row in resumes
            )
        else:
            resume_items = "<li>No active resume files uploaded.</li>"
        return f"""
        <section class="profile-panel">
          <h2>Resume/Profile</h2>
          <p class="hint">Uploading or removing files does not run LLM profile extraction automatically, so token usage stays under your control.</p>
          {bg_status}
          <form class="refresh" method="post" action="/resumes/upload" enctype="multipart/form-data">
            <input type="file" name="resume" accept=".pdf,.docx,.txt,.md" required />
            <button type="submit">Upload resume</button>
          </form>
          <form class="top-action" method="post" action="/profile/extract-llm">
            <button type="submit">Extract/update profile with LLM</button>
          </form>
          <h3>Active resume files</h3>
          <ul>{resume_items}</ul>
          <h3>Extracted profile</h3>
          {profile_html}
        </section>
        """
    finally:
        if close_db:
            db.conn.close()


def render_rating_badge_for_job(db: Database, job_id: int) -> str:
    rating = db.latest_job_rating(job_id)
    if rating:
        return f'<p class="rating-badge">Deep fit score: {esc(rating["overall_score"])} · {esc(rating["recommendation"])}</p>'
    fast_rating = db.latest_job_fast_rating(job_id)
    if fast_rating:
        return f'<p class="rating-badge">Fast rating: {esc(fast_rating_bucket_label(fast_rating["bucket"]))} · {esc(fast_rating["confidence"])} confidence</p>'
    return ""


def render_rating_section(db: Database, job_id: int) -> str:
    rating = db.latest_job_rating(job_id)
    rate_form = f"""
          <form class="top-action" method="post" action="/jobs/{job_id}/rate">
            <button type="submit">Rate this job with LLM</button>
          </form>
    """
    if not rating:
        return rate_form + "<p class=\"hint\">No LLM rating yet. Run LLM profile extraction first, then rate this job.</p>"
    data = json.loads(rating["rating_json"])
    evidence = ", ".join(str(x) for x in data.get("strongest_evidence", [])[:8]) or "None listed"
    gaps = ", ".join(str(x) for x in data.get("main_gaps", [])[:8]) or "None listed"
    tailoring = "; ".join(str(x) for x in data.get("resume_tailoring_notes", [])[:6]) or "None listed"
    return rate_form + f"""
          <section class="rating-panel">
            <h2>LLM job rating</h2>
            <p><strong>Fit score: {esc(rating['overall_score'])}</strong> — {esc(rating['recommendation'])}</p>
            <dl>
              <dt>Skill fit</dt><dd>{esc(rating['skill_fit_score'])}</dd>
              <dt>Practical fit</dt><dd>{esc(rating['practical_fit_score'])}</dd>
              <dt>Apply decision</dt><dd>{esc(data.get('apply_decision', ''))}</dd>
              <dt>Strongest evidence</dt><dd>{esc(evidence)}</dd>
              <dt>Main gaps</dt><dd>{esc(gaps)}</dd>
              <dt>Tailoring notes</dt><dd>{esc(tailoring)}</dd>
              <dt>Reasoning</dt><dd>{esc(data.get('interview_probability_reasoning', ''))}</dd>
            </dl>
            <p class="hint">Rater: {esc(rating['rater_version'])} · Model: {esc(rating['model_name'])} · Created {esc(display_local_time(rating['created_at']))}</p>
          </section>
    """


def render_bulk_rating_controls(params: dict[str, list[str]], page_job_ids: list[int], return_to: str) -> str:
    query_inputs = []
    for key, values in params.items():
        if key in {"flash"}:
            continue
        for value in values:
            query_inputs.append(f'<input type="hidden" name="{esc(key)}" value="{esc(value)}" />')
    page_inputs = "".join(f'<input type="hidden" name="job_id" value="{esc(job_id)}" />' for job_id in page_job_ids)
    common_return = f'<input type="hidden" name="return_to" value="{esc(return_to)}" />'
    return f"""
        <section class="top-action" aria-label="Bulk LLM rating controls">
          <form method="post" action="/jobs/rate-fast-bulk">
            <input type="hidden" name="scope" value="all" />
            {common_return}
            {''.join(query_inputs)}
            <button type="submit">Fast rate unrated matching jobs</button>
          </form>
          <form method="post" action="/jobs/rate-fast-bulk">
            <input type="hidden" name="scope" value="page" />
            {common_return}
            {page_inputs}
            <button type="submit">Fast rate unrated jobs on this page</button>
          </form>
          <form method="post" action="/jobs/rate-bulk">
            <input type="hidden" name="scope" value="fast_gte_7" />
            {common_return}
            {''.join(query_inputs)}
            <button type="submit">Deep rate fast-rated &gt;=7 matching jobs</button>
          </form>
          <form method="post" action="/jobs/rate-bulk">
            <input type="hidden" name="scope" value="fast_gte_7_page" />
            {common_return}
            {page_inputs}
            <button type="submit">Deep rate fast-rated &gt;=7 jobs on this page</button>
          </form>
          <form method="post" action="/jobs/rate-bulk">
            <input type="hidden" name="scope" value="all" />
            {common_return}
            {''.join(query_inputs)}
            <button type="submit">Deep rate all matching jobs with LLM</button>
          </form>
          <form method="post" action="/jobs/rate-bulk">
            <input type="hidden" name="scope" value="page" />
            {common_return}
            {page_inputs}
            <button type="submit">Deep rate jobs on this page with LLM</button>
          </form>
          <small class="hint wide">Fast rating is the cheap recall-heavy gate. Deep rating is the current full LLM v2 rating and runs in the background.</small>
        </section>
    """


def render_job_action(r: sqlite3.Row, return_to: str) -> str:
    if r["is_hidden"]:
        visibility_action = f"/jobs/{r['id']}/unhide"
        visibility_label = "Unhide"
    else:
        visibility_action = f"/jobs/{r['id']}/hide"
        visibility_label = "Hide"
    if r["is_saved"]:
        saved_action = f"/jobs/{r['id']}/unsave"
        saved_label = "Unsave"
    else:
        saved_action = f"/jobs/{r['id']}/save"
        saved_label = "Save"
    return f"""
              <form class="inline" method="post" action="{esc(saved_action)}">
                <input type="hidden" name="return_to" value="{esc(return_to)}" />
                <button type="submit">{saved_label}</button>
              </form>
              <form class="inline" method="post" action="{esc(visibility_action)}">
                <input type="hidden" name="return_to" value="{esc(return_to)}" />
                <button type="submit">{visibility_label}</button>
              </form>
            """


def render_index(params: dict[str, list[str]]) -> str:
    rows, total, page, per_page, total_pages = query_jobs(params)
    q = params.get("q", [""])[0]
    location = params.get("location", [""])[0]
    company = params.get("company", [""])[0]
    status = params.get("status", [""])[0]
    source_status = params.get("source_status", [status])[0]
    review_status = params.get("review_status", [""])[0]
    rating_status = params.get("rating_status", [""])[0]
    saved_mode = params.get("saved", [""])[0]
    hidden_mode = params.get("hidden", [""])[0] or "exclude"
    sort_key = params.get("sort", ["default"])[0] or "default"
    exclude_title = params.get("exclude_title", [""])[0]
    flash = params.get("flash", [""])[0]
    flash_html = f'<p class="flash">{esc(flash)}</p>' if flash else ""
    bulk_job = background_job_snapshot("llm_bulk_rating")
    fast_job = background_job_snapshot("llm_fast_rating")
    bulk_status_html = f'<p class="flash">{esc(bulk_job["message"])}</p>' if bulk_job else ""
    fast_status_html = f'<p class="flash">{esc(fast_job["message"])}</p>' if fast_job else ""
    start_num = ((page - 1) * per_page + 1) if total else 0
    end_num = min(page * per_page, total)
    prev_link = f"/?{query_string(params, page=page - 1)}" if page > 1 else ""
    next_link = f"/?{query_string(params, page=page + 1)}" if page < total_pages else ""
    prev_html = f'<a class="button" href="{esc(prev_link)}">Previous</a>' if prev_link else '<span class="button disabled">Previous</span>'
    next_html = f'<a class="button" href="{esc(next_link)}">Next</a>' if next_link else '<span class="button disabled">Next</span>'
    pagination = f"""
        <nav class="pagination">
          {prev_html}
          <span>Page {page} of {total_pages}</span>
          {next_html}
        </nav>
    """
    cards = []
    rating_db = Database(); rating_db.init()
    current_query = urllib.parse.urlencode({k: v[0] for k, v in params.items() if v and k != "flash"})
    current_return = f"/?{current_query}" if current_query else "/"
    page_job_ids = [int(r["id"]) for r in rows]
    full_refresh_help = "Full refresh uses only company and always expires missing jobs in the selected company scope."
    search_refresh_help = "Search refresh uses optional query, location, and limit. Empty query is allowed and search refresh never expires missing jobs."
    source_search_help = "Search mode finds new jobs from company career sites before saving them here. Databricks is usually faster because one career-site response already includes job descriptions. NVIDIA can be slower because the app asks NVIDIA for extra details for each matching job. NVIDIA's career site decides what fields match your words; exact fields are not guaranteed. It usually searches job posting text such as title and description. For Databricks, we download the job list first, then keep jobs where the title, location, or description contains your words."
    source_location_help = "Source location filter checks the location text from the company career site before saving results. Use commas for multiple locations; matches any entry."
    dashboard_search_help = "Regular dashboard search only searches jobs that are already saved in this app. It checks title, company, location, and description."
    dashboard_location_help = "Dashboard location filter only checks the saved job location field. Use commas for multiple locations; matches any entry."
    exclude_title_help = "Comma-separated title keywords to hide from this dashboard view only. This checks job titles only and does not change job status."
    sort_help = "Sort the current saved-job result set without changing filters. Choose newest, oldest, last seen, highest LLM score, company, or title."
    for r in rows:
        job_action = render_job_action(r, current_return)
        rating_badge = render_rating_badge_for_job(rating_db, int(r["id"]))
        cards.append(
            f"""
            <article class="card status-{esc(r['source_status'])} review-{esc(r['review_status'])} {'saved' if r['is_saved'] else ''}">
              <div class="meta"><strong>{esc(r['company_name'])}</strong> · {esc(r['source'])} · source: {esc(r['source_status'])} · review: {esc(r['review_status'])}{' · saved' if r['is_saved'] else ''}{' · hidden' if r['is_hidden'] else ''}</div>
              <h2><a href="/jobs/{r['id']}">{esc(r['title'])}</a></h2>
              <p class="location">{esc(r['location'])}</p>
              {audit_badges(r)}
              {rating_badge}
              <p class="reason">{esc(r['filter_reason'])}</p>
              <p class="dates">Found {esc(display_local_time(r['first_seen_at']))} · Last seen {esc(display_local_time(r['last_seen_at']))}</p>
              <a class="button" href="/jobs/{r['id']}/open" target="_blank" rel="noopener">Open official job</a>
              {job_action}
            </article>
            """
        )
    return PAGE.format(
        title="Job Search Phase 0",
        body=f"""
        <header>
          <h1>Job Search Pipeline — Phase 0</h1>
          <p>Databricks + NVIDIA jobs from official career APIs. Showing {start_num}-{end_num} of {total} matching jobs.</p>
        </header>
        {flash_html}
        {fast_status_html}
        {bulk_status_html}
        <details class="dashboard-panel refresh-panel" data-panel-key="refresh" open>
          <summary>Refresh jobs</summary>
        <section class="refresh-tabs" aria-label="Source refresh controls">
          <form class="refresh-tab" method="post" action="/refresh">
            <h2>Full refresh tab {tooltip(full_refresh_help)}</h2>
            <input type="hidden" name="mode" value="full" />
            <select name="company">
              <option value="">All companies</option>
              <option value="databricks" {'selected' if company == 'databricks' else ''}>Databricks</option>
              <option value="nvidia" {'selected' if company == 'nvidia' else ''}>NVIDIA</option>
            </select>
            <button type="submit">Full refresh</button>
          </form>
          <form class="refresh-tab" method="post" action="/refresh">
            <h2>Search refresh tab {tooltip(search_refresh_help)}</h2>
            <input type="hidden" name="mode" value="search" />
            <select name="company">
              <option value="">All companies</option>
              <option value="databricks" {'selected' if company == 'databricks' else ''}>Databricks</option>
              <option value="nvidia" {'selected' if company == 'nvidia' else ''}>NVIDIA</option>
            </select>
            <label class="source-query">
              <span>Pre-search filter {tooltip(source_search_help)}</span>
              <input name="source_q" value="{esc(q)}" placeholder="Optional words, e.g. software engineer" />
            </label>
            <label class="source-query">
              <span>Source location {tooltip(source_location_help)}</span>
              <input name="source_location" value="{esc(location)}" placeholder="Optional locations, e.g. Remote, California, Seattle" />
            </label>
            <input name="limit" type="number" min="1" placeholder="Optional limit" />
            <button type="submit">Search refresh</button>
          </form>
        </section>
        {render_refresh_status_section()}
        </details>
        <details class="dashboard-panel profile-panel-wrapper" data-panel-key="profile" open>
          <summary>Resume/Profile upload & extraction</summary>
        {render_profile_section()}
        </details>
        <details class="dashboard-panel filters-panel" data-panel-key="filters" open>
          <summary>Search & filter saved jobs</summary>
        <form id="unhide-all-form" method="post" action="/jobs/unhide-all">
          <input type="hidden" name="return_to" value="{esc(current_return)}" />
        </form>
        <form class="filters" method="get">
          <label class="search-filter">
            <span>Search saved jobs {tooltip(dashboard_search_help)}</span>
            <input name="q" value="{esc(q)}" placeholder="Search saved jobs by title, company, location, or description" />
          </label>
          <label class="search-filter">
            <span>Location {tooltip(dashboard_location_help)}</span>
            <input name="location" value="{esc(location)}" placeholder="Filter saved jobs by location, e.g. Remote, California" />
          </label>
          <label class="search-filter">
            <span>Exclude titles containing {tooltip(exclude_title_help)}</span>
            <input name="exclude_title" value="{esc(exclude_title)}" placeholder="Hide titles containing: manager, director" />
          </label>
          <select name="company">
            <option value="">All companies</option>
            <option value="databricks" {'selected' if company == 'databricks' else ''}>Databricks</option>
            <option value="nvidia" {'selected' if company == 'nvidia' else ''}>NVIDIA</option>
          </select>
          <select name="source_status">
            <option value="">All source statuses</option>
            {''.join(f'<option value="{s}" {"selected" if source_status == s else ""}>{s}</option>' for s in ['newly_discovered','active','expired'])}
          </select>
          <select name="review_status">
            <option value="">All review statuses</option>
            {''.join(f'<option value="{s}" {"selected" if review_status == s else ""}>{s}</option>' for s in ['unreviewed','reviewed','applied'])}
          </select>
          <select name="rating_status">
            <option value="">All rating states</option>
            {''.join(f'<option value="{s}" {"selected" if rating_status == s else ""}>{s}</option>' for s in ['unrated','fast_rated','needs_manual_review','deep_rated'])}
          </select>
          <select name="saved">
            <option value="" {'selected' if saved_mode == '' else ''}>All saved states</option>
            <option value="saved" {'selected' if saved_mode == 'saved' else ''}>Saved only</option>
            <option value="unsaved" {'selected' if saved_mode == 'unsaved' else ''}>Unsaved only</option>
          </select>
          <select name="hidden">
            <option value="exclude" {'selected' if hidden_mode == 'exclude' else ''}>Exclude hidden</option>
            <option value="include" {'selected' if hidden_mode == 'include' else ''}>Include hidden</option>
            <option value="only" {'selected' if hidden_mode == 'only' else ''}>Only hidden</option>
          </select>
          <label class="search-filter compact-filter">
            <span>Sort {tooltip(sort_help)}</span>
            <select name="sort">
              {sort_options_html(sort_key)}
            </select>
          </label>
          <select name="per_page">
            {''.join(f'<option value="{n}" {"selected" if per_page == n else ""}>{n} per page</option>' for n in [50, 100, 250, 500])}
          </select>
          <input type="hidden" name="page" value="1" />
          <button class="secondary-action" type="submit" form="unhide-all-form">Unhide all jobs</button>
          <button type="submit">Filter</button>
        </form>
        </details>
        <details class="dashboard-panel rating-panel-wrapper" data-panel-key="rating" open>
          <summary>LLM bulk rating</summary>
        {render_bulk_rating_controls(params, page_job_ids, current_return)}
        </details>
        <p class="hint">Refresh from terminal with: <code>python3 -m jobsearch.app refresh --limit 25</code>. Omit <code>--limit</code> for a full run.</p>
        {pagination}
        <section class="grid">{''.join(cards) or '<p>No jobs found yet. Run a refresh first.</p>'}</section>
        {pagination}
        <script>
        document.querySelectorAll("details.dashboard-panel[data-panel-key]").forEach((panel) => {{
          const key = panel.dataset.panelKey;
          const stored = localStorage.getItem(`jobsearch.panel.${{key}}`);
          if (stored === "closed") {{ panel.open = false; }}
          if (stored === "open") {{ panel.open = true; }}
          panel.addEventListener("toggle", () => {{
            localStorage.setItem(`jobsearch.panel.${{key}}`, panel.open ? "open" : "closed");
          }});
        }});
        </script>
        """,
    )

def render_job_from_db(db: Database, job_id: int) -> str:
    db.mark_reviewed(job_id)
    r = get_job_from_db(db, job_id)
    if r is None:
        return PAGE.format(title="Not found", body="<h1>Job not found</h1><p><a href='/'>Back</a></p>")
    description = esc(r["description_text"] or "No description stored.").replace("\n", "<br>")
    job_action = render_job_action(r, f"/jobs/{job_id}")
    rating_section = render_rating_section(db, job_id)
    return PAGE.format(
        title=esc(r["title"]),
        body=f"""
        <p><a href="/">← Back to jobs</a></p>
        <article class="detail">
          <h1>{esc(r['title'])}</h1>
          <p class="meta"><strong>{esc(r['company_name'])}</strong> · {esc(r['source'])} · source: {esc(r['source_status'])} · review: {esc(r['review_status'])}{' · saved' if r['is_saved'] else ''}{' · hidden' if r['is_hidden'] else ''}</p>
          <p><a class="button" href="/jobs/{job_id}/open" target="_blank" rel="noopener">Open official application URL</a>{job_action}</p>
          <dl>
            <dt>Location</dt><dd>{esc(r['location'])}</dd>
            <dt>Remote type</dt><dd>{esc(r['remote_type'])}</dd>
            <dt>Source job ID</dt><dd>{esc(r['source_job_id'])}</dd>
            <dt>Requisition ID</dt><dd>{esc(r['requisition_id'])}</dd>
            <dt>Filter reason</dt><dd>{esc(r['filter_reason'])}</dd>
            <dt>First seen</dt><dd>{esc(display_local_time(r['first_seen_at']))}</dd>
            <dt>Last seen</dt><dd>{esc(display_local_time(r['last_seen_at']))}</dd>
          </dl>
          {render_audit_section(r)}
          {rating_section}
          <h2>Description</h2>
          <div class="description">{description}</div>
        </article>
        """,
    )


def render_job(job_id: int) -> str:
    db = Database()
    db.init()
    return render_job_from_db(db, job_id)


PAGE = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{ color-scheme: light; --bg:#f8fafc; --panel:#ffffff; --text:#0f172a; --muted:#475569; --accent:#2563eb; --accent-soft:#dbeafe; --line:#dbe3ef; }}
body {{ margin:0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:var(--bg); color:var(--text); }}
main {{ max-width:1180px; margin:0 auto; padding:32px 18px 64px; }}
a {{ color:var(--accent); }}
header h1 {{ margin-bottom:4px; color:#020617; }}
.filters, .refresh, .refresh-tabs, .top-action, .profile-panel {{ display:flex; gap:10px; flex-wrap:wrap; margin:16px 0; padding:14px; background:var(--panel); border:1px solid var(--line); border-radius:14px; box-shadow:0 8px 24px rgba(15,23,42,.06); }}
.dashboard-panel {{ margin:18px 0; padding:0 14px 14px; background:var(--panel); border:1px solid var(--line); border-radius:14px; box-shadow:0 8px 24px rgba(15,23,42,.06); }}
.dashboard-panel > summary {{ cursor:pointer; font-weight:700; color:#020617; padding:14px 0; }}
.dashboard-panel > .refresh-tabs, .dashboard-panel > .filters, .dashboard-panel > .top-action, .dashboard-panel > .profile-panel {{ margin:0 0 12px; box-shadow:none; }}
.profile-panel {{ display:block; }}
.refresh-tabs {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); }}
.refresh-tab {{ display:flex; gap:10px; flex-wrap:wrap; align-content:start; padding:12px; border:1px solid var(--line); border-radius:12px; background:#f8fafc; }}
.refresh-tab h2 {{ flex-basis:100%; margin:0 0 4px; font-size:18px; color:#020617; }}
.refresh-status {{ margin:0 0 12px; padding:12px; border:1px solid var(--line); border-radius:12px; background:#f8fafc; }}
.refresh-status h2 {{ margin:0 0 4px; font-size:18px; color:#020617; }}
.refresh-status ul {{ margin:8px 0 0; padding-left:18px; }}
.refresh-run {{ margin:8px 0; }}
.refresh-run .meta {{ display:block; font-size:13px; }}
.refresh-run.status-error {{ color:#991b1b; }}
.error {{ color:#991b1b; }}
input, select, button {{ border-radius:10px; border:1px solid #cbd5e1; padding:10px 12px; background:#ffffff; color:var(--text); }}
input {{ min-width:280px; flex:1; }}
input:focus, select:focus {{ outline:2px solid var(--accent-soft); border-color:var(--accent); }}
button, .button {{ display:inline-block; background:var(--accent); color:white; text-decoration:none; border:0; padding:9px 12px; border-radius:10px; }}
.inline {{ display:inline-block; margin-left:8px; }}
.inline button {{ background:#334155; }}
.check {{ display:flex; align-items:center; gap:6px; color:var(--muted); }}
.check input {{ min-width:auto; flex:0; }}
.source-query, .search-filter {{ display:flex; flex-direction:column; gap:4px; flex:1; min-width:280px; color:var(--muted); }}
.source-query input, .search-filter input {{ min-width:0; width:100%; }}
.tooltip {{ position:relative; display:inline-flex; align-items:center; justify-content:center; width:18px; height:18px; border-radius:50%; background:var(--accent); color:white; font-size:12px; cursor:help; }}
.tooltip::after {{ content:attr(data-tooltip); position:absolute; left:50%; bottom:calc(100% + 8px); transform:translateX(-50%); display:none; min-width:260px; max-width:420px; padding:8px 10px; border-radius:10px; background:#0f172a; color:white; font-weight:400; font-size:12px; line-height:1.35; box-shadow:0 10px 24px rgba(15,23,42,.22); z-index:10; }}
.tooltip:hover::after, .tooltip:focus::after {{ display:block; }}
.secondary-action {{ background:#64748b; color:white; }}
.compact-filter {{ flex:0 0 220px; min-width:220px; }}
.compact-filter select {{ width:100%; }}
.wide {{ flex-basis:100%; }}
.grid {{ display:grid; grid-template-columns: repeat(auto-fill, minmax(310px, 1fr)); gap:14px; }}
.card, .detail {{ background:var(--panel); border:1px solid var(--line); border-radius:16px; padding:18px; box-shadow: 0 10px 30px rgba(15,23,42,.08); }}
.card h2 {{ font-size:18px; margin:8px 0; }}
.meta, .dates, .reason, .location, .hint {{ color:var(--muted); }}
.flash {{ padding:12px 14px; background:#eff6ff; border:1px solid #bfdbfe; color:#1e3a8a; border-radius:12px; }}
.audit-badges {{ display:flex; gap:6px; flex-wrap:wrap; margin:8px 0; }}
.audit-badge {{ display:inline-block; padding:4px 8px; border-radius:999px; background:#eff6ff; color:#1d4ed8; border:1px solid #bfdbfe; font-size:12px; }}
.status-newly_discovered {{ border-color:#93c5fd; box-shadow: 0 10px 30px rgba(37,99,235,.10); }}
.status-expired {{ opacity:.72; }}
dl {{ display:grid; grid-template-columns: 150px 1fr; gap:8px 16px; }}
dt {{ color:var(--muted); }}
.description {{ line-height:1.55; white-space:normal; }}
code {{ background:#e0f2fe; color:#0f172a; padding:2px 6px; border-radius:6px; }}
</style>
</head>
<body><main>{body}</main></body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/":
            body = render_index(params)
            self.send_html(200, body)
            return
        m = re.match(r"^/jobs/(\d+)/open$", parsed.path)
        if m:
            url = Database().mark_applied(int(m.group(1)))
            if url:
                self.send_redirect(url)
            else:
                self.send_html(404, PAGE.format(title="Not found", body="<h1>Job not found</h1>"))
            return
        m = re.match(r"^/jobs/(\d+)$", parsed.path)
        if m:
            self.send_html(200, render_job(int(m.group(1))))
            return
        self.send_html(404, PAGE.format(title="Not found", body="<h1>Not found</h1>"))

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/resumes/upload":
            try:
                length = int(self.headers.get("Content-Length", "0") or 0)
                body = self.rfile.read(length)
                field = parse_multipart_file_field(self.headers.get("Content-Type", ""), body, "resume")
                if field is None:
                    raise ValueError("Choose a resume file to upload.")
                db = Database(); db.init()
                db.upload_resume_file(field["filename"], field["content"], field["content_type"])
                self.send_redirect("/?flash=" + urllib.parse.quote("Resume uploaded. Profile extraction was not run automatically."))
            except Exception as exc:
                self.send_redirect("/?flash=" + urllib.parse.quote(f"Resume upload failed: {exc}"))
            return
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length).decode("utf-8")
        params = urllib.parse.parse_qs(body)
        m = re.match(r"^/resumes/(\d+)/remove$", parsed.path)
        if m:
            db = Database(); db.init()
            db.remove_resume_file(int(m.group(1)))
            self.send_redirect("/?flash=" + urllib.parse.quote("Resume removed. Any profile using it was marked stale/inactive."))
            return
        if parsed.path == "/profile/extract":
            try:
                db = Database(); db.init()
                db.extract_profile_from_active_resumes()
                self.send_redirect("/?flash=" + urllib.parse.quote("Profile extraction complete."))
            except Exception as exc:
                self.send_redirect("/?flash=" + urllib.parse.quote(f"Profile extraction failed: {exc}"))
            return
        if parsed.path == "/profile/extract-llm":
            try:
                job = start_llm_profile_extraction_background(DB_PATH, force=True)
                self.send_redirect("/?flash=" + urllib.parse.quote(job["message"]))
            except Exception as exc:
                self.send_redirect("/?flash=" + urllib.parse.quote(f"Could not start LLM profile extraction: {exc}"))
            return
        m = re.match(r"^/jobs/(\d+)/rate$", parsed.path)
        if m:
            job_id = int(m.group(1))
            try:
                db = Database(); db.init()
                row = db.rate_job_with_llm(job_id, force=True)
                self.send_redirect(f"/jobs/{job_id}?flash=" + urllib.parse.quote(f"LLM rating complete: {row['overall_score']} {row['recommendation']}"))
            except Exception as exc:
                self.send_redirect(f"/jobs/{job_id}?flash=" + urllib.parse.quote(f"LLM rating failed: {exc}"))
            return
        m = re.match(r"^/jobs/(\d+)/(hide|unhide|save|unsave|review)$", parsed.path)
        if m:
            db = Database()
            action = m.group(2)
            if action == "hide":
                db.hide_job(int(m.group(1)))
            elif action == "unhide":
                db.unhide_job(int(m.group(1)))
            elif action == "save":
                db.save_job(int(m.group(1)))
            elif action == "unsave":
                db.unsave_job(int(m.group(1)))
            elif action == "review":
                db.mark_reviewed(int(m.group(1)))
            return_to = params.get("return_to", ["/"])[0] or "/"
            self.send_redirect(return_to)
            return
        if parsed.path == "/jobs/unhide-all":
            count = Database().unhide_all_jobs()
            return_to = params.get("return_to", ["/"])[0] or "/"
            separator = "&" if "?" in return_to else "?"
            self.send_redirect(f"{return_to}{separator}flash=" + urllib.parse.quote(f"Unhid {count} jobs."))
            return
        if parsed.path == "/jobs/rate-fast-bulk":
            try:
                scope = params.get("scope", ["all"])[0] or "all"
                return_to = params.get("return_to", ["/"])[0] or "/"
                db = Database(); db.init()
                try:
                    if scope == "page":
                        candidate_ids = [int(value) for value in params.get("job_id", [])]
                        job_ids = [job_id for job_id in candidate_ids if db.latest_job_rating(job_id) is None and db.latest_job_fast_rating(job_id) is None]
                    else:
                        scoped_params = dict(params)
                        scoped_params["rating_status"] = ["unrated"]
                        job_ids = query_job_ids_from_db(db, scoped_params, scope="all")
                finally:
                    db.conn.close()
                job = start_llm_fast_rating_background(DB_PATH, job_ids, force=True)
                separator = "&" if "?" in return_to else "?"
                self.send_redirect(f"{return_to}{separator}flash=" + urllib.parse.quote(job["message"]))
            except Exception as exc:
                return_to = params.get("return_to", ["/"])[0] or "/"
                separator = "&" if "?" in return_to else "?"
                self.send_redirect(f"{return_to}{separator}flash=" + urllib.parse.quote(f"Could not start fast LLM rating: {exc}"))
            return
        if parsed.path == "/jobs/rate-bulk":
            try:
                scope = params.get("scope", ["all"])[0] or "all"
                return_to = params.get("return_to", ["/"])[0] or "/"
                if scope == "page":
                    job_ids = [int(value) for value in params.get("job_id", [])]
                elif scope == "fast_gte_7_page":
                    candidate_ids = [int(value) for value in params.get("job_id", [])]
                    db = Database(); db.init()
                    try:
                        job_ids = [job_id for job_id in candidate_ids if current_fast_rating_bucket(db, job_id) == "gte_7"]
                    finally:
                        db.conn.close()
                elif scope == "fast_gte_7":
                    db = Database(); db.init()
                    try:
                        candidate_ids = query_job_ids_from_db(db, params, scope="all")
                        job_ids = [job_id for job_id in candidate_ids if current_fast_rating_bucket(db, job_id) == "gte_7"]
                    finally:
                        db.conn.close()
                else:
                    db = Database(); db.init()
                    try:
                        job_ids = query_job_ids_from_db(db, params, scope="all")
                    finally:
                        db.conn.close()
                job = start_llm_bulk_rating_background(DB_PATH, job_ids, force=True)
                separator = "&" if "?" in return_to else "?"
                self.send_redirect(f"{return_to}{separator}flash=" + urllib.parse.quote(job["message"]))
            except Exception as exc:
                return_to = params.get("return_to", ["/"])[0] or "/"
                separator = "&" if "?" in return_to else "?"
                self.send_redirect(f"{return_to}{separator}flash=" + urllib.parse.quote(f"Could not start bulk LLM rating: {exc}"))
            return
        if parsed.path != "/refresh":
            self.send_html(404, PAGE.format(title="Not found", body="<h1>Not found</h1>"))
            return
        try:
            request, redirect_params = parse_refresh_form(params)
            summaries = refresh(**request)
            summary_text = "; ".join(
                f"{s.get('company')}: {s.get('status')} found={s.get('found', 0)} created={s.get('created', 0)} updated={s.get('updated', 0)} expired={s.get('expired', 0)}"
                for s in summaries
            )
            redirect_params["flash"] = f"Refresh complete. {summary_text}"
        except Exception as exc:
            redirect_params = {"flash": f"Refresh failed: {exc}"}
        self.send_redirect("/?" + urllib.parse.urlencode(redirect_params))

    def send_redirect(self, location: str):
        self.send_response(303)
        self.send_header("Location", location)
        self.end_headers()

    def log_message(self, format, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))

    def send_html(self, status: int, body: str):
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def benchmark_fast_rating(
    sample_size: int = 100,
    db_path: Path = DB_PATH,
    llm_client=call_openai_compatible_json,
    model_name: str | None = None,
    fast_mode: str = "default",
    save: bool = False,
) -> dict[str, Any]:
    """Run a fast-rating benchmark on a random unrated sample; dry-run/no-write by default."""
    db = Database(db_path)
    db.init()
    rows = db.conn.execute(
        """
        SELECT j.id FROM jobs j
        WHERE j.is_hidden=0
          AND NOT EXISTS (SELECT 1 FROM job_ratings jr WHERE jr.job_id=j.id AND jr.is_current=1)
          AND NOT EXISTS (SELECT 1 FROM job_fast_ratings jfr WHERE jfr.job_id=j.id AND jfr.is_current=1)
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (sample_size,),
    ).fetchall()
    job_ids = [int(row["id"]) for row in rows]
    start = time.monotonic()
    resolved_model_name = fast_model_name_for_mode(fast_mode, model_name)
    result = db.fast_rate_jobs_with_llm(job_ids, llm_client=llm_client, model_name=resolved_model_name, force=True, persist=save)
    elapsed = time.monotonic() - start
    db.conn.close()
    return {
        **result,
        "dry_run": not save,
        "saved": save,
        "fast_mode": fast_mode,
        "fast_model_name": resolved_model_name or configured_llm_model(),
        "elapsed_seconds": round(elapsed, 2),
        "jobs_per_minute": round((len(job_ids) / elapsed) * 60, 2) if elapsed else None,
        "note": (
            "Dry run: called the LLM but did not write fast ratings. Re-run with --save to persist results. "
            "Token/cost usage depends on provider accounting; check your provider dashboard for exact billed tokens."
            if not save else
            "Saved fast ratings to job_fast_ratings. Token/cost usage depends on provider accounting; check your provider dashboard for exact billed tokens."
        ),
    }


def compare_fast_vs_deep_rating(
    sample_size: int = 100,
    db_path: Path = DB_PATH,
    fast_llm_client=call_openai_compatible_json,
    deep_llm_client=call_openai_compatible_json,
    model_name: str | None = None,
    fast_model_name: str | None = None,
    deep_model_name: str | None = None,
    fast_mode: str = "default",
    threshold: float = 7.0,
    save: bool = False,
) -> dict[str, Any]:
    """Run fast and deep LLM ratings on the same sample and compare fast buckets against deep score bands."""
    db = Database(db_path)
    db.init()
    rows = db.conn.execute(
        """
        SELECT j.id, j.title, j.company_name FROM jobs j
        WHERE j.is_hidden=0
          AND NOT EXISTS (SELECT 1 FROM job_ratings jr WHERE jr.job_id=j.id AND jr.is_current=1)
          AND NOT EXISTS (SELECT 1 FROM job_fast_ratings jfr WHERE jfr.job_id=j.id AND jfr.is_current=1)
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (sample_size,),
    ).fetchall()
    start = time.monotonic()
    resolved_fast_model_name = fast_model_name_for_mode(fast_mode, fast_model_name or model_name)
    resolved_deep_model_name = deep_model_name or model_name
    confusion = {"true_positive": 0, "false_positive": 0, "true_negative": 0, "false_negative": 0, "manual_review": 0}
    bucket_counts: dict[str, int] = {}
    examples: list[dict[str, Any]] = []
    false_negatives: list[dict[str, Any]] = []
    manual_reviews: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    compared = 0
    for row in rows:
        job_id = int(row["id"])
        try:
            fast_row = db.fast_rate_job_with_llm(job_id, llm_client=fast_llm_client, model_name=resolved_fast_model_name, force=True, persist=save)
            deep_row = db.rate_job_with_llm(job_id, llm_client=deep_llm_client, model_name=resolved_deep_model_name, force=True, persist=save)
            bucket = str(fast_row["bucket"])
            score = float(deep_row["overall_score"])
            deep_positive = score >= threshold
            bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
            compared += 1
            if bucket == "needs_manual_review":
                confusion["manual_review"] += 1
                outcome = "manual_review"
            elif bucket == "gte_7" and deep_positive:
                confusion["true_positive"] += 1
                outcome = "true_positive"
            elif bucket == "gte_7" and not deep_positive:
                confusion["false_positive"] += 1
                outcome = "false_positive"
            elif bucket == "lt_7" and deep_positive:
                confusion["false_negative"] += 1
                outcome = "false_negative"
            else:
                confusion["true_negative"] += 1
                outcome = "true_negative"
            example = {
                "job_id": job_id,
                "title": row["title"],
                "company": row["company_name"],
                "fast_bucket": bucket,
                "deep_score": score,
                "outcome": outcome,
            }
            examples.append(example)
            if outcome == "false_negative":
                false_negatives.append(example)
            if outcome == "manual_review":
                manual_reviews.append(example)
        except Exception as exc:
            errors.append({"job_id": job_id, "error": str(exc)})
    elapsed = time.monotonic() - start
    db.conn.close()
    tp = confusion["true_positive"]
    fp = confusion["false_positive"]
    tn = confusion["true_negative"]
    fn = confusion["false_negative"]
    precision = round(tp / (tp + fp), 4) if (tp + fp) else None
    recall = round(tp / (tp + fn), 4) if (tp + fn) else None
    accuracy = round((tp + tn) / (tp + fp + tn + fn), 4) if (tp + fp + tn + fn) else None
    return {
        "requested": len(rows),
        "compared": compared,
        "failed": len(errors),
        "errors": errors,
        "dry_run": not save,
        "saved": save,
        "fast_mode": fast_mode,
        "fast_model_name": resolved_fast_model_name or configured_llm_model(),
        "deep_model_name": resolved_deep_model_name or configured_llm_model(),
        "threshold": threshold,
        "confusion_matrix": confusion,
        "precision": precision,
        "recall": recall,
        "accuracy_excluding_manual_review": accuracy,
        "bucket_counts": bucket_counts,
        "elapsed_seconds": round(elapsed, 2),
        "jobs_per_minute": round((compared / elapsed) * 60, 2) if elapsed else None,
        "false_negatives": false_negatives,
        "manual_reviews": manual_reviews,
        "examples": examples[:25],
        "note": (
            "Dry run: called both fast and deep LLM raters but did not write ratings. "
            "needs_manual_review is counted separately, not as pass/fail. Use provider dashboard for exact tokens/cost."
            if not save else
            "Saved both fast and deep ratings. needs_manual_review is counted separately. Use provider dashboard for exact tokens/cost."
        ),
    }


def _empty_comparison_summary(model_name: str | None) -> dict[str, Any]:
    return {
        "model_name": model_name,
        "confusion_matrix": {"true_positive": 0, "false_positive": 0, "true_negative": 0, "false_negative": 0, "manual_review": 0},
        "bucket_counts": {},
        "precision": None,
        "recall": None,
        "accuracy_excluding_manual_review": None,
        "false_negatives": [],
        "manual_reviews": [],
        "examples": [],
    }


def _add_fast_deep_comparison(summary: dict[str, Any], example: dict[str, Any], threshold: float) -> None:
    bucket = str(example["fast_bucket"])
    score = float(example["deep_score"])
    deep_positive = score >= threshold
    summary["bucket_counts"][bucket] = summary["bucket_counts"].get(bucket, 0) + 1
    if bucket == "needs_manual_review":
        outcome = "manual_review"
    elif bucket == "gte_7" and deep_positive:
        outcome = "true_positive"
    elif bucket == "gte_7" and not deep_positive:
        outcome = "false_positive"
    elif bucket == "lt_7" and deep_positive:
        outcome = "false_negative"
    else:
        outcome = "true_negative"
    example["outcome"] = outcome
    summary["confusion_matrix"][outcome] += 1
    summary["examples"].append(example)
    if outcome == "false_negative":
        summary["false_negatives"].append(example)
    if outcome == "manual_review":
        summary["manual_reviews"].append(example)


def _finalize_comparison_summary(summary: dict[str, Any]) -> None:
    matrix = summary["confusion_matrix"]
    tp = matrix["true_positive"]
    fp = matrix["false_positive"]
    tn = matrix["true_negative"]
    fn = matrix["false_negative"]
    summary["precision"] = round(tp / (tp + fp), 4) if (tp + fp) else None
    summary["recall"] = round(tp / (tp + fn), 4) if (tp + fn) else None
    summary["accuracy_excluding_manual_review"] = round((tp + tn) / (tp + fp + tn + fn), 4) if (tp + fp + tn + fn) else None
    summary["examples"] = summary["examples"][:25]


def compare_rating_models(
    sample_size: int = 100,
    db_path: Path = DB_PATH,
    deep_llm_client=call_openai_compatible_json,
    default_fast_llm_client=call_openai_compatible_json,
    openrouter_fast_llm_client=deepseek_v4_flash_openrouter_client,
    deep_model_name: str | None = None,
    default_fast_model_name: str | None = None,
    openrouter_fast_model_name: str | None = None,
    threshold: float = 7.0,
    save: bool = False,
) -> dict[str, Any]:
    """Compare deep rating, previous/default fast rating, and DeepSeek v4 Flash fast rating on one sample."""
    db = Database(db_path)
    db.init()
    rows = db.conn.execute(
        """
        SELECT j.id, j.title, j.company_name FROM jobs j
        WHERE j.is_hidden=0
          AND NOT EXISTS (SELECT 1 FROM job_ratings jr WHERE jr.job_id=j.id AND jr.is_current=1)
          AND NOT EXISTS (SELECT 1 FROM job_fast_ratings jfr WHERE jfr.job_id=j.id AND jfr.is_current=1)
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (sample_size,),
    ).fetchall()
    resolved_deep_model_name = deep_model_name or configured_llm_model()
    resolved_default_fast_model_name = default_fast_model_name or resolved_deep_model_name
    resolved_openrouter_fast_model_name = openrouter_fast_model_name or DEEPSEEK_V4_FLASH_OPENROUTER_MODEL
    summaries = {
        "previous_fast": _empty_comparison_summary(resolved_default_fast_model_name),
        "openrouter_v4_flash": _empty_comparison_summary(resolved_openrouter_fast_model_name),
    }
    errors: list[dict[str, Any]] = []
    compared = 0
    start = time.monotonic()
    for row in rows:
        job_id = int(row["id"])
        try:
            deep_row = db.rate_job_with_llm(job_id, llm_client=deep_llm_client, model_name=resolved_deep_model_name, force=True, persist=save)
            previous_fast_row = db.fast_rate_job_with_llm(job_id, llm_client=default_fast_llm_client, model_name=resolved_default_fast_model_name, force=True, persist=save)
            openrouter_fast_row = db.fast_rate_job_with_llm(job_id, llm_client=openrouter_fast_llm_client, model_name=resolved_openrouter_fast_model_name, force=True, persist=save)
            deep_score = float(deep_row["overall_score"])
            base = {"job_id": job_id, "title": row["title"], "company": row["company_name"], "deep_score": deep_score}
            _add_fast_deep_comparison(summaries["previous_fast"], {**base, "fast_bucket": str(previous_fast_row["bucket"])}, threshold)
            _add_fast_deep_comparison(summaries["openrouter_v4_flash"], {**base, "fast_bucket": str(openrouter_fast_row["bucket"])}, threshold)
            compared += 1
        except Exception as exc:
            errors.append({"job_id": job_id, "error": str(exc)})
    elapsed = time.monotonic() - start
    db.conn.close()
    for summary in summaries.values():
        _finalize_comparison_summary(summary)
    return {
        "requested": len(rows),
        "compared": compared,
        "failed": len(errors),
        "errors": errors,
        "dry_run": not save,
        "saved": save,
        "threshold": threshold,
        "deep_model_name": resolved_deep_model_name,
        "fast_models": summaries,
        "elapsed_seconds": round(elapsed, 2),
        "jobs_per_minute": round((compared / elapsed) * 60, 2) if elapsed else None,
        "note": (
            "Dry run: called deep rating, previous/default fast rating, and OpenRouter DeepSeek v4 Flash fast rating on the same jobs without writing ratings."
            if not save else
            "Saved deep rating plus both fast-rating model results. Use provider dashboards for exact tokens/cost."
        ),
    }


def serve(host: str, port: int) -> None:
    Database().init()
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Dashboard: http://{host}:{port}")
    server.serve_forever()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Phase 0 job sourcing pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db", help="Initialize SQLite database and seed companies")

    refresh_p = sub.add_parser("refresh", help="Fetch jobs from configured sources")
    refresh_p.add_argument("--company", choices=["databricks", "nvidia"], help="Refresh one company only")
    refresh_p.add_argument("--limit", type=int, help="Limit jobs fetched per company; applies to --mode search only")
    refresh_p.add_argument("--mode", choices=sorted(VALID_REFRESH_MODES), default="full", help="full expires missing jobs; search supports optional query/location/limit and never expires")
    refresh_p.add_argument("--search", default="", help="Optional search query for --mode search")
    refresh_p.add_argument("--location", default="", help="Optional location text filter for --mode search")

    serve_p = sub.add_parser("serve", help="Run local dashboard")
    serve_p.add_argument("--host", default="127.0.0.1")
    serve_p.add_argument("--port", default=8787, type=int)

    bench_p = sub.add_parser("benchmark-fast-rating", help="Benchmark fast LLM pre-rating on a random unrated sample; dry-run/no-write by default")
    bench_p.add_argument("--sample-size", type=int, default=100)
    bench_p.add_argument("--fast-mode", choices=["default", "openrouter-v4-flash"], default="default", help="Fast rater provider/model mode; openrouter-v4-flash uses DeepSeek v4 Flash via OPENROUTER_API_KEY")
    bench_p.add_argument("--save", action="store_true", help="Persist benchmark fast-rating results to job_fast_ratings")

    compare_p = sub.add_parser("compare-fast-deep-rating", help="Run fast and deep LLM raters side by side on a sample; dry-run/no-write by default")
    compare_p.add_argument("--sample-size", type=int, default=100)
    compare_p.add_argument("--threshold", type=float, default=7.0, help="Deep-rating score threshold for treating a job as >=7")
    compare_p.add_argument("--fast-mode", choices=["default", "openrouter-v4-flash"], default="default", help="Fast rater provider/model mode; openrouter-v4-flash uses DeepSeek v4 Flash via OPENROUTER_API_KEY while deep rating keeps the normal configured model")
    compare_p.add_argument("--save", action="store_true", help="Persist both fast and deep rating results")

    model_compare_p = sub.add_parser("compare-rating-models", help="Compare deep rating, previous/default fast rating, and DeepSeek v4 Flash fast rating on the same sample; dry-run/no-write by default")
    model_compare_p.add_argument("--sample-size", type=int, default=100)
    model_compare_p.add_argument("--threshold", type=float, default=7.0, help="Deep-rating score threshold for treating a job as >=7")
    model_compare_p.add_argument("--save", action="store_true", help="Persist deep rating plus both fast-rating results")

    args = parser.parse_args(argv)
    if args.cmd == "init-db":
        Database().init()
        print(f"Initialized {DB_PATH}")
        return 0
    if args.cmd == "refresh":
        summaries = refresh(slug=args.company, limit=args.limit, mode=args.mode, search_text=args.search, location_filter=args.location)
        print(json.dumps(summaries, indent=2))
        return 0
    if args.cmd == "serve":
        serve(args.host, args.port)
        return 0
    if args.cmd == "benchmark-fast-rating":
        print(json.dumps(benchmark_fast_rating(sample_size=args.sample_size, llm_client=fast_llm_client_for_mode(args.fast_mode), fast_mode=args.fast_mode, save=args.save), indent=2))
        return 0
    if args.cmd == "compare-fast-deep-rating":
        print(json.dumps(compare_fast_vs_deep_rating(sample_size=args.sample_size, fast_llm_client=fast_llm_client_for_mode(args.fast_mode), fast_mode=args.fast_mode, threshold=args.threshold, save=args.save), indent=2))
        return 0
    if args.cmd == "compare-rating-models":
        print(json.dumps(compare_rating_models(sample_size=args.sample_size, threshold=args.threshold, save=args.save), indent=2))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
