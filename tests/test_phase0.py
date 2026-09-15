import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from jobsearch.app import (
    Database,
    FAST_RATING_PROMPT_VERSION,
    GreenhouseConnector,
    NormalizedJob,
    NvidiaWorkdayConnector,
    audit_badges,
    background_job_snapshot,
    build_ingestion_audit,
    build_profile_from_texts,
    call_openai_compatible_json,
    filter_job,
    load_environment_files,
    load_local_env,
    rate_job_fit,
    benchmark_fast_rating,
    compare_fast_vs_deep_rating,
    compare_rating_models,
    deepseek_v4_flash_openrouter_client,
    fast_rating_bucket_label,
    get_job_from_db,
    parse_refresh_form,
    query_jobs_from_db,
    query_job_ids_from_db,
    render_audit_section,
    render_index,
    render_bulk_rating_controls,
    render_profile_section,
    render_refresh_status_section,
    render_job_action,
    render_job_from_db,
    reset_background_jobs_for_tests,
    display_local_time,
    start_llm_bulk_rating_background,
    start_llm_fast_rating_background,
    should_expire_missing_after_refresh,
    start_llm_profile_extraction_background,
    strip_html,
)


class Phase0Tests(unittest.TestCase):
    def test_app_module_does_not_depend_on_removed_cgi_module_at_import_time(self):
        source = (Path(__file__).resolve().parents[1] / "jobsearch" / "app.py").read_text()
        self.assertNotIn("import cgi", source)
        self.assertNotIn("cgi.FieldStorage", source)

    def test_strip_html(self):
        self.assertEqual(strip_html("<p>Hello<br>World</p>"), "Hello\nWorld")

    def test_display_local_time_converts_utc_iso_to_pacific_time(self):
        self.assertEqual(display_local_time("2026-06-21T00:30:00+00:00"), "2026-06-20 17:30 PDT")
        self.assertEqual(display_local_time("2026-01-21T08:30:00Z"), "2026-01-21 00:30 PST")
        self.assertEqual(display_local_time(None), "")

    def test_refresh_status_renders_local_time_for_run_timestamps(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            db.conn.execute(
                """
                INSERT INTO job_fetch_runs(company_id, started_at, finished_at, status, jobs_found, jobs_created, jobs_updated, jobs_expired)
                VALUES (?, ?, ?, 'success', 2, 1, 1, 0)
                """,
                (company["id"], "2026-06-21T00:30:00+00:00", "2026-06-21T00:35:00+00:00"),
            )
            db.conn.commit()

            html = render_refresh_status_section(db)

            self.assertIn("started 2026-06-20 17:30 PDT; finished 2026-06-20 17:35 PDT", html)
            self.assertNotIn("2026-06-21T00:30:00+00:00", html)

    def test_filter_job_keeps_reviewable_jobs_new_without_candidate_keywords(self):
        job = NormalizedJob(
            company_name="Example",
            source="test",
            source_job_id="1",
            requisition_id=None,
            title="Software Engineer",
            location="Remote - California",
            remote_type="remote",
            department="Engineering",
            employment_type=None,
            salary_min=None,
            salary_max=None,
            currency=None,
            job_url="https://example.com/job",
            apply_url="https://example.com/apply",
            description_raw_html="<p>Python platform engineering</p>",
            description_text="Python platform engineering",
            posted_at=None,
        )
        status, reason = filter_job(job)
        self.assertEqual(status, "new")
        self.assertEqual(reason, "ready for rating")

    def test_filter_job_keeps_manager_titles_new_without_filtered_out_status(self):
        job = NormalizedJob(
            company_name="Example",
            source="test",
            source_job_id="1",
            requisition_id=None,
            title="Engineering Manager",
            location="Remote - California",
            remote_type="remote",
            department="Engineering",
            employment_type=None,
            salary_min=None,
            salary_max=None,
            currency=None,
            job_url="https://example.com/job",
            apply_url="https://example.com/apply",
            description_raw_html="<p>Python platform engineering</p>",
            description_text="Python platform engineering",
            posted_at=None,
        )
        status, reason = filter_job(job)
        self.assertEqual(status, "new")
        self.assertEqual(reason, "ready for rating")

    def test_database_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            companies = db.companies()
            self.assertEqual([c["slug"] for c in companies], ["databricks", "nvidia"])
            self.assertEqual(db.companies("databricks")[0]["ats_type"], "greenhouse")
            self.assertEqual(db.companies("nvidia")[0]["ats_type"], "workday")
            tables = [r["name"] for r in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            self.assertIn("job_ingestion_audits", tables)
            self.assertIn("resume_files", tables)
            self.assertIn("profile_extractions", tables)

    def test_resume_schema_upload_and_remove_do_not_auto_extract_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            columns = {row["name"] for row in db.conn.execute("PRAGMA table_info(resume_files)")}
            self.assertIn("original_filename", columns)
            self.assertIn("extracted_text", columns)
            self.assertIn("is_active", columns)

            resume_id = db.upload_resume_file("resume.txt", b"Python SQL machine learning", "text/plain")
            self.assertEqual(len(db.active_resume_files()), 1)
            self.assertEqual(db.conn.execute("SELECT COUNT(*) FROM profile_extractions").fetchone()[0], 0)

            db.remove_resume_file(resume_id)
            self.assertEqual(len(db.active_resume_files()), 0)
            self.assertEqual(db.conn.execute("SELECT COUNT(*) FROM profile_extractions").fetchone()[0], 0)

    def test_manual_profile_extractor_uses_only_active_resumes_and_marks_removed_profiles_inactive(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            first_id = db.upload_resume_file("resume.txt", b"Built Python data pipelines with SQL and Docker for machine learning projects.", "text/plain")
            removed_id = db.upload_resume_file("old.txt", b"Cobol mainframe", "text/plain")
            db.remove_resume_file(removed_id)

            profile = db.extract_profile_from_active_resumes()
            profile_json = json.loads(profile["profile_json"])

            self.assertEqual(json.loads(profile["source_resume_ids_json"]), [first_id])
            self.assertIn("python", profile_json["skills"])
            self.assertIn("sql", profile_json["skills"])
            self.assertNotIn("cobol", profile_json["skills"])
            self.assertTrue(db.latest_profile_is_current())

            db.remove_resume_file(first_id)
            stale = db.conn.execute("SELECT is_active FROM profile_extractions WHERE id=?", (profile["id"],)).fetchone()
            self.assertEqual(stale["is_active"], 0)
            self.assertFalse(db.latest_profile_is_current())

    def test_profile_extractor_uses_only_resume_derived_terms_without_profile_constants(self):
        profile = build_profile_from_texts([
            """
            Custom orbital bakery program managed sourdough telemetry, yeast robotics,
            vacuum oven optimization, and lunar logistics dashboards for 4+ years.
            """
        ])

        joined_skills = " ".join(profile["skills"])
        self.assertIn("sourdough", joined_skills)
        self.assertIn("telemetry", joined_skills)
        self.assertEqual(profile["domains"], [])
        self.assertEqual(profile["proof_points"], [])
        self.assertEqual(profile["target_directions"], [])
        self.assertGreaterEqual(profile["baseline_years_experience"], 4)

    def test_rate_job_fit_uses_only_terms_from_extracted_profile_json(self):
        profile = {
            "target_roles": ["Orbital Bakery Operations Analyst"],
            "target_industries": ["space food logistics"],
            "seniority": {"years_experience": 4},
            "core_strengths": ["sourdough telemetry", "vacuum oven optimization"],
            "technical_skills": ["yeast robotics", "lunar dashboards"],
            "domain_skills": ["orbital bakery", "lunar logistics"],
            "proof_points": [{"name": "Bakery Mission Control", "supports": ["vacuum oven optimization", "lunar logistics dashboards"]}],
            "weaknesses_or_gaps": ["marine diesel repair"],
            "practical_constraints": {"location": "remote preferred"},
        }
        job = NormalizedJob(
            company_name="Example",
            source="manual",
            source_job_id="orbital-1",
            requisition_id=None,
            title="Orbital Bakery Operations Analyst",
            location="Remote",
            remote_type=None,
            department="Space Food Logistics",
            employment_type=None,
            salary_min=None,
            salary_max=None,
            currency=None,
            job_url="https://example.com/job",
            apply_url="https://example.com/apply",
            description_raw_html="",
            description_text="Lead sourdough telemetry, yeast robotics, vacuum oven optimization, and lunar logistics dashboards.",
            posted_at=None,
        )

        rating = rate_job_fit(profile, job)

        self.assertGreaterEqual(rating["overall_score"], 8.0)
        self.assertIn("Orbital Bakery Operations Analyst", rating["categories"]["core_job_function_match"]["matches"])
        self.assertIn("yeast robotics", rating["categories"]["technical_tool_match"]["matches"])
        self.assertIn("lunar logistics", rating["categories"]["domain_match"]["matches"])
        self.assertFalse(rating["gaps"])

    def test_rate_job_fit_penalizes_only_profile_extracted_gaps(self):
        profile = {
            "target_roles": ["Orbital Bakery Operations Analyst"],
            "technical_skills": ["yeast robotics"],
            "weaknesses_or_gaps": ["marine diesel repair"],
            "seniority": {"years_experience": 2},
        }
        job = NormalizedJob(
            company_name="Example",
            source="manual",
            source_job_id="profile-gap",
            requisition_id=None,
            title="Marine Diesel Repair Lead",
            location="Remote",
            remote_type=None,
            department="Operations",
            employment_type=None,
            salary_min=None,
            salary_max=None,
            currency=None,
            job_url="https://example.com/job",
            apply_url="https://example.com/apply",
            description_raw_html="",
            description_text="Marine diesel repair leadership role requiring 6+ years of experience.",
            posted_at=None,
        )

        rating = rate_job_fit(profile, job)

        self.assertLess(rating["overall_score"], 6.0)
        self.assertEqual(rating["recommendation"], "Skip or very low priority")
        self.assertIn("marine diesel repair", " ".join(rating["gaps"]).lower())
        self.assertIn("requires 6+ years", " ".join(rating["gaps"]).lower())
        self.assertLess(rating["practical_fit_score"], rating["skill_fit_score"])

    def test_dashboard_renders_upload_and_llm_extract_only_controls(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"Python SQL", "text/plain")
            html = render_profile_section(db)
            self.assertIn("Resume/Profile", html)
            self.assertIn('enctype="multipart/form-data"', html)
            self.assertIn('action="/resumes/upload"', html)
            self.assertIn("Extract/update profile with LLM", html)
            self.assertIn('action="/profile/extract-llm"', html)
            self.assertNotIn("Extract/update profile locally", html)
            self.assertNotIn('action="/profile/extract"', html)
            self.assertNotIn("Manual extraction only", html)
            self.assertIn("resume.txt", html)

    def test_dashboard_renders_richer_extracted_profile_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file(
                "resume.txt",
                b"Custom orbital bakery program managed sourdough telemetry and yeast robotics for 4+ years.",
                "text/plain",
            )
            db.extract_profile_from_active_resumes()
            html = render_profile_section(db)
            self.assertIn("Skills", html)
            self.assertIn("sourdough", html)
            self.assertIn("Target directions", html)
            self.assertIn("Experience baseline", html)

    def test_local_env_loader_uses_dotenv_without_overriding_existing_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text(
                "# local secrets\n"
                "JOBSEARCH_LLM_API_KEY=from-dotenv\n"
                "JOBSEARCH_LLM_MODEL=\"dotenv-model\"\n"
                "OPENAI_API_KEY=should-not-override\n",
                encoding="utf-8",
            )
            with patch.dict("os.environ", {"OPENAI_API_KEY": "existing-key"}, clear=True):
                load_local_env(env_path)
                self.assertEqual(os.environ["JOBSEARCH_LLM_API_KEY"], "from-dotenv")
                self.assertEqual(os.environ["JOBSEARCH_LLM_MODEL"], "dotenv-model")
                self.assertEqual(os.environ["OPENAI_API_KEY"], "existing-key")

    def test_environment_loader_uses_project_env_then_hermes_env_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root_env = Path(tmp) / "project.env"
            hermes_env = Path(tmp) / "hermes.env"
            root_env.write_text(
                "JOBSEARCH_LLM_MODEL=project-model\n"
                "JOBSEARCH_LLM_API_KEY=project-key\n",
                encoding="utf-8",
            )
            hermes_env.write_text(
                "OPENROUTER_API_KEY=hermes-openrouter-key\n"
                "JOBSEARCH_LLM_MODEL=hermes-should-not-override\n",
                encoding="utf-8",
            )
            with patch.dict("os.environ", {}, clear=True):
                load_environment_files(root_env=root_env, hermes_env=hermes_env)
                self.assertEqual(os.environ["JOBSEARCH_LLM_MODEL"], "project-model")
                self.assertEqual(os.environ["JOBSEARCH_LLM_API_KEY"], "project-key")
                self.assertEqual(os.environ["OPENROUTER_API_KEY"], "hermes-openrouter-key")

    def test_openai_compatible_json_client_sends_json_mode_request(self):
        fake_response = MagicMock()
        fake_response.read.return_value = json.dumps({"choices": [{"message": {"content": json.dumps({"ok": True})}}]}).encode("utf-8")
        fake_cm = MagicMock()
        fake_cm.__enter__.return_value = fake_response
        with patch.dict("os.environ", {"JOBSEARCH_LLM_API_KEY": "test-key", "JOBSEARCH_LLM_BASE_URL": "https://llm.example/v1"}, clear=True):
            with patch("jobsearch.app.urllib.request.urlopen", return_value=fake_cm) as urlopen:
                result = call_openai_compatible_json("system", "user", model_name="unit-model")

        self.assertEqual(result, {"ok": True})
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://llm.example/v1/chat/completions")
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["model"], "unit-model")
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertEqual(request.headers["Authorization"], "Bearer test-key")

    def test_deepseek_v4_flash_openrouter_client_uses_openrouter_key_base_and_model(self):
        fake_response = MagicMock()
        fake_response.read.return_value = json.dumps({"choices": [{"message": {"content": json.dumps({"bucket": "gte_7"})}}]}).encode("utf-8")
        fake_cm = MagicMock()
        fake_cm.__enter__.return_value = fake_response

        with patch.dict("os.environ", {"OPENROUTER_API_KEY": "or-key", "JOBSEARCH_LLM_API_KEY": "normal-key"}, clear=True):
            with patch("jobsearch.app.urllib.request.urlopen", return_value=fake_cm) as urlopen:
                result = deepseek_v4_flash_openrouter_client("system", "user")

        self.assertEqual(result, {"bucket": "gte_7"})
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://openrouter.ai/api/v1/chat/completions")
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["model"], "deepseek/deepseek-v4-flash")
        self.assertEqual(request.headers["Authorization"], "Bearer or-key")

    def test_llm_profile_extraction_stores_comprehensive_profile_and_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"Google Control Tower and Tesla warehouse optimization with SQL Python GCP.", "text/plain")
            fake_llm = MagicMock(return_value={
                "candidate_summary": "Supply-chain analytics candidate",
                "target_roles": ["Supply Chain Data Analyst", "TPM"],
                "target_industries": ["tech supply chain"],
                "seniority": {"years_experience": 3, "best_fit_levels": ["analyst"]},
                "core_strengths": ["capacity analytics"],
                "technical_skills": ["SQL", "Python", "GCP"],
                "domain_skills": ["supply chain", "warehouse optimization"],
                "proof_points": [{"name": "Google Control Tower", "supports": ["capacity planning"]}],
                "weaknesses_or_gaps": ["backend engineering"],
                "practical_constraints": {"work_authorization": None},
                "resume_bullet_inventory": [{"source": "Google", "text": "Control Tower dashboards"}],
            })

            profile_row = db.extract_llm_profile_from_active_resumes(fake_llm, model_name="test-model")
            profile = json.loads(profile_row["profile_json"])

            self.assertEqual(profile_row["extraction_method"], "llm")
            self.assertEqual(profile_row["model_name"], "test-model")
            self.assertEqual(profile_row["prompt_version"], "llm-profile-v1")
            self.assertEqual(profile["candidate_summary"], "Supply-chain analytics candidate")
            self.assertEqual(profile["target_roles"], ["Supply Chain Data Analyst", "TPM"])
            self.assertIn("resume", fake_llm.call_args.kwargs["user_prompt"].lower())
            self.assertIn("7-category", fake_llm.call_args.kwargs["user_prompt"].lower())

    def test_llm_job_rating_persists_and_reuses_current_rating(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"Google Control Tower supply chain SQL Python", "text/plain")
            profile_row = db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"], "proof_points": []}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            job = NormalizedJob(
                company_name="Databricks", source="manual", source_job_id="rating-1", requisition_id=None,
                title="Supply Chain Data Analyst", location="California", remote_type=None, department="Operations",
                employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job",
                apply_url="https://example.com/apply", description_raw_html="", description_text="SQL Python capacity planning supply chain analytics",
                posted_at=None,
            )
            _, _, ids = db.upsert_jobs(company, [job], refresh_mode="search")
            fake_llm = MagicMock(return_value={
                "overall_score": 9.2,
                "skill_fit_score": 9.3,
                "practical_fit_score": 9.1,
                "recommendation": "Apply ASAP",
                "categories": {"core_job_function_match": {"score": 9.5, "reason": "Direct supply-chain analytics match"}},
                "strongest_evidence": ["Google Control Tower"],
                "main_gaps": [],
                "practical_notes": [],
                "resume_tailoring_notes": ["Emphasize capacity dashboards"],
                "interview_probability_reasoning": "Strong evidence match",
                "apply_decision": "apply",
            })

            first = db.rate_job_with_llm(ids[0], fake_llm, model_name="test-model")
            second = db.rate_job_with_llm(ids[0], fake_llm, model_name="test-model")
            rating = db.latest_job_rating(ids[0])

            self.assertEqual(first["id"], second["id"])
            self.assertEqual(fake_llm.call_count, 1)
            self.assertEqual(rating["overall_score"], 9.2)
            self.assertEqual(rating["recommendation"], "Apply ASAP")
            self.assertEqual(rating["profile_extraction_id"], profile_row["id"])

    def test_bulk_llm_rating_rates_selected_jobs_and_reuses_cached_ratings(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"Google Control Tower supply chain SQL Python", "text/plain")
            db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"], "proof_points": []}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks", source="manual", source_job_id=f"bulk-{i}", requisition_id=None,
                    title=f"Data Analyst {i}", location="California", remote_type=None, department="Operations",
                    employment_type=None, salary_min=None, salary_max=None, currency=None, job_url=f"https://example.com/job/{i}",
                    apply_url=f"https://example.com/apply/{i}", description_raw_html="", description_text="SQL Python analytics",
                    posted_at=None,
                )
                for i in range(3)
            ]
            _, _, ids = db.upsert_jobs(company, jobs, refresh_mode="search")
            fake_llm = MagicMock(return_value={
                "overall_score": 8.1, "skill_fit_score": 8.2, "practical_fit_score": 8.0,
                "recommendation": "Good fit", "categories": {}, "strongest_evidence": ["SQL"],
                "main_gaps": [], "practical_notes": [], "resume_tailoring_notes": [],
                "interview_probability_reasoning": "Good analytics fit", "apply_decision": "apply",
            })

            first = db.rate_jobs_with_llm(ids[:2], fake_llm, model_name="test-model")
            second = db.rate_jobs_with_llm(ids[:2], fake_llm, model_name="test-model")

            self.assertEqual(first["requested"], 2)
            self.assertEqual(first["rated"], 2)
            self.assertEqual(first["failed"], 0)
            self.assertEqual(second["rated"], 2)
            self.assertEqual(fake_llm.call_count, 2)
            self.assertEqual(db.conn.execute("SELECT COUNT(*) FROM job_ratings").fetchone()[0], 2)

    def test_fast_llm_rating_persists_resume_driven_bucket_and_rating_status_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"Custom role evidence: underwater basket weaving Python", "text/plain")
            db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "custom candidate", "target_roles": ["Underwater Basket Weaver"], "technical_skills": ["Python"]}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            _, _, ids = db.upsert_jobs(company, [NormalizedJob(
                company_name="Databricks", source="manual", source_job_id="fast-rating-1", requisition_id=None,
                title="Underwater Basket Weaver", location="California", remote_type=None, department="Operations",
                employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/fast",
                apply_url="https://example.com/apply/fast", description_raw_html="", description_text="Weave baskets underwater using Python automation.",
                posted_at=None,
            )], refresh_mode="search")
            fake_llm = MagicMock(return_value={
                "bucket": "gte_7",
                "confidence": "high",
                "reason_codes": ["resume_target_role_match"],
                "short_reason": "Matches the extracted target role.",
            })

            row = db.fast_rate_job_with_llm(ids[0], fake_llm, model_name="test-model")
            rows, total, *_ = query_jobs_from_db(db, {"rating_status": ["fast_rated"]})

            self.assertEqual(row["bucket"], "gte_7")
            self.assertEqual(fast_rating_bucket_label(row["bucket"]), ">=7 fast pass")
            self.assertEqual(total, 1)
            self.assertEqual(rows[0]["id"], ids[0])
            self.assertIn("Underwater Basket Weaver", fake_llm.call_args.kwargs["user_prompt"])
            self.assertIn("Prefer false positives over false negatives", fake_llm.call_args.kwargs["user_prompt"])
            self.assertIn("choose lt_7 only when", fake_llm.call_args.kwargs["user_prompt"])
            self.assertIn("uncertain but promising", fake_llm.call_args.kwargs["user_prompt"])
            prompt = fake_llm.call_args.kwargs["user_prompt"]
            self.assertIn("DeepSeek/cheap fast models can be over-strict", prompt)
            self.assertIn("Profile-agnostic decision examples", prompt)
            self.assertIn("extracted target role", prompt)
            self.assertIn("extracted transferable skill", prompt)
            self.assertIn("Do not use lt_7 for merely imperfect matches", prompt)
            self.assertNotIn("Supply Chain Program Manager", prompt)
            self.assertNotIn("SQL/Python", prompt)
            self.assertNotIn("Google Control Tower", prompt)
            self.assertEqual(FAST_RATING_PROMPT_VERSION, "llm-fast-rating-v5")

    def test_rating_status_filter_separates_unrated_fast_manual_and_deep_states(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"SQL Python analytics", "text/plain")
            db.extract_llm_profile_from_active_resumes(MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"]}), model_name="test-model")
            company = db.companies("databricks")[0]
            jobs = [NormalizedJob(
                company_name="Databricks", source="manual", source_job_id=f"rating-state-{i}", requisition_id=None,
                title=f"Data Analyst {i}", location="California", remote_type=None, department="Operations",
                employment_type=None, salary_min=None, salary_max=None, currency=None, job_url=f"https://example.com/job/state/{i}",
                apply_url=f"https://example.com/apply/state/{i}", description_raw_html="", description_text="SQL Python analytics",
                posted_at=None,
            ) for i in range(4)]
            _, _, ids = db.upsert_jobs(company, jobs, refresh_mode="search")
            db.fast_rate_job_with_llm(ids[1], MagicMock(return_value={"bucket": "gte_7", "confidence": "medium", "reason_codes": [], "short_reason": "Plausible."}), model_name="test-model")
            db.fast_rate_job_with_llm(ids[2], MagicMock(return_value={"bucket": "needs_manual_review", "confidence": "low", "reason_codes": [], "short_reason": "Ambiguous."}), model_name="test-model")
            db.rate_job_with_llm(ids[3], MagicMock(return_value={
                "overall_score": 8.4, "skill_fit_score": 8.4, "practical_fit_score": 8.4,
                "recommendation": "Good fit", "categories": {}, "strongest_evidence": [], "main_gaps": [],
                "practical_notes": [], "resume_tailoring_notes": [], "interview_probability_reasoning": "", "apply_decision": "apply",
            }), model_name="test-model")

            def filtered(status):
                return {row["id"] for row in query_jobs_from_db(db, {"rating_status": [status]})[0]}

            self.assertEqual(filtered("unrated"), {ids[0]})
            self.assertEqual(filtered("fast_rated"), {ids[1]})
            self.assertEqual(filtered("needs_manual_review"), {ids[2]})
            self.assertEqual(filtered("deep_rated"), {ids[3]})

    def test_benchmark_fast_rating_defaults_to_dry_run_without_database_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "jobs.sqlite"
            db = Database(db_path)
            db.init()
            db.upload_resume_file("resume.txt", b"SQL Python analytics", "text/plain")
            db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"], "technical_skills": ["SQL", "Python"]}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            _, _, ids = db.upsert_jobs(company, [NormalizedJob(
                company_name="Databricks", source="manual", source_job_id="dry-bench-1", requisition_id=None,
                title="Data Analyst", location="California", remote_type=None, department="Operations",
                employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/dry-bench",
                apply_url="https://example.com/apply/dry-bench", description_raw_html="", description_text="SQL Python analytics",
                posted_at=None,
            )], refresh_mode="search")
            db.conn.close()
            fake_llm = MagicMock(return_value={
                "bucket": "gte_7",
                "confidence": "high",
                "reason_codes": ["profile_match"],
                "short_reason": "Strong analytics match.",
            })

            result = benchmark_fast_rating(sample_size=100, db_path=db_path, llm_client=fake_llm, model_name="test-model")

            verify_db = Database(db_path)
            verify_db.init()
            self.assertEqual(result["dry_run"], True)
            self.assertEqual(result["rated"], 1)
            self.assertEqual(result["bucket_counts"], {"gte_7": 1})
            self.assertEqual(verify_db.conn.execute("SELECT COUNT(*) FROM job_fast_ratings").fetchone()[0], 0)
            rows, total, *_ = query_jobs_from_db(verify_db, {"rating_status": ["unrated"]})
            self.assertEqual(total, 1)
            self.assertEqual(rows[0]["id"], ids[0])

    def test_benchmark_fast_rating_save_option_persists_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "jobs.sqlite"
            db = Database(db_path)
            db.init()
            db.upload_resume_file("resume.txt", b"SQL Python analytics", "text/plain")
            db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"], "technical_skills": ["SQL", "Python"]}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            db.upsert_jobs(company, [NormalizedJob(
                company_name="Databricks", source="manual", source_job_id="save-bench-1", requisition_id=None,
                title="Data Analyst", location="California", remote_type=None, department="Operations",
                employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/save-bench",
                apply_url="https://example.com/apply/save-bench", description_raw_html="", description_text="SQL Python analytics",
                posted_at=None,
            )], refresh_mode="search")
            db.conn.close()
            fake_llm = MagicMock(return_value={
                "bucket": "gte_7",
                "confidence": "high",
                "reason_codes": ["profile_match"],
                "short_reason": "Strong analytics match.",
            })

            result = benchmark_fast_rating(sample_size=100, db_path=db_path, llm_client=fake_llm, model_name="test-model", save=True)

            verify_db = Database(db_path)
            verify_db.init()
            self.assertEqual(result["dry_run"], False)
            self.assertEqual(result["rated"], 1)
            self.assertEqual(result["bucket_counts"], {"gte_7": 1})
            self.assertEqual(verify_db.conn.execute("SELECT COUNT(*) FROM job_fast_ratings").fetchone()[0], 1)

    def test_compare_fast_vs_deep_rating_dry_run_reports_confusion_metrics_without_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "jobs.sqlite"
            db = Database(db_path)
            db.init()
            db.upload_resume_file("resume.txt", b"SQL Python analytics", "text/plain")
            db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"], "technical_skills": ["SQL", "Python"]}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            db.upsert_jobs(company, [
                NormalizedJob(
                    company_name="Databricks", source="manual", source_job_id="compare-strong", requisition_id=None,
                    title="Data Analyst", location="California", remote_type=None, department="Operations",
                    employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/compare-strong",
                    apply_url="https://example.com/apply/compare-strong", description_raw_html="", description_text="SQL Python analytics",
                    posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks", source="manual", source_job_id="compare-weak", requisition_id=None,
                    title="Java Architect", location="California", remote_type=None, department="Engineering",
                    employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/compare-weak",
                    apply_url="https://example.com/apply/compare-weak", description_raw_html="", description_text="Java architecture",
                    posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks", source="manual", source_job_id="compare-fn", requisition_id=None,
                    title="Solutions Engineer", location="California", remote_type=None, department="Field Engineering",
                    employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/compare-fn",
                    apply_url="https://example.com/apply/compare-fn", description_raw_html="", description_text="Technical customer-facing SQL analytics solutions work",
                    posted_at=None,
                ),
            ], refresh_mode="search")
            db.conn.close()

            fast_models_seen = []
            deep_models_seen = []

            def fake_fast(**kwargs):
                fast_models_seen.append(kwargs["model_name"])
                if '"title": "Data Analyst"' in kwargs["user_prompt"]:
                    bucket = "gte_7"
                elif '"title": "Solutions Engineer"' in kwargs["user_prompt"]:
                    bucket = "lt_7"
                else:
                    bucket = "needs_manual_review"
                return {"bucket": bucket, "confidence": "high", "reason_codes": [], "short_reason": "test"}

            def fake_deep(**kwargs):
                deep_models_seen.append(kwargs["model_name"])
                if '"title": "Data Analyst"' in kwargs["user_prompt"]:
                    score = 8.2
                elif '"title": "Solutions Engineer"' in kwargs["user_prompt"]:
                    score = 7.8
                else:
                    score = 5.5
                return {
                    "overall_score": score,
                    "skill_fit_score": score,
                    "practical_fit_score": score,
                    "recommendation": "test",
                    "categories": {},
                    "strongest_evidence": [],
                    "main_gaps": [],
                    "practical_notes": [],
                    "resume_tailoring_notes": [],
                    "interview_probability_reasoning": "test",
                    "apply_decision": "test",
                }

            result = compare_fast_vs_deep_rating(
                sample_size=10,
                db_path=db_path,
                fast_llm_client=fake_fast,
                deep_llm_client=fake_deep,
                fast_model_name="fast-test-model",
                deep_model_name="deep-test-model",
            )

            verify_db = Database(db_path)
            verify_db.init()
            self.assertTrue(result["dry_run"])
            self.assertEqual(result["compared"], 3)
            self.assertEqual(result["confusion_matrix"]["true_positive"], 1)
            self.assertEqual(result["confusion_matrix"]["manual_review"], 1)
            self.assertEqual(result["confusion_matrix"]["false_negative"], 1)
            self.assertEqual(result["recall"], 0.5)
            self.assertEqual(result["precision"], 1.0)
            self.assertEqual(result["fast_model_name"], "fast-test-model")
            self.assertEqual(result["deep_model_name"], "deep-test-model")
            self.assertEqual(set(fast_models_seen), {"fast-test-model"})
            self.assertEqual(set(deep_models_seen), {"deep-test-model"})
            self.assertEqual(result["manual_reviews"], [{
                "job_id": next(example["job_id"] for example in result["examples"] if example["title"] == "Java Architect"),
                "title": "Java Architect",
                "company": "Databricks",
                "fast_bucket": "needs_manual_review",
                "deep_score": 5.5,
                "outcome": "manual_review",
            }])
            self.assertEqual(result["false_negatives"], [{
                "job_id": next(example["job_id"] for example in result["examples"] if example["title"] == "Solutions Engineer"),
                "title": "Solutions Engineer",
                "company": "Databricks",
                "fast_bucket": "lt_7",
                "deep_score": 7.8,
                "outcome": "false_negative",
            }])
            self.assertEqual(verify_db.conn.execute("SELECT COUNT(*) FROM job_fast_ratings").fetchone()[0], 0)
            self.assertEqual(verify_db.conn.execute("SELECT COUNT(*) FROM job_ratings").fetchone()[0], 0)

    def test_compare_rating_models_runs_deep_default_fast_and_openrouter_fast_on_same_sample(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "jobs.sqlite"
            db = Database(db_path)
            db.init()
            db.upload_resume_file("resume.txt", b"SQL Python analytics", "text/plain")
            db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"], "technical_skills": ["SQL", "Python"]}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            db.upsert_jobs(company, [
                NormalizedJob(
                    company_name="Databricks", source="manual", source_job_id="models-strong", requisition_id=None,
                    title="Data Analyst", location="California", remote_type=None, department="Data",
                    employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/models-strong",
                    apply_url="https://example.com/apply/models-strong", description_raw_html="", description_text="SQL Python analytics",
                    posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks", source="manual", source_job_id="models-miss", requisition_id=None,
                    title="Solutions Engineer", location="California", remote_type=None, department="Field Engineering",
                    employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/models-miss",
                    apply_url="https://example.com/apply/models-miss", description_raw_html="", description_text="Technical customer-facing SQL analytics solutions work",
                    posted_at=None,
                ),
            ], refresh_mode="search")
            db.conn.close()

            calls = []

            def fake_deep(**kwargs):
                calls.append(("deep", kwargs["model_name"]))
                score = 8.2 if '"title": "Data Analyst"' in kwargs["user_prompt"] else 7.8
                return {
                    "overall_score": score,
                    "skill_fit_score": score,
                    "practical_fit_score": score,
                    "recommendation": "test",
                    "categories": {},
                    "strongest_evidence": [],
                    "main_gaps": [],
                    "practical_notes": [],
                    "resume_tailoring_notes": [],
                    "interview_probability_reasoning": "test",
                    "apply_decision": "test",
                }

            def fake_default_fast(**kwargs):
                calls.append(("default_fast", kwargs["model_name"]))
                bucket = "gte_7" if '"title": "Data Analyst"' in kwargs["user_prompt"] else "lt_7"
                return {"bucket": bucket, "confidence": "high", "reason_codes": [], "short_reason": "test"}

            def fake_openrouter_fast(**kwargs):
                calls.append(("openrouter_fast", kwargs["model_name"]))
                return {"bucket": "gte_7", "confidence": "high", "reason_codes": [], "short_reason": "test"}

            result = compare_rating_models(
                sample_size=10,
                db_path=db_path,
                deep_llm_client=fake_deep,
                default_fast_llm_client=fake_default_fast,
                openrouter_fast_llm_client=fake_openrouter_fast,
                deep_model_name="deep-test-model",
                default_fast_model_name="deep-test-model",
                openrouter_fast_model_name="deepseek/deepseek-v4-flash",
            )

            verify_db = Database(db_path)
            verify_db.init()
            self.assertTrue(result["dry_run"])
            self.assertEqual(result["compared"], 2)
            self.assertEqual(result["deep_model_name"], "deep-test-model")
            self.assertEqual(result["fast_models"]["previous_fast"]["model_name"], "deep-test-model")
            self.assertEqual(result["fast_models"]["openrouter_v4_flash"]["model_name"], "deepseek/deepseek-v4-flash")
            self.assertEqual(result["fast_models"]["previous_fast"]["confusion_matrix"]["false_negative"], 1)
            self.assertEqual(result["fast_models"]["previous_fast"]["recall"], 0.5)
            self.assertEqual(result["fast_models"]["openrouter_v4_flash"]["confusion_matrix"]["true_positive"], 2)
            self.assertEqual(result["fast_models"]["openrouter_v4_flash"]["recall"], 1.0)
            self.assertEqual({kind for kind, _ in calls}, {"deep", "default_fast", "openrouter_fast"})
            self.assertEqual(verify_db.conn.execute("SELECT COUNT(*) FROM job_fast_ratings").fetchone()[0], 0)
            self.assertEqual(verify_db.conn.execute("SELECT COUNT(*) FROM job_ratings").fetchone()[0], 0)

    def test_llm_rating_recommendation_is_normalized_to_score_band(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"SQL Python supply chain analytics", "text/plain")
            db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"], "proof_points": []}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            _, _, ids = db.upsert_jobs(company, [NormalizedJob(
                company_name="Databricks", source="manual", source_job_id="calibration-1", requisition_id=None,
                title="Analytics Engineer", location="California", remote_type=None, department="Operations",
                employment_type=None, salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job/calibration",
                apply_url="https://example.com/apply/calibration", description_raw_html="", description_text="SQL Python analytics",
                posted_at=None,
            )], refresh_mode="search")
            fake_llm = MagicMock(return_value={
                "overall_score": 8.3, "skill_fit_score": 8.5, "practical_fit_score": 8.0,
                "recommendation": "Apply ASAP", "categories": {}, "strongest_evidence": ["SQL"],
                "main_gaps": ["Minor seniority gap"], "practical_notes": [], "resume_tailoring_notes": [],
                "interview_probability_reasoning": "Good but not excellent fit", "apply_decision": "Apply ASAP",
            })

            row = db.rate_job_with_llm(ids[0], fake_llm, model_name="test-model")
            rating = json.loads(row["rating_json"])

            self.assertEqual(row["overall_score"], 8.3)
            self.assertEqual(row["recommendation"], "Good stretch — apply if interested")
            self.assertEqual(rating["recommendation"], "Good stretch — apply if interested")
            self.assertEqual(rating["apply_decision"], "Good stretch — apply if interested")

    def test_bulk_llm_rating_uses_bounded_concurrency_and_reports_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"SQL Python supply chain analytics", "text/plain")
            db.extract_llm_profile_from_active_resumes(
                MagicMock(return_value={"candidate_summary": "analytics", "target_roles": ["analyst"], "proof_points": []}),
                model_name="test-model",
            )
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks", source="manual", source_job_id=f"fast-{i}", requisition_id=None,
                    title=f"Data Analyst {i}", location="California", remote_type=None, department="Operations",
                    employment_type=None, salary_min=None, salary_max=None, currency=None, job_url=f"https://example.com/fast/{i}",
                    apply_url=f"https://example.com/apply/fast/{i}", description_raw_html="", description_text="SQL Python analytics",
                    posted_at=None,
                )
                for i in range(4)
            ]
            _, _, ids = db.upsert_jobs(company, jobs, refresh_mode="search")
            progress_events = []

            def slow_llm(**_kwargs):
                time.sleep(0.1)
                return {
                    "overall_score": 8.7, "skill_fit_score": 8.8, "practical_fit_score": 8.6,
                    "recommendation": "Strong fit", "categories": {}, "strongest_evidence": ["SQL"],
                    "main_gaps": [], "practical_notes": [], "resume_tailoring_notes": [],
                    "interview_probability_reasoning": "Strong analytics fit", "apply_decision": "apply",
                }

            start = time.monotonic()
            summary = db.rate_jobs_with_llm(ids, slow_llm, model_name="test-model", max_workers=4, progress_callback=progress_events.append)
            elapsed = time.monotonic() - start

            self.assertLess(elapsed, 0.3)
            self.assertEqual(summary["requested"], 4)
            self.assertEqual(summary["rated"], 4)
            self.assertEqual(summary["failed"], 0)
            self.assertEqual(summary["completed"], 4)
            self.assertTrue(any(event["status"] == "rated" for event in progress_events))
            self.assertEqual(db.conn.execute("SELECT COUNT(*) FROM job_ratings").fetchone()[0], 4)

    def test_query_job_ids_supports_all_matching_without_pagination_and_page_scope_uses_visible_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks", source="manual", source_job_id=f"page-{i}", requisition_id=None,
                    title=f"Data Analyst {i}", location="California", remote_type=None, department="Operations",
                    employment_type=None, salary_min=None, salary_max=None, currency=None, job_url=f"https://example.com/job/{i}",
                    apply_url=f"https://example.com/apply/{i}", description_raw_html="", description_text="SQL Python analytics",
                    posted_at=None,
                )
                for i in range(3)
            ]
            _, _, ids = db.upsert_jobs(company, jobs, refresh_mode="search")

            all_ids = query_job_ids_from_db(db, {"company": ["databricks"], "per_page": ["2"], "page": ["1"]}, scope="all")
            page_ids = query_job_ids_from_db(db, {"company": ["databricks"], "per_page": ["2"], "page": ["1"]}, scope="page")

            self.assertEqual(set(all_ids), set(ids))
            self.assertEqual(len(page_ids), 2)
            self.assertTrue(set(page_ids).issubset(set(ids)))

    def test_bulk_rating_background_returns_before_rating_finishes(self):
        reset_background_jobs_for_tests()
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def slow_runner(_db, _job_ids):
            started.set()
            release.wait(timeout=2)
            finished.set()
            return {"requested": 2, "rated": 2, "failed": 0}

        start = time.monotonic()
        job = start_llm_bulk_rating_background(Path("/tmp/unused.sqlite"), [1, 2], runner=slow_runner)
        elapsed = time.monotonic() - start

        self.assertLess(elapsed, 0.2)
        self.assertEqual(job["status"], "running")
        self.assertTrue(started.wait(timeout=1))
        self.assertFalse(finished.is_set())

        release.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            job = start_llm_bulk_rating_background(Path("/tmp/unused.sqlite"), [1, 2], runner=slow_runner)
            if job["status"] == "succeeded":
                break
            time.sleep(0.01)
        self.assertEqual(job["status"], "succeeded")
        self.assertIn("Rated 2 of 2 jobs", job["message"])
        reset_background_jobs_for_tests()

    def test_fast_rating_background_returns_before_rating_finishes(self):
        reset_background_jobs_for_tests()
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def slow_runner(_db, _job_ids):
            started.set()
            release.wait(timeout=2)
            finished.set()
            return {"requested": 2, "rated": 2, "failed": 0}

        start = time.monotonic()
        job = start_llm_fast_rating_background(Path("/tmp/unused.sqlite"), [1, 2], runner=slow_runner)
        elapsed = time.monotonic() - start

        self.assertLess(elapsed, 0.2)
        self.assertEqual(job["status"], "running")
        self.assertTrue(started.wait(timeout=1))
        self.assertFalse(finished.is_set())

        release.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            job = background_job_snapshot("llm_fast_rating") or job
            if job["status"] == "succeeded":
                break
            time.sleep(0.01)
        self.assertEqual(job["status"], "succeeded")
        self.assertIn("Fast-rated 2 of 2 jobs", job["message"])
        reset_background_jobs_for_tests()

    def test_dashboard_renders_bulk_rating_buttons(self):
        html = render_bulk_rating_controls({"q": ["analyst"], "company": ["databricks"], "page": ["2"]}, [10, 11], "/?q=analyst&company=databricks&page=2")

        self.assertIn("Fast rate unrated matching jobs", html)
        self.assertIn("Fast rate unrated jobs on this page", html)
        self.assertIn("Deep rate fast-rated &gt;=7 matching jobs", html)
        self.assertIn("Deep rate all matching jobs with LLM", html)
        self.assertIn("Deep rate jobs on this page with LLM", html)
        self.assertIn('name="scope" value="all"', html)
        self.assertIn('name="scope" value="page"', html)
        self.assertIn('name="job_id" value="10"', html)
        self.assertIn('name="job_id" value="11"', html)
        self.assertIn('name="q" value="analyst"', html)
        self.assertIn('name="company" value="databricks"', html)

    def test_llm_profile_extraction_background_returns_before_llm_finishes(self):
        reset_background_jobs_for_tests()
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def slow_runner(_db):
            started.set()
            release.wait(timeout=2)
            finished.set()
            return {"model_name": "slow-test-model"}

        start = time.monotonic()
        job = start_llm_profile_extraction_background(Path("/tmp/unused.sqlite"), runner=slow_runner)
        elapsed = time.monotonic() - start

        self.assertLess(elapsed, 0.2)
        self.assertEqual(job["status"], "running")
        self.assertTrue(started.wait(timeout=1))
        self.assertFalse(finished.is_set())

        release.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            job = start_llm_profile_extraction_background(Path("/tmp/unused.sqlite"), runner=slow_runner)
            if job["status"] == "succeeded":
                break
            time.sleep(0.01)
        self.assertEqual(job["status"], "succeeded")
        self.assertIn("slow-test-model", job["message"])
        reset_background_jobs_for_tests()

    def test_dashboard_renders_llm_profile_and_job_rating_controls(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            db.upload_resume_file("resume.txt", b"SQL Python", "text/plain")
            db.extract_llm_profile_from_active_resumes(MagicMock(return_value={"candidate_summary": "LLM summary", "target_roles": ["analyst"]}), model_name="test-model")
            company = db.companies("databricks")[0]
            _, _, ids = db.upsert_jobs(company, [NormalizedJob(
                company_name="Databricks", source="manual", source_job_id="ui-rating", requisition_id=None,
                title="Data Analyst", location="California", remote_type=None, department="Operations", employment_type=None,
                salary_min=None, salary_max=None, currency=None, job_url="https://example.com/job", apply_url="https://example.com/apply",
                description_raw_html="", description_text="SQL Python analytics", posted_at=None,
            )], refresh_mode="search")
            db.rate_job_with_llm(ids[0], MagicMock(return_value={
                "overall_score": 8.8, "skill_fit_score": 8.8, "practical_fit_score": 8.8,
                "recommendation": "Strong fit", "categories": {}, "strongest_evidence": ["SQL"],
                "main_gaps": [], "practical_notes": [], "resume_tailoring_notes": [],
                "interview_probability_reasoning": "Good fit", "apply_decision": "apply",
            }), model_name="test-model")

            profile_html = render_profile_section(db)
            detail_html = render_job_from_db(db, ids[0])

            self.assertIn("Extract/update profile with LLM", profile_html)
            self.assertIn("LLM summary", profile_html)
            self.assertIn("Rate this job with LLM", detail_html)
            self.assertIn("Fit score: 8.8", detail_html)
            self.assertIn("Strong fit", detail_html)

    def test_candidate_and_filtered_out_statuses_migrate_to_new(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            now = "2026-01-01T00:00:00+00:00"
            for status, source_id in [("candidate", "legacy-candidate"), ("filtered_out", "legacy-filtered")]:
                db.conn.execute(
                    """
                    INSERT INTO jobs(
                        company_id, company_name, source, source_job_id, title, job_url, apply_url,
                        first_seen_at, last_seen_at, status, filter_reason, content_hash, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (company["id"], "Databricks", "greenhouse", source_id, "Software Engineer", "https://example.com/job", "https://example.com/job", now, now, status, "legacy reason", f"hash-{source_id}", now, now),
                )
            db.conn.commit()
            db.migrate_deprecated_statuses_to_new()
            rows = db.conn.execute("SELECT status, filter_reason FROM jobs ORDER BY source_job_id").fetchall()
            self.assertEqual([row["status"] for row in rows], ["new", "new"])
            self.assertEqual([row["filter_reason"] for row in rows], ["ready for rating", "ready for rating"])

    def test_ingestion_audit_warns_for_missing_required_fields_and_extra_keys(self):
        job = NormalizedJob(
            company_name="Example",
            source="testsource",
            source_job_id="1",
            requisition_id=None,
            title="Untitled",
            location="",
            remote_type=None,
            department=None,
            employment_type=None,
            salary_min=None,
            salary_max=None,
            currency=None,
            job_url="https://example.com/job",
            apply_url="https://example.com/job",
            description_raw_html="",
            description_text="",
            posted_at=None,
            raw_json={"title": "Untitled", "unexpected_field": "kept in raw"},
        )
        audit = build_ingestion_audit(job)
        self.assertIn("location", audit["required_missing"])
        self.assertIn("description_text", audit["required_missing"])
        self.assertIn("posted_at", audit["required_missing"])
        self.assertIn("Missing description", audit["warnings"])
        self.assertIn("unexpected_field", audit["extra_keys"])
        self.assertEqual(audit["description_length"], 0)
        self.assertEqual(audit["location_status"], "missing")

    def test_upsert_stores_latest_audit_and_dashboard_badge_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            job = NormalizedJob(
                company_name="Databricks",
                source="greenhouse",
                source_job_id="audit-1",
                requisition_id="audit-1",
                title="Software Engineer",
                location="San Francisco, California",
                remote_type="onsite",
                department="Engineering",
                employment_type=None,
                salary_min=None,
                salary_max=None,
                currency=None,
                job_url="https://example.com/job",
                apply_url="https://example.com/job",
                description_raw_html="",
                description_text="",
                posted_at="2026-01-01",
                raw_json={"id": "audit-1", "title": "Software Engineer", "custom_source_field": True},
            )
            db.upsert_jobs(company, [job])
            audit_rows = db.conn.execute("SELECT * FROM job_ingestion_audits").fetchall()
            self.assertEqual(len(audit_rows), 1)
            warnings = json.loads(audit_rows[0]["warnings_json"])
            self.assertIn("Missing description", warnings)
            rows, total, *_ = query_jobs_from_db(db, {"q": ["Software Engineer"]})
            self.assertEqual(total, 1)
            self.assertIn("Missing description", rows[0]["audit_warnings_json"])
            self.assertIn("⚠ Missing description", audit_badges(rows[0]))
            detail_row = get_job_from_db(db, rows[0]["id"])
            audit_html = render_audit_section(detail_row)
            self.assertIn("Ingestion audit", audit_html)
            self.assertIn("Missing description", audit_html)
            self.assertIn("Raw snapshot available", audit_html)
            self.assertIn("Description length", audit_html)

    def test_dashboard_renders_separate_full_and_search_refresh_tabs(self):
        html = render_index({})
        self.assertIn('class="refresh-tabs"', html)
        self.assertIn('Full refresh tab', html)
        self.assertIn('Search refresh tab', html)
        self.assertIn('Daily full refresh status', html)
        self.assertIn('Scheduled for 12:00 AM daily', html)

    def test_refresh_status_section_shows_latest_success_and_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            databricks = db.companies("databricks")[0]
            nvidia = db.companies("nvidia")[0]
            success_run = db.start_run(databricks["id"])
            db.finish_run(success_run, "success", found=10, created=2, updated=7, expired=1)
            failed_run = db.start_run(nvidia["id"])
            db.finish_run(failed_run, "error", found=0, created=0, updated=0, expired=0, error="HTTP 500")

            html = render_refresh_status_section(db)

            self.assertIn("Daily full refresh status", html)
            self.assertIn("Databricks", html)
            self.assertIn("NVIDIA", html)
            self.assertIn("✅ success", html)
            self.assertIn("❌ failed", html)
            self.assertIn("found=10 created=2 updated=7 expired=1", html)
            self.assertIn("Error: HTTP 500", html)

    def test_dashboard_control_panels_are_ordered_and_hideable(self):
        html = render_index({})

        refresh_idx = html.index('<details class="dashboard-panel refresh-panel"')
        profile_idx = html.index('<details class="dashboard-panel profile-panel-wrapper"')
        filters_idx = html.index('<details class="dashboard-panel filters-panel"')
        rating_idx = html.index('<details class="dashboard-panel rating-panel-wrapper"')

        self.assertLess(refresh_idx, profile_idx)
        self.assertLess(profile_idx, filters_idx)
        self.assertLess(filters_idx, rating_idx)
        self.assertIn('<summary>Refresh jobs</summary>', html)
        self.assertIn('<summary>Resume/Profile upload & extraction</summary>', html)
        self.assertIn('<summary>Search & filter saved jobs</summary>', html)
        self.assertIn('<summary>LLM bulk rating</summary>', html)

    def test_dashboard_collapsible_panel_state_persists_across_refresh(self):
        html = render_index({})

        self.assertIn('data-panel-key="refresh"', html)
        self.assertIn('data-panel-key="profile"', html)
        self.assertIn('data-panel-key="filters"', html)
        self.assertIn('data-panel-key="rating"', html)
        self.assertIn('localStorage.getItem(`jobsearch.panel.${key}`)', html)
        self.assertIn('localStorage.setItem(`jobsearch.panel.${key}`, panel.open ? "open" : "closed")', html)
        self.assertIn('querySelectorAll("details.dashboard-panel[data-panel-key]")', html)

    def test_unhide_all_jobs_button_is_inside_search_filter_panel(self):
        html = render_index({})
        filters_idx = html.index('<details class="dashboard-panel filters-panel"')
        unhide_idx = html.index('action="/jobs/unhide-all"')
        filter_idx = html.index('<button type="submit">Filter</button>')
        rating_idx = html.index('<details class="dashboard-panel rating-panel-wrapper"')

        self.assertGreater(unhide_idx, filters_idx)
        self.assertLess(unhide_idx, filter_idx)
        self.assertLess(unhide_idx, rating_idx)
        self.assertIn('class="secondary-action"', html)
        self.assertIn('name="mode" value="full"', html)
        self.assertIn('name="mode" value="search"', html)
        self.assertNotIn('value="fast"', html)
        self.assertIn("Full refresh uses only company and always expires missing jobs in the selected company scope.", html)
        self.assertIn("Search refresh uses optional query, location, and limit. Empty query is allowed and search refresh never expires missing jobs.", html)
        self.assertIn('name="source_q"', html)
        self.assertIn('name="source_location"', html)
        self.assertIn('name="location"', html)
        self.assertIn("Source location filter checks the location text from the company career site before saving results. Use commas for multiple locations; matches any entry.", html)
        self.assertIn("Dashboard location filter only checks the saved job location field. Use commas for multiple locations; matches any entry.", html)
        self.assertIn("Search mode finds new jobs from company career sites before saving them here.", html)
        self.assertIn("Databricks is usually faster because one career-site response already includes job descriptions.", html)
        self.assertIn("NVIDIA can be slower because the app asks NVIDIA for extra details for each matching job.", html)
        self.assertIn("NVIDIA's career site decides what fields match your words; exact fields are not guaranteed.", html)
        self.assertIn("It usually searches job posting text such as title and description.", html)
        self.assertIn("For Databricks, we download the job list first, then keep jobs where the title, location, or description contains your words.", html)
        self.assertIn("Regular dashboard search only searches jobs that are already saved in this app.", html)
        self.assertIn("It checks title, company, location, and description.", html)
        self.assertNotIn('value="candidate"', html)
        self.assertNotIn('value="filtered_out"', html)
        self.assertNotIn('value="hidden"', html)
        self.assertIn('value="newly_discovered"', html)
        self.assertIn('name="source_status"', html)
        self.assertIn('name="review_status"', html)
        self.assertIn('name="saved"', html)
        self.assertIn('name="exclude_title"', html)
        self.assertIn('Exclude titles containing', html)
        self.assertNotIn('Exclude title keywords', html)
        self.assertIn('color-scheme: light', html)
        self.assertIn('--bg:#f8fafc', html)
        self.assertIn('--accent:#2563eb', html)
        self.assertIn('.status-newly_discovered', html)
        self.assertIn('name="hidden"', html)
        self.assertIn('Only hidden', html)

    def test_dashboard_renders_sort_select_in_filter_panel(self):
        html = render_index({})
        filters_idx = html.index('<details class="dashboard-panel filters-panel"')
        sort_idx = html.index('name="sort"')
        rating_idx = html.index('<details class="dashboard-panel rating-panel-wrapper"')

        self.assertGreater(sort_idx, filters_idx)
        self.assertLess(sort_idx, rating_idx)
        self.assertIn('<option value="newest"', html)
        self.assertIn('<option value="rating_desc"', html)
        self.assertIn('Highest LLM fit score', html)

    def test_query_jobs_supports_user_selected_sort_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks", source="greenhouse", source_job_id="z-job", requisition_id="z-job",
                    title="Zebra Analyst", location="US Remote", remote_type="remote", department="Operations",
                    employment_type=None, salary_min=None, salary_max=None, currency=None,
                    job_url="https://example.com/z", apply_url="https://example.com/z",
                    description_raw_html="Analytics", description_text="Analytics", posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks", source="greenhouse", source_job_id="a-job", requisition_id="a-job",
                    title="Alpha Analyst", location="US Remote", remote_type="remote", department="Operations",
                    employment_type=None, salary_min=None, salary_max=None, currency=None,
                    job_url="https://example.com/a", apply_url="https://example.com/a",
                    description_raw_html="Analytics", description_text="Analytics", posted_at=None,
                ),
            ]
            db.upsert_jobs(company, jobs)

            rows, *_ = query_jobs_from_db(db, {"sort": ["title_asc"]})

            self.assertEqual([row["title"] for row in rows], ["Alpha Analyst", "Zebra Analyst"])

    def test_tooltip_explanations_are_hidden_until_hover(self):
        html = render_index({})

        self.assertIn('class="tooltip"', html)
        self.assertIn('data-tooltip="Search mode finds new jobs from company career sites before saving them here.', html)
        self.assertNotIn('<small class="hint wide">Search mode finds new jobs from company career sites before saving them here.', html)
        self.assertNotIn('<small class="hint wide">Regular dashboard search only searches jobs that are already saved in this app.', html)

    def test_parse_search_refresh_allows_empty_query_and_prefilters_dashboard(self):
        request, redirect_params = parse_refresh_form(
            {"mode": ["search"], "source_q": [""], "source_location": ["California, Remote"], "company": ["nvidia"], "limit": ["25"]}
        )
        self.assertEqual(
            request,
            {"mode": "search", "search_text": "", "location_filter": "California, Remote", "slug": "nvidia", "limit": 25},
        )
        self.assertNotIn("q", redirect_params)
        self.assertEqual(redirect_params["location"], "California, Remote")
        self.assertEqual(redirect_params["company"], "nvidia")

    def test_parse_search_refresh_with_query_prefilters_dashboard(self):
        request, redirect_params = parse_refresh_form(
            {"mode": ["search"], "source_q": ["software engineer"], "source_location": ["California, Remote"], "company": ["nvidia"], "limit": ["25"]}
        )
        self.assertEqual(
            request,
            {"mode": "search", "search_text": "software engineer", "location_filter": "California, Remote", "slug": "nvidia", "limit": 25},
        )
        self.assertEqual(redirect_params["q"], "software engineer")
        self.assertEqual(redirect_params["location"], "California, Remote")
        self.assertEqual(redirect_params["company"], "nvidia")

    def test_parse_full_refresh_ignores_search_and_limit_filters(self):
        request, redirect_params = parse_refresh_form(
            {"mode": ["full"], "source_q": ["software engineer"], "source_location": ["Remote"], "company": ["databricks"], "limit": ["25"]}
        )
        self.assertEqual(request, {"mode": "full", "search_text": "", "location_filter": "", "slug": "databricks", "limit": None})
        self.assertEqual(redirect_params["company"], "databricks")
        self.assertNotIn("q", redirect_params)
        self.assertNotIn("location", redirect_params)

    def test_hidden_jobs_are_excluded_by_default_and_included_on_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="visible",
                    requisition_id="visible",
                    title="Software Engineer Backend",
                    location="US Remote",
                    remote_type="remote",
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/visible",
                    apply_url="https://example.com/visible",
                    description_raw_html="Python platform engineering",
                    description_text="Python platform engineering",
                    posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="hidden",
                    requisition_id="hidden",
                    title="Software Engineer Hidden",
                    location="US Remote",
                    remote_type="remote",
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/hidden",
                    apply_url="https://example.com/hidden",
                    description_raw_html="Python platform engineering",
                    description_text="Python platform engineering",
                    posted_at=None,
                ),
            ]
            _created, _updated, seen_ids = db.upsert_jobs(company, jobs)
            db.hide_job(seen_ids[1])

            default_rows, default_total, *_ = query_jobs_from_db(db, {})
            include_rows, include_total, *_ = query_jobs_from_db(db, {"hidden": ["include"]})
            hidden_rows, hidden_total, *_ = query_jobs_from_db(db, {"hidden": ["only"]})

            self.assertEqual(default_total, 1)
            self.assertEqual(default_rows[0]["source_job_id"], "visible")
            self.assertEqual(include_total, 2)
            self.assertEqual(hidden_total, 1)
            self.assertEqual(hidden_rows[0]["source_job_id"], "hidden")

    def test_hide_unhide_actions_render_on_cards_and_detail_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="visible-action",
                    requisition_id="visible-action",
                    title="Visible Action Engineer",
                    location="US Remote",
                    remote_type="remote",
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/visible-action",
                    apply_url="https://example.com/visible-action",
                    description_raw_html="Python platform engineering",
                    description_text="Python platform engineering",
                    posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="hidden-action",
                    requisition_id="hidden-action",
                    title="Hidden Action Engineer",
                    location="US Remote",
                    remote_type="remote",
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/hidden-action",
                    apply_url="https://example.com/hidden-action",
                    description_raw_html="Python platform engineering",
                    description_text="Python platform engineering",
                    posted_at=None,
                ),
            ]
            _created, _updated, seen_ids = db.upsert_jobs(company, jobs)
            db.hide_job(seen_ids[1])
            visible = get_job_from_db(db, seen_ids[0])
            hidden = get_job_from_db(db, seen_ids[1])

            visible_action = render_job_action(visible, "/?hidden=include")
            hidden_action = render_job_action(hidden, "/?hidden=only")
            visible_detail = render_job_from_db(db, seen_ids[0])
            hidden_detail = render_job_from_db(db, seen_ids[1])

            self.assertIn(f'action="/jobs/{seen_ids[0]}/hide"', visible_action)
            self.assertIn(">Hide</button>", visible_action)
            self.assertNotIn("/unhide", visible_action)
            self.assertIn(f'action="/jobs/{seen_ids[1]}/unhide"', hidden_action)
            self.assertIn(">Unhide</button>", hidden_action)
            self.assertIn(f'action="/jobs/{seen_ids[0]}/hide"', visible_detail)
            self.assertIn(f'action="/jobs/{seen_ids[1]}/unhide"', hidden_detail)

    def test_unhide_all_restores_hidden_jobs_and_dashboard_has_top_button(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="hidden-one",
                    requisition_id="hidden-one",
                    title="Hidden One",
                    location="US Remote",
                    remote_type="remote",
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/hidden-one",
                    apply_url="https://example.com/hidden-one",
                    description_raw_html="Python platform engineering",
                    description_text="Python platform engineering",
                    posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="hidden-two",
                    requisition_id="hidden-two",
                    title="Hidden Two",
                    location="US Remote",
                    remote_type="remote",
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/hidden-two",
                    apply_url="https://example.com/hidden-two",
                    description_raw_html="Python platform engineering",
                    description_text="Python platform engineering",
                    posted_at=None,
                ),
            ]
            _created, _updated, seen_ids = db.upsert_jobs(company, jobs)
            db.hide_job(seen_ids[0])
            db.hide_job(seen_ids[1])

            self.assertEqual(db.unhide_all_jobs(), 2)
            rows, total, *_ = query_jobs_from_db(db, {"hidden": ["only"]})

            self.assertEqual(total, 0)
            html = render_index({})
            self.assertIn('action="/jobs/unhide-all"', html)
            self.assertIn("Unhide all jobs", html)

    def test_review_schema_columns_and_legacy_status_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            columns = {row["name"] for row in db.conn.execute("PRAGMA table_info(jobs)")}

            self.assertIn("source_status", columns)
            self.assertIn("review_status", columns)
            self.assertIn("is_hidden", columns)
            self.assertIn("is_saved", columns)
            self.assertIn("saved_at", columns)
            self.assertIn("reviewed_at", columns)
            self.assertIn("applied_at", columns)

            company = db.companies("databricks")[0]
            db.conn.execute(
                """
                INSERT INTO jobs(company_id, company_name, source, source_job_id, title, job_url, apply_url, first_seen_at, last_seen_at, status, filter_reason, content_hash, created_at, updated_at)
                VALUES (?, ?, 'greenhouse', 'legacy-hidden', 'Legacy Hidden', 'https://example.com/job', 'https://example.com/apply', '2026-01-01', '2026-01-01', 'hidden', 'old', 'hash', '2026-01-01', '2026-01-01')
                """,
                (company["id"], company["name"]),
            )
            db.conn.execute(
                """
                INSERT INTO jobs(company_id, company_name, source, source_job_id, title, job_url, apply_url, first_seen_at, last_seen_at, status, filter_reason, content_hash, created_at, updated_at)
                VALUES (?, ?, 'greenhouse', 'legacy-new', 'Legacy New', 'https://example.com/job2', 'https://example.com/apply2', '2026-01-01', '2026-01-01', 'new', 'old', 'hash2', '2026-01-01', '2026-01-01')
                """,
                (company["id"], company["name"]),
            )
            db.conn.commit()
            db.migrate_review_state_columns()
            rows = {row["source_job_id"]: row for row in db.conn.execute("SELECT * FROM jobs WHERE source_job_id LIKE 'legacy-%'")}

            self.assertEqual(rows["legacy-hidden"]["is_hidden"], 1)
            self.assertEqual(rows["legacy-hidden"]["source_status"], "active")
            self.assertEqual(rows["legacy-new"]["source_status"], "newly_discovered")
            self.assertEqual(rows["legacy-new"]["review_status"], "unreviewed")

    def test_full_refresh_turns_existing_newly_discovered_jobs_active(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            job = NormalizedJob(
                company_name="Databricks", source="greenhouse", source_job_id="same", requisition_id="same",
                title="Same Job", location="US Remote", remote_type="remote", department="Engineering",
                employment_type=None, salary_min=None, salary_max=None, currency=None,
                job_url="https://example.com/same", apply_url="https://example.com/same",
                description_raw_html="Python", description_text="Python", posted_at=None,
            )
            db.upsert_jobs(company, [job], refresh_mode="search")
            row = db.conn.execute("SELECT source_status FROM jobs WHERE source_job_id='same'").fetchone()
            self.assertEqual(row["source_status"], "newly_discovered")

            db.upsert_jobs(company, [job], refresh_mode="full")
            row = db.conn.execute("SELECT source_status FROM jobs WHERE source_job_id='same'").fetchone()
            self.assertEqual(row["source_status"], "active")

    def test_save_buttons_and_apply_open_route_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            job = NormalizedJob(
                company_name="Databricks", source="greenhouse", source_job_id="save-me", requisition_id="save-me",
                title="Save Me", location="US Remote", remote_type="remote", department="Engineering",
                employment_type=None, salary_min=None, salary_max=None, currency=None,
                job_url="https://example.com/save-me", apply_url="https://example.com/apply-save-me",
                description_raw_html="Python", description_text="Python", posted_at=None,
            )
            _created, _updated, seen_ids = db.upsert_jobs(company, [job])
            row = get_job_from_db(db, seen_ids[0])
            action = render_job_action(row, "/")
            detail = render_job_from_db(db, seen_ids[0])
            self.assertIn(f'action="/jobs/{seen_ids[0]}/save"', action)
            self.assertIn(">Save</button>", action)
            self.assertIn(f'href="/jobs/{seen_ids[0]}/open"', detail)

            db.save_job(seen_ids[0])
            row = get_job_from_db(db, seen_ids[0])
            self.assertEqual(row["is_saved"], 1)
            self.assertIn(f'action="/jobs/{seen_ids[0]}/unsave"', render_job_action(row, "/"))
            self.assertIn(">Unsave</button>", render_job_action(row, "/"))

            url = db.mark_applied(seen_ids[0])
            row = get_job_from_db(db, seen_ids[0])
            self.assertEqual(url, "https://example.com/apply-save-me")
            self.assertEqual(row["review_status"], "applied")
            self.assertIsNotNone(row["applied_at"])

    def test_visiting_local_job_detail_marks_unapplied_job_reviewed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks", source="greenhouse", source_job_id="review-me", requisition_id="review-me",
                    title="Review Me", location="US Remote", remote_type="remote", department="Engineering",
                    employment_type=None, salary_min=None, salary_max=None, currency=None,
                    job_url="https://example.com/review-me", apply_url="https://example.com/apply-review-me",
                    description_raw_html="Python", description_text="Python", posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks", source="greenhouse", source_job_id="already-applied", requisition_id="already-applied",
                    title="Already Applied", location="US Remote", remote_type="remote", department="Engineering",
                    employment_type=None, salary_min=None, salary_max=None, currency=None,
                    job_url="https://example.com/applied", apply_url="https://example.com/apply-applied",
                    description_raw_html="Python", description_text="Python", posted_at=None,
                ),
            ]
            _created, _updated, seen_ids = db.upsert_jobs(company, jobs)
            db.mark_applied(seen_ids[1])

            html = render_job_from_db(db, seen_ids[0])
            reviewed = get_job_from_db(db, seen_ids[0])
            applied = get_job_from_db(db, seen_ids[1])
            self.assertIsNotNone(reviewed)
            self.assertIsNotNone(applied)

            self.assertIn("review: reviewed", html)
            self.assertEqual(reviewed["review_status"], "reviewed")
            self.assertIsNotNone(reviewed["reviewed_at"])
            self.assertEqual(applied["review_status"], "applied")

    def test_dashboard_filters_source_review_saved_and_hidden_separately(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = []
            for sid, title in [("new", "New Job"), ("active", "Active Job"), ("applied", "Applied Job")]:
                jobs.append(NormalizedJob(
                    company_name="Databricks", source="greenhouse", source_job_id=sid, requisition_id=sid,
                    title=title, location="US Remote", remote_type="remote", department="Engineering",
                    employment_type=None, salary_min=None, salary_max=None, currency=None,
                    job_url=f"https://example.com/{sid}", apply_url=f"https://example.com/{sid}",
                    description_raw_html="Python", description_text="Python", posted_at=None,
                ))
            _created, _updated, seen_ids = db.upsert_jobs(company, jobs)
            db.conn.execute("UPDATE jobs SET source_status='active' WHERE id=?", (seen_ids[1],))
            db.save_job(seen_ids[1])
            db.mark_applied(seen_ids[2])

            source_rows, source_total, *_ = query_jobs_from_db(db, {"source_status": ["active"]})
            review_rows, review_total, *_ = query_jobs_from_db(db, {"review_status": ["applied"]})
            saved_rows, saved_total, *_ = query_jobs_from_db(db, {"saved": ["saved"]})

            self.assertEqual(source_total, 1)
            self.assertEqual(source_rows[0]["source_job_id"], "active")
            self.assertEqual(review_total, 1)
            self.assertEqual(review_rows[0]["source_job_id"], "applied")
            self.assertEqual(saved_total, 1)
            self.assertEqual(saved_rows[0]["source_job_id"], "active")

    def test_dashboard_exclude_title_keywords_are_view_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="manager",
                    requisition_id="manager",
                    title="Engineering Manager",
                    location="US Remote",
                    remote_type="remote",
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/manager",
                    apply_url="https://example.com/manager",
                    description_raw_html="People leadership",
                    description_text="People leadership",
                    posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="engineer",
                    requisition_id="engineer",
                    title="Software Engineer",
                    location="US Remote",
                    remote_type="remote",
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/engineer",
                    apply_url="https://example.com/engineer",
                    description_raw_html="Python platform engineering",
                    description_text="Python platform engineering",
                    posted_at=None,
                ),
            ]
            db.upsert_jobs(company, jobs)

            rows, total, *_ = query_jobs_from_db(db, {"exclude_title": ["manager"]})

            self.assertEqual(total, 1)
            self.assertEqual(rows[0]["source_job_id"], "engineer")
            statuses = db.conn.execute("SELECT source_job_id, status FROM jobs ORDER BY source_job_id").fetchall()
            self.assertEqual([(row["source_job_id"], row["status"]) for row in statuses], [("engineer", "new"), ("manager", "new")])

    def test_dashboard_location_filter_only_checks_saved_location_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            jobs = [
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="sf",
                    requisition_id="sf",
                    title="Software Engineer",
                    location="San Francisco, CA",
                    remote_type=None,
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/sf",
                    apply_url="https://example.com/sf",
                    description_raw_html="Python platform engineering",
                    description_text="Python platform engineering",
                    posted_at=None,
                ),
                NormalizedJob(
                    company_name="Databricks",
                    source="greenhouse",
                    source_job_id="ny",
                    requisition_id="ny",
                    title="California Systems Engineer",
                    location="New York, NY",
                    remote_type=None,
                    department="Engineering",
                    employment_type=None,
                    salary_min=None,
                    salary_max=None,
                    currency=None,
                    job_url="https://example.com/ny",
                    apply_url="https://example.com/ny",
                    description_raw_html="California customer systems",
                    description_text="California customer systems",
                    posted_at=None,
                ),
            ]
            db.upsert_jobs(company, jobs)

            rows, total, *_ = query_jobs_from_db(db, {"location": ["California"]})

            self.assertEqual(total, 0)
            self.assertEqual(rows, [])
            rows, total, *_ = query_jobs_from_db(db, {"location": ["San Francisco"]})
            self.assertEqual(total, 1)
            self.assertEqual(rows[0]["source_job_id"], "sf")
            rows, total, *_ = query_jobs_from_db(db, {"location": ["San Francisco, New York"]})
            self.assertEqual(total, 2)
            self.assertEqual({row["source_job_id"] for row in rows}, {"sf", "ny"})

    def test_dashboard_renders_hide_controls_and_hidden_filter(self):
        html = render_index({})
        self.assertIn('name="hidden"', html)
        self.assertIn('Exclude hidden', html)
        self.assertIn('Include hidden', html)
        self.assertIn('Only hidden', html)
        self.assertNotIn('name="include_hidden"', html)
        self.assertIn('method="post" action="/jobs/', html)
        self.assertIn('Hide', html)

    def test_full_refresh_always_expires_and_search_refresh_never_expires(self):
        self.assertTrue(should_expire_missing_after_refresh(mode="full", limit=None, search_text=""))
        self.assertTrue(should_expire_missing_after_refresh(mode="full", limit=25, search_text="software", location_filter="Remote, California"))
        self.assertFalse(should_expire_missing_after_refresh(mode="search", limit=None, search_text=""))
        self.assertFalse(should_expire_missing_after_refresh(mode="search", limit=25, search_text="software", location_filter="Remote"))

    def test_greenhouse_connector_uses_standard_list_detail_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("databricks")[0]
            payload = {
                "jobs": [
                    {
                        "id": 1,
                        "requisition_id": "GH1",
                        "title": "Software Engineer",
                        "absolute_url": "https://example.com/job/1",
                        "location": {"name": "US Remote"},
                        "departments": [{"name": "Engineering"}],
                        "content": "<p>Python systems</p>",
                        "first_published": "Today",
                    },
                    {
                        "id": 2,
                        "requisition_id": "GH2",
                        "title": "Sales Manager",
                        "absolute_url": "https://example.com/job/2",
                        "location": {"name": "US Remote"},
                        "departments": [{"name": "Sales"}],
                        "content": "<p>Sales pipeline</p>",
                    },
                ]
            }

            with patch("jobsearch.app.http_json", return_value=payload) as mock_http:
                connector = GreenhouseConnector()
                raw_jobs = connector.fetch_list(company, search_text="python", location_filter="Seattle, Remote")
                enriched = connector.fetch_detail_if_needed(company, raw_jobs[0])
                jobs = connector.fetch(company, search_text="python", location_filter="Seattle, Remote")

            self.assertEqual(mock_http.call_count, 2)
            mock_http.assert_any_call(company["source_api_url"])
            self.assertIs(enriched, raw_jobs[0])
            self.assertEqual(len(raw_jobs), 1)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].description_text, "Python systems")

    def test_nvidia_connector_uses_standard_list_detail_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("nvidia")[0]
            calls = []

            def fake_http_json(url, method="GET", payload=None):
                calls.append((method, url, payload))
                if method == "POST":
                    return {
                        "total": 1,
                        "jobPostings": [
                            {
                                "title": "Senior Software Engineer",
                                "externalPath": "/job/us/Senior-Software-Engineer_JR123",
                                "locationsText": "US Remote",
                                "postedOn": "Today",
                                "bulletFields": ["JR123"],
                            }
                        ],
                    }
                return {
                    "jobPostingInfo": {
                        "title": "Senior Software Engineer",
                        "location": "US Remote",
                        "jobReqId": "JR123",
                        "jobDescription": "<p>CUDA Python systems</p>",
                        "postedOn": "Today",
                    }
                }

            with patch("jobsearch.app.http_json", side_effect=fake_http_json):
                connector = NvidiaWorkdayConnector()
                listings = connector.fetch_list(company, limit=1, mode="search", search_text="software", location_filter="Seattle, Remote")
                enriched = connector.fetch_detail_if_needed(company, listings[0])
                jobs = connector.fetch(company, limit=1, mode="search", search_text="software", location_filter="Seattle, Remote")

            self.assertEqual(len(listings), 1)
            self.assertIn("listing", enriched)
            self.assertIn("detail", enriched)
            self.assertEqual(jobs[0].source_job_id, "JR123")
            self.assertEqual(jobs[0].description_text, "CUDA Python systems")
            self.assertEqual(calls[0][0], "POST")
            self.assertEqual(calls[0][2]["searchText"], "software")

    def test_nvidia_pagination_preserves_first_positive_total(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "jobs.sqlite")
            db.init()
            company = db.companies("nvidia")[0]

            def fake_http_json(url, method="GET", payload=None):
                if method == "POST":
                    offset = payload["offset"]
                    total = 60 if offset == 0 else 0
                    return {
                        "total": total,
                        "jobPostings": [
                            {
                                "title": f"Software Engineer {i}",
                                "externalPath": f"/job/test/Software-Engineer-{i}_JR{i}",
                                "locationsText": "US Remote",
                                "postedOn": "Today",
                                "bulletFields": [f"JR{i}"],
                            }
                            for i in range(offset, offset + payload["limit"])
                        ],
                    }
                return {"jobPostingInfo": {"jobDescription": "Python distributed systems", "postedOn": "Today"}}

            with patch("jobsearch.app.http_json", side_effect=fake_http_json):
                jobs = NvidiaWorkdayConnector().fetch(company, limit=60, mode="full")

            self.assertEqual(len(jobs), 60)
            self.assertEqual(jobs[-1].source_job_id, "JR59")


if __name__ == "__main__":
    unittest.main()
