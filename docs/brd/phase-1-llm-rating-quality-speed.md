# Phase 1 BRD — LLM Rating Quality and Speed Improvements

## Project Pause Status

**Status:** paused / maintenance-only as of 2026-09-14 because the user has finished the current job search.

This BRD captures the Phase 1 rating-quality and speed work that was implemented or validated before pausing. Treat remaining future considerations as restart candidates, not active commitments.

## 1. Business Objective

Improve the job-search app's LLM rating system so it produces better-calibrated job-fit rankings and finishes large rating batches faster while remaining observable and safe for local single-user use.

The rating system should help the user prioritize applications by expected interview probability plus role fit, not just title similarity. It should surface credible high-priority roles, avoid score compression, and make long-running bulk rating status visible.

## 2. Background

The current implementation can rate jobs with an OpenAI-compatible chat-completions API and persists structured JSON ratings in SQLite. In production use, ratings are overly compressed: many good roles cluster around 8.0–8.4 and none exceed 8.4, while recommendation strings sometimes say `Apply ASAP` for scores below the documented threshold. Bulk rating is sequential and only persists successful rating rows, so sleep/API stalls appear as unexplained dead zones.

## 3. Scope

### In scope

- Calibrate the LLM rating prompt and post-processing so numeric score bands and recommendation labels agree, while ensuring all candidate-fit signals come only from the extracted profile JSON.
- Make 8.5+ reachable for credible strong-fit roles without making 9.0+ common.
- Preserve the user's weighted rubric: core function 30%, seniority 20%, domain 15%, tools 15%, evidence 10%, gaps 5%, strategic value 5%.
- Add deterministic recommendation normalization based on numeric `overall_score`.
- Add per-job bulk rating progress and attempt tracking in memory for the dashboard/background job status.
- Speed up bulk rating via bounded concurrency while using one SQLite writer connection pattern safely.
- Keep local DB/runtime artifacts ignored by Git.

### Out of scope

- Changing source ingestion connectors.
- Building resume tailoring or cover-letter generation.
- Migrating from SQLite to Postgres.
- Full persistent audit table for rating attempts; this can come later if in-memory progress is insufficient.
- Browser automation or application submission.

## 4. Functional Requirements

### Rating quality

1. The LLM prompt must define score anchors clearly:
   - 9.0–9.5: rare near-ideal fit with strong direct evidence and practical feasibility.
   - 8.5–8.9: strong credible interview target with manageable gaps; apply with tailoring.
   - 8.0–8.4: good stretch or adjacent fit.
   - 7.0–7.9: medium stretch.
   - 6.0–6.9: reach.
   - below 6.0: skip or low priority.
2. Recommendation labels must be normalized from `overall_score` so they cannot contradict the numeric band.
3. Rating prompts and deterministic/debug rating helpers must use only the current extracted profile JSON for candidate-specific fit signals. They must not contain hardcoded candidate-specific roles, domains, companies, tools, proof points, calibration examples, or hidden profile keyword lists.
4. Category names returned by the LLM should be accepted in both current forms (`domain`, `technical_tool`) and rubric forms (`domain_match`, `technical_tool_match`) so downstream UI remains robust.
5. Rating JSON must retain the LLM's reasoning/evidence/gaps even if the recommendation label is normalized.

### Rating speed and observability

1. Bulk deep LLM rating should support bounded concurrency with a conservative default.
2. The system must avoid sharing one SQLite connection across worker threads.
3. The background job status should show at least requested/rated/failed/completed counts and the current/last job ID.
4. Bulk rating should remain idempotent: already-current ratings are reused unless `force=True`.
5. Failures for individual jobs should not stop the whole batch.
6. Add a recall-heavy fast LLM pre-rating pass that classifies unrated jobs into `gte_7`, `lt_7`, or `needs_manual_review` using the resume-extracted profile JSON rather than hardcoded backend signals.
7. Keep the current LLM v2/deep rating path as the expensive downstream rating, and run it only on manually selected jobs or fast-rated `gte_7` jobs.
8. The CLI fast-rating benchmark must be dry-run/no-write by default; persisting sample results must require an explicit `--save` flag.
9. Add a dedicated three-way model comparison CLI that runs deep rating, the previous/default fast rating using the same configured model as deep rating, and the OpenRouter DeepSeek v4 Flash fast rating on the same sample. It should report separate fast-vs-deep confusion metrics for both fast raters and stay dry-run/no-write by default.

## 5. Data and Storage Strategy

Use separate persisted state for fast ratings and deep ratings. Deep ratings remain in `job_ratings`; fast ratings go in a new `job_fast_ratings` table keyed by job, active profile, job-content hash, rater version, and model. Current rating state is derived rather than mixed with user review state:

- `unrated`: no current fast or deep rating.
- `fast_rated`: current fast rating exists and no current deep rating.
- `deep_rated`: current deep rating exists.
- `needs_manual_review`: current fast rating bucket is `needs_manual_review` and no current deep rating.

Review state remains user/action-driven (`unreviewed`, `reviewed`, `applied`) and separate from rating state.

Future iteration may add a `job_rating_attempts` table with job ID, started/finished timestamps, status, latency, HTTP/provider error, model, and retry count.

## 6. UX Requirements

- Dashboard/background status should communicate that bulk rating is running and include live progress counts when available.
- Recommendation labels in job cards/details should align with score thresholds.
- Search/filter UI should expose separate review-status and rating-status filters.
- LLM bulk controls should expose explicit buttons for fast-rating unrated jobs and deep-rating fast-rated `gte_7` jobs.
- Fast rating should be explicitly recall-biased using only the uploaded-resume-derived profile JSON and the current job JSON. Choose `gte_7` for plausible 7+ fits or adjacent/transferable roles according to the extracted profile; choose `needs_manual_review` when evidence is ambiguous; choose `lt_7` only for clear blockers or roles clearly irrelevant to the extracted profile. Precision is secondary because deep rating is the verifier.
- `llm-fast-rating-v5` should include DeepSeek/cheap-model-specific guardrails against over-strict rejection and explicit “do not use `lt_7` for merely imperfect matches” wording, but examples must be schema-level and profile-agnostic. Do not hardcode the user's current target domains, tools, companies, or proof points into the fast-rating prompt because uploaded resumes and extracted profiles are expected to change.
- The v5 recall-tuning pass specifically addresses observed DeepSeek v4 Flash false negatives caused by treating international location, work authorization, clearance, stretch seniority, unfamiliar title wording, missing specialized subdomain experience, and adjacent customer-facing/data-platform roles as automatic rejections. For fast triage, those should become `needs_manual_review` when the profile has any transferable bridge; `lt_7` should be reserved for no-overlap roles or explicit conflicts with no meaningful bridge.
- Fast-rating prompt/version changes should not change the configured model by default; model selection remains controlled by existing OpenAI-compatible environment variables unless an explicit fast mode is chosen.
- Add an explicit `openrouter-v4-flash` fast mode for benchmarking fast rating through OpenRouter using `deepseek/deepseek-v4-flash`, with `OPENROUTER_API_KEY` and `https://openrouter.ai/api/v1`; deep rating should continue using the normal configured model unless separately overridden.
- Existing source, review, saved, and hidden workflows should remain unchanged.
- CLI usage should make persistence explicit: `python -m jobsearch.app benchmark-fast-rating --sample-size 100` runs a no-write benchmark, while adding `--save` persists fast ratings.
- CLI comparison usage should be available as `python -m jobsearch.app compare-fast-deep-rating --sample-size 100`; it should be no-write by default and use `--save` only when the user explicitly wants to persist both ratings.
- Three-way model comparison usage should be available as `python -m jobsearch.app compare-rating-models --sample-size 100`; it should compare the configured deep model, previous/default fast rating using that same model, and OpenRouter DeepSeek v4 Flash fast rating on the same sample, no-write by default.
- CLI comparison output should include all false negatives separately from the capped `examples` list so recall misses can be inspected without saving dry-run ratings.

## 7. Non-Functional Requirements

- Tests must cover calibration and concurrency behavior before production code changes.
- Default concurrency should be conservative enough to avoid aggressive API rate limits.
- Code should continue to work with any OpenAI-compatible endpoint configured via existing environment variables.
- Full test suite must pass before push.

## 8. Acceptance Criteria

- A rating with `overall_score=8.7` is normalized to a strong-fit recommendation, not downgraded or mislabeled.
- A rating with `overall_score=8.3` cannot retain `Apply ASAP`; it is normalized to the 8.0–8.4 band label.
- Bulk rating with a fake slow LLM client completes substantially faster with concurrency than sequential execution in tests.
- Bulk rating records individual job failures and continues processing remaining jobs.
- Existing tests continue to pass.

## 8.1 Pause Checkpoint

- Deep ratings in the local database are persisted with `gpt-4o-mini`.
- Fast ratings are implemented as a separate concept/table but no fast-rating rows are persisted in the live database at pause time.
- DeepSeek/OpenRouter support is documented for explicit benchmarking/comparison, not as the default dashboard fast-rating model.
- CLI comparisons are designed to be dry-run/no-write unless `--save` is explicitly provided.

## 9. Future Considerations

Because the project is paused, these are optional restart items rather than active commitments:

- Add persistent rating-attempt logs and dashboard stall diagnostics.
- Add retry/backoff for transient HTTP 429/5xx failures.
- Add a dashboard control for concurrency and batch size.
- Add model comparison/evaluation runs against known reference jobs.
- Add RAG over resume bullet inventory, project notes, company preferences, and prior application outcomes.
