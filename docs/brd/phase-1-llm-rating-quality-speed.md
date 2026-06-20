# Phase 1 BRD — LLM Rating Quality and Speed Improvements

## 1. Business Objective

Improve the job-search app's LLM rating system so it produces better-calibrated job-fit rankings and finishes large rating batches faster while remaining observable and safe for local single-user use.

The rating system should help the user prioritize applications by expected interview probability plus role fit, not just title similarity. It should surface credible high-priority roles, avoid score compression, and make long-running bulk rating status visible.

## 2. Background

The current implementation can rate jobs with an OpenAI-compatible chat-completions API and persists structured JSON ratings in SQLite. In production use, ratings are overly compressed: many good roles cluster around 8.0–8.4 and none exceed 8.4, while recommendation strings sometimes say `Apply ASAP` for scores below the documented threshold. Bulk rating is sequential and only persists successful rating rows, so sleep/API stalls appear as unexplained dead zones.

## 3. Scope

### In scope

- Calibrate the LLM rating prompt and post-processing so numeric score bands and recommendation labels agree.
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
3. Category names returned by the LLM should be accepted in both current forms (`domain`, `technical_tool`) and rubric forms (`domain_match`, `technical_tool_match`) so downstream UI remains robust.
4. Rating JSON must retain the LLM's reasoning/evidence/gaps even if the recommendation label is normalized.

### Rating speed and observability

1. Bulk LLM rating should support bounded concurrency with a conservative default.
2. The system must avoid sharing one SQLite connection across worker threads.
3. The background job status should show at least requested/rated/failed/completed counts and the current/last job ID.
4. Bulk rating should remain idempotent: already-current ratings are reused unless `force=True`.
5. Failures for individual jobs should not stop the whole batch.

## 5. Data and Storage Strategy

Use existing SQLite tables for persisted ratings. Do not add a persistent attempt table in this iteration. Store bulk progress in the existing in-memory `_BACKGROUND_JOBS` registry so dashboard refreshes can show progress during a running batch.

Future iteration may add a `job_rating_attempts` table with job ID, started/finished timestamps, status, latency, HTTP/provider error, model, and retry count.

## 6. UX Requirements

- Dashboard/background status should communicate that bulk rating is running and include live progress counts when available.
- Recommendation labels in job cards/details should align with score thresholds.
- Existing filters and review workflows should remain unchanged.

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

## 9. Future Considerations

- Add persistent rating-attempt logs and dashboard stall diagnostics.
- Add retry/backoff for transient HTTP 429/5xx failures.
- Add a dashboard control for concurrency and batch size.
- Add model comparison/evaluation runs against known reference jobs.
- Add RAG over resume bullet inventory, project notes, company preferences, and prior application outcomes.
