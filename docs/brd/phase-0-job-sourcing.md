# Phase 0 BRD — Automated Job Sourcing Pipeline

## 1. Business Objective

Build the first version of a local-first job-search application that automatically pulls jobs from selected company career sites, normalizes them into a common schema, stores them, and displays them in a dashboard for manual review and later LLM-based scoring.

Initial companies:

- Databricks
- NVIDIA

Phase 0 does **not** perform fit scoring, resume tailoring, or application submission. It creates the job data pipeline and review surface that later phases will use.

## 2. Background

The broader job-search app roadmap is:

- **Phase 0:** Automatically source jobs from target companies.
- **Phase 1:** Score job fit using direct LLM calls with structured candidate/resume/job JSON and a fixed rubric.
- **Phase 2:** Generate tailored resume bullets and cover letters.
- **Phase 3:** Add tracker/dashboard/export workflow.
- **Phase 4:** Assisted apply with user review.
- **Phase 5:** Guarded full automation for trusted high-confidence sources.

For the rating approach, the product starts with direct LLM scoring and is designed so RAG can be added later over resume bullets, projects, prior applications, and company notes.

## 3. Scope

### In scope

- Pull jobs from Databricks and NVIDIA career sources.
- Normalize jobs from different ATS systems into one schema.
- Store jobs in SQLite.
- Deduplicate jobs by company/source/source job ID.
- Track first-seen and last-seen timestamps.
- Track source lifecycle separately from user review state: `source_status` (`newly_discovered`, `active`, `expired`) and `review_status` (`unreviewed`, `reviewed`, `applied`).
- Treat title keyword exclusion as a transparent dashboard-only view filter; do not persist `candidate` or `filtered_out` status from backend keyword rules.
- Provide a simple dashboard to browse/filter jobs.
- Provide job detail pages with official application URLs.
- Provide a manual refresh command/API.

### Out of scope

- LLM fit scoring.
- Resume tailoring.
- Cover-letter generation.
- User profile/resume management.
- Browser automation or application submission.
- Scraping LinkedIn, Indeed, or Google Jobs.

## 4. Source Strategy

### Databricks

Databricks uses a Greenhouse-compatible public jobs API:

```text
https://api.greenhouse.io/v1/boards/databricks/jobs?content=true
```

The connector should follow the standard list/detail contract:

1. `fetch_list()` fetches the JSON payload and iterates through `jobs[]`.
2. Extract job ID, title, URL, location, departments, offices, publish/update timestamps, and HTML content.
3. `fetch_detail_if_needed()` is a no-op because `content=true` already includes the full job description in the list response.
4. Strip HTML into readable text.
5. Normalize into the internal job schema.

Reliability expectation: high.

### NVIDIA

NVIDIA uses Workday CXS APIs.

Listing endpoint:

```text
POST https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite/jobs
```

Payload:

```json
{
  "appliedFacets": {},
  "limit": 20,
  "offset": 0,
  "searchText": ""
}
```

Each listing includes an `externalPath`. Job detail can be fetched at:

```text
GET https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite{externalPath}
```

The connector should follow the standard list/detail contract:

1. `fetch_list()` pages through listing results.
2. Extract title, external path, location text, posted text, and requisition ID from listing rows.
3. `fetch_detail_if_needed()` calls the Workday detail endpoint for each listing to obtain the full job description before the job is considered review-ready.
4. Strip HTML into readable text.
5. Normalize into the internal job schema.

Reliability expectation: medium-high. Workday pagination and fields may be inconsistent. NVIDIA's Workday CXS endpoint currently rejects page sizes above 20 with HTTP 400, so the connector must cap Workday listing requests at `limit <= 20` and paginate in multiple requests for larger refreshes. The Workday `total` field is reliable on the first page but may be returned as `0` on subsequent pages; the connector must preserve the first positive total and continue pagination until it reaches that total, reaches the user-requested total limit, or receives an empty page.

## 4.1 Refresh Modes

The refresh command supports two modes:

- **Full mode (`--mode full`)**: completeness mode. Fetch listing pages and full detail pages for every returned listing. Use for first full syncs, audits, and occasional comprehensive refreshes. This is slower for NVIDIA because Workday is capped at 20 listing records per request and every job also needs a detail request.
- **Search mode (`--mode search --search "query"`)**: targeted mode. Use the provided query to narrow source results when the source supports server-side search, especially NVIDIA Workday via `searchText`. Sources without server-side search can fetch normally and locally filter results to the search query. Fetch full details for all returned search results.

Fast mode was intentionally removed because lightweight listing-only records are not useful enough for fit review without full descriptions, and search mode is a clearer way to limit the job set. Only an unrestricted full refresh may mark unseen jobs as expired; limited and search refreshes are intentionally partial and must not expire jobs that are outside the fetched subset.

A daily full refresh should be scheduled at 12:00 AM using the host scheduler/Hermes cron rather than hidden browser automation. The dashboard must make scheduled refreshes observable by showing the most recent refresh status per company from `job_fetch_runs`, including whether it succeeded or failed, start/finish timestamps displayed in local Pacific time for readability, counts found/created/updated/expired, and any error message. Manual refreshes may use the same status surface because both scheduled and manual refreshes write to `job_fetch_runs`. Persisted timestamps remain UTC/ISO strings; local-time conversion is display-only.

## 5. Data Storage

Use local SQLite first:

```text
data/jobs.sqlite
```

SQLite is sufficient for a single-user local MVP and can be migrated to Postgres later.

### Tables

#### `companies`

Stores target companies and source configuration.

Fields:

- `id`
- `name`
- `slug`
- `ats_type`
- `careers_url`
- `source_api_url`
- `enabled`
- `created_at`
- `updated_at`

#### `jobs`

Stores normalized jobs.

Fields:

- `id`
- `company_id`
- `company_name`
- `source`
- `source_job_id`
- `requisition_id`
- `title`
- `location`
- `remote_type`
- `department`
- `employment_type`
- `salary_min`
- `salary_max`
- `currency`
- `job_url`
- `apply_url`
- `description_raw_html`
- `description_text`
- `posted_at`
- `first_seen_at`
- `last_seen_at`
- `source_status` (`newly_discovered`, `active`, `expired`)
- `review_status` (`unreviewed`, `reviewed`, `applied`)
- `is_hidden`
- `is_saved`
- `saved_at`
- `reviewed_at`
- `applied_at`
- `filter_reason`
- `content_hash`
- `created_at`
- `updated_at`

#### `job_fetch_runs`

Stores pipeline run history.

Fields:

- `id`
- `company_id`
- `started_at`
- `finished_at`
- `status`
- `jobs_found`
- `jobs_created`
- `jobs_updated`
- `jobs_expired`
- `error_message`

#### `job_raw_snapshots`

Stores append-only raw source JSON snapshots for debugging connector changes and recovering fields that were not normalized into SQL columns.

Fields:

- `id`
- `job_id`
- `source`
- `raw_json`
- `fetched_at`

#### `resume_files`

Stores local resume uploads that are active inputs to profile extraction.

Fields:

- `id`
- `original_filename`
- `stored_filename`
- `stored_path`
- `content_type`
- `content_hash`
- `file_size`
- `extracted_text`
- `extraction_error`
- `is_active`
- `uploaded_at`
- `removed_at`

#### `profile_extractions`

Stores manually-created profile snapshots from active resume files for later job-rating inputs. Extraction is triggered only by the user pressing the profile extraction button.

Fields:

- `id`
- `source_resume_ids_json`
- `source_resume_hash`
- `profile_hash`
- `profile_json`
- `extractor_version`
- `is_active`
- `created_at`

#### `job_ingestion_audits`

Stores append-only normalization/schema-quality audit records for each fetched job. The latest audit per job should power dashboard warning badges and the job-detail audit section.

Fields:

- `id`
- `job_id`
- `source`
- `required_missing_json`
- `optional_missing_json`
- `extra_keys_json`
- `warnings_json`
- `description_length`
- `location_status`
- `detail_fetch_status`
- `created_at`

Audit behavior:

- Required fields should include source job ID, title, job URL, apply URL, location, description text, and posted timestamp.
- Optional fields should include department, employment type, salary fields, currency, remote type, and requisition ID.
- Extra source keys should be captured from the raw JSON top level so schema drift can be reviewed later.
- Missing required fields, blank descriptions, unparsed locations, and detail-fetch failures should create warnings.
- The main dashboard should display compact warning badges only when the latest audit has warnings.
- The job detail page should display a full ingestion audit section with required missing fields, optional missing fields, warnings, description length, location status, detail fetch status, and raw snapshot availability.

## 6. Normalized Job Schema

All connectors should produce this shape before database upsert:

```json
{
  "company_name": "NVIDIA",
  "source": "workday",
  "source_job_id": "JR2018101",
  "requisition_id": "JR2018101",
  "title": "Senior Production Engineer - DGX Cloud",
  "location": "US-CA-Remote / Multiple",
  "remote_type": "remote",
  "department": "Engineering",
  "job_url": "https://...",
  "apply_url": "https://...",
  "description_raw_html": "<p>...</p>",
  "description_text": "...",
  "posted_at": null,
  "status": "new"
}
```

## 7. Dashboard View Filtering

Phase 0 should not assign `candidate` or `filtered_out` status from hidden hardcoded keyword lists. The future rating/scoring system will determine fit recommendations transparently.

### Include keywords

Removed from status assignment. Hardcoded include keywords should not create `candidate` jobs. If role keywords are reintroduced before the rating system, they should be visible user-controlled hints rather than backend-only status rules.

### Exclude titles containing

The dashboard label should be `Exclude titles containing` so users understand the filter only checks saved job titles, unlike saved-job search which checks title, company, location, and description. The user may enter comma-separated title keywords such as `manager, director, sales`; matching saved jobs are omitted from the current dashboard result set only. This filter must not mutate `jobs.status` and must not use a hidden hardcoded backend list.

### Location keywords

Removed from status assignment. Location relevance should be handled by the existing source-location filter, dashboard location filter, and later transparent rating criteria.

### Status assignment

- `new`: default/reviewable status for fetched active jobs; this preserves fetched jobs for later rating without calling them candidates.
- `hidden`: user-hidden job.
- `expired`: previously seen job no longer present in latest full source run.

## 8. User Experience

### Main dashboard

The dashboard should show:

- Company
- Job title
- Location
- Source
- Date found
- Last seen
- Status
- Filter reason
- Link to official job URL
- Link to detail view
- Accurate result counts, e.g. `Showing 1-100 of 778 matching jobs`
- Pagination controls so the app does not silently cap the dashboard at a fixed 500-job result set

The dashboard should not hide jobs behind a hard-coded display cap. It may use page size controls for browser performance, but the user must be able to navigate through the full matching result set.

The dashboard search/filter panel should include user-controlled sorting. Supported sort options should include newest first, oldest first, last seen recently, highest LLM fit score, company A-Z, and title A-Z. Sorting must preserve existing filters and pagination should reset to page 1 when changed from the filter form.

The main dashboard control areas should be ordered as collapsible panels: refresh jobs first, resume/profile upload and extraction second, search/filter saved jobs third, and LLM bulk rating last. Collapsed/open panel state should persist locally across page refreshes and filter submissions so panels the user collapsed do not automatically reopen. The unhide-all action belongs inside the search/filter panel next to the Filter button; it should be visually secondary/gray because it is a bulk visibility action, not the primary filter action.

Inline tooltip explanations should only appear when the user hovers or focuses the tooltip marker, rather than rendering long explanatory text directly in the dashboard layout.

### Visual theme

The dashboard should use a mainly white/light surface with black text, blue primary controls/links, and subtle blue highlights for active/new content. Cards should remain clearly separated with light gray borders and soft shadows rather than the previous dark theme.

### Resume/profile upload and extraction

The UI should support a local resume/profile area before rating is implemented:

- Upload one or more resume files (`.pdf`, `.docx`, `.txt`, `.md`) into local app storage.
- List active uploaded resume files with upload time and extraction status.
- Remove a resume file from the active profile input set without deleting unrelated job data.
- Provide a manual `Extract/update profile` button. Uploading or removing a resume must **not** automatically run profile extraction, because future extraction may spend LLM tokens.
- When extraction is triggered, read only currently active resume files, extract their text, and build/store a structured profile snapshot for later rating.
- If a resume is removed, any profile snapshot that used that file should be marked inactive/stale so removed-resume information is not used by the rating system.
- Show the latest profile summary and whether it is current for the active resume set or stale after upload/remove changes.
- The initial extractor may be local/deterministic, but the data model should support later LLM-based extraction with `extractor_version`, `profile_hash`, and source resume IDs.

### Profile extraction and fit-rating rubric

Profile extraction and job rating should follow the user-provided reference model: ratings estimate expected interview probability plus role fit, not title similarity alone.

The profile extractor should preserve structured evidence that can support ratings and tailoring:

- Target directions: tech supply chain, TPM, analytics, infrastructure, China/US/global exposure, and management track.
- Strong proof points from the resume/reference: Google Control Tower, Tesla warehouse optimization, Industrial Engineering + MS Data Science, Enron ML, and Malema internship when present.
- Domains/background: manufacturing, PCBA, supply chain, logistics, capacity, warehouse optimization, data analytics, forecasting, operations, quality/process/SOP work.
- Tools/technical signals: SQL, Python, GCP, dashboards, optimization, ML, forecasting, BI tools, WMS/OMS, OR/analytics tooling.
- Practical fit signals and blockers when available: internships/current-enrollment requirements, work authorization/location constraints, language requirements, years/seniority mismatches, and hard technical-lead requirements outside the resume evidence.

Fit scoring should return an overall 0-10 score plus category-level scores/reasons using these weights:

| Category | Weight | Meaning |
| --- | ---: | --- |
| Core job-function match | 30% | Whether the job asks for work the user has done: analytics, planning, TPM, sourcing, logistics, ML, operations leadership, etc. |
| Experience/seniority match | 20% | Whether years/seniority fit the user's 3+ years Accenture/Google + MSDS profile. |
| Domain match | 15% | Whether the job connects to manufacturing, PCBA, supply chain, logistics, capacity, warehouse optimization, or data analytics. |
| Technical/tool match | 15% | Match on SQL, Python, GCP, dashboards, optimization, ML, forecasting, BI tools, WMS/OMS, etc. |
| Evidence strength from resume | 10% | Whether strong bullets/projects can prove the match, e.g. Google Control Tower, Tesla warehouse optimization, Enron ML, Malema internship. |
| Gap severity | 5% | Whether gaps are small/trainable or hard blockers like Java backend tech lead, Japanese fluency, UK work authorization, or 10 years direct leadership. |
| Strategic career value | 5% | Whether the role moves toward tech supply chain, TPM, analytics, infrastructure, China/US/global exposure, or management track. |

Score interpretation:

- 9.0-9.5: Excellent fit; apply ASAP; resume can be tailored strongly with existing experience.
- 8.5-8.9: Strong fit; worth applying; some gaps but credible story.
- 8.0-8.4: Good stretch; apply if interested and prepare around gaps.
- 7.0-7.9: Medium stretch; related but one or two major gaps exist.
- 6.0-6.9: Reach; apply only if the user really wants that path.
- Below 6.0: Skip or very low priority; usually wrong function/seniority.

The rating output should distinguish skill fit from practical fit when practical blockers exist, such as internship eligibility or location/work authorization constraints.

LLM extraction/rating implementation requirements:

- Add an OpenAI-compatible chat-completions client configured by environment variables: `JOBSEARCH_LLM_API_KEY` or `OPENAI_API_KEY`, optional `JOBSEARCH_LLM_BASE_URL`, and optional `JOBSEARCH_LLM_MODEL`. The local app should also auto-load a gitignored project-root `.env` file for these settings so the user can configure LLM features without exporting shell variables each launch.
- Keep the local extractor as a no-token fallback/debug option in code/tests, but do not expose it as a dashboard action. The dashboard should expose a clearly-labeled `Extract/update profile with LLM` button for the comprehensive profile.
- LLM profile extraction input should include active resume extracted text plus the user's reference rubric. Output must be strict JSON following the profile schema, including candidate summary, target roles, target industries, seniority, core strengths, technical skills, domain skills, proof points, weaknesses/gaps, practical constraints, and resume bullet inventory.
- Add `profile_extractions.extraction_method`, `model_name`, and `prompt_version` columns when absent. LLM profile rows should use an extractor version such as `llm-profile-v1` and method `llm`.
- Add a `job_ratings` table for cached ratings keyed by `job_id`, `profile_extraction_id`, `profile_hash`, `job_content_hash`, `rubric_version`, `rater_version`, and `model_name`.
- The dashboard should expose `Rate this job with LLM` on job detail pages. Ratings should be manual; no automatic rating on refresh or page load.
- The dashboard should expose bulk LLM rating controls as explicit manual actions: `Rate all matching jobs with LLM` for the full current dashboard filter result set, and `Rate jobs on this page with LLM` for only the currently visible paginated rows. Both actions should run in the background, reuse cached current ratings when profile/job/model/rubric keys match, report queued/running/success/failure status on the dashboard, and avoid changing job source/review/saved/hidden states.
- Rating should use the latest active/current LLM profile when available and fail clearly if no active/current profile exists or no LLM API key is configured.
- Rating output must be strict JSON with overall score, skill fit, practical fit, recommendation, category breakdown, strongest evidence, main gaps, practical notes, resume tailoring notes, interview probability reasoning, and apply decision.
- Job cards should show a compact rating badge when a current rating exists. Job detail pages should show the detailed rating breakdown.
- Cache controls: if a current rating exists for the same profile/job/rubric/model hash, reuse it unless the user explicitly re-rates.
- Token control: all LLM actions are explicit button clicks; single-job rating is synchronous for now, while profile extraction starts a background job and immediately redirects so the browser does not sit in loading mode during long LLM calls. Batch rating is out of scope until single-job flow is verified.

### Filters/search

The UI should support:

- Company filter
- Separate source status filter for `newly_discovered`, `active`, and `expired`.
- Separate review status filter for `unreviewed`, `reviewed`, and `applied`.
- Saved filter with `All`, `Saved only`, and `Unsaved only` modes.
- Free-text search across title, location, company, and description.
- Dashboard-only `Exclude titles containing` filter: comma-separated terms hide matching saved-job titles from the current view without changing job status.
- Hidden-job visibility dropdown with `Exclude hidden`, `Include hidden`, and `Only hidden` modes. Default dashboard views should exclude hidden jobs.
- Search/filter panel sort dropdown supports recommended/default, newest first, oldest first, last seen recently, highest LLM fit score, company A-Z, and title A-Z while preserving the active filters.
- Per-job hide/unhide action to remove individual jobs from the active review list or restore them without deleting the job record or applied-job history. Job cards and detail pages should show `Hide` for non-hidden jobs and `Unhide` for hidden jobs.
- Per-job save/unsave action. Job cards and detail pages should show `Save` for unsaved jobs and `Unsave` for saved jobs.
- Job detail page visits should mark unapplied jobs as `review_status='reviewed'` and set `reviewed_at`; this must not downgrade jobs already marked `applied`.
- Search/filter-panel `Unhide all jobs` action to restore all currently hidden jobs; it should appear to the left of the Filter button and use secondary/gray styling.
- Source refresh controls should be split into separate Full Refresh and Search Refresh tabs so the user can choose the correct mental model without mixing full-sync fields with scoped-search fields.
- Full Refresh tab should include only a company selector and a Full refresh submit button. Full refresh ignores query, source location, and limit fields. A full refresh always expires missing jobs within the selected company scope: all companies when company is `All companies`, or only the selected company when a single company is selected. During a full refresh, jobs that were already `newly_discovered` and are seen again should transition to `active`; newly inserted jobs remain `newly_discovered`; missing non-hidden jobs become `expired`.
- Search Refresh tab should include company, optional pre-search query, optional comma-separated source location filter, and optional limit. Search refresh may be run with an empty pre-search query, including location-only, limit-only, or location+limit scoped refreshes. Search refresh never expires missing jobs.
- Search-mode source refresh should pass its query/location/limit to the backend refresh command and redirect back to the dashboard with the same query/company/location as display pre-filters so the newly fetched search set is immediately visible.
- Source refresh query field should include plain-English tooltip/help copy explaining what it searches: in search mode, the app asks the company career site for jobs matching the typed words when that source supports it; for Databricks, where the API returns a broader list first, the app narrows those jobs locally by checking job title, location, and job description text. The tooltip should avoid promising exact NVIDIA fields because Workday/NVIDIA controls server-side matching; describe it as NVIDIA career-site search that likely considers job-posting text such as title and description, but exact fields are not guaranteed. It should also note that Databricks search is usually faster in the current connector because one Greenhouse response includes job descriptions, while NVIDIA requires extra detail requests per result.
- Search refresh controls should include an optional location filter. The source location filter narrows fetched/saved source results to jobs whose location text contains any comma-separated location entry, e.g. `Remote, California, Seattle`, and search-mode redirects should carry the same location filter into the regular dashboard location filter so newly fetched results are immediately visible.
- The regular dashboard search/filter bar should also include plain-English help copy explaining that it searches already-saved jobs in the local database, including job title, company, location, and description text.
- The regular dashboard search/filter bar should include a separate optional location filter that only checks the saved job location field, supports comma-separated OR entries, and can be combined with broad keyword search. Examples: `Remote`, `California`, `Seattle`, `Remote, California, Seattle`, or `US`.

### Job detail view

The detail page should show:

- Title
- Company
- Location
- Source
- Source job ID / requisition ID
- Job URL / apply URL, exposed through a local `Open official application URL` route that marks `review_status='applied'` before redirecting to the real job URL.
- Status
- Description text
- Pipeline metadata

## 9. Functional Requirements

- FR1: The system can initialize a SQLite database with required tables.
- FR2: The system can seed Databricks and NVIDIA company configs.
- FR3: The system can fetch Databricks jobs from Greenhouse API.
- FR4: The system can fetch NVIDIA jobs from Workday CXS API.
- FR5: The system can normalize both sources into one schema.
- FR6: The system can upsert jobs idempotently.
- FR7: The system can record fetch run stats.
- FR8: The system can mark missing jobs as expired.
- FR9: The system can display jobs in a local browser dashboard.
- FR10: The system can show a job detail page and official URL.
- FR11: The system can upload/list/remove local resume files without automatically running profile extraction.
- FR12: The system can manually extract/update a structured background profile from active resume files and mark profile snapshots stale/inactive when source resumes are removed.
- FR13: The system can rate jobs with the 7-category weighted rubric and return overall score, recommendation, category scores, evidence, gaps, strategic value, and practical-fit notes.
- FR14: The system can manually run LLM profile extraction from active resumes/reference rubric using an OpenAI-compatible provider, store provenance, and show the comprehensive profile in the dashboard.
- FR15: The system can manually rate an individual job with the latest active LLM profile, cache the result, and display compact/detail rating views in the dashboard.

## 10. Non-Functional Requirements

- Local-first; no external DB required.
- Avoid brittle browser scraping when public JSON APIs are available.
- Use polite request headers and pagination.
- Avoid application submission or side effects.
- Store raw snapshots to debug source changes.
- Keep connectors modular so more ATS sources can be added later.

## 11. Acceptance Criteria

Phase 0 is complete when:

1. Running a refresh command fetches at least one Databricks job and one NVIDIA job.
2. Jobs are stored in SQLite under `data/jobs.sqlite`.
3. Re-running refresh does not create duplicates.
4. Dashboard lists stored jobs.
5. User can filter by company/status/search text.
6. User can open a job detail page.
7. Each job includes an official application URL.
8. Fetch runs are logged in `job_fetch_runs`.

## 12. Future Considerations

- Add a scheduled refresh cron job.
- Add company-source management in the UI.
- Add Workday facet-based filtering.
- Add scoring queue for Phase 1.
- Add RAG later for evidence-backed scoring and tailoring.
