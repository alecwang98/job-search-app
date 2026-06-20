# Job Search App

Local-first app for finding and reviewing suitable jobs.

## Current status

Phase 0 MVP is implemented:

- Databricks connector via Greenhouse API
- NVIDIA connector via Workday CXS API
- SQLite storage at `data/jobs.sqlite`
- Basic target-role/location filtering
- Fetch run logging
- Local dashboard for browsing jobs

See the BRD: `docs/brd/phase-0-job-sourcing.md`.

## Commands

Initialize database:

```bash
python3 -m jobsearch.app init-db
```

Fetch a small sample for verification:

```bash
python3 -m jobsearch.app refresh --limit 10
```

Refresh modes:

```bash
# Full: fetch listings and detail for every returned job
python3 -m jobsearch.app refresh --mode full --limit 100

# Search: targeted query; NVIDIA uses Workday searchText, Databricks is filtered locally
python3 -m jobsearch.app refresh --mode search --search "software engineer" --limit 50
```

Default mode is `full`. Use `--limit` for testing/smaller runs; omit it only when you want a full unrestricted refresh.

Fetch one company:

```bash
python3 -m jobsearch.app refresh --company databricks --limit 25
python3 -m jobsearch.app refresh --company nvidia --limit 25
```

Run a full refresh:

```bash
python3 -m jobsearch.app refresh
```

Start dashboard:

```bash
python3 -m jobsearch.app serve --port 8787
```

LLM profile extraction and job rating require an OpenAI-compatible API key. The app reads the normal process environment and also auto-loads a local gitignored `.env` file from the project root:

```bash
cp .env.example .env
# edit .env and set JOBSEARCH_LLM_API_KEY=your_api_key_here
```

Optional OpenAI-compatible settings:

```bash
JOBSEARCH_LLM_BASE_URL=https://api.openai.com/v1
JOBSEARCH_LLM_MODEL=gpt-4o-mini
```

Then restart the dashboard so the new environment is loaded.

Then open:

```text
http://127.0.0.1:8787
```

The dashboard includes:

- Refresh panel with separate Full refresh and Search refresh controls plus a daily full-refresh status display. The status display reads the latest saved `job_fetch_runs` rows, so both manual refreshes and scheduled refreshes show success/failure, timestamps, counts, and errors.
- Resume/Profile area for uploading `.pdf`, `.docx`, `.txt`, or `.md` resume files. Uploading/removing files does not run profile extraction automatically; use the explicit `Extract/update profile` button when you want to update the structured profile used by future ratings. Removing a resume marks any profile snapshot that used it inactive/stale so removed information is not used later.
- Stored-job filters for company, source status, review status, saved state, hidden visibility, page size, free-text search, location, and dashboard-only `Exclude titles containing`. The regular dashboard search only searches jobs that are already saved in this app, and checks title, company, location, and description. The separate dashboard location filter only checks the saved job location field and supports comma-separated OR entries such as `Remote, California, Seattle`.
- Source status is app/source-driven: `newly_discovered`, `active`, or `expired`. Newly inserted jobs start as `newly_discovered`; a later full refresh changes already-seen matching jobs to `active`; missing jobs become `expired`.
- Review status is user-driven: `unreviewed`, `reviewed`, or `applied`. Visiting the local job detail page marks an unapplied job `reviewed`; opening the official application URL through the dashboard marks the job `applied` before redirecting to the real job URL.
- Saved and hidden are independent flags. Job cards and detail pages include status-aware `Save`/`Unsave` and `Hide`/`Unhide` buttons. The dashboard also includes a top-level `Unhide all jobs` button.
- The dashboard uses a light white/black/blue theme with blue primary actions and subtle blue highlights for newly discovered job cards.
- Hidden jobs are excluded by default; use the hidden visibility dropdown to choose `Exclude hidden`, `Include hidden`, or `Only hidden`.
- Hardcoded include/location/exclude keyword rules no longer assign `candidate` or `filtered_out` status. Comma-separated `Exclude titles containing` terms hide matching titles from the current dashboard view only and do not change job status.
- Separate source refresh tabs for `Full refresh` and `Search refresh`.
- Full refresh uses only the company selector and always expires missing jobs in the selected company scope. Selecting `All companies` expires missing jobs for each refreshed company; selecting one company expires missing jobs only for that company.
- Search refresh uses optional pre-search text, optional comma-separated source location, optional limit, and company. Empty search text is allowed, so use search refresh for location-only, limit-only, or location+limit scoped refreshes. Search refresh never expires missing jobs.
- The source refresh pre-search field explains in plain English that search mode finds new jobs from company career sites before saving them here. Databricks is usually faster because one Greenhouse response already includes job descriptions. NVIDIA can be slower because the app makes extra detail requests for each matching job. NVIDIA's career site controls which fields match the typed words; exact fields are not guaranteed, but it usually searches job-posting text such as title and description.
- The source location filter checks career-site location text before saving results and supports comma-separated OR entries, e.g. `Remote, California, Seattle` matches any job whose location contains one or more of those terms.
- Search-mode refresh pre-fills the dashboard text/company/location filters after completion so the newly fetched search results are immediately visible.
- Raw source JSON is stored append-only in `job_raw_snapshots`, while normalized review fields live in `jobs`.
- Ingestion audits are stored append-only in `job_ingestion_audits`. The latest audit per job powers warning badges on dashboard cards and an `Ingestion audit` section on job detail pages for missing required fields, optional missing fields, extra raw keys, description length, location status, detail fetch status, and raw snapshot availability.

## Notes

Connectors use a standard list/detail contract:

- Greenhouse/Databricks: `fetch_list()` already receives full descriptions from `content=true`, so `fetch_detail_if_needed()` is a no-op.
- Workday/NVIDIA: `fetch_list()` receives lightweight listing rows, then `fetch_detail_if_needed()` calls the Workday detail endpoint for each listing so saved jobs are review-ready with full descriptions.

The NVIDIA full refresh can be slower because Workday returns listings first and the app fetches detail records for descriptions. Daily background refresh is a future option, but is intentionally not scheduled yet.
