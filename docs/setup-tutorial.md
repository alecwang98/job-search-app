# Fresh Setup Tutorial

This tutorial is for someone cloning the repo on a new machine. It starts with an empty local database because runtime data is intentionally not committed to Git.

## 1. Clone the repository

```bash
git clone https://github.com/alecwang98/job-search-app.git
cd job-search-app
```

If you are working from a feature branch, check it out first:

```bash
git checkout improve-rating-quality-speed
```

## 2. Create a Python environment

Use Python 3.11+ when possible.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

For tests/development, install the dev requirements too:

```bash
python -m pip install -r requirements-dev.txt
```

Notes:

- Core ingestion and dashboard code mostly uses Python standard library modules.
- `PyMuPDF` from `requirements.txt` is needed for PDF resume text extraction.
- `.docx`, `.txt`, and `.md` resume extraction do not need extra packages.

## 3. Initialize local data

The database is local runtime data and is not included in Git.

```bash
python -m jobsearch.app init-db
```

This creates:

```text
data/jobs.sqlite
```

The whole `data/` directory is gitignored. It can contain SQLite files, uploaded resumes, raw source snapshots, and logs.

## 4. Fetch jobs

Fetch a small sample first:

```bash
python -m jobsearch.app refresh --limit 10
```

Then run either a scoped refresh or a full refresh:

```bash
# Scoped full refresh with complete job details
python -m jobsearch.app refresh --mode full --limit 100

# Search refresh; NVIDIA uses Workday search, Databricks filters locally
python -m jobsearch.app refresh --mode search --search "software engineer" --limit 50

# Full unrestricted refresh; slower, especially for NVIDIA
python -m jobsearch.app refresh
```

Refresh behavior:

- Full refresh fetches listing and detail data and may expire missing jobs in the selected company scope.
- Search refresh is partial and never expires missing jobs.
- NVIDIA can be slower because Workday returns lightweight listings first and the app fetches detail pages separately.

## 5. Start the dashboard

```bash
python -m jobsearch.app serve --port 8787
```

Open:

```text
http://127.0.0.1:8787
```

The dashboard reads from SQLite on each request, so new refresh results appear after reloading the browser.

## 6. Optional LLM setup

LLM profile extraction and job rating are optional. They require an OpenAI-compatible API key.

```bash
cp .env.example .env
```

Edit `.env`:

```bash
JOBSEARCH_LLM_API_KEY=your_api_key_here
JOBSEARCH_LLM_MODEL=gpt-4o-mini
# Optional for non-OpenAI-compatible providers:
# JOBSEARCH_LLM_BASE_URL=https://api.openai.com/v1
```

Then restart the dashboard so the environment is reloaded.

Optional OpenRouter key for explicit DeepSeek comparison commands:

```bash
OPENROUTER_API_KEY=your_openrouter_key_here
```

The app loads project-root `.env` first, then falls back to the default Hermes `.env` for missing shared keys. Real secrets must stay in `.env`; `.env` is gitignored.

## 7. Optional scheduled refresh

There is no built-in scheduler that starts automatically for new clones.

On the original local machine, a separate Hermes cron job ran a daily full refresh with a script outside this repo. A new user must create their own OS/Hermes cron job if they want scheduled refreshes.

A suitable command is:

```bash
cd /path/to/job-search-app
PYTHONPATH=. python -m jobsearch.app refresh --mode full
```

Recommended behavior for a scheduler:

- Run at most daily unless actively using the app.
- Log output under local `data/`.
- Stay quiet on success and notify only on failure.
- Use dashboard `job_fetch_runs` rows to inspect latest refresh status.

## 8. Run tests

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
```

Expected result at the pause checkpoint:

```text
64 passed
```

## 9. What is not included in Git

The repository does not include local runtime/private data:

- `data/jobs.sqlite`
- SQLite `-wal`/`-shm` files
- uploaded resumes
- raw source snapshots
- refresh logs
- `.env` secrets

A fresh clone starts empty and creates its own local database with `init-db` and `refresh`.
