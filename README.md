# Seoul Hotel Reviews (YouTube RAG)
Turn YouTube hotel reviews into a grounded, cited Q&A experience. The pipeline ingests channels/videos/captions, extracts structured review points with BigQuery AI, aligns quotes to timestamps, and answers questions with citations.

---

## 0) What you’ll build
- **BigQuery‑native** AI pipeline (asia‑northeast3): `AI.GENERATE_BOOL`, `AI.GENERATE`, `ML.GENERATE_EMBEDDING`, `VECTOR_SEARCH`, IVF index
- **KO/EN** multilingual embeddings (`text-multilingual-embedding-002`)
- **FastAPI** backend (`/ask`) + minimal static UI

Repo map
```
app/          FastAPI backend + static UI
ingest/       Ingest utilities (YouTube API, yt‑dlp, BQ upserts)
sql/          BigQuery AI & vector pipeline (00 → 11)
out/          JSONL staging files
sample_data/  One‑click restore script and snapshots
```

---

## 1) Prerequisites
- Google Cloud project with **BigQuery** & **Vertex AI** enabled.
- **BigQuery Connection** (region `asia-northeast3`) named `llm_conn` (type: *Cloud AI*). Grant callers `roles/bigquery.connectionUser`.
- **bq CLI** installed & authenticated.
- YouTube Data API v3 **API key** for metadata (only needed for Method B); **yt‑dlp** for captions (Method B).
- Python 3.10+.

Install Python deps (example):
```
pip install fastapi uvicorn python-dotenv requests yt-dlp google-cloud-bigquery
```

Environment variables (`.env`):
```
GCP_PROJECT_ID=your-project
BQ_LOCATION=asia-northeast3
BQ_DATASET_RAW=youtube_raw
BQ_DATASET_PROC=youtube_proc
YT_API_KEY=AIza...                 # Needed only for Method B
LLM_CONN=your-project.asia-northeast3.llm_conn
```

> Keep **all** datasets and the connection in `asia-northeast3`.

---

## 2) PowerShell helper for SQL
Use this function to run all SQL files (it replaces `<PROJECT_ID>` and strips comments):

```powershell
function Invoke-BqSqlFile {
  param([Parameter(Mandatory=$true)][string]$Path)

  if (-not (Test-Path $Path)) { throw "File not found: $Path" }
  if (-not $env:GCP_PROJECT_ID) { throw "GCP_PROJECT_ID not set" }
  if (-not $env:BQ_LOCATION)    { $env:BQ_LOCATION = "asia-northeast3" }

  $sql = Get-Content -Raw -Path $Path
  $sql = $sql -replace "<PROJECT_ID>", $env:GCP_PROJECT_ID

  # strip comments
  $sql = [regex]::Replace($sql, '(?s)/\*.*?\*/', '')
  $sql = [regex]::Replace($sql, '^\s*--.*$', '', 'Multiline')
  $sql = $sql.Trim()

  $args = @('query', "--location=$($env:BQ_LOCATION)", '--nouse_legacy_sql', '--format=none', '--quiet')
  $sql | & bq @args
}
```

> On macOS/Linux (no PowerShell), you can run: `bq query --location=asia-northeast3 --nouse_legacy_sql < file.sql` after manually replacing `<PROJECT_ID>` in the file.

---

## 3) Sign in & Data Ingest
First sign in:
```
gcloud auth application-default login
gcloud auth login
```

### (Recommended) Method A — Rebuild service **without** fetching from YouTube (use sample data)
This restores `youtube_raw.*` tables from `sample_data/` snapshots.

```powershell
# 1) From repo root, go to the sample_data folder (or use absolute path)
cd .\sample_data

# 2) Set your project/region
$env:GCP_PROJECT_ID = "your-project-id"
$env:BQ_LOCATION    = "asia-northeast3"

# 3) One‑click restore
.\restore_youtube_raw.ps1
```
After restore, skip fetching and proceed to **Section 5** (Process transcripts → reviews → embeddings).

### Method B — Fetch via APIs (channels, videos, captions)
> **Not recommended** for quick demos: the YouTube Data API consumes quota, and yt‑dlp caption fetches can hit HTTP 429.

```powershell
# Prepare schemas
Invoke-BqSqlFile .\sql\00_schema.sql

# Channels
python -m ingest.p01_fetch_channels
python .\ingest\load_to_bq.py --target channels

# Videos
python -m ingest.p02_fetch_videos
python -m ingest.load_to_bq --target videos

# Captions
Invoke-BqSqlFile .\sql\01_label_from_metadata.sql
Invoke-BqSqlFile .\sql\02_select_subtitle_targets.sql
python -m ingest.p03_export_subtitle_targets
python -m ingest.p04_fetch_subtitles
python -m ingest.load_to_bq --target subtitles
```

---

## 4) Process transcripts → reviews → embeddings
Run in order:
```
Invoke-BqSqlFile .\sql\03_review_points_from_subtitles.sql
Invoke-BqSqlFile .\sql\04_subtitles_to_chunks.sql
Invoke-BqSqlFile .\sql\05_create_remote_embedding_model.sql
Invoke-BqSqlFile .\sql\06_embed_chunks.sql
Invoke-BqSqlFile .\sql\07_create_chunk_vector_index.sql
Invoke-BqSqlFile .\sql\08_evidence_align_vindex.sql
Invoke-BqSqlFile .\sql\09_embed_review_points.sql
Invoke-BqSqlFile .\sql\10_rag_candidates_semantic.sql
Invoke-BqSqlFile .\sql\11_answer_from_question.sql
```
What these do (short):
- **03** LLM extracts strict‑JSON review points from full transcripts and parses them.
- **04** Build 30s/15s‑stride chunks from subtitle segments.
- **05** Create remote multilingual embedding model.
- **06** Embed transcript chunks (skip already embedded).
- **07** Create IVF COSINE vector index on chunk embeddings.
- **08** Align each evidence quote to a timestamp: vector search → literal → forced fill, with diag/logs.
- **09** Embed finalized review points (with video/channel meta) for semantic retrieval.
- **10** TVF for semantic retrieval over review embeddings with filters; LLM gate for same‑hotel relevance.
- **11** Stored procedure that returns a concise summary + citation table as markdown.

---

## 5) Run the app
Start the backend:
```
uvicorn app.main:app --reload --port 8080
```
Open http://localhost:8080 and ask questions.

Sample API call:
```bash
curl -X POST http://localhost:8080/ask \
  -H 'Content-Type: application/json' \
  -d '{
    "question":"Which hotels in Seoul have great views?",
    "lang_filter":"en",
    "exclude_paid_ads":true,
    "min_views":0,
    "min_subs":0,
    "top_k":5
  }'
```

**Response shape**
```json
{
  "summary": "...",
  "citations": [
    {"review":"Hotel X — views: ...","link":"https://youtu.be/ID?t=123s","video_title":"...","channel_title":"...","evidence_sec":123}
  ]
}
```
