# restore_youtube_raw.ps1
# Purpose: Load local NDJSON (.jsonl) files into BigQuery staging → commit into youtube_raw.
# Inputs:
#   Env:   $env:GCP_PROJECT_ID, $env:BQ_LOCATION (default asia-northeast3); optional $env:LOCAL_DATA_DIR
#   Files (preferred in current folder or $env:LOCAL_DATA_DIR):
#          sample_raw_channels.jsonl
#          sample_raw_videos.jsonl
#          sample_raw_subtitles_segments.jsonl
#          sample_raw_subtitles_full.jsonl
#   SQL in current folder:
#          01_prepare_youtube_raw.sql
#          02_commit_youtube_raw.sql
# Outputs:
#   <PROJECT_ID>.youtube_raw.{channels,videos,subtitles_segments,subtitles_full} populated; row counts printed.
# Deps:
#   - bq CLI installed & authenticated
#   - BigQuery permissions to CREATE/LOAD/TRUNCATE/INSERT/DROP
#   - Billing enabled (DML requires billing)
# Run:
#   cd .\sample_data
#   $env:GCP_PROJECT_ID="your-project-id"
#   $env:BQ_LOCATION="asia-northeast3"
#   .\restore_youtube_raw.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --- Sanity checks ---
if (-not $env:GCP_PROJECT_ID) { throw "GCP_PROJECT_ID not set (ex: `$env:GCP_PROJECT_ID='your-project-id')" }
if (-not $env:BQ_LOCATION)    { $env:BQ_LOCATION = "asia-northeast3" }
if (-not (Get-Command bq -ErrorAction SilentlyContinue)) {
  throw "'bq' CLI not found. Install Google Cloud SDK and run 'gcloud auth application-default login'."
}

# --- Helper: run a SQL file with <PROJECT_ID> substitution ---
function Invoke-BqSqlFile {
  param([Parameter(Mandatory=$true)][string]$Path)
  if (-not (Test-Path $Path)) { throw "File not found: $Path" }

  $sql = Get-Content -Raw -Path $Path
  $sql = $sql -replace "<PROJECT_ID>", $env:GCP_PROJECT_ID

  # strip comments
  $sql = [regex]::Replace($sql, '(?s)/\*.*?\*/', '')
  $sql = [regex]::Replace($sql, '^\s*--.*$', '', 'Multiline')
  $sql = $sql.Trim()
  if (-not $sql) { throw "Empty SQL after comment stripping: $Path" }

  $args = @(
    'query',
    "--project_id=$($env:GCP_PROJECT_ID)",
    "--location=$($env:BQ_LOCATION)",
    '--nouse_legacy_sql',
    '--format=none',
    '--quiet'
  )
  $sql | & bq @args
  if ($LASTEXITCODE -ne 0) { throw "bq query failed for $Path (exit $LASTEXITCODE)" }
}

# --- Paths ---
$HERE = $PSScriptRoot
if (-not $HERE) { $HERE = (Resolve-Path .).Path }

# Data directory: prefer LOCAL_DATA_DIR if valid; otherwise current folder
if ($env:LOCAL_DATA_DIR -and (Test-Path $env:LOCAL_DATA_DIR)) {
  $DATA_DIR = (Resolve-Path $env:LOCAL_DATA_DIR).Path
} else {
  $DATA_DIR = $HERE
}

# SQL files expected in current folder
$SQL_PREP   = Join-Path $HERE '01_prepare_youtube_raw.sql'
$SQL_COMMIT = Join-Path $HERE '02_commit_youtube_raw.sql'
if (-not (Test-Path $SQL_PREP))   { throw "Missing SQL: $SQL_PREP" }
if (-not (Test-Path $SQL_COMMIT)) { throw "Missing SQL: $SQL_COMMIT" }

# Staging targets → candidate file names (prefer prepared *.jsonl)
$files = @{
  "youtube_stage.channels_stage"           = @("sample_raw_channels.jsonl","channels.jsonl","sample_raw_channels.json","channels.json")
  "youtube_stage.videos_stage"             = @("sample_raw_videos.jsonl","videos.jsonl","sample_raw_videos.json","videos.json")
  "youtube_stage.subtitles_segments_stage" = @("sample_raw_subtitles_segments.jsonl","subtitles_segments.jsonl","sample_raw_subtitles_segments.json","subtitles_segments.json")
  "youtube_stage.subtitles_full_stage"     = @("sample_raw_subtitles_full.jsonl","subtitles_full.jsonl","sample_raw_subtitles_full.json","subtitles_full.json")
}

Write-Host "Project : $($env:GCP_PROJECT_ID)"
Write-Host "Location: $($env:BQ_LOCATION)"
Write-Host "DataDir : $DATA_DIR"
Write-Host "PrepSQL: $SQL_PREP"
Write-Host "Commit : $SQL_COMMIT"
Write-Host ""

# 1) Prepare datasets/tables
Write-Host "[1/4] Preparing datasets & staging tables..." -ForegroundColor Cyan
Invoke-BqSqlFile $SQL_PREP

# 2) Load NDJSON → staging tables
Write-Host "[2/4] Loading NDJSON files into staging tables..." -ForegroundColor Cyan
foreach ($kv in $files.GetEnumerator()) {
  $table = $kv.Key
  $cands = $kv.Value
  $src = $null

  foreach ($name in $cands) {
    $p = Join-Path $DATA_DIR $name
    if (Test-Path $p) { $src = (Resolve-Path $p).Path; break }
  }
  if (-not $src) { throw "File not found for $table in $DATA_DIR (tried: $($cands -join ', '))" }

  $len = (Get-Item $src).Length
  if ($len -le 0) { throw "Source file is empty: $src" }

  Write-Host ("  -> Loading {0}  →  {1}:{2}" -f $src, $env:GCP_PROJECT_ID, $table)
  & bq --location=$env:BQ_LOCATION load `
      --source_format=NEWLINE_DELIMITED_JSON `
      --ignore_unknown_values `
      --replace `
      "$($env:GCP_PROJECT_ID):$table" "$src" | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "[Load] $table failed with exit code $LASTEXITCODE" }
}

# 3) Commit (populate finals and clean up staging)
Write-Host "[3/4] Committing to final tables & cleaning staging..." -ForegroundColor Cyan
Invoke-BqSqlFile $SQL_COMMIT

Write-Host "`n[OK] Restore complete." -ForegroundColor Green
