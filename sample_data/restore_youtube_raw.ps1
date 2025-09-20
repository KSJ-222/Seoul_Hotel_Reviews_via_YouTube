# restore_youtube_raw.ps1
# Purpose: One-shot local restore of YouTube RAW data into BigQuery via staging → commit SQL.
# Inputs:  Env: $env:GCP_PROJECT_ID, $env:BQ_LOCATION (default asia-northeast3); optional $env:LOCAL_DATA_DIR
#          Files under data dir: channels.jsonl|.json, videos.jsonl|.json, subtitles_segments.jsonl|.json, subtitles_full.jsonl|.json
#          SQL: .\sql\01_prepare_youtube_raw.sql, .\sql\02_commit_youtube_raw.sql
# Outputs: <PROJECT_ID>.youtube_raw.{channels,videos,subtitles_segments,subtitles_full} populated; row counts printed.
# Deps:    bq CLI installed & authenticated; BigQuery permissions to CREATE/LOAD/TRUNCATE/INSERT/DROP; PowerShell 5+.
# Run:     $env:GCP_PROJECT_ID="your-project-id"; $env:BQ_LOCATION="asia-northeast3"; .\restore_youtube_raw.ps1
# Notes:   Idempotent-ish: final tables are truncated then filled; staging tables are dropped. Data dir auto-detected unless LOCAL_DATA_DIR is set.

$ErrorActionPreference = "Stop"

# --- Invoke-BqSqlFile (keep user-defined contract) ---
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

# --- Locate data directory (override with $env:LOCAL_DATA_DIR if desired) ---
$DATA_DIR = $env:LOCAL_DATA_DIR
if (-not $DATA_DIR -or -not (Test-Path $DATA_DIR)) {
  # Priority: user absolute path → repo\sample_rawdata → ./sample_rawdata
  $candidates = @(
    "C:\Users\user\Desktop\yt_hotel_reivews\sample_rawdata",
    (Join-Path (Get-Location) "repo\sample_rawdata"),
    (Join-Path (Get-Location) "sample_rawdata")
  )
  $DATA_DIR = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
  if (-not $DATA_DIR) { throw "Local data folder not found. Set `$env:LOCAL_DATA_DIR or create .\repo\sample_rawdata" }
}

$SQL_PREP   = ".\sql\01_prepare_youtube_raw.sql"
$SQL_COMMIT = ".\sql\02_commit_youtube_raw.sql"

$files = @{
  "youtube_stage.channels_stage"           = @("channels.jsonl","channels.json")
  "youtube_stage.videos_stage"             = @("videos.jsonl","videos.json")
  "youtube_stage.subtitles_segments_stage" = @("subtitles_segments.jsonl","subtitles_segments.json")
  "youtube_stage.subtitles_full_stage"     = @("subtitles_full.jsonl","subtitles_full.json")
}

Write-Host "Project : $($env:GCP_PROJECT_ID)"
Write-Host "Location: $($env:BQ_LOCATION)"
Write-Host "DataDir : $DATA_DIR"

# 1) Prepare datasets/tables
Invoke-BqSqlFile $SQL_PREP

# 2) Load local JSONL → staging tables
foreach ($kv in $files.GetEnumerator()) {
  $table = $kv.Key
  $cands = $kv.Value
  $src = $null
  foreach ($name in $cands) {
    $p = Join-Path $DATA_DIR $name
    if (Test-Path $p) { $src = $p; break }
  }
  if (-not $src) { throw "File not found for $table in $DATA_DIR (tried: $($cands -join ', '))" }

  Write-Host "Loading $src -> $($env:GCP_PROJECT_ID):$table ..."
  & bq --location=$env:BQ_LOCATION load --source_format=NEWLINE_DELIMITED_JSON --replace `
      "$($env:GCP_PROJECT_ID):$table" "$src" | Out-Null
}

# 3) Commit (populate finals and clean up staging)
Invoke-BqSqlFile $SQL_COMMIT

# 4) Print row counts
& bq query --project_id=$env:GCP_PROJECT_ID --location=$env:BQ_LOCATION --nouse_legacy_sql @"
SELECT 'channels' AS table, COUNT(*) AS rows FROM `$($env:GCP_PROJECT_ID).youtube_raw.channels`
UNION ALL SELECT 'videos', COUNT(*) FROM `$($env:GCP_PROJECT_ID).youtube_raw.videos`
UNION ALL SELECT 'segments', COUNT(*) FROM `$($env:GCP_PROJECT_ID).youtube_raw.subtitles_segments`
UNION ALL SELECT 'full', COUNT(*) FROM `$($env:GCP_PROJECT_ID).youtube_raw.subtitles_full`;
"@

Write-Host "`n[OK] Restore complete." -ForegroundColor Green
