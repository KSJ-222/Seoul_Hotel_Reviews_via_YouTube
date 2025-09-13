from __future__ import annotations
import os
from typing import List, Dict, Any
from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# .env 로드
load_dotenv(find_dotenv())

PROJECT = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "youtube_raw")
LOCATION = "asia-northeast3"

# 전역 BigQuery 클라이언트 (고정 location)
client = bigquery.Client(project=PROJECT, location=LOCATION)


def ensure_tables():
    """데이터셋/테이블 없으면 생성 (SUBTITLES_RAW)"""
    dataset_id = f"{PROJECT}.{DATASET}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        ds = bigquery.Dataset(dataset_id)
        ds.location = LOCATION
        client.create_dataset(ds)

    table_id = f"{dataset_id}.SUBTITLES_RAW"
    try:
        client.get_table(table_id)
    except NotFound:
        schema = [
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("subtitle_lang", "STRING"),
            bigquery.SchemaField("subtitle_text", "STRING"),
            bigquery.SchemaField("source", "STRING"),  # manual | auto
            bigquery.SchemaField("snippet_count", "INT64"),
            bigquery.SchemaField(
                "snippets", "RECORD", mode="REPEATED",
                fields=[
                    bigquery.SchemaField("idx", "INT64"),
                    bigquery.SchemaField("start", "FLOAT64"),
                    bigquery.SchemaField("end", "FLOAT64"),
                    bigquery.SchemaField("duration", "FLOAT64"),
                    bigquery.SchemaField("text", "STRING"),
                ]
            ),
            bigquery.SchemaField("errors", "STRING", mode="REPEATED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
        table_ref = bigquery.Table(table_id, schema=schema)
        client.create_table(table_ref)


def _load_tmp(table_id: str, rows: List[Dict[str, Any]]):
    """
    JSON 행을 {table}_TMP에 적재(WRITE_TRUNCATE).
    TMP 테이블을 본 테이블 스키마로 강제 생성/로딩하여
    ARRAY<STRUCT> 등 스키마 오추론 문제를 방지.
    """
    tmp_table_id = table_id + "_TMP"

    # 잘못된 스키마의 TMP가 남아있을 수 있으므로 삭제
    client.delete_table(tmp_table_id, not_found_ok=True)

    # 본 테이블 스키마 가져와 동일 스키마로 로드
    base_table = client.get_table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=base_table.schema,
    )
    job = client.load_table_from_json(rows, tmp_table_id, job_config=job_config)
    job.result()


def _build_merge_condition_from_keys(keys: List[str]) -> str:
    """
    dedupe_on 키 목록으로 MERGE ON 조건 생성 (NULL-safe).
    """
    conds = []
    for k in keys:
        k = k.strip()
        if not k:
            continue
        conds.append(f"T.{k} IS NOT DISTINCT FROM S.{k}")
    return " AND ".join(conds) if conds else "FALSE"


def upsert_subtitles_raw(rows: List[Dict[str, Any]], dedupe_on: str = "video_id,subtitle_lang"):
    """
    SUBTITLES_RAW에 UPSERT. rows: [{...}]
    - 기본 dedupe 키: "video_id,subtitle_lang" (NULL-safe 비교)
    """
    table = f"{PROJECT}.{DATASET}.SUBTITLES_RAW"
    if not rows:
        return

    # 타입/필드 보정(충돌 방지)
    for r in rows:
        if "snippet_count" in r and r["snippet_count"] is not None:
            r["snippet_count"] = int(r["snippet_count"])
        if "snippets" in r and r["snippets"]:
            fixed = []
            for s in r["snippets"]:
                fixed.append({
                    "idx": int(s.get("idx") or 0),
                    "start": float(s.get("start") or 0.0),
                    "end": float(s.get("end") or 0.0),
                    "duration": float(s.get("duration") or 0.0),
                    "text": s.get("text"),
                })
            r["snippets"] = fixed

    # TMP 적재(스키마 고정)
    _load_tmp(table, rows)

    # dedupe 기준 구성 (NULL-safe)
    keys = [k.strip() for k in dedupe_on.split(",") if k.strip()]
    merge_condition = _build_merge_condition_from_keys(keys)

    # UNNEST 시 NULL 대비를 위해 빈 배열 캐스팅 정의
    empty_snippets_cast = (
        "CAST([] AS ARRAY<STRUCT<"
        "idx INT64, start FLOAT64, `end` FLOAT64, duration FLOAT64, text STRING"
        ">>)"
    )

    query = f"""
    MERGE `{table}` T
    USING (
      SELECT
        S.video_id,
        S.subtitle_lang,
        S.subtitle_text,
        S.source,
        S.snippet_count,
        S.errors,
        SAFE_CAST(S.ingested_at AS TIMESTAMP) AS ingested_at,
        -- ✅ UPDATE/INSERT에서 그대로 쓰기 위해 미리 재조립
        (
          SELECT ARRAY_AGG(STRUCT(
            x.idx      AS idx,
            x.start    AS start,
            x.`end`    AS `end`,
            x.duration AS duration,
            x.text     AS text
          ))
          FROM UNNEST(IFNULL(S.snippets, {empty_snippets_cast})) AS x
        ) AS snippets_fixed
      FROM `{table}_TMP` AS S
    ) AS S
    ON {_build_merge_condition_from_keys([k.strip() for k in dedupe_on.split(',') if k.strip()])}
    WHEN MATCHED THEN UPDATE SET
      subtitle_lang = S.subtitle_lang,
      subtitle_text = S.subtitle_text,
      source        = S.source,
      snippet_count = S.snippet_count,
      snippets      = S.snippets_fixed,   -- ✅ 서브쿼리 금지 -> 미리 계산한 컬럼 사용
      errors        = S.errors,
      ingested_at   = S.ingested_at
    WHEN NOT MATCHED THEN INSERT (
      video_id, subtitle_lang, subtitle_text, source,
      snippet_count, snippets, errors, ingested_at
    )
    VALUES (
      S.video_id, S.subtitle_lang, S.subtitle_text, S.source,
      S.snippet_count, S.snippets_fixed, S.errors, S.ingested_at
    );
    """

    client.query(query).result()
    client.delete_table(table + "_TMP", not_found_ok=True)
