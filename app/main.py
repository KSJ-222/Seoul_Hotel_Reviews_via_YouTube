# app/main.py
"""
Backend for the Seoul hotel review chatbot.

- GET  /      : serves the static index.html
- POST /ask   : accepts question + 4 filters, queries BigQuery TVF ONCE, returns summary + citations

Env:
  GCP_PROJECT_ID    (required)
  BQ_LOCATION       (default: asia-northeast3)
  LLM_CONN          (default: <PROJECT_ID>.asia-northeast3.llm_conn)

Run locally:
  uvicorn app.main:app --reload --port 8080
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
if not PROJECT_ID:
    raise RuntimeError("GCP_PROJECT_ID env is required")

BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-northeast3")
LLM_CONN    = os.getenv("LLM_CONN", f"{PROJECT_ID}.asia-northeast3.llm_conn")

app = FastAPI(title="Seoul Hotel Reviews (YouTube RAG)")

# CORS (open for demo; restrict in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
def home():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(index_path)


@app.post("/ask")
async def ask(payload: Dict[str, Any]):
    try:
        # UI defaults aligned: lang=en, top_k=5
        q_text           = str(payload.get("question") or "").strip()
        lang_filter      = str(payload.get("lang_filter") or "en")
        exclude_paid_ads = bool(payload.get("exclude_paid_ads"))
        min_views        = int(payload.get("min_views") or 0)
        min_subs         = int(payload.get("min_subs") or 0)
        top_k            = int(payload.get("top_k") or 5)

        if not q_text:
            raise HTTPException(status_code=400, detail="Missing 'question'.")

        # ---- 1) Call TVF ONCE to retrieve candidates (filters applied inside the TVF) ----
        sql_cand = f"""
        SELECT video_id, channel_title, video_title, hotel_norm, aspect, sentiment,
               review_summary, yt_link, evidence_sec, sim, pop, score
        FROM `{PROJECT_ID}`.youtube_proc.rag_candidates_semantic(
          @q_text, @lang_filter, @exclude_paid_ads, @min_views, @min_subs, @top_k
        )
        ORDER BY score DESC
        """
        job = bq_client.query(
            sql_cand,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("q_text", "STRING", q_text),
                    bigquery.ScalarQueryParameter("lang_filter", "STRING", lang_filter),
                    bigquery.ScalarQueryParameter("exclude_paid_ads", "BOOL", exclude_paid_ads),
                    bigquery.ScalarQueryParameter("min_views", "INT64", min_views),
                    bigquery.ScalarQueryParameter("min_subs", "INT64", min_subs),
                    bigquery.ScalarQueryParameter("top_k", "INT64", top_k),
                ]
            ),
        )
        rows = list(job.result())

        # ---- 2) Build citations (exactly 4 columns) and local bullets (no second TVF call) ----
        citations: List[Dict[str, Optional[str]]] = []
        bullets_parts: List[str] = []

        for r in rows:
            hotel_norm = r["hotel_norm"] or ""
            aspect = r["aspect"] or ""
            sentiment = r["sentiment"] or ""
            review_summary = r["review_summary"] or ""

            review_text = f"{hotel_norm} — {aspect}: {review_summary}".strip()
            citations.append({
                "review": review_text,
                "link": r["yt_link"],
                "video_title": r["video_title"],
                "channel_title": r["channel_title"],
                "evidence_sec": int(r["evidence_sec"] or 0),  # frontend renders MM:SS
            })

            bullets_parts.append(f"- {hotel_norm} — {aspect} ({sentiment}): {review_summary}")

        bullets = "\n".join(bullets_parts)

        # ---- 3) Summarize: single AI.GENERATE call with parameterized prompt; connection_id as STRING LITERAL ----
        if not rows:
            summary_text = "No review candidates matched your question or filters."
        else:
            prompt = (
                "Answer the user's question in the SAME language as the question (detect automatically). "
                "Write 2–3 concise sentences, focusing on key takeaways. "
                "Do not fabricate; use only the bullets below.\n"
                f"Question: {q_text}\n\n"
                f"Bullets:\n{bullets}"
            )

            sql_summary = f"""
            SELECT (
              AI.GENERATE(
                @prompt,
                connection_id => '{LLM_CONN}',  -- must be a string literal
                endpoint      => 'gemini-2.5-flash',
                model_params  => JSON '{{"generation_config":{{"temperature":0}}}}'
              )
            ).result AS summary
            """
            job2 = bq_client.query(
                sql_summary,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("prompt", "STRING", prompt),
                    ]
                ),
            )
            summary_rows = list(job2.result())
            summary_text = (summary_rows[0]["summary"] if summary_rows else "").strip()

        return JSONResponse({"summary": summary_text, "citations": citations})

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
