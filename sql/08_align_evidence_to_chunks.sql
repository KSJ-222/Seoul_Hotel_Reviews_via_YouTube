-- sql/08_align_evidence_to_chunks.sql
-- Purpose: Align each review's evidence_quote to a timestamp using vector search first, then literal fallback, and force-fill if still unmatched.
-- Inputs:  <PROJECT_ID>.youtube_proc.video_review_points_raw, <PROJECT_ID>.youtube_proc.chunk_embeddings, <PROJECT_ID>.youtube_proc.chunks
-- Outputs: <PROJECT_ID>.youtube_proc.video_review_points (final), <PROJECT_ID>.youtube_proc.vrp_force_log (append), <PROJECT_ID>.youtube_proc.vrp_align_diag (report)
-- Deps:    BigQuery remote model <PROJECT_ID>.youtube_proc.text_multi_emb; optional vector index on chunk_embeddings(embedding)
-- Run:     Invoke-BqSqlFile .\sql\08_align_evidence_to_chunks.sql
-- Notes:   Incremental & idempotent. Order: vector search → literal match → forced backfill.

CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>`.youtube_proc;

-- Final table (ensure exists) + distance column
CREATE TABLE IF NOT EXISTS `<PROJECT_ID>`.youtube_proc.video_review_points (
  video_id       STRING,
  lang           STRING,
  hotel_norm     STRING,
  brand          STRING,
  city           STRING,
  country        STRING,
  aspect         STRING,
  sentiment      STRING,
  review_summary STRING,
  evidence_quote STRING,
  evidence_sec   INT64,
  distance       FLOAT64,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
ALTER TABLE `<PROJECT_ID>`.youtube_proc.video_review_points
ADD COLUMN IF NOT EXISTS distance FLOAT64;

-- 1) Pending keys (row-level)
CREATE TEMP TABLE _pending AS
SELECT
  r.video_id, r.lang, r.hotel_norm, r.brand, r.city, r.country,
  r.aspect, r.sentiment, r.review_summary, r.evidence_quote
FROM `<PROJECT_ID>`.youtube_proc.video_review_points_raw r
LEFT JOIN `<PROJECT_ID>`.youtube_proc.video_review_points f
  ON  f.video_id       = r.video_id
  AND f.lang           = r.lang
  AND f.hotel_norm     = r.hotel_norm
  AND f.aspect         = r.aspect
  AND f.evidence_quote = r.evidence_quote
WHERE f.video_id IS NULL
  AND COALESCE(r.evidence_quote,'') <> '';

IF (SELECT COUNT(*) FROM _pending) = 0 THEN
  SELECT 'No new rows to align.' AS info, 0 AS inserted_rows;
ELSE
  -- 2) Assign qid + normalize quotes (lowercase, collapse spaces, remove control chars, limit length)
  CREATE TEMP TABLE _qsrc AS
  SELECT
    GENERATE_UUID() AS qid,
    video_id, lang, hotel_norm, brand, city, country,
    aspect, sentiment, review_summary, evidence_quote,
    SUBSTR(
      REGEXP_REPLACE(REGEXP_REPLACE(LOWER(evidence_quote), r'\s+', ' '), r'[\x00-\x1F]+', ''),
      1, 500
    ) AS quote_norm
  FROM _pending;

  -- 3) Embed normalized quotes
  CREATE TEMP TABLE _qvec AS
  SELECT qid, ml_generate_embedding_result AS qvec
  FROM ML.GENERATE_EMBEDDING(
    MODEL `<PROJECT_ID>`.youtube_proc.text_multi_emb,
    (SELECT quote_norm AS content, qid FROM _qsrc)
  );

  -- 4) Query table with unified "embedding" column
  CREATE TEMP TABLE _query_tbl AS
  SELECT
    s.qid,
    s.video_id, s.lang, s.hotel_norm, s.brand, s.city, s.country,
    s.aspect, s.sentiment, s.review_summary, s.evidence_quote,
    s.quote_norm,
    v.qvec AS embedding
  FROM _qsrc s
  JOIN _qvec v USING (qid);

  -- 5) Vector search by (video_id, lang); keep nearest (top_k=10 → best per qid)
  CREATE TEMP TABLE _aligned_vs (
    qid            STRING,
    video_id       STRING,
    lang           STRING,
    hotel_norm     STRING,
    brand          STRING,
    city           STRING,
    country        STRING,
    aspect         STRING,
    sentiment      STRING,
    review_summary STRING,
    evidence_quote STRING,
    evidence_sec   INT64,
    distance       FLOAT64
  );

  BEGIN
    FOR pair IN (SELECT DISTINCT video_id, lang FROM _query_tbl) DO
      INSERT INTO _aligned_vs
      SELECT
        q.qid,
        q.video_id, q.lang, q.hotel_norm, q.brand, q.city, q.country,
        q.aspect, q.sentiment, q.review_summary, q.evidence_quote,
        r.base.start_sec AS evidence_sec,
        r.distance
      FROM VECTOR_SEARCH(
        (
          SELECT embedding, video_id, lang, chunk_id, start_sec, end_sec
          FROM `<PROJECT_ID>`.youtube_proc.chunk_embeddings
          WHERE video_id = pair.video_id AND lang = pair.lang
        ),
        'embedding',
        (
          SELECT embedding, qid
          FROM _query_tbl
          WHERE video_id = pair.video_id AND lang = pair.lang
        ),
        top_k => 10,
        distance_type => 'COSINE'
      ) AS r
      JOIN _query_tbl q
        ON q.qid = r.query.qid
      QUALIFY ROW_NUMBER() OVER (PARTITION BY q.qid ORDER BY r.distance ASC) = 1;
    END FOR;
  END;

  -- 6) Literal fallback (normalized substring match)
  CREATE TEMP TABLE _chunks_norm AS
  SELECT
    video_id, lang, start_sec, end_sec,
    SUBSTR(
      REGEXP_REPLACE(REGEXP_REPLACE(LOWER(chunk_text), r'\s+', ' '), r'[\x00-\x1F]+', ''),
      1, 5000
    ) AS chunk_text_norm
  FROM `<PROJECT_ID>`.youtube_proc.chunks;

  CREATE TEMP TABLE _aligned_literal AS
  SELECT
    q.qid,
    q.video_id, q.lang, q.hotel_norm, q.brand, q.city, q.country,
    q.aspect, q.sentiment, q.review_summary, q.evidence_quote,
    c.start_sec AS evidence_sec,
    CAST(NULL AS FLOAT64) AS distance
  FROM _query_tbl q
  LEFT JOIN _aligned_vs v ON v.qid = q.qid
  JOIN _chunks_norm c
    ON c.video_id = q.video_id AND c.lang = q.lang
   AND c.chunk_text_norm IS NOT NULL
   AND q.quote_norm IS NOT NULL
   AND q.quote_norm <> ''
   AND STRPOS(c.chunk_text_norm, q.quote_norm) > 0
  WHERE v.qid IS NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY q.qid ORDER BY c.start_sec) = 1;

  -- 7) Combine VS + literal
  CREATE TEMP TABLE _aligned_all AS
  SELECT * FROM _aligned_vs
  UNION ALL
  SELECT * FROM _aligned_literal;

  -- 8) Upsert aligned rows
  MERGE `<PROJECT_ID>`.youtube_proc.video_review_points AS T
  USING (
    SELECT
      video_id, lang, hotel_norm, brand, city, country,
      aspect, sentiment, review_summary, evidence_quote,
      evidence_sec, distance
    FROM _aligned_all
  ) AS S
  ON  T.video_id       = S.video_id
  AND T.lang           = S.lang
  AND T.hotel_norm     = S.hotel_norm
  AND T.aspect         = S.aspect
  AND T.evidence_quote = S.evidence_quote
  WHEN NOT MATCHED THEN
    INSERT (video_id, lang, hotel_norm, brand, city, country,
            aspect, sentiment, review_summary, evidence_quote, evidence_sec, distance)
    VALUES (S.video_id, S.lang, S.hotel_norm, S.brand, S.city, S.country,
            S.aspect, S.sentiment, S.review_summary, S.evidence_quote, S.evidence_sec, S.distance);

  -- 9) Forced backfill: still missing → first chunk start (or 0); distance = NULL
  CREATE TEMP TABLE _not_aligned AS
  SELECT q.*
  FROM _query_tbl q
  LEFT JOIN _aligned_all a ON a.qid = q.qid
  LEFT JOIN `<PROJECT_ID>`.youtube_proc.video_review_points f
    ON  f.video_id       = q.video_id
    AND f.lang           = q.lang
    AND f.hotel_norm     = q.hotel_norm
    AND f.aspect         = q.aspect
    AND f.evidence_quote = q.evidence_quote
  WHERE a.qid IS NULL
    AND f.video_id IS NULL;

  -- First chunk per (video_id, lang)
  CREATE TEMP TABLE _first_chunk AS
  SELECT video_id, lang, MIN(start_sec) AS min_start
  FROM `<PROJECT_ID>`.youtube_proc.chunks
  GROUP BY 1,2;

  -- Build forced rows
  CREATE TEMP TABLE _forced AS
  SELECT
    n.video_id, n.lang, n.hotel_norm, n.brand, n.city, n.country,
    n.aspect, n.sentiment, n.review_summary, n.evidence_quote,
    COALESCE(fc.min_start, 0) AS evidence_sec,
    CAST(NULL AS FLOAT64)     AS distance
  FROM _not_aligned n
  LEFT JOIN _first_chunk fc
    ON fc.video_id = n.video_id AND fc.lang = n.lang;

  -- Forced MERGE (dup-safe)
  MERGE `<PROJECT_ID>`.youtube_proc.video_review_points AS T
  USING _forced AS S
  ON  T.video_id       = S.video_id
  AND T.lang           = S.lang
  AND T.hotel_norm     = S.hotel_norm
  AND T.aspect         = S.aspect
  AND T.evidence_quote = S.evidence_quote
  WHEN NOT MATCHED THEN
    INSERT (video_id, lang, hotel_norm, brand, city, country,
            aspect, sentiment, review_summary, evidence_quote, evidence_sec, distance)
    VALUES (S.video_id, S.lang, S.hotel_norm, S.brand, S.city, S.country,
            S.aspect, S.sentiment, S.review_summary, S.evidence_quote, S.evidence_sec, S.distance);

  -- Forced insert log (append)
  CREATE TABLE IF NOT EXISTS `<PROJECT_ID>`.youtube_proc.vrp_force_log (
    inserted_at TIMESTAMP,
    video_id    STRING,
    lang        STRING,
    hotel_norm  STRING,
    aspect      STRING,
    evidence_quote STRING,
    note        STRING
  );
  INSERT INTO `<PROJECT_ID>`.youtube_proc.vrp_force_log
  SELECT CURRENT_TIMESTAMP(), video_id, lang, hotel_norm, aspect, evidence_quote, 'FORCED_MIN_CHUNK_OR_ZERO'
  FROM _forced;

  -- 10) Diagnostics (per key reason)
  CREATE OR REPLACE TABLE `<PROJECT_ID>`.youtube_proc.vrp_align_diag AS
  WITH base_counts AS (
    SELECT video_id, lang, COUNT(*) AS base_chunks
    FROM `<PROJECT_ID>`.youtube_proc.chunk_embeddings
    GROUP BY 1,2
  )
  SELECT
    q.video_id, q.lang, q.hotel_norm, q.aspect, q.evidence_quote,
    COALESCE(b.base_chunks,0) AS base_chunks,
    CASE
      WHEN v.qid IS NOT NULL THEN 'VS_OK'
      WHEN l.qid IS NOT NULL THEN 'LITERAL_OK'
      WHEN f.video_id IS NOT NULL THEN 'FORCED'
      ELSE 'NO_MATCH'
    END AS reason
  FROM _query_tbl q
  LEFT JOIN _aligned_vs v       ON v.qid = q.qid
  LEFT JOIN _aligned_literal l  ON l.qid = q.qid
  LEFT JOIN _forced f
    ON f.video_id = q.video_id AND f.lang = q.lang
   AND f.hotel_norm = q.hotel_norm AND f.aspect = q.aspect
   AND f.evidence_quote = q.evidence_quote
  LEFT JOIN base_counts b ON b.video_id = q.video_id AND b.lang = q.lang;

  -- Summary
  SELECT
    (SELECT COUNT(*) FROM _pending)                                                            AS pending_rows,
    (SELECT COUNT(*) FROM _aligned_vs)                                                         AS vs_rows,
    (SELECT COUNT(*) FROM _aligned_literal)                                                    AS literal_rows,
    (SELECT COUNT(*) FROM _forced)                                                             AS forced_rows,
    (SELECT COUNT(*) FROM `<PROJECT_ID>`.youtube_proc.vrp_align_diag WHERE reason='NO_MATCH')  AS still_no_match,
    (SELECT COUNT(*) FROM `<PROJECT_ID>`.youtube_proc.video_review_points)                     AS final_total_rows;
END IF;
