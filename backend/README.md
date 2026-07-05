# Backend

This backend now implements the Phase 1 evidence pipeline with compact GPT payloads and file-backed run artifacts.

## What Phase 1 Includes
- FastAPI app
- `GET /health`
- `POST /analyze-feedback`
- strict request validation
- mock response matching the target analysis schema
- pytest coverage for health and validation

## What Phase 2 Adds
- Google Play Spotify review collection
- normalization into the raw feedback schema
- Google Play source counts in source summary and metrics
- pytest coverage for collector normalization and API integration

## Local Setup

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run Locally

```bash
cd backend
source .venv/bin/activate
uvicorn app.main:app --reload
```

Logs are written to:
- `backend/logs/backend.log`
- Run artifacts are written to:
- `backend/data/runs/{run_id}/`

Open:
- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/docs`

## Run Tests

```bash
cd backend
source .venv/bin/activate
pytest
```

## How To Test After Each Stage

### Phase 1
Verify:
- `GET /health` returns `healthy`
- `POST /analyze-feedback` accepts the Spotify brief from the spec
- missing required fields return `422`
- malformed time window returns `422`
- empty topic lists still succeed with warnings

Example request:

```bash
curl -X POST http://127.0.0.1:8000/analyze-feedback \
  -H "Content-Type: application/json" \
  -d '{
    "product": "Spotify",
    "research_scope": "Music Discovery",
    "research_goal": "Opportunity Discovery",
    "analysis_time_window": {"type": "relative", "value": "12_months"},
    "included_topics": ["recommendations", "music discovery", "personalization"],
    "excluded_topics": ["pricing", "billing", "podcasts"],
    "max_runtime_seconds": 120,
    "debug": false
  }'
```

### Phase 2
Verify:
- Google Play collector returns normalized review records
- tests confirm source mapping to the raw feedback schema
- `/analyze-feedback` includes Google Play counts in source summary and metrics

Commands:

```bash
cd backend
source ../.venv39/bin/activate
pytest tests/test_google_play.py tests/test_pipeline_google_play.py
```

Live API check:
- start `uvicorn app.main:app --reload`
- call `/analyze-feedback`
- confirm `metrics.source_distribution.google_play` is greater than `0` if collection succeeds
- if store access fails, confirm the response still returns `200` with `status = partial_success` and a Google Play warning

### Phase 3
Verify:
- source discovery generates dynamic query seeds
- Reddit collector returns normalized discussion records
- Reddit collector fans out across multiple relevant queries instead of a single search fetch
- Reddit failure becomes a warning, not a fatal API failure
- short-lived Reddit `429` responses are retried with backoff, and already collected Reddit records are preserved if a later query still fails

Commands:

```bash
cd backend
source ../.venv39/bin/activate
pytest tests/test_reddit.py tests/test_pipeline_reddit.py
```

Live API check:
- start `uvicorn app.main:app --reload`
- call `/analyze-feedback`
- confirm `metrics.source_distribution.reddit` is greater than `0` if Reddit collection succeeds
- if Reddit access fails, confirm the API still returns `200` with `status = partial_success` and a Reddit warning
- note: Reddit collection now uses the public RSS search feed as the fallback path, so repeated back-to-back manual calls may hit a short rate limit; if that happens, wait about a minute and retry
- note: the collector now aggregates multiple brief-derived Reddit searches, so total Reddit coverage is broader, but still limited to publicly accessible RSS search results rather than full comment-tree crawling

### Phase 4
Verify:
- App Store collector returns normalized records when available
- App Store collector walks paginated RSS review pages until exhausted or capped
- App Store failure produces `partial_success` when other sources still work

Commands:

```bash
cd backend
source ../.venv39/bin/activate
pytest tests/test_app_store.py tests/test_pipeline_app_store.py
```

Live API check:
- start `uvicorn app.main:app --reload`
- call `/analyze-feedback`
- confirm `metrics.source_distribution.app_store` is greater than `0` if the App Store RSS feed succeeds
- if App Store access fails, confirm the API still returns `200` with `status = partial_success` and an App Store warning

### Collection Breadth
Current source behavior:
- Google Play walks continuation-token pages until exhaustion or a source cap is reached.
- App Store walks paginated RSS pages until exhaustion or a source cap is reached.
- Reddit uses a smaller prioritized set of locked-brief-derived RSS searches, caches recent query results on disk, and deduplicates overlapping discussions.
- Reddit is intentionally paced more conservatively for public RSS, with longer inter-query delays and fewer retries, to trade some latency for higher odds of multiple successful query pulls within the runtime budget.

Important limitation:
- Public-source indexing, anti-bot controls, and rate limits can still prevent true “all feedback ever” coverage. The backend now avoids the earlier single-page bottleneck, but it still reports only what the accessible public surfaces expose during the run.
- `processing_summary.source_warning_codes` now distinguishes cases like `reddit_partial` vs `reddit_failed` for downstream GPT/report generation.
- Reddit now stops early after repeated RSS `429`s instead of continuing to hang through the full query list.
- A run now stays `completed` when Reddit produced usable records but later degraded; `partial_success` is reserved for full source failure.
- Interactive runs now apply runtime-aware Google Play and App Store caps when `max_runtime_seconds` is small, and those caps are disclosed in warnings and processing notes rather than applied silently.
- The Action response now applies transport-safe compaction when needed by returning only the top response clusters and truncating quote text, while keeping metrics, charts, and source summaries aligned to the full analyzed result set.

### Runtime Visibility
The backend now logs live collection and processing progress to the server console:
- source collection start/completion
- Google Play page progress
- App Store page progress
- Reddit query progress, retries, and rate-limit backoff
- cleaning, relevance, deduplication, and clustering stage completion

Collection is also executed in parallel across Reddit, Google Play, and App Store to reduce wall-clock time for live runs.

The same logs are also written to `backend/logs/backend.log`, so you can share that file directly for debugging.

### Artifact-backed responses
The backend now returns a compact Action-safe response plus artifact URLs for full evidence retrieval.

Main behavior:
- `POST /analyze-feedback` returns a compact payload for GPT synthesis
- the full evidence is saved under `data/runs/{run_id}/`
- `GET /runs/{run_id}/manifest` returns the available artifact list
- `GET /runs/{run_id}/artifact/{artifact_name}` returns a known artifact file
- `quality_diagnostics.json` exposes evidence-quality and contamination diagnostics

Important:
- GPT should use `compact_gpt_payload` first
- GPT / Agent 0 must infer and confirm `success_criteria` before the backend call; the backend never infers them
- GPT / Agent 0 should infer the rest of the draft brief where reasonable, show the full locked brief, and wait for an explicit `go ahead` before calling the backend
- if deeper evidence is required, GPT should fetch artifacts such as `all_clusters_compact.json`, `all_clusters.json`, `all_clusters.csv`, `research_question_coverage.json`, `charts_data.json`, `quality_diagnostics.json`, `opportunity_traceability_compact.json`, `opportunity_traceability.json`, `success_criteria_impact_mapping_compact.json`, `success_criteria_impact_mapping.json`, `segment_evidence.json`, or `evidence_appendix.md`
- this avoids payload-size failures while preserving full analysis depth
- the final Markdown report should be returned directly in chat as a downloadable Markdown output, not written back into the backend artifact folder as part of the normal flow

### Phase 5
Verify:
- empty/noisy records are removed
- included topic rules retain relevant discovery feedback
- excluded topic rules remove out-of-scope feedback
- debug mode exposes filtering reasons when implemented

Commands:

```bash
cd backend
source ../.venv39/bin/activate
pytest tests/test_relevance.py tests/test_pipeline_phase5.py
```

Live API check:
- start `uvicorn app.main:app --reload`
- call `/analyze-feedback` with normal Spotify discovery topics
- confirm `metrics.records_after_cleaning` is less than or equal to `total_records_collected`
- confirm `metrics.records_relevant` is less than or equal to `records_after_cleaning`
- use `debug: true` to inspect relevance notes in `processing_notes`

### Phase 6
Verify:
- exact duplicates are removed
- normalized duplicates are removed
- near duplicates above threshold are removed without collapsing clearly distinct feedback

Commands:

```bash
cd backend
source ../.venv39/bin/activate
pytest tests/test_dedupe.py tests/test_pipeline_phase6.py
```

Live API check:
- start `uvicorn app.main:app --reload`
- call `/analyze-feedback` with `debug: true`
- confirm `metrics.records_after_deduplication` is less than or equal to `metrics.records_relevant`
- inspect `processing_notes` for `removed_exact_duplicate`, `removed_normalized_duplicate`, or `removed_near_duplicate` when duplicates are present

### Phase 7
Verify:
- related feedback lands in the same cluster
- cluster names are deterministic
- mixed positive and negative evidence is preserved via cluster signal fields instead of being flattened into one pure-opportunity count
- representative quotes come from real collected records

Commands:

```bash
cd backend
source ../.venv39/bin/activate
pytest tests/test_clustering.py tests/test_pipeline_phase7.py
```

Live API check:
- start `uvicorn app.main:app --reload`
- call `/analyze-feedback` with `debug: true`
- confirm `metrics.cluster_count` is greater than `1` when multiple themes exist
- inspect `feedback_clusters[*].dominant_signal`, `pain_point_evidence_count`, `positive_validation_count`, and `mixed_signal_flag`

### Phase 8
Verify:
- metrics counters reconcile with processed record counts
- chart-ready JSON is populated and matches schema
- source summaries and top cluster outputs are internally consistent

Commands:

```bash
cd backend
source ../.venv39/bin/activate
pytest tests/test_pipeline_phase8.py
```

Live API check:
- start `uvicorn app.main:app --reload`
- call `/analyze-feedback` with `debug: true`
- confirm `metrics.cluster_count` matches the number of non-empty clusters
- confirm `charts_data.cluster_signal_distribution` matches `metrics.dominant_signal_distribution`
- confirm `source_summary`, `top_clusters`, and `source_distribution_by_cluster` are all cluster-aware and internally consistent

### Phase 9
Verify:
- end-to-end `/analyze-feedback` returns real processed evidence
- warnings and processing notes reflect source failures or limitations
- sample response can be used by the Custom GPT without manual edits

Artifacts:
- `docs/sample_response.json`

### Phase 10
Verify:
- `docs/openapi_schema.yaml` imports successfully into Custom GPT Actions
- `docs/custom_gpt_instructions.md` enforces brief clarification before action calls
- the full conversational workflow runs without manual user-side configuration

Artifacts:
- `docs/openapi_schema.yaml`
- `docs/custom_gpt_instructions.md`
- `docs/sample_response.json`

## Custom GPT Setup

1. Start or deploy the backend so `POST /analyze-feedback` is reachable over HTTPS.
2. In Custom GPT Actions, import `docs/openapi_schema.yaml`.
3. Point the action server/base URL to your backend deployment.
4. Copy the guidance from `docs/custom_gpt_instructions.md` into the GPT instructions.
5. Keep the product fixed to Spotify and let the GPT clarify only the remaining brief fields.
6. Use `docs/sample_response.json` as a reference artifact when checking report formatting and evidence usage.

Recommended validation flow:
- verify the action imports without schema errors
- send a short Spotify discovery prompt through the GPT
- confirm the GPT infers the missing brief fields and asks only minimal follow-up questions
- confirm the GPT presents the full locked brief and waits for `go ahead` before calling the action
- confirm the GPT uses backend counts, quotes, metrics, and warnings without inventing evidence
