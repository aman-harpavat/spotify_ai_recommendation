from __future__ import annotations

from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from copy import deepcopy
import logging
import math
from time import perf_counter
from typing import Any, Callable, Optional

from app.config import settings
from app.collectors.app_store import (
    DEFAULT_APP_STORE_APP_ID,
    collect_app_store_reviews,
)
from app.collectors.google_play import (
    DEFAULT_GOOGLE_PLAY_APP_ID,
    collect_google_play_reviews,
)
from app.collectors.reddit import collect_reddit_feedback_with_warnings
from app.processing.cleaner import clean_feedback_items
from app.processing.clustering import cluster_feedback_items
from app.processing.dedupe import deduplicate_feedback_items
from app.processing.relevance import (
    filter_relevant_feedback,
    score_opportunity_signal,
    score_relevance,
)
from app.schemas import (
    AnalyzeFeedbackRequest,
    AnalyzeFeedbackResponse,
    ArtifactManifest,
    ChartClusterValue,
    ClusterSignalValue,
    ChartsDataPayload,
    ChartRatingValue,
    ChartValueBySource,
    ChartValueOverTime,
    ClusterItem,
    CompactGPTPayload,
    EvidenceBackedSegment,
    MetricsPayload,
    OpportunityItem,
    ProcessingSummary,
    QualityDiagnostics,
    QuoteItem,
    RawFeedbackItem,
    ResearchQuestionCoverage,
    SuccessCriteriaImpactItem,
    SupportingResearchQuestionItem,
    SourceDateRange,
    SourceDistributionByCluster,
    SourceLimitation,
    SourceSummaryItem,
    TopClusterMetric,
)
from app.services.artifacts import (
    build_artifact_manifest,
    ensure_run_dir,
    write_csv_artifact,
    write_json_artifact,
    write_markdown_artifact,
)
from app.services.source_discovery import build_reddit_query_seeds
from app.utils.dates import (
    is_within_relative_window,
    month_bucket,
    relative_window_months_equivalent,
)
from app.utils.ids import make_run_id

logger = logging.getLogger(__name__)
StatusCallback = Callable[[str, int, Optional[str]], None]


def collect_reddit_feedback(
    queries: list[str],
    *,
    limit: int,
    query_delay_seconds: float,
    max_retries: int,
    backoff_seconds: float,
    max_total_seconds: float,
) -> tuple[list[RawFeedbackItem], list[str]]:
    return collect_reddit_feedback_with_warnings(
        queries,
        limit=limit,
        query_delay_seconds=query_delay_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        max_total_seconds=max_total_seconds,
    )


def build_mock_analysis_response(
    request: AnalyzeFeedbackRequest,
    *,
    run_id: str | None = None,
    status_callback: StatusCallback | None = None,
) -> AnalyzeFeedbackResponse:
    """Build a compact Action-safe response plus file-backed artifacts."""
    run_id = run_id or make_run_id()
    ensure_run_dir(run_id)
    run_started_at = perf_counter()
    collection_plan = _build_collection_plan(request)
    reddit_queries = build_reddit_query_seeds(request)[
        : int(collection_plan["reddit_query_count"])
    ]
    _emit_status(
        status_callback,
        "collecting_sources",
        10,
        "Collecting Google Play, App Store, and Reddit feedback.",
    )
    logger.info(
        "analyze_feedback started run_id=%s product=%s scope=%s goal=%s reddit_queries=%s collection_plan=%s",
        run_id,
        request.product,
        request.research_scope,
        request.research_goal,
        len(reddit_queries),
        collection_plan,
    )
    (
        google_play_feedback,
        google_play_warnings,
        reddit_feedback,
        reddit_warnings,
        app_store_feedback,
        app_store_warnings,
    ) = _collect_all_sources(collection_plan=collection_plan, reddit_queries=reddit_queries)
    collected_feedback = reddit_feedback + google_play_feedback + app_store_feedback
    _emit_status(
        status_callback,
        "cleaning_feedback",
        40,
        "Cleaning and normalizing collected feedback.",
    )
    logger.info("cleaning started records=%s", len(collected_feedback))
    cleaned_feedback = clean_feedback_items(collected_feedback)
    logger.info("cleaning completed records=%s", len(cleaned_feedback))
    in_window_feedback, out_of_window_feedback = _split_feedback_by_time_window(
        cleaned_feedback,
        request,
    )
    logger.info(
        "time-window filtering completed in_window=%s out_of_window=%s",
        len(in_window_feedback),
        len(out_of_window_feedback),
    )
    _emit_status(
        status_callback,
        "filtering_relevance",
        55,
        "Filtering for discovery-relevant evidence within the requested time window.",
    )
    logger.info("relevance filtering started records=%s", len(in_window_feedback))
    relevant_feedback, relevance_debug_notes = filter_relevant_feedback(
        in_window_feedback,
        request,
        debug=request.debug,
    )
    expanded_collection_applied = False
    expanded_collection_reason: str | None = None
    if _should_expand_collection(
        collection_plan=collection_plan,
        relevant_feedback=relevant_feedback,
    ):
        expanded_collection_applied = True
        expanded_collection_reason = (
            "Low relevant in-window record count triggered a second-pass scale expansion for Google Play and App Store."
        )
        expansion_plan = _build_expanded_collection_plan(collection_plan)
        logger.info("expanded collection started plan=%s", expansion_plan)
        (
            extra_google_play_feedback,
            extra_google_play_warnings,
            extra_reddit_feedback,
            extra_reddit_warnings,
            extra_app_store_feedback,
            extra_app_store_warnings,
        ) = _collect_all_sources(
            collection_plan=expansion_plan,
            reddit_queries=[],
            include_reddit=False,
        )
        google_play_feedback = _merge_feedback_lists(
            google_play_feedback, extra_google_play_feedback
        )
        reddit_feedback = _merge_feedback_lists(reddit_feedback, extra_reddit_feedback)
        app_store_feedback = _merge_feedback_lists(
            app_store_feedback, extra_app_store_feedback
        )
        google_play_warnings.extend(extra_google_play_warnings)
        reddit_warnings.extend(extra_reddit_warnings)
        app_store_warnings.extend(extra_app_store_warnings)
        collected_feedback = reddit_feedback + google_play_feedback + app_store_feedback
        cleaned_feedback = clean_feedback_items(collected_feedback)
        in_window_feedback, out_of_window_feedback = _split_feedback_by_time_window(
            cleaned_feedback,
            request,
        )
        relevant_feedback, relevance_debug_notes = filter_relevant_feedback(
            in_window_feedback,
            request,
            debug=request.debug,
        )

    logger.info("relevance filtering completed records=%s", len(relevant_feedback))
    _emit_status(
        status_callback,
        "deduplicating_feedback",
        68,
        "Removing exact and near duplicates while preserving duplicate pressure metrics.",
    )
    logger.info("deduplication started records=%s", len(relevant_feedback))
    deduped_feedback, dedupe_debug_notes, dedupe_stats = deduplicate_feedback_items(
        relevant_feedback,
        debug=request.debug,
    )
    logger.info("deduplication completed records=%s", len(deduped_feedback))
    _emit_status(
        status_callback,
        "clustering_feedback",
        80,
        "Grouping related feedback into clusters and traceability structures.",
    )
    logger.info("clustering started records=%s", len(deduped_feedback))
    clusters, clustering_debug_notes = cluster_feedback_items(
        deduped_feedback,
        debug=request.debug,
    )
    clusters = _annotate_clusters(
        clusters,
        request.research_questions,
        request.success_criteria,
    )
    logger.info("clustering completed clusters=%s", len(clusters))

    total_collected = len(collected_feedback)
    total_cleaned = len(cleaned_feedback)
    total_in_window = len(in_window_feedback)
    total_out_of_window = len(out_of_window_feedback)
    total_relevant = len(relevant_feedback)
    total_deduped = len(deduped_feedback)
    rating_distribution = _build_rating_distribution(deduped_feedback)
    feedback_over_time = _build_feedback_over_time(deduped_feedback)
    source_distribution = _build_source_distribution(deduped_feedback)
    contamination_warnings = _build_source_contamination_warnings(
        in_window_feedback,
        request,
    )
    source_failures = _build_source_failures(
        google_play_feedback=google_play_feedback,
        google_play_warnings=google_play_warnings,
        reddit_feedback=reddit_feedback,
        reddit_warnings=reddit_warnings,
        app_store_feedback=app_store_feedback,
        app_store_warnings=app_store_warnings,
    )
    source_warning_codes = _build_source_warning_codes(
        google_play_feedback=google_play_feedback,
        google_play_warnings=google_play_warnings,
        reddit_feedback=reddit_feedback,
        reddit_warnings=reddit_warnings,
        app_store_feedback=app_store_feedback,
        app_store_warnings=app_store_warnings,
    )
    quality_diagnostics = _build_quality_diagnostics(
        total_collected=total_collected,
        in_window_records=total_in_window,
        out_of_window_records=total_out_of_window,
        relevant_records=total_relevant,
        deduped_records=total_deduped,
        clusters=clusters,
        contamination_warnings=contamination_warnings,
        expanded_collection_applied=expanded_collection_applied,
        expanded_collection_reason=expanded_collection_reason,
    )

    fallback_quote = QuoteItem(
        text="Source collection is active, but no quote-ready records were available.",
        source="reddit",
        url="https://www.reddit.com",
        date="2026-01-15T00:00:00Z",
    )
    if not clusters:
        clusters = [
            ClusterItem(
                cluster_id="cluster_001",
                cluster_name="No relevant feedback",
                cluster_summary="No cluster could be formed because no relevant deduplicated records remained.",
                cluster_size=0,
                cluster_cohesion_score=0.0,
                frequency=0,
                dominant_signal="mixed",
                pain_point_evidence_count=0,
                positive_validation_count=0,
                request_signal_count=0,
                mixed_signal_flag=False,
                source_distribution={"reddit": 0, "google_play": 0, "app_store": 0},
                time_distribution={},
                representative_quotes=[fallback_quote],
                example_feedback_ids=[],
                keywords=["no evidence"],
                mapped_research_questions=request.research_questions[:2],
                mapped_success_criteria=request.success_criteria[:2],
                repeat_listening_cause_tags=[],
                relevance_score=0.0,
            )
        ]
    representative_quotes = _build_top_level_representative_quotes(
        clusters,
        deduped_feedback,
    )
    dominant_signal_distribution = _build_cluster_signal_distribution(clusters)

    relevant_counts_by_source = _build_source_distribution(relevant_feedback)
    source_summary = [
        SourceSummaryItem(
            source_name="Reddit",
            source_type="discussion",
            queries_used=reddit_queries,
            records_collected=len(reddit_feedback),
            records_relevant=relevant_counts_by_source["reddit"],
            date_range=_derive_date_range(reddit_feedback),
            notes=(
                "Reddit collection is active in Phase 3 as a qualitative depth source using public RSS search queries derived from the locked brief. "
                f"Phase 5 applies cleaning and scope relevance filtering. {relevant_counts_by_source['reddit']} relevant records currently contribute across {sum(1 for cluster in clusters if cluster.source_distribution['reddit'] > 0)} clusters."
            ),
        ),
        SourceSummaryItem(
            source_name="Google Play",
            source_type="review",
            queries_used=[request.product],
            records_collected=len(google_play_feedback),
            records_relevant=relevant_counts_by_source["google_play"],
            date_range=_derive_date_range(google_play_feedback),
            notes=(
                (
                    f"Google Play country filter: {request.country}. "
                    if request.country
                    else "Google Play country filter: none explicitly requested. "
                )
                +
                f"Google Play collection is active in Phase 2 with continuation-based paging until the requested time window is exhausted or the background job hits a safety page budget of {collection_plan['google_play_max_pages']} pages for this run. Phase 5 applies cleaning and "
                f"scope relevance filtering before records count as relevant. {relevant_counts_by_source['google_play']} relevant records currently contribute across {sum(1 for cluster in clusters if cluster.source_distribution['google_play'] > 0)} clusters."
            ),
        ),
        SourceSummaryItem(
            source_name="App Store",
            source_type="review",
            queries_used=[request.product],
            records_collected=len(app_store_feedback),
            records_relevant=relevant_counts_by_source["app_store"],
            date_range=_derive_date_range(app_store_feedback),
            notes=(
                (
                    f"App Store storefront filter: {request.country}. "
                    if request.country
                    else "App Store storefront filter: no explicit country was requested, so the public RSS fallback storefront is US. "
                )
                +
                f"App Store collection is active in Phase 4 using paginated public customer reviews RSS pages until the requested time window is exhausted or the background job hits a safety page budget of {collection_plan['app_store_max_pages']} pages for this run. "
                f"Phase 5 applies cleaning and scope relevance filtering. {relevant_counts_by_source['app_store']} relevant records currently contribute across {sum(1 for cluster in clusters if cluster.source_distribution['app_store'] > 0)} clusters."
            ),
        ),
    ]
    source_limitations = _build_source_limitations(
        google_play_warnings=google_play_warnings,
        reddit_warnings=reddit_warnings,
        app_store_warnings=app_store_warnings,
        contamination_warnings=contamination_warnings,
        time_window_violations=total_out_of_window,
    )

    metrics = MetricsPayload(
        total_records_collected=total_collected,
        records_after_cleaning=total_cleaned,
        records_relevant=total_relevant,
        records_after_deduplication=total_deduped,
        exact_duplicates_removed=dedupe_stats["exact_duplicates_removed"],
        normalized_duplicates_removed=dedupe_stats["normalized_duplicates_removed"],
        near_duplicates_removed=dedupe_stats["near_duplicates_removed"],
        cluster_count=len([cluster for cluster in clusters if cluster.frequency > 0]),
        dominant_signal_distribution=dominant_signal_distribution,
        source_distribution=source_distribution,
        rating_distribution=rating_distribution,
        top_clusters=[
            TopClusterMetric(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster.cluster_name,
                frequency=cluster.frequency,
            )
            for cluster in clusters[:5]
        ],
    )
    research_question_coverage = _build_research_question_coverage(
        request.research_questions,
        clusters,
        total_relevant,
    )

    charts_data = ChartsDataPayload(
        feedback_by_source=[
            ChartValueBySource(source="reddit", count=source_distribution["reddit"]),
            ChartValueBySource(source="google_play", count=source_distribution["google_play"]),
            ChartValueBySource(source="app_store", count=source_distribution["app_store"]),
        ],
        feedback_over_time=feedback_over_time,
        top_clusters=[
            ChartClusterValue(
                cluster_name=cluster.cluster_name,
                frequency=cluster.frequency,
            )
            for cluster in clusters[:5]
        ],
        rating_distribution=[
            ChartRatingValue(rating=rating, count=count)
            for rating, count in rating_distribution.items()
        ],
        source_distribution_by_cluster=[
            SourceDistributionByCluster(
                cluster_name=cluster.cluster_name,
                reddit=cluster.source_distribution["reddit"],
                google_play=cluster.source_distribution["google_play"],
                app_store=cluster.source_distribution["app_store"],
            )
            for cluster in clusters[:5]
        ],
        cluster_signal_distribution=[
            ClusterSignalValue(signal=signal, count=count)
            for signal, count in dominant_signal_distribution.items()
        ],
    )

    warnings: list[str] = (
        _prefix_warning_codes("reddit", source_warning_codes.get("reddit", []), reddit_warnings)
        + _prefix_warning_codes("google_play", source_warning_codes.get("google_play", []), google_play_warnings)
        + _prefix_warning_codes("app_store", source_warning_codes.get("app_store", []), app_store_warnings)
    )
    if collection_plan["fast_mode"]:
        warnings.append(
            "Runtime-aware source safety budgets were applied for this interactive run to keep response time within the requested budget: "
            f"google_play_pages={collection_plan['google_play_max_pages']}, "
            f"app_store_pages={collection_plan['app_store_max_pages']}."
        )
    if not request.included_topics:
        warnings.append(
            "included_topics is empty; continuing with broader scope and reduced precision."
        )
    if not request.excluded_topics:
        warnings.append(
            "excluded_topics is empty; continuing without explicit exclusions."
        )
    if expanded_collection_applied and expanded_collection_reason:
        warnings.append(expanded_collection_reason)
    if total_out_of_window:
        warnings.append(
            f"{total_out_of_window} record(s) were excluded from analysis because they fell outside the requested time window."
        )
    warnings.extend(contamination_warnings)
    warnings.extend(
        limitation.limitation
        for limitation in source_limitations
        if limitation.limitation not in warnings
    )
    warnings.extend(_build_research_question_gap_warnings(research_question_coverage))

    processing_notes = [
        "Google Play collection is active in Phase 2.",
        "Reddit collection is active in Phase 3 as a depth source for nuance, workarounds, and behavioral context.",
        "App Store collection is active in Phase 4 using paginated public customer reviews RSS pages.",
        "Phase 5 applies cleaning and rule-based relevance filtering.",
        "Phase 5 now enforces requested time-window purity before relevance scoring and chart generation.",
        "Phase 6 applies exact, normalized, and near-duplicate removal before downstream counting.",
        "Near duplicates are collapsed for analysis hygiene, but duplicate-pressure counts are preserved in the response so repeated dissatisfaction is not lost.",
        "Phase 7 applies semantic TF-IDF clustering plus cluster-merge logic so related records group together instead of defaulting to one cluster per record.",
        "Phase 8 aligns metrics, source summaries, and chart-ready JSON to real clusters and cluster signal distributions.",
        "Collection now continues until a source is exhausted or a source-level cap is reached; public-source indexing and rate limits may still constrain full coverage.",
        "Research-question coverage is computed so the final report can explicitly answer the brief instead of only summarizing clusters.",
    ]
    if collection_plan["fast_mode"]:
        processing_notes.append(
            "Runtime-aware source caps were applied for this interactive run and disclosed in warnings/source summaries to avoid silent sampling."
        )
    if request.debug:
        processing_notes.extend(relevance_debug_notes[:20])
        processing_notes.extend(dedupe_debug_notes[:20])
        processing_notes.extend(clustering_debug_notes[:20])

    tiered_clusters = _build_cluster_tiers(clusters)
    opportunities = _build_opportunities(
        clusters,
        request.research_questions,
        request.success_criteria,
        request.research_scope,
    )
    evidence_backed_segments = _build_evidence_backed_segments(
        clusters,
        total_relevant,
    )
    response_clusters, cluster_compaction_applied = _compact_clusters_for_response(
        tiered_clusters["tier_1"]
    )
    response_opportunities = _compact_opportunities_for_response(
        opportunities[: settings.compact_payload_top_opportunities]
    )
    response_segments = _compact_segments_for_response(
        evidence_backed_segments[: settings.compact_payload_top_segments]
    )
    response_quotes = _compact_quotes_for_response(
        representative_quotes[: settings.response_max_top_level_quotes] or [fallback_quote]
    )
    processing_notes = processing_notes[: settings.response_max_processing_notes]
    if cluster_compaction_applied:
        warnings.append(
            "Transport-safe response compaction was applied for this run: only the top "
            f"{settings.compact_payload_top_clusters} clusters were returned in the response body, while metrics and charts still reflect the full analyzed result set."
        )
        processing_notes.append(
            "Transport-safe response compaction trimmed cluster payload size for the Action response without changing the underlying metrics, charts, or source summaries."
        )

    compact_metrics = _build_compact_metrics(metrics)
    compact_charts_summary = _build_compact_charts_summary(charts_data)
    compact_artifact_manifest = ArtifactManifest(run_id=run_id, artifacts=[])
    compact_gpt_payload = CompactGPTPayload(
        locked_brief=request.model_dump(),
        success_criteria=request.success_criteria,
        source_summary=source_summary,
        processing_summary=ProcessingSummary(
            records_collected=total_collected,
            records_after_cleaning=total_cleaned,
            records_relevant=total_relevant,
            records_after_deduplication=total_deduped,
            exact_duplicates_removed=dedupe_stats["exact_duplicates_removed"],
            normalized_duplicates_removed=dedupe_stats["normalized_duplicates_removed"],
            near_duplicates_removed=dedupe_stats["near_duplicates_removed"],
            source_failures=source_failures,
            source_warning_codes=source_warning_codes,
            expanded_collection_applied=expanded_collection_applied,
            expanded_collection_reason=expanded_collection_reason,
        ),
        quality_diagnostics=quality_diagnostics,
        research_question_coverage=_compact_research_question_coverage(
            research_question_coverage
        ),
        top_clusters=response_clusters,
        top_opportunities=response_opportunities,
        top_metrics=compact_metrics,
        charts_data_summary=compact_charts_summary,
        representative_quotes=response_quotes,
        opportunity_traceability_summary=_build_opportunity_traceability_summary(
            response_opportunities
        ),
        success_criteria_impact_summary=_build_success_criteria_impact_summary(
            response_opportunities
        ),
        evidence_backed_segments=response_segments,
        brief_alignment_summary=_build_brief_alignment_summary(response_opportunities),
        source_limitations=[item.limitation for item in source_limitations],
        artifact_manifest=compact_artifact_manifest,
    )
    _emit_status(
        status_callback,
        "writing_artifacts",
        92,
        "Writing compact and deep evidence artifacts for GPT retrieval.",
    )
    _write_run_artifacts(
        run_id=run_id,
        raw_feedback=collected_feedback,
        clean_feedback=cleaned_feedback,
        time_window_value=request.analysis_time_window.value,
        all_clusters=clusters,
        source_summary=source_summary,
        charts_data=charts_data,
        compact_gpt_payload=compact_gpt_payload,
        quality_diagnostics=quality_diagnostics,
        research_question_coverage=research_question_coverage,
        opportunities=opportunities,
        evidence_backed_segments=evidence_backed_segments,
        processing_notes=processing_notes,
        warnings=warnings,
        source_limitations=source_limitations,
        tiered_clusters=tiered_clusters,
    )
    artifact_manifest = build_artifact_manifest(run_id)

    final_status = "partial_success" if source_failures else "completed"
    _emit_status(
        status_callback,
        "completed",
        100,
        "Analysis completed and artifacts are ready.",
    )
    logger.info(
        "analyze_feedback completed run_id=%s status=%s elapsed_seconds=%.2f total_collected=%s relevant=%s deduped=%s clusters=%s",
        run_id,
        final_status,
        perf_counter() - run_started_at,
        total_collected,
        total_relevant,
        total_deduped,
        len(clusters),
    )
    return AnalyzeFeedbackResponse(
        run_id=run_id,
        status=final_status,
        locked_brief=request.model_dump(),
        source_summary=source_summary,
        processing_summary=ProcessingSummary(
            records_collected=total_collected,
            records_after_cleaning=total_cleaned,
            records_relevant=total_relevant,
            records_after_deduplication=total_deduped,
            exact_duplicates_removed=dedupe_stats["exact_duplicates_removed"],
            normalized_duplicates_removed=dedupe_stats["normalized_duplicates_removed"],
            near_duplicates_removed=dedupe_stats["near_duplicates_removed"],
            source_failures=source_failures,
            source_warning_codes=source_warning_codes,
            expanded_collection_applied=expanded_collection_applied,
            expanded_collection_reason=expanded_collection_reason,
        ),
        quality_diagnostics=quality_diagnostics,
        research_question_coverage=[],
        feedback_clusters=response_clusters[: settings.response_top_level_clusters],
        opportunities=response_opportunities[: settings.response_top_level_opportunities],
        evidence_backed_segments=response_segments[: settings.response_top_level_segments],
        metrics=metrics,
        charts_data=charts_data,
        representative_quotes=[],
        compact_gpt_payload=compact_gpt_payload,
        artifact_manifest=artifact_manifest,
        processing_notes=processing_notes,
        source_limitations=source_limitations,
        warnings=warnings,
    )


def _emit_status(
    status_callback: StatusCallback | None,
    stage: str,
    progress_percent: int,
    message: str,
) -> None:
    if status_callback is not None:
        status_callback(stage, progress_percent, message)


def _collect_google_play_feedback(
    *,
    time_window_value: str,
    max_pages: int,
    country: str | None,
) -> tuple[list[RawFeedbackItem], list[str]]:
    try:
        return _call_google_play_collector(time_window_value, max_pages, country), []
    except Exception as exc:
        warning = (
            "Google Play collection failed; returning contract-compatible response with no Google Play records. "
            f"Reason: {exc}"
        )
        return [], [warning]


def _collect_reddit_feedback(
    queries: list[str],
    *,
    collection_plan: dict[str, int | float | bool],
) -> tuple[list[RawFeedbackItem], list[str]]:
    try:
        try:
            result = collect_reddit_feedback(
                queries,
                limit=int(collection_plan["reddit_result_limit"]),
                query_delay_seconds=float(collection_plan["reddit_query_delay_seconds"]),
                max_retries=int(collection_plan["reddit_max_retries"]),
                backoff_seconds=float(collection_plan["reddit_backoff_seconds"]),
                max_total_seconds=float(collection_plan["reddit_max_total_seconds"]),
            )
        except TypeError:
            # Backward compatibility for tests or shims that monkeypatch the older single-arg signature.
            result = collect_reddit_feedback(queries)
        if isinstance(result, tuple):
            return result
        return result, []
    except Exception as exc:
        warning = (
            "Reddit collection failed; returning contract-compatible response with no Reddit records. "
            f"Reason: {exc}"
        )
        return [], [warning]


def _collect_app_store_feedback(
    *,
    time_window_value: str,
    max_pages: int,
    country: str | None,
) -> tuple[list[RawFeedbackItem], list[str]]:
    try:
        records = _call_app_store_collector(time_window_value, max_pages, country)
        return records, []
    except Exception as exc:
        warning = (
            "App Store collection failed; returning contract-compatible response with no App Store records. "
            f"Reason: {exc}"
        )
        return [], [warning]


def _collect_all_sources(
    *,
    collection_plan: dict[str, int | float | bool | str],
    reddit_queries: list[str],
    include_reddit: bool = True,
) -> tuple[
    list[RawFeedbackItem],
    list[str],
    list[RawFeedbackItem],
    list[str],
    list[RawFeedbackItem],
    list[str],
]:
    collection_started_at = perf_counter()
    executor = ThreadPoolExecutor(max_workers=3)
    google_play_future = executor.submit(
        _collect_google_play_feedback,
        time_window_value=str(collection_plan["time_window_value"]),
        max_pages=int(collection_plan["google_play_max_pages"]),
        country=str(collection_plan["country"]) if collection_plan["country"] else None,
    )
    reddit_future = (
        executor.submit(
            _collect_reddit_feedback,
            reddit_queries,
            collection_plan=collection_plan,
        )
        if include_reddit
        else None
    )
    app_store_future = executor.submit(
        _collect_app_store_feedback,
        time_window_value=str(collection_plan["time_window_value"]),
        max_pages=int(collection_plan["app_store_max_pages"]),
        country=str(collection_plan["country"]) if collection_plan["country"] else None,
    )

    google_play_feedback, google_play_warnings = _await_collection_future(
        future=google_play_future,
        source_name="Google Play",
        timeout_seconds=float(collection_plan["google_play_timeout_seconds"]),
    )
    if reddit_future is not None:
        reddit_feedback, reddit_warnings = _await_collection_future(
            future=reddit_future,
            source_name="Reddit",
            timeout_seconds=float(collection_plan["reddit_timeout_seconds"]),
        )
    else:
        reddit_feedback, reddit_warnings = [], []
    app_store_feedback, app_store_warnings = _await_collection_future(
        future=app_store_future,
        source_name="App Store",
        timeout_seconds=float(collection_plan["app_store_timeout_seconds"]),
    )
    executor.shutdown(wait=False, cancel_futures=True)

    logger.info(
        "collection completed elapsed_seconds=%.2f google_play=%s reddit=%s app_store=%s",
        perf_counter() - collection_started_at,
        len(google_play_feedback),
        len(reddit_feedback),
        len(app_store_feedback),
    )
    return (
        google_play_feedback,
        google_play_warnings,
        reddit_feedback,
        reddit_warnings,
        app_store_feedback,
        app_store_warnings,
    )


def _await_collection_future(
    *,
    future: Future[tuple[list[RawFeedbackItem], list[str]]],
    source_name: str,
    timeout_seconds: float,
) -> tuple[list[RawFeedbackItem], list[str]]:
    try:
        return future.result(timeout=timeout_seconds)
    except TimeoutError:
        warning = (
            f"{source_name} collection timed out after {timeout_seconds:.0f}s; "
            "continuing with partial results from the other sources."
        )
        logger.warning(
            "%s collection_timeout timeout_seconds=%.2f",
            source_name.lower().replace(" ", "_"),
            timeout_seconds,
        )
        return [], [warning]


def _call_google_play_collector(
    time_window_value: str,
    max_pages: int,
    country: str | None,
) -> list[RawFeedbackItem]:
    try:
        return collect_google_play_reviews(
            app_id=DEFAULT_GOOGLE_PLAY_APP_ID,
            country=country,
            count=None,
            time_window_value=time_window_value,
            max_pages=max_pages,
        )
    except TypeError:
        # Backward compatibility for tests or shims that monkeypatch the older single-arg signature.
        return collect_google_play_reviews(DEFAULT_GOOGLE_PLAY_APP_ID)


def _call_app_store_collector(
    time_window_value: str,
    max_pages: int,
    country: str | None,
) -> list[RawFeedbackItem]:
    try:
        return collect_app_store_reviews(
            app_id=DEFAULT_APP_STORE_APP_ID,
            country=country,
            limit=None,
            max_pages=max_pages,
            time_window_value=time_window_value,
        )
    except TypeError:
        # Backward compatibility for tests or shims that monkeypatch the older single-arg signature.
        return collect_app_store_reviews(DEFAULT_APP_STORE_APP_ID)


def _build_collection_plan(
    request: AnalyzeFeedbackRequest,
) -> dict[str, int | float | bool | str]:
    fast_mode = request.max_runtime_seconds <= 120
    months_equivalent = relative_window_months_equivalent(
        request.analysis_time_window.value
    )
    google_play_max_pages = _estimate_store_page_cap(
        months_equivalent=months_equivalent,
        hard_cap=settings.google_play_page_safety_cap,
        minimum_pages=10,
        pages_per_month=15,
    )
    app_store_max_pages = _estimate_store_page_cap(
        months_equivalent=months_equivalent,
        hard_cap=settings.app_store_page_safety_cap,
        minimum_pages=8,
        pages_per_month=10,
    )
    google_play_timeout_seconds = min(
        900.0,
        max(90.0, request.max_runtime_seconds * 0.50),
    )
    app_store_timeout_seconds = min(
        720.0,
        max(90.0, request.max_runtime_seconds * 0.40),
    )
    reddit_result_limit = (
        settings.fast_mode_reddit_result_limit
        if fast_mode
        else settings.full_mode_reddit_result_limit
    )
    reddit_query_delay_seconds = (
        settings.fast_mode_reddit_query_delay_seconds
        if fast_mode
        else settings.reddit_query_delay_seconds
    )
    reddit_max_retries = (
        settings.fast_mode_reddit_max_retries
        if fast_mode
        else settings.reddit_max_retries
    )
    reddit_backoff_seconds = (
        settings.fast_mode_reddit_backoff_seconds
        if fast_mode
        else settings.reddit_backoff_seconds
    )
    reddit_max_total_seconds = (
        settings.fast_mode_reddit_max_total_seconds
        if fast_mode
        else settings.full_mode_reddit_max_total_seconds
    )

    return {
        "fast_mode": fast_mode,
        "time_window_value": request.analysis_time_window.value,
        "country": request.country,
        "google_play_max_pages": google_play_max_pages,
        "app_store_max_pages": app_store_max_pages,
        "google_play_timeout_seconds": google_play_timeout_seconds,
        "app_store_timeout_seconds": app_store_timeout_seconds,
        "reddit_query_count": (
            settings.fast_mode_reddit_max_queries
            if fast_mode
            else settings.reddit_max_queries_per_run
        ),
        "reddit_result_limit": reddit_result_limit,
        "reddit_query_delay_seconds": reddit_query_delay_seconds,
        "reddit_max_retries": reddit_max_retries,
        "reddit_backoff_seconds": reddit_backoff_seconds,
        "reddit_timeout_seconds": reddit_max_total_seconds + 5.0,
        "reddit_max_total_seconds": reddit_max_total_seconds,
    }


def _estimate_store_page_cap(
    *,
    months_equivalent: float,
    hard_cap: int,
    minimum_pages: int,
    pages_per_month: int,
) -> int:
    estimated_pages = minimum_pages + int(math.ceil(months_equivalent * pages_per_month))
    return max(minimum_pages, min(hard_cap, estimated_pages))


def _build_expanded_collection_plan(
    collection_plan: dict[str, int | float | bool | str],
) -> dict[str, int | float | bool | str]:
    expanded_google_play_pages = min(
        settings.google_play_page_safety_cap,
        max(int(collection_plan["google_play_max_pages"]), 80),
    )
    expanded_app_store_pages = min(
        settings.app_store_page_safety_cap,
        max(int(collection_plan["app_store_max_pages"]), 60),
    )
    expanded = deepcopy(collection_plan)
    expanded["google_play_max_pages"] = expanded_google_play_pages
    expanded["app_store_max_pages"] = expanded_app_store_pages
    return expanded


def _should_expand_collection(
    *,
    collection_plan: dict[str, int | float | bool | str],
    relevant_feedback: list[RawFeedbackItem],
) -> bool:
    current_pages = int(collection_plan["google_play_max_pages"]) + int(
        collection_plan["app_store_max_pages"]
    )
    max_pages = settings.google_play_page_safety_cap + settings.app_store_page_safety_cap
    return (
        len(relevant_feedback) < settings.low_relevant_record_expansion_threshold
        and current_pages < max_pages
    )


def _merge_feedback_lists(
    baseline: list[RawFeedbackItem],
    incoming: list[RawFeedbackItem],
) -> list[RawFeedbackItem]:
    seen = {item.feedback_id for item in baseline}
    merged = baseline[:]
    for item in incoming:
        if item.feedback_id in seen:
            continue
        seen.add(item.feedback_id)
        merged.append(item)
    return merged


def _split_feedback_by_time_window(
    feedback_items: list[RawFeedbackItem],
    request: AnalyzeFeedbackRequest,
) -> tuple[list[RawFeedbackItem], list[RawFeedbackItem]]:
    in_window: list[RawFeedbackItem] = []
    out_of_window: list[RawFeedbackItem] = []
    for item in feedback_items:
        if is_within_relative_window(item.date, request.analysis_time_window.value):
            in_window.append(item)
        else:
            out_of_window.append(item)
    return in_window, out_of_window


def _compact_clusters_for_response(
    clusters: list[ClusterItem],
) -> tuple[list[ClusterItem], bool]:
    limited_clusters = clusters[: settings.compact_payload_top_clusters]
    compacted_clusters = [_compact_cluster(cluster) for cluster in limited_clusters]
    return compacted_clusters, len(clusters) > len(limited_clusters)


def _compact_research_question_coverage(
    coverage_items: list[ResearchQuestionCoverage],
) -> list[ResearchQuestionCoverage]:
    compacted: list[ResearchQuestionCoverage] = []
    for item in coverage_items:
        compacted.append(
            ResearchQuestionCoverage(
                question_id=item.question_id,
                question=item.question,
                evidence_strength=item.evidence_strength,
                relevant_cluster_ids=item.relevant_cluster_ids[:6],
                source_coverage=item.source_coverage,
                record_count=item.record_count,
                summary=item.summary,
                evidence_gaps=item.evidence_gaps[:3],
            )
        )
    return compacted


def _compact_opportunities_for_response(
    opportunities: list[OpportunityItem],
) -> list[OpportunityItem]:
    compacted: list[OpportunityItem] = []
    for item in opportunities:
        compacted.append(
            OpportunityItem(
                opportunity_id=item.opportunity_id,
                opportunity_name=item.opportunity_name,
                opportunity_statement=item.opportunity_statement,
                derived_from_cluster_id=item.derived_from_cluster_id,
                dominant_signal=item.dominant_signal,
                frequency=item.frequency,
                source_distribution=item.source_distribution,
                supporting_cluster_ids=item.supporting_cluster_ids[:4],
                supporting_research_questions=item.supporting_research_questions[:3],
                success_criteria_impact=item.success_criteria_impact[:3],
                brief_alignment_score=item.brief_alignment_score,
                brief_alignment_rationale=item.brief_alignment_rationale,
                representative_quotes=[],
                top_pain_points=item.top_pain_points[:3],
            )
        )
    return compacted


def _compact_segments_for_response(
    segments: list[EvidenceBackedSegment],
) -> list[EvidenceBackedSegment]:
    compacted: list[EvidenceBackedSegment] = []
    for item in segments:
        compacted.append(
            EvidenceBackedSegment(
                segment_name=item.segment_name,
                description=item.description,
                estimated_record_count=item.estimated_record_count,
                percentage_of_relevant_records=item.percentage_of_relevant_records,
                source_distribution=item.source_distribution,
                supporting_cluster_ids=item.supporting_cluster_ids[:4],
                representative_quotes=_compact_quotes_for_response(
                    item.representative_quotes[:1]
                ),
                primary_JTBDs=item.primary_JTBDs[:2],
                top_pain_points=item.top_pain_points[:3],
                confidence_level=item.confidence_level,
                confidence_rationale=item.confidence_rationale,
            )
        )
    return compacted


def _compact_cluster(cluster: ClusterItem) -> ClusterItem:
    return ClusterItem(
        cluster_id=cluster.cluster_id,
        cluster_name=cluster.cluster_name,
        cluster_summary=cluster.cluster_summary,
        cluster_tier=cluster.cluster_tier,
        cluster_size=cluster.cluster_size,
        cluster_cohesion_score=cluster.cluster_cohesion_score,
        frequency=cluster.frequency,
        dominant_signal=cluster.dominant_signal,
        pain_point_evidence_count=cluster.pain_point_evidence_count,
        positive_validation_count=cluster.positive_validation_count,
        request_signal_count=cluster.request_signal_count,
        mixed_signal_flag=cluster.mixed_signal_flag,
        source_distribution=cluster.source_distribution,
        time_distribution=cluster.time_distribution,
        representative_quotes=_compact_quotes_for_response(
            cluster.representative_quotes[: settings.response_max_cluster_quotes]
        ),
        example_feedback_ids=cluster.example_feedback_ids[
            : settings.response_max_example_feedback_ids
        ],
        keywords=cluster.keywords,
        mapped_research_questions=cluster.mapped_research_questions[:3],
        mapped_success_criteria=cluster.mapped_success_criteria[:3],
        repeat_listening_cause_tags=cluster.repeat_listening_cause_tags[:3],
        relevance_score=cluster.relevance_score,
    )


def _compact_quotes_for_response(quotes: list[QuoteItem]) -> list[QuoteItem]:
    return [
        QuoteItem(
            text=_truncate_text(quote.text, settings.response_max_quote_chars),
            source=quote.source,
            url=quote.url,
            date=quote.date,
        )
        for quote in quotes
    ]


def _truncate_text(text: str, max_chars: int) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= max_chars:
        return normalized
    sentence_candidates = [
        sentence.strip()
        for sentence in normalized.replace("!", ".").replace("?", ".").split(".")
        if sentence.strip()
    ]
    short_complete = [
        f"{sentence}."
        for sentence in sentence_candidates
        if len(sentence) + 1 <= max_chars
    ]
    if short_complete:
        return min(short_complete, key=len)
    if max_chars <= 3:
        return normalized[:max_chars]
    return normalized[: max_chars - 3].rstrip() + "..."


def _annotate_clusters(
    clusters: list[ClusterItem],
    research_questions: list[str],
    success_criteria: list[str],
) -> list[ClusterItem]:
    annotated: list[ClusterItem] = []
    for cluster in clusters:
        cluster.repeat_listening_cause_tags = _derive_repeat_listening_cause_tags(cluster)
        cluster.mapped_research_questions = _map_cluster_to_research_questions(
            cluster,
            research_questions,
        )
        cluster.mapped_success_criteria = _map_cluster_to_success_criteria(
            cluster,
            success_criteria,
        )
        annotated.append(cluster)
    return annotated


def _build_cluster_tiers(clusters: list[ClusterItem]) -> dict[str, list[ClusterItem]]:
    tier_1_cutoff = settings.compact_payload_top_clusters
    tier_2_cutoff = settings.compact_payload_tier_2_limit

    tier_1 = []
    tier_2 = []
    tier_3 = []
    for index, cluster in enumerate(clusters):
        if index < tier_1_cutoff:
            cluster.cluster_tier = "tier_1"
            tier_1.append(cluster)
        elif index < tier_2_cutoff:
            cluster.cluster_tier = "tier_2"
            tier_2.append(cluster)
        else:
            cluster.cluster_tier = "tier_3"
            tier_3.append(cluster)
    return {"tier_1": tier_1, "tier_2": tier_2, "tier_3": tier_3}


def _map_cluster_to_research_questions(
    cluster: ClusterItem,
    research_questions: list[str],
) -> list[str]:
    cluster_text = " ".join(
        [
            cluster.cluster_name.lower(),
            cluster.cluster_summary.lower(),
            " ".join(cluster.keywords).lower(),
            " ".join(cluster.repeat_listening_cause_tags).lower(),
        ]
    )
    mappings: list[str] = []
    for question in research_questions:
        question_text = question.lower()
        if "discover new music" in question_text and any(
            term in cluster_text for term in ["discover", "novelty", "new music", "release radar"]
        ):
            mappings.append(question)
        elif "frustrations with recommendations" in question_text and any(
            term in cluster_text for term in ["recommend", "repet", "trust", "control"]
        ):
            mappings.append(question)
        elif "listening behaviors" in question_text and any(
            term in cluster_text for term in ["behavior", "playlist", "mood", "context", "control"]
        ):
            mappings.append(question)
        elif "repetitive listening" in question_text and cluster.repeat_listening_cause_tags:
            mappings.append(question)
        elif "user segments" in question_text and any(
            term in cluster_text for term in ["segment", "mood", "context", "control", "behavior"]
        ):
            mappings.append(question)
        elif "unmet needs" in question_text and any(
            term in cluster_text for term in ["need", "control", "novelty", "trust", "context"]
        ):
            mappings.append(question)

    if not mappings and research_questions:
        mappings.append(research_questions[0])
    return mappings


def _derive_repeat_listening_cause_tags(cluster: ClusterItem) -> list[str]:
    text = " ".join(
        [
            cluster.cluster_name.lower(),
            cluster.cluster_summary.lower(),
            " ".join(cluster.keywords).lower(),
            " ".join(quote.text.lower() for quote in cluster.representative_quotes),
        ]
    )
    tags: list[str] = []
    rule_map = {
        "algorithmic repetition": ["same artists", "same songs", "repet", "algorithm"],
        "over-personalization": ["personalization", "too tailored", "familiar"],
        "user comfort/familiarity": ["comfort", "familiar", "safe choice"],
        "weak novelty controls": ["more control", "novelty", "fresh", "new artists"],
        "lack of context/mood awareness": ["mood", "context", "situation"],
        "low trust in recommendations": ["trust", "bad recommendations", "irrelevant"],
        "playlist loop behavior": ["playlist", "loop", "same playlist"],
    }
    for tag, triggers in rule_map.items():
        if any(trigger in text for trigger in triggers):
            tags.append(tag)
    return tags


def _map_cluster_to_success_criteria(
    cluster: ClusterItem,
    success_criteria: list[str],
) -> list[str]:
    cluster_text = " ".join(
        [
            cluster.cluster_name.lower(),
            cluster.cluster_summary.lower(),
            " ".join(cluster.keywords).lower(),
            " ".join(cluster.repeat_listening_cause_tags).lower(),
        ]
    )
    mappings: list[str] = []
    for criterion in success_criteria:
        criterion_text = criterion.lower()
        if any(term in criterion_text for term in ["discover", "discovery", "new music"]) and any(
            term in cluster_text for term in ["discover", "novelty", "release radar", "new artists"]
        ):
            mappings.append(criterion)
        elif any(term in criterion_text for term in ["repet", "repeat"]) and any(
            term in cluster_text for term in ["repet", "loop", "familiar", "algorithmic repetition"]
        ):
            mappings.append(criterion)
        elif any(term in criterion_text for term in ["relevance", "novelty", "balance", "recommend"]) and any(
            term in cluster_text for term in ["recommend", "trust", "control", "novelty", "relevance"]
        ):
            mappings.append(criterion)
        elif any(word in cluster_text for word in criterion_text.split()):
            mappings.append(criterion)
    if not mappings and success_criteria:
        mappings.append(success_criteria[0])
    return mappings


def _build_research_question_coverage(
    research_questions: list[str],
    clusters: list[ClusterItem],
    total_relevant: int,
) -> list[ResearchQuestionCoverage]:
    coverage_items: list[ResearchQuestionCoverage] = []
    for index, question in enumerate(research_questions, start=1):
        relevant_clusters = [
            cluster for cluster in clusters if question in cluster.mapped_research_questions
        ]
        source_coverage = {"reddit": 0, "google_play": 0, "app_store": 0}
        record_count = 0
        for cluster in relevant_clusters:
            record_count += cluster.frequency
            for source, count in cluster.source_distribution.items():
                source_coverage[source] = source_coverage.get(source, 0) + count

        if record_count >= max(settings.low_record_count_threshold * 3, total_relevant // 5 if total_relevant else 0):
            evidence_strength = "high"
        elif record_count >= settings.low_record_count_threshold:
            evidence_strength = "medium"
        else:
            evidence_strength = "low"

        summary = _build_research_question_summary(question, relevant_clusters, evidence_strength)
        evidence_gaps = []
        if record_count < settings.low_record_count_threshold:
            evidence_gaps.append("Low relevant record count for this question.")
        if source_coverage["reddit"] == 0:
            evidence_gaps.append("No Reddit depth evidence mapped to this question.")
        if sum(1 for count in source_coverage.values() if count > 0) < 2:
            evidence_gaps.append("Limited cross-source coverage for this question.")

        coverage_items.append(
            ResearchQuestionCoverage(
                question_id=f"rq_{index}",
                question=question,
                evidence_strength=evidence_strength,
                relevant_cluster_ids=[cluster.cluster_id for cluster in relevant_clusters],
                source_coverage=source_coverage,
                record_count=record_count,
                summary=summary,
                evidence_gaps=evidence_gaps,
            )
        )
    return coverage_items


def _build_opportunities(
    clusters: list[ClusterItem],
    research_questions: list[str],
    success_criteria: list[str],
    research_scope: str,
) -> list[OpportunityItem]:
    question_index = {question: f"rq_{index}" for index, question in enumerate(research_questions, start=1)}
    opportunities: list[OpportunityItem] = []
    for index, cluster in enumerate(clusters, start=1):
        if cluster.frequency <= 0:
            continue
        opportunities.append(
            OpportunityItem(
                opportunity_id=f"opp_{index:03d}",
                opportunity_name=_build_opportunity_name(cluster),
                opportunity_statement=_build_opportunity_statement(cluster),
                derived_from_cluster_id=cluster.cluster_id,
                dominant_signal=cluster.dominant_signal,
                frequency=cluster.frequency,
                source_distribution=cluster.source_distribution,
                supporting_cluster_ids=[cluster.cluster_id],
                supporting_research_questions=[
                    SupportingResearchQuestionItem(
                        question_id=question_index.get(question, f"rq_{index}"),
                        question=question,
                        support_level=_support_level_from_cluster(cluster),
                        supporting_cluster_ids=[cluster.cluster_id],
                        evidence_summary=(
                            f"{cluster.cluster_name} directly informs this question through {cluster.frequency} relevant record(s)."
                        ),
                    )
                    for question in cluster.mapped_research_questions
                ],
                success_criteria_impact=[
                    SuccessCriteriaImpactItem(
                        criterion=criterion,
                        impact_level=_impact_level_for_criterion(cluster, criterion),
                        rationale=_criterion_rationale(cluster, criterion),
                        supporting_cluster_ids=[cluster.cluster_id],
                    )
                    for criterion in _criteria_for_cluster(cluster, success_criteria)
                ],
                brief_alignment_score=_brief_alignment_score(cluster, research_scope),
                brief_alignment_rationale=_brief_alignment_rationale(cluster, research_scope),
                representative_quotes=_compact_quotes_for_response(cluster.representative_quotes[:2]),
                top_pain_points=cluster.keywords[:3] or cluster.repeat_listening_cause_tags[:3],
            )
        )
    return opportunities


def _build_evidence_backed_segments(
    clusters: list[ClusterItem],
    total_relevant: int,
) -> list[EvidenceBackedSegment]:
    segments: list[EvidenceBackedSegment] = []
    for cluster in clusters[: min(8, len(clusters))]:
        segment_name = _segment_name_from_cluster(cluster)
        segments.append(
            EvidenceBackedSegment(
                segment_name=segment_name,
                description=(
                    f"Users represented by {cluster.cluster_name.lower()} who share similar discovery challenges and behaviors."
                ),
                estimated_record_count=cluster.frequency,
                percentage_of_relevant_records=round(
                    (cluster.frequency / total_relevant) * 100, 2
                )
                if total_relevant
                else 0.0,
                source_distribution=cluster.source_distribution,
                supporting_cluster_ids=[cluster.cluster_id],
                representative_quotes=_compact_quotes_for_response(cluster.representative_quotes[:2]),
                primary_JTBDs=_primary_jtbds_from_cluster(cluster),
                top_pain_points=cluster.keywords[:3] or cluster.repeat_listening_cause_tags[:3],
                confidence_level=_support_level_from_cluster(cluster),
                confidence_rationale=(
                    f"Confidence is {_support_level_from_cluster(cluster)} because the segment is grounded in {cluster.frequency} relevant record(s) across {sum(1 for count in cluster.source_distribution.values() if count > 0)} source(s)."
                ),
            )
        )
    return segments


def _build_research_question_summary(
    question: str,
    relevant_clusters: list[ClusterItem],
    evidence_strength: str,
) -> str:
    if not relevant_clusters:
        return (
            f"Current evidence is {evidence_strength} for this question because no strong cluster mapping was found."
        )

    top_cluster = relevant_clusters[0]
    return (
        f"Evidence is {evidence_strength} for this question, led by {top_cluster.cluster_name.lower()} "
        f"and {max(0, len(relevant_clusters) - 1)} additional mapped cluster(s)."
    )


def _build_opportunity_name(cluster: ClusterItem) -> str:
    base = cluster.keywords[0].title() if cluster.keywords else cluster.cluster_name
    if cluster.dominant_signal == "pain":
        return f"Improve {base}"
    if cluster.dominant_signal == "mixed":
        return f"Clarify {base} experience"
    return f"Strengthen {base}"


def _build_opportunity_statement(cluster: ClusterItem) -> str:
    primary_need = cluster.keywords[0] if cluster.keywords else "discovery experience"
    return (
        f"Create a better {primary_need} experience for users affected by {cluster.cluster_name.lower()}, "
        "grounded in the evidence signals collected in this run."
    )


def _support_level_from_cluster(cluster: ClusterItem) -> str:
    if cluster.frequency >= 10:
        return "high"
    if cluster.frequency >= 4:
        return "medium"
    return "low"


def _impact_level_for_criterion(cluster: ClusterItem, criterion: str) -> str:
    criterion_text = criterion.lower()
    if criterion in cluster.mapped_success_criteria and (
        cluster.frequency >= 8 or cluster.dominant_signal == "pain"
    ):
        return "high"
    if criterion in cluster.mapped_success_criteria and cluster.frequency >= 3:
        return "medium"
    if any(word in cluster.cluster_summary.lower() for word in criterion_text.split()):
        return "medium"
    return "low"


def _criterion_rationale(cluster: ClusterItem, criterion: str) -> str:
    return (
        f"{cluster.cluster_name} maps to this criterion through {cluster.frequency} relevant record(s) "
        f"and the cluster evidence around {', '.join(cluster.keywords[:2]) or 'user feedback'}."
    )


def _criteria_for_cluster(
    cluster: ClusterItem,
    success_criteria: list[str],
) -> list[str]:
    if cluster.mapped_success_criteria:
        return cluster.mapped_success_criteria
    return success_criteria[:1]


def _brief_alignment_score(cluster: ClusterItem, research_scope: str) -> str:
    score = 0
    if cluster.mapped_research_questions:
        score += 1
    if cluster.mapped_success_criteria:
        score += 1
    if research_scope.lower() in cluster.cluster_summary.lower() or any(
        word in cluster.cluster_summary.lower() for word in research_scope.lower().split()
    ):
        score += 1
    if cluster.frequency >= 8:
        score += 1
    if cluster.dominant_signal == "pain":
        score += 1
    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _brief_alignment_rationale(cluster: ClusterItem, research_scope: str) -> str:
    return (
        f"Alignment is {_brief_alignment_score(cluster, research_scope)} because the cluster supports "
        f"{len(cluster.mapped_research_questions)} research question(s), "
        f"{len(cluster.mapped_success_criteria)} success criteria, and remains relevant to {research_scope}."
    )


def _segment_name_from_cluster(cluster: ClusterItem) -> str:
    if cluster.repeat_listening_cause_tags:
        return f"{cluster.repeat_listening_cause_tags[0].title()} users"
    if cluster.keywords:
        return f"{cluster.keywords[0].title()}-focused users"
    return f"{cluster.cluster_name} users"


def _primary_jtbds_from_cluster(cluster: ClusterItem) -> list[str]:
    if any("discover" in keyword for keyword in cluster.keywords):
        return ["Discover relevant new music without repetitive recommendations"]
    if any("playlist" in keyword for keyword in cluster.keywords):
        return ["Find playlists that match the user's current intent and mood"]
    return ["Achieve better discovery outcomes with more relevant recommendations"]


def _build_research_question_gap_warnings(
    research_question_coverage: list[ResearchQuestionCoverage],
) -> list[str]:
    warnings: list[str] = []
    for item in research_question_coverage:
        if item.record_count < settings.low_record_count_threshold:
            warnings.append(f"Low relevant record count for research question: {item.question}")
    return warnings


def _build_source_contamination_warnings(
    feedback_items: list[RawFeedbackItem],
    request: AnalyzeFeedbackRequest,
) -> list[str]:
    rejection_counts = Counter()
    for item in feedback_items:
        is_relevant, _, reason = score_relevance(item, request)
        if is_relevant:
            continue
        if item.source == "reddit" and reason.startswith("reddit_"):
            rejection_counts[reason] += 1

    warnings: list[str] = []
    if rejection_counts:
        warnings.append(
            "Reddit contamination was filtered out before analysis: "
            + ", ".join(
                f"{reason}={count}" for reason, count in sorted(rejection_counts.items())
            )
        )
    return warnings


def _build_quality_diagnostics(
    *,
    total_collected: int,
    in_window_records: int,
    out_of_window_records: int,
    relevant_records: int,
    deduped_records: int,
    clusters: list[ClusterItem],
    contamination_warnings: list[str],
    expanded_collection_applied: bool,
    expanded_collection_reason: str | None,
) -> QualityDiagnostics:
    actual_clusters = [cluster for cluster in clusters if cluster.frequency > 0]
    cluster_count = len(actual_clusters)
    single_record_cluster_count = sum(
        1 for cluster in actual_clusters if cluster.frequency == 1
    )
    average_records_per_cluster = round(
        (deduped_records / cluster_count) if cluster_count else 0.0,
        2,
    )
    relevant_rate = round(
        (relevant_records / total_collected) if total_collected else 0.0,
        4,
    )
    dedupe_rate = round(
        ((relevant_records - deduped_records) / relevant_records)
        if relevant_records
        else 0.0,
        4,
    )
    return QualityDiagnostics(
        total_collected=total_collected,
        in_window_records=in_window_records,
        out_of_window_records=out_of_window_records,
        relevant_records=relevant_records,
        relevant_rate=relevant_rate,
        dedupe_rate=dedupe_rate,
        cluster_count=cluster_count,
        average_records_per_cluster=average_records_per_cluster,
        single_record_cluster_count=single_record_cluster_count,
        source_contamination_warnings=contamination_warnings,
        time_window_violations=out_of_window_records,
        expanded_collection_applied=expanded_collection_applied,
        expanded_collection_reason=expanded_collection_reason,
    )


def _build_source_limitations(
    *,
    google_play_warnings: list[str],
    reddit_warnings: list[str],
    app_store_warnings: list[str],
    contamination_warnings: list[str],
    time_window_violations: int,
) -> list[SourceLimitation]:
    limitations: list[SourceLimitation] = []
    if any("rate limited" in warning.lower() or "429" in warning for warning in reddit_warnings):
        limitations.append(
            SourceLimitation(
                source="reddit",
                limitation="Reddit collection was limited by rate limits; analysis uses partial Reddit data as qualitative depth signal.",
                severity="medium",
            )
        )
    if google_play_warnings:
        limitations.append(
            SourceLimitation(
                source="google_play",
                limitation="Google Play collection had partial or failed coverage during this run.",
                severity="medium",
            )
        )
    if app_store_warnings:
        limitations.append(
            SourceLimitation(
                source="app_store",
                limitation="App Store collection had partial or failed coverage during this run.",
                severity="medium",
            )
        )
    for warning in contamination_warnings:
        limitations.append(
            SourceLimitation(
                source="reddit",
                limitation=warning,
                severity="low",
            )
        )
    if time_window_violations:
        limitations.append(
            SourceLimitation(
                source="all_sources",
                limitation=(
                    f"{time_window_violations} record(s) were outside the requested time window and excluded from analysis outputs."
                ),
                severity="low",
            )
        )
    return limitations


def _build_opportunity_traceability_summary(
    opportunities: list[OpportunityItem],
) -> list[dict[str, Any]]:
    return [
        {
            "opportunity_id": item.opportunity_id,
            "opportunity_name": item.opportunity_name,
            "question_count": len(item.supporting_research_questions),
            "supporting_cluster_ids": item.supporting_cluster_ids,
            "brief_alignment_score": item.brief_alignment_score,
        }
        for item in opportunities[:10]
    ]


def _build_success_criteria_impact_summary(
    opportunities: list[OpportunityItem],
) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for item in opportunities[:10]:
        summary.append(
            {
                "opportunity_id": item.opportunity_id,
                "opportunity_name": item.opportunity_name,
                "success_criteria_impact": [
                    impact.model_dump() for impact in item.success_criteria_impact
                ],
            }
        )
    return summary


def _build_brief_alignment_summary(
    opportunities: list[OpportunityItem],
) -> dict[str, Any]:
    return {
        "high_alignment_opportunities": [
            item.opportunity_id
            for item in opportunities
            if item.brief_alignment_score == "high"
        ],
        "medium_alignment_opportunities": [
            item.opportunity_id
            for item in opportunities
            if item.brief_alignment_score == "medium"
        ],
        "low_alignment_opportunities": [
            item.opportunity_id
            for item in opportunities
            if item.brief_alignment_score == "low"
        ],
    }


def _build_compact_metrics(metrics: MetricsPayload) -> dict[str, Any]:
    return {
        "total_records_collected": metrics.total_records_collected,
        "records_relevant": metrics.records_relevant,
        "records_after_deduplication": metrics.records_after_deduplication,
        "cluster_count": metrics.cluster_count,
        "top_clusters": [item.model_dump() for item in metrics.top_clusters],
        "source_distribution": metrics.source_distribution,
        "dominant_signal_distribution": metrics.dominant_signal_distribution,
    }


def _build_compact_charts_summary(charts_data: ChartsDataPayload) -> dict[str, Any]:
    return {
        "feedback_by_source": [item.model_dump() for item in charts_data.feedback_by_source],
        "top_clusters": [item.model_dump() for item in charts_data.top_clusters[:10]],
        "cluster_signal_distribution": [
            item.model_dump() for item in charts_data.cluster_signal_distribution
        ],
        "feedback_over_time_points": len(charts_data.feedback_over_time),
    }


def _write_run_artifacts(
    *,
    run_id: str,
    raw_feedback: list[RawFeedbackItem],
    clean_feedback: list[RawFeedbackItem],
    time_window_value: str,
    all_clusters: list[ClusterItem],
    source_summary: list[SourceSummaryItem],
    charts_data: ChartsDataPayload,
    compact_gpt_payload: CompactGPTPayload,
    quality_diagnostics: QualityDiagnostics,
    research_question_coverage: list[ResearchQuestionCoverage],
    opportunities: list[OpportunityItem],
    evidence_backed_segments: list[EvidenceBackedSegment],
    processing_notes: list[str],
    warnings: list[str],
    source_limitations: list[SourceLimitation],
    tiered_clusters: dict[str, list[ClusterItem]],
) -> None:
    artifacts_started_at = perf_counter()
    logger.info(
        "artifact writing started run_id=%s raw=%s clean=%s clusters=%s opportunities=%s segments=%s",
        run_id,
        len(raw_feedback),
        len(clean_feedback),
        len(all_clusters),
        len(opportunities),
        len(evidence_backed_segments),
    )
    write_csv_artifact(
        run_id,
        "all_feedback_raw.csv",
        [_feedback_row(item, time_window_value=time_window_value) for item in raw_feedback],
        fieldnames=_feedback_fieldnames(),
    )
    write_csv_artifact(
        run_id,
        "all_feedback_clean.csv",
        [_feedback_row(item, time_window_value=time_window_value) for item in clean_feedback],
        fieldnames=_feedback_fieldnames(),
    )
    write_csv_artifact(
        run_id,
        "all_clusters.csv",
        [_cluster_row(item) for item in all_clusters],
        fieldnames=_cluster_fieldnames(),
    )
    write_json_artifact(
        run_id,
        "all_clusters.json",
        {
            "tier_1": [item.model_dump() for item in tiered_clusters["tier_1"]],
            "tier_2": [item.model_dump() for item in tiered_clusters["tier_2"]],
            "tier_3": [item.model_dump() for item in tiered_clusters["tier_3"]],
        },
    )
    write_json_artifact(
        run_id,
        "all_clusters_compact.json",
        _write_compact_cluster_artifacts(run_id, tiered_clusters),
    )
    write_csv_artifact(
        run_id,
        "source_summary.csv",
        [item.model_dump() for item in source_summary],
        fieldnames=[
            "source_name",
            "source_type",
            "queries_used",
            "records_collected",
            "records_relevant",
            "date_range",
            "notes",
        ],
    )
    write_json_artifact(run_id, "charts_data.json", charts_data.model_dump())
    write_json_artifact(
        run_id,
        "quality_diagnostics.json",
        quality_diagnostics.model_dump(),
    )
    write_json_artifact(
        run_id,
        "research_question_coverage.json",
        [item.model_dump() for item in research_question_coverage],
    )
    write_json_artifact(
        run_id,
        "opportunity_traceability.json",
        [item.model_dump() for item in opportunities],
    )
    write_json_artifact(
        run_id,
        "opportunity_traceability_compact.json",
        _write_compact_opportunity_artifacts(run_id, opportunities),
    )
    write_json_artifact(
        run_id,
        "segment_evidence.json",
        [item.model_dump() for item in evidence_backed_segments],
    )
    write_json_artifact(
        run_id,
        "success_criteria_impact_mapping.json",
        [
            {
                "opportunity_id": item.opportunity_id,
                "opportunity_name": item.opportunity_name,
                "success_criteria_impact": [
                    impact.model_dump() for impact in item.success_criteria_impact
                ],
            }
            for item in opportunities
        ],
    )
    write_json_artifact(
        run_id,
        "success_criteria_impact_mapping_compact.json",
        [
            _compact_success_criteria_impact_row(item)
            for item in opportunities
        ],
    )
    write_json_artifact(run_id, "compact_gpt_payload.json", compact_gpt_payload.model_dump())
    write_markdown_artifact(
        run_id,
        "processing_notes.md",
        _build_processing_notes_markdown(
            processing_notes,
            warnings,
            source_limitations,
            quality_diagnostics,
        ),
    )
    write_markdown_artifact(
        run_id,
        "evidence_appendix.md",
        _build_evidence_appendix_markdown(all_clusters, research_question_coverage),
    )
    logger.info(
        "artifact writing completed run_id=%s elapsed_seconds=%.2f artifact_count=%s",
        run_id,
        perf_counter() - artifacts_started_at,
        17,
    )


def _feedback_fieldnames() -> list[str]:
    return [
        "feedback_id",
        "source",
        "source_type",
        "date",
        "in_requested_time_window",
        "text",
        "url",
        "rating",
        "engagement_score",
        "engagement_comments",
        "engagement_thumbs_up",
        "subreddit",
        "query_used",
        "app_version",
        "title",
        "country",
        "storefront",
    ]


def _feedback_row(item: RawFeedbackItem, *, time_window_value: str) -> dict[str, Any]:
    return {
        "feedback_id": item.feedback_id,
        "source": item.source,
        "source_type": item.source_type,
        "date": item.date,
        "in_requested_time_window": is_within_relative_window(item.date, time_window_value),
        "text": item.text,
        "url": item.url,
        "rating": item.rating,
        "engagement_score": item.engagement.score,
        "engagement_comments": item.engagement.comments,
        "engagement_thumbs_up": item.engagement.thumbs_up,
        "subreddit": item.metadata.subreddit,
        "query_used": item.metadata.query_used,
        "app_version": item.metadata.app_version,
        "title": item.metadata.title,
        "country": item.metadata.country,
        "storefront": item.metadata.storefront,
    }


def _cluster_fieldnames() -> list[str]:
    return [
        "cluster_id",
        "cluster_name",
        "cluster_tier",
        "cluster_summary",
        "cluster_size",
        "cluster_cohesion_score",
        "frequency",
        "dominant_signal",
        "pain_point_evidence_count",
        "positive_validation_count",
        "request_signal_count",
        "mixed_signal_flag",
        "source_distribution",
        "time_distribution",
        "representative_quotes",
        "example_feedback_ids",
        "keywords",
        "mapped_research_questions",
        "mapped_success_criteria",
        "repeat_listening_cause_tags",
        "relevance_score",
    ]


def _cluster_row(cluster: ClusterItem) -> dict[str, Any]:
    return {
        "cluster_id": cluster.cluster_id,
        "cluster_name": cluster.cluster_name,
        "cluster_tier": cluster.cluster_tier,
        "cluster_summary": cluster.cluster_summary,
        "cluster_size": cluster.cluster_size,
        "cluster_cohesion_score": cluster.cluster_cohesion_score,
        "frequency": cluster.frequency,
        "dominant_signal": cluster.dominant_signal,
        "pain_point_evidence_count": cluster.pain_point_evidence_count,
        "positive_validation_count": cluster.positive_validation_count,
        "request_signal_count": cluster.request_signal_count,
        "mixed_signal_flag": cluster.mixed_signal_flag,
        "source_distribution": cluster.source_distribution,
        "time_distribution": cluster.time_distribution,
        "representative_quotes": [quote.model_dump() for quote in cluster.representative_quotes],
        "example_feedback_ids": cluster.example_feedback_ids,
        "keywords": cluster.keywords,
        "mapped_research_questions": cluster.mapped_research_questions,
        "mapped_success_criteria": cluster.mapped_success_criteria,
        "repeat_listening_cause_tags": cluster.repeat_listening_cause_tags,
        "relevance_score": cluster.relevance_score,
    }


def _compact_cluster_artifact_row(cluster: ClusterItem) -> dict[str, Any]:
    representative_quote = (
        _truncate_text(cluster.representative_quotes[0].text, settings.response_max_quote_chars)
        if cluster.representative_quotes
        else ""
    )
    return {
        "cluster_id": cluster.cluster_id,
        "cluster_name": cluster.cluster_name,
        "cluster_tier": cluster.cluster_tier,
        "cluster_summary": cluster.cluster_summary,
        "cluster_size": cluster.cluster_size,
        "cluster_cohesion_score": cluster.cluster_cohesion_score,
        "frequency": cluster.frequency,
        "dominant_signal": cluster.dominant_signal,
        "source_distribution": cluster.source_distribution,
        "mapped_research_questions": cluster.mapped_research_questions,
        "mapped_success_criteria": cluster.mapped_success_criteria,
        "repeat_listening_cause_tags": cluster.repeat_listening_cause_tags,
        "keywords": cluster.keywords[:5],
        "representative_quote": representative_quote,
        "representative_quote_source": (
            cluster.representative_quotes[0].source if cluster.representative_quotes else None
        ),
        "representative_quote_date": (
            cluster.representative_quotes[0].date if cluster.representative_quotes else None
        ),
        "relevance_score": cluster.relevance_score,
    }


def _write_compact_cluster_artifacts(
    run_id: str,
    tiered_clusters: dict[str, list[ClusterItem]],
) -> dict[str, Any]:
    part_size = settings.compact_cluster_artifact_part_size
    max_parts_per_tier = settings.compact_cluster_artifact_max_parts_per_tier
    parts: list[dict[str, Any]] = []
    for tier_name in ["tier_1", "tier_2", "tier_3"]:
        compact_rows = [
            _compact_cluster_artifact_row(item)
            for item in tiered_clusters[tier_name]
        ]
        for index in range(max_parts_per_tier):
            start = index * part_size
            if start >= len(compact_rows):
                break
            end = None if index == max_parts_per_tier - 1 else start + part_size
            artifact_name = f"all_clusters_compact_{tier_name}_part_{index + 1}.json"
            payload = compact_rows[start:end]
            write_json_artifact(run_id, artifact_name, payload)
            parts.append(
                {
                    "artifact_name": artifact_name,
                    "tier": tier_name,
                    "part": index + 1,
                    "cluster_count": len(payload),
                }
            )
    return {
        "total_clusters": sum(len(items) for items in tiered_clusters.values()),
        "part_size": part_size,
        "parts": parts,
    }


def _compact_opportunity_traceability_row(item: OpportunityItem) -> dict[str, Any]:
    return {
        "opportunity_id": item.opportunity_id,
        "opportunity_name": item.opportunity_name,
        "derived_from_cluster_id": item.derived_from_cluster_id,
        "frequency": item.frequency,
        "dominant_signal": item.dominant_signal,
        "source_distribution": item.source_distribution,
        "supporting_cluster_ids": item.supporting_cluster_ids[:4],
        "supporting_research_questions": [
            {
                "question_id": question.question_id,
                "question": question.question,
                "support_level": question.support_level,
                "supporting_cluster_ids": question.supporting_cluster_ids[:3],
            }
            for question in item.supporting_research_questions[:3]
        ],
        "brief_alignment_score": item.brief_alignment_score,
        "brief_alignment_rationale": item.brief_alignment_rationale,
        "top_pain_points": item.top_pain_points[:3],
    }


def _write_compact_opportunity_artifacts(
    run_id: str,
    opportunities: list[OpportunityItem],
) -> dict[str, Any]:
    compact_rows = [_compact_opportunity_traceability_row(item) for item in opportunities]
    part_size = settings.compact_opportunity_artifact_part_size
    max_parts = settings.compact_opportunity_artifact_max_parts
    parts: list[dict[str, Any]] = []

    for index in range(max_parts):
        start = index * part_size
        if start >= len(compact_rows):
            break
        end = None if index == max_parts - 1 else start + part_size
        artifact_name = f"opportunity_traceability_compact_part_{index + 1}.json"
        payload = compact_rows[start:end]
        write_json_artifact(run_id, artifact_name, payload)
        parts.append(
            {
                "artifact_name": artifact_name,
                "part": index + 1,
                "opportunity_count": len(payload),
            }
        )

    return {
        "total_opportunities": len(compact_rows),
        "part_size": part_size,
        "parts": parts,
    }


def _compact_success_criteria_impact_row(item: OpportunityItem) -> dict[str, Any]:
    return {
        "opportunity_id": item.opportunity_id,
        "opportunity_name": item.opportunity_name,
        "success_criteria_impact": [
            {
                "criterion": impact.criterion,
                "impact_level": impact.impact_level,
                "supporting_cluster_ids": impact.supporting_cluster_ids[:3],
            }
            for impact in item.success_criteria_impact[:3]
        ],
    }


def _build_processing_notes_markdown(
    processing_notes: list[str],
    warnings: list[str],
    source_limitations: list[SourceLimitation],
    quality_diagnostics: QualityDiagnostics,
) -> str:
    lines = ["# Processing Notes", ""]
    lines.extend([f"- {note}" for note in processing_notes])
    lines.extend(
        [
            "",
            "# Quality Diagnostics",
            "",
            f"- Total collected: {quality_diagnostics.total_collected}",
            f"- In-window records: {quality_diagnostics.in_window_records}",
            f"- Out-of-window records: {quality_diagnostics.out_of_window_records}",
            f"- Relevant records: {quality_diagnostics.relevant_records}",
            f"- Relevant rate: {quality_diagnostics.relevant_rate}",
            f"- Dedupe rate: {quality_diagnostics.dedupe_rate}",
            f"- Cluster count: {quality_diagnostics.cluster_count}",
            f"- Average records per cluster: {quality_diagnostics.average_records_per_cluster}",
            f"- Single-record clusters: {quality_diagnostics.single_record_cluster_count}",
        ]
    )
    if quality_diagnostics.source_contamination_warnings:
        lines.extend(
            [
                f"- Contamination warnings: {'; '.join(quality_diagnostics.source_contamination_warnings)}"
            ]
        )
    lines.extend(["", "# Warnings", ""])
    lines.extend([f"- {warning}" for warning in warnings] or ["- None"])
    lines.extend(["", "# Source Limitations", ""])
    lines.extend([f"- {item.source}: {item.limitation}" for item in source_limitations] or ["- None"])
    return "\n".join(lines) + "\n"


def _build_evidence_appendix_markdown(
    all_clusters: list[ClusterItem],
    research_question_coverage: list[ResearchQuestionCoverage],
) -> str:
    lines = ["# Evidence Appendix", "", "## Research Question Coverage", ""]
    for item in research_question_coverage:
        lines.append(f"### {item.question}")
        lines.append(f"- Evidence strength: {item.evidence_strength}")
        lines.append(f"- Record count: {item.record_count}")
        lines.append(f"- Relevant clusters: {', '.join(item.relevant_cluster_ids) or 'None'}")
        lines.append(f"- Summary: {item.summary}")
        if item.evidence_gaps:
            lines.append(f"- Evidence gaps: {'; '.join(item.evidence_gaps)}")
        lines.append("")

    lines.extend(["## Cluster Appendix", ""])
    for cluster in all_clusters:
        lines.append(f"### {cluster.cluster_id} - {cluster.cluster_name}")
        lines.append(f"- Tier: {cluster.cluster_tier}")
        lines.append(f"- Frequency: {cluster.frequency}")
        lines.append(f"- Research questions: {', '.join(cluster.mapped_research_questions) or 'None'}")
        lines.append(
            f"- Repeat listening causes: {', '.join(cluster.repeat_listening_cause_tags) or 'None'}"
        )
        lines.append(f"- Summary: {cluster.cluster_summary}")
        if cluster.representative_quotes:
            lines.append(f"- Representative quote: {cluster.representative_quotes[0].text}")
        lines.append("")
    return "\n".join(lines) + "\n"


def _derive_date_range(feedback_items: list[RawFeedbackItem]) -> SourceDateRange:
    if not feedback_items:
        return SourceDateRange(start="2025-06-20", end="2026-06-20")

    dates = sorted(item.date[:10] for item in feedback_items)
    return SourceDateRange(start=dates[0], end=dates[-1])


def _build_rating_distribution(feedback_items: list[RawFeedbackItem]) -> dict[str, int]:
    counts = {str(rating): 0 for rating in range(1, 6)}
    for item in feedback_items:
        if item.rating and 1 <= item.rating <= 5:
            counts[str(item.rating)] += 1
    return counts


def _build_source_distribution(feedback_items: list[RawFeedbackItem]) -> dict[str, int]:
    counts = {"reddit": 0, "google_play": 0, "app_store": 0}
    for item in feedback_items:
        if item.source in counts:
            counts[item.source] += 1
    return counts


def _build_feedback_over_time(
    feedback_items: list[RawFeedbackItem],
) -> list[ChartValueOverTime]:
    month_counts = Counter(month_bucket(item.date) for item in feedback_items)
    if not month_counts:
        return [
            ChartValueOverTime(month="2026-01", count=0),
            ChartValueOverTime(month="2026-02", count=0),
        ]

    return [
        ChartValueOverTime(month=month, count=month_counts[month])
        for month in sorted(month_counts)
    ]


def _build_time_distribution_map(
    feedback_over_time: list[ChartValueOverTime],
) -> dict[str, int]:
    return {entry.month: entry.count for entry in feedback_over_time}


def _build_cluster_signal_distribution(clusters: list[ClusterItem]) -> dict[str, int]:
    counts = {"pain": 0, "positive": 0, "mixed": 0}
    for cluster in clusters:
        if cluster.frequency <= 0:
            continue
        if cluster.dominant_signal in counts:
            counts[cluster.dominant_signal] += 1
    return counts


def _build_top_level_representative_quotes(
    clusters: list[ClusterItem],
    deduped_feedback: list[RawFeedbackItem],
) -> list[QuoteItem]:
    quotes: list[QuoteItem] = []
    seen: set[tuple[str, str]] = set()

    for cluster in clusters:
        for quote in cluster.representative_quotes:
            key = (quote.text, quote.url)
            if key in seen:
                continue
            seen.add(key)
            quotes.append(quote)
            break

    if len(quotes) >= 5:
        return quotes[:5]

    fallback_quotes = _select_representative_quotes(deduped_feedback)
    for quote in fallback_quotes:
        key = (quote.text, quote.url)
        if key in seen:
            continue
        seen.add(key)
        quotes.append(quote)
        if len(quotes) == 5:
            break

    return quotes


def _select_representative_quotes(
    feedback_items: list[RawFeedbackItem],
) -> list[QuoteItem]:
    sorted_items = sorted(
        feedback_items,
        key=lambda item: (
            score_opportunity_signal(item),
            max(
                item.engagement.thumbs_up,
                item.engagement.score,
                item.engagement.comments,
            ),
            -(item.rating or 0),
        ),
        reverse=True,
    )
    quotes: list[QuoteItem] = []
    for item in sorted_items[:5]:
        quotes.append(
            QuoteItem(
                text=item.text,
                source=item.source,
                url=item.url,
                date=item.date,
            )
        )
    return quotes


def _build_source_failures(
    *,
    google_play_feedback: list[RawFeedbackItem],
    google_play_warnings: list[str],
    reddit_feedback: list[RawFeedbackItem],
    reddit_warnings: list[str],
    app_store_feedback: list[RawFeedbackItem],
    app_store_warnings: list[str],
) -> list[str]:
    failures: list[str] = []
    if reddit_warnings and not reddit_feedback:
        failures.append("reddit")
    if google_play_warnings and not google_play_feedback:
        failures.append("google_play")
    if app_store_warnings and not app_store_feedback:
        failures.append("app_store")
    return failures


def _build_source_warning_codes(
    *,
    google_play_feedback: list[RawFeedbackItem],
    google_play_warnings: list[str],
    reddit_feedback: list[RawFeedbackItem],
    reddit_warnings: list[str],
    app_store_feedback: list[RawFeedbackItem],
    app_store_warnings: list[str],
) -> dict[str, list[str]]:
    warning_codes: dict[str, list[str]] = {}

    if reddit_warnings:
        warning_codes["reddit"] = ["reddit_partial" if reddit_feedback else "reddit_failed"]
    if google_play_warnings:
        warning_codes["google_play"] = [
            "google_play_partial" if google_play_feedback else "google_play_failed"
        ]
    if app_store_warnings:
        warning_codes["app_store"] = [
            "app_store_partial" if app_store_feedback else "app_store_failed"
        ]

    return warning_codes


def _prefix_warning_codes(
    source: str,
    codes: list[str],
    warnings: list[str],
) -> list[str]:
    if not warnings:
        return []

    prefix = ",".join(codes) if codes else f"{source}_warning"
    return [f"[{prefix}] {warning}" for warning in warnings]
