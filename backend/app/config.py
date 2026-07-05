from pathlib import Path

from pydantic import BaseModel


BACKEND_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseModel):
    app_name: str = "AI Product Discovery Copilot"
    app_version: str = "0.1.0"
    default_max_runtime_seconds: int = 1800
    outbound_request_timeout_seconds: float = 15.0
    log_file_path: str = str(BACKEND_DIR / "logs" / "backend.log")
    log_level: str = "INFO"
    cache_dir_path: str = str(BACKEND_DIR / "cache")
    runs_dir_path: str = str(BACKEND_DIR / "data" / "runs")
    async_run_poll_wait_cap_seconds: int = 40
    background_worker_count: int = 2
    reddit_max_queries_per_run: int = 5
    reddit_expanded_max_queries_per_run: int = 8
    reddit_max_consecutive_rate_limits: int = 2
    reddit_cache_ttl_seconds: int = 1800
    reddit_query_delay_seconds: float = 8.0
    reddit_max_retries: int = 2
    reddit_backoff_seconds: float = 10.0
    fast_mode_reddit_max_queries: int = 3
    fast_mode_reddit_result_limit: int = 80
    fast_mode_reddit_query_delay_seconds: float = 1.5
    fast_mode_reddit_max_retries: int = 1
    fast_mode_reddit_backoff_seconds: float = 2.5
    fast_mode_reddit_max_total_seconds: float = 20.0
    full_mode_reddit_result_limit: int = 200
    full_mode_reddit_max_total_seconds: float = 90.0
    fast_mode_google_play_timeout_seconds: float = 20.0
    fast_mode_app_store_timeout_seconds: float = 20.0
    full_mode_google_play_timeout_seconds: float = 35.0
    full_mode_app_store_timeout_seconds: float = 35.0
    google_play_page_safety_cap: int = 1000
    app_store_page_safety_cap: int = 500
    compact_payload_top_clusters: int = 6
    compact_payload_tier_2_limit: int = 40
    compact_payload_top_opportunities: int = 4
    compact_payload_top_segments: int = 2
    compact_cluster_artifact_part_size: int = 8
    compact_cluster_artifact_max_parts_per_tier: int = 20
    compact_opportunity_artifact_part_size: int = 30
    compact_opportunity_artifact_max_parts: int = 6
    response_top_level_clusters: int = 0
    response_top_level_opportunities: int = 0
    response_top_level_segments: int = 0
    response_max_cluster_quotes: int = 1
    response_max_top_level_quotes: int = 3
    response_max_example_feedback_ids: int = 2
    response_max_quote_chars: int = 140
    response_max_processing_notes: int = 12
    low_record_count_threshold: int = 3
    low_relevant_record_expansion_threshold: int = 25


settings = Settings()
