from datetime import datetime
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app
from app.schemas import ClusterItem, QuoteItem


client = TestClient(app)


def valid_payload() -> dict:
    return {
        "product": "Spotify",
        "research_scope": "Music Discovery",
        "research_goal": "Opportunity Discovery",
        "analysis_time_window": {"type": "relative", "value": "3_months"},
        "included_topics": ["recommendations", "music discovery", "personalization"],
        "excluded_topics": ["pricing", "billing", "podcasts"],
        "research_questions": [
            "Why do users struggle to discover new music?",
            "What are the most common frustrations with recommendations?",
            "What listening behaviors are users trying to achieve?",
            "What causes repetitive listening?",
            "Which user segments experience different discovery challenges?",
            "What unmet needs emerge consistently?",
        ],
        "success_criteria": [
            "Improve meaningful music discovery",
            "Reduce repetitive listening",
            "Improve recommendation relevance and novelty balance",
        ],
        "max_runtime_seconds": 1800,
        "debug": False,
    }


def _cluster(index: int) -> ClusterItem:
    return ClusterItem(
        cluster_id=f"cluster_{index:03d}",
        cluster_name=f"Cluster {index}",
        cluster_summary="Summary",
        cluster_tier="tier_1",
        cluster_size=max(1, 30 - index),
        cluster_cohesion_score=0.51,
        frequency=max(1, 30 - index),
        dominant_signal="pain",
        pain_point_evidence_count=1,
        positive_validation_count=0,
        request_signal_count=1,
        mixed_signal_flag=False,
        source_distribution={"reddit": 0, "google_play": 1, "app_store": 0},
        time_distribution={"2026-06": 1},
        representative_quotes=[
            QuoteItem(
                text="Short complete quote.",
                source="google_play",
                url="https://play.google.com",
                date="2026-06-20T00:00:00Z",
            )
        ],
        example_feedback_ids=[f"fb_{index}"],
        keywords=["discovery"],
        mapped_research_questions=["Why do users struggle to discover new music?"],
        mapped_success_criteria=["Improve meaningful music discovery"],
        repeat_listening_cause_tags=["algorithmic repetition"],
        relevance_score=1.0,
    )


def test_artifacts_are_created_and_retrievable(monkeypatch, tmp_path: Path) -> None:
    from app.collectors.google_play import normalize_google_play_review
    from app.config import settings
    from app.services import pipeline

    monkeypatch.setattr(settings, "runs_dir_path", str(tmp_path / "runs"))
    sample_google_play = [
        normalize_google_play_review(
                {
                    "reviewId": "gp_1",
                    "content": "Spotify keeps recommending the same artists in Discover Weekly.",
                    "score": 2,
                    "thumbsUpCount": 2,
                    "at": datetime(2026, 5, 1, 12, 0, 0),
                    "reviewCreatedVersion": "9.0.0",
                }
            ),
        normalize_google_play_review(
                {
                    "reviewId": "gp_2",
                    "content": "Release Radar helps me find new music.",
                    "score": 5,
                    "thumbsUpCount": 3,
                    "at": datetime(2026, 5, 2, 12, 0, 0),
                    "reviewCreatedVersion": "9.0.0",
                }
            ),
    ]

    monkeypatch.setattr(pipeline, "collect_google_play_reviews", lambda app_id: sample_google_play)
    monkeypatch.setattr(pipeline, "collect_reddit_feedback", lambda queries: [])
    monkeypatch.setattr(pipeline, "collect_app_store_reviews", lambda app_id: [])

    response = client.post("/analyze-feedback", json=valid_payload())

    assert response.status_code == 200
    body = response.json()
    run_id = body["run_id"]
    run_dir = tmp_path / "runs" / run_id
    assert (run_dir / "all_feedback_raw.csv").exists()
    assert (run_dir / "all_clusters.csv").exists()
    assert (run_dir / "all_clusters_compact.json").exists()
    assert (run_dir / "all_clusters_compact_tier_1_part_1.json").exists()
    assert (run_dir / "opportunity_traceability.json").exists()
    assert (run_dir / "opportunity_traceability_compact_part_1.json").exists()
    assert (run_dir / "segment_evidence.json").exists()
    assert (run_dir / "success_criteria_impact_mapping.json").exists()
    assert (run_dir / "quality_diagnostics.json").exists()
    assert body["artifact_manifest"]["artifacts"]
    assert len(body["compact_gpt_payload"]["top_clusters"]) <= 10
    assert body["opportunities"] == []
    assert body["compact_gpt_payload"]["top_opportunities"]
    assert body["compact_gpt_payload"]["top_opportunities"][0]["success_criteria_impact"]
    assert body["compact_gpt_payload"]["success_criteria"] == body["locked_brief"]["success_criteria"]
    assert body["quality_diagnostics"]["cluster_count"] >= 1

    manifest_response = client.get(f"/runs/{run_id}/manifest")
    assert manifest_response.status_code == 200
    manifest = manifest_response.json()
    assert any(item["name"] == "all_clusters.csv" for item in manifest["artifacts"])
    assert any(item["name"] == "all_clusters_compact.json" for item in manifest["artifacts"])
    assert any(item["name"] == "opportunity_traceability.json" for item in manifest["artifacts"])
    assert any(item["name"] == "quality_diagnostics.json" for item in manifest["artifacts"])

    artifact_response = client.get(f"/runs/{run_id}/artifact/all_clusters.csv")
    assert artifact_response.status_code == 200
    assert "cluster_id" in artifact_response.text

    compact_clusters_response = client.get(
        f"/runs/{run_id}/artifact/all_clusters_compact.json"
    )
    assert compact_clusters_response.status_code == 200
    compact_clusters_payload = compact_clusters_response.json()
    assert "parts" in compact_clusters_payload
    assert compact_clusters_payload["parts"][0]["artifact_name"] == "all_clusters_compact_tier_1_part_1.json"

    compact_cluster_tier_response = client.get(
        f"/runs/{run_id}/artifact/all_clusters_compact_tier_1_part_1.json"
    )
    assert compact_cluster_tier_response.status_code == 200
    compact_cluster_tier_payload = compact_cluster_tier_response.json()
    assert "cluster_id" in compact_cluster_tier_payload[0]
    assert "representative_quote" in compact_cluster_tier_payload[0]

    traceability_response = client.get(
        f"/runs/{run_id}/artifact/opportunity_traceability.json"
    )
    assert traceability_response.status_code == 200
    assert traceability_response.json()[0]["success_criteria_impact"]

    diagnostics_response = client.get(
        f"/runs/{run_id}/artifact/quality_diagnostics.json"
    )
    assert diagnostics_response.status_code == 200
    assert "relevant_rate" in diagnostics_response.json()


def test_artifact_endpoint_blocks_unknown_or_traversal_names(monkeypatch, tmp_path: Path) -> None:
    from app.config import settings

    monkeypatch.setattr(settings, "runs_dir_path", str(tmp_path / "runs"))

    manifest_response = client.get("/runs/run_missing/manifest")
    assert manifest_response.status_code == 404

    unknown_response = client.get("/runs/run_missing/artifact/not_real.csv")
    assert unknown_response.status_code == 404

    traversal_response = client.get("/runs/run_missing/artifact/..%2Fall_clusters.csv")
    assert traversal_response.status_code in {400, 404}


def test_compact_payload_excludes_long_tail_clusters_but_artifacts_keep_all(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.collectors.google_play import normalize_google_play_review
    from app.config import settings
    from app.services import pipeline

    monkeypatch.setattr(settings, "runs_dir_path", str(tmp_path / "runs"))
    monkeypatch.setattr(
        pipeline,
        "collect_google_play_reviews",
        lambda app_id: [
            normalize_google_play_review(
                {
                    "reviewId": "gp_1",
                    "content": "Spotify keeps recommending the same artists in Discover Weekly.",
                    "score": 2,
                    "thumbsUpCount": 2,
                    "at": datetime(2026, 2, 1, 12, 0, 0),
                    "reviewCreatedVersion": "9.0.0",
                }
            )
        ],
    )
    monkeypatch.setattr(pipeline, "collect_reddit_feedback", lambda queries: [])
    monkeypatch.setattr(pipeline, "collect_app_store_reviews", lambda app_id: [])
    monkeypatch.setattr(
        pipeline,
        "cluster_feedback_items",
        lambda items, debug=False: ([_cluster(index) for index in range(1, 26)], []),
    )

    response = client.post("/analyze-feedback", json=valid_payload())

    assert response.status_code == 200
    body = response.json()
    assert len(body["compact_gpt_payload"]["top_clusters"]) == 6
    assert len(body["feedback_clusters"]) == 0

    run_id = body["run_id"]
    artifact_response = client.get(f"/runs/{run_id}/artifact/all_clusters.json")
    assert artifact_response.status_code == 200
    artifact_payload = artifact_response.json()
    total_clusters = sum(len(artifact_payload[tier]) for tier in ["tier_1", "tier_2", "tier_3"])
    assert total_clusters == 25

    compact_artifact_response = client.get(
        f"/runs/{run_id}/artifact/all_clusters_compact.json"
    )
    assert compact_artifact_response.status_code == 200
    compact_artifact_payload = compact_artifact_response.json()
    compact_total_clusters = sum(part["cluster_count"] for part in compact_artifact_payload["parts"])
    assert compact_total_clusters == 25
    assert len(compact_artifact_payload["parts"]) >= 4


def test_final_report_can_be_saved_and_retrieved(monkeypatch, tmp_path: Path) -> None:
    from app.collectors.google_play import normalize_google_play_review
    from app.config import settings
    from app.services import pipeline

    monkeypatch.setattr(settings, "runs_dir_path", str(tmp_path / "runs"))
    monkeypatch.setattr(
        pipeline,
        "collect_google_play_reviews",
        lambda app_id: [
            normalize_google_play_review(
                {
                    "reviewId": "gp_1",
                    "content": "Spotify keeps recommending the same artists in Discover Weekly.",
                    "score": 2,
                    "thumbsUpCount": 2,
                    "at": datetime(2026, 2, 1, 12, 0, 0),
                    "reviewCreatedVersion": "9.0.0",
                }
            )
        ],
    )
    monkeypatch.setattr(pipeline, "collect_reddit_feedback", lambda queries: [])
    monkeypatch.setattr(pipeline, "collect_app_store_reviews", lambda app_id: [])

    response = client.post("/analyze-feedback", json=valid_payload())
    run_id = response.json()["run_id"]

    save_response = client.post(
        f"/runs/{run_id}/final-report",
        json={"markdown": "# Final Report\n\nSome findings."},
    )
    assert save_response.status_code == 200
    assert save_response.json()["artifact_name"] == "final_report.md"

    artifact_response = client.get(f"/runs/{run_id}/artifact/final_report.md")
    assert artifact_response.status_code == 200
    assert "Final Report" in artifact_response.text

    manifest_response = client.get(f"/runs/{run_id}/manifest")
    assert manifest_response.status_code == 200
    assert any(
        item["name"] == "final_report.md"
        for item in manifest_response.json()["artifacts"]
    )


def test_async_start_and_status_flow(monkeypatch, tmp_path: Path) -> None:
    from app.collectors.google_play import normalize_google_play_review
    from app.config import settings
    from app.services import pipeline

    monkeypatch.setattr(settings, "runs_dir_path", str(tmp_path / "runs"))
    monkeypatch.setattr(
        pipeline,
        "collect_google_play_reviews",
        lambda app_id: [
            normalize_google_play_review(
                {
                    "reviewId": "gp_async_1",
                    "content": "Spotify keeps surfacing the same music in Discover Weekly.",
                    "score": 2,
                    "thumbsUpCount": 1,
                    "at": datetime(2026, 5, 1, 12, 0, 0),
                    "reviewCreatedVersion": "9.0.0",
                }
            )
        ],
    )
    monkeypatch.setattr(pipeline, "collect_reddit_feedback", lambda queries: [])
    monkeypatch.setattr(pipeline, "collect_app_store_reviews", lambda app_id: [])

    start_response = client.post("/analyze-feedback/start", json=valid_payload())
    assert start_response.status_code == 200
    started = start_response.json()
    assert started["run_id"].startswith("run_")
    assert started["status"] in {"queued", "running", "completed", "partial_success"}
    assert started["estimated_seconds_remaining"] >= 0

    run_id = started["run_id"]
    status_response = client.get(f"/runs/{run_id}/status?wait_seconds=1")
    assert status_response.status_code == 200
    status_payload = status_response.json()
    assert status_payload["run_id"] == run_id
    assert status_payload["status"] in {"running", "completed", "partial_success"}


def test_latest_run_status_returns_most_recent_async_run(monkeypatch, tmp_path: Path) -> None:
    from app.collectors.google_play import normalize_google_play_review
    from app.config import settings
    from app.services import pipeline

    monkeypatch.setattr(settings, "runs_dir_path", str(tmp_path / "runs"))
    monkeypatch.setattr(
        pipeline,
        "collect_google_play_reviews",
        lambda app_id: [
            normalize_google_play_review(
                {
                    "reviewId": "gp_latest_1",
                    "content": "Spotify recommendations need better novelty.",
                    "score": 3,
                    "thumbsUpCount": 1,
                    "at": datetime(2026, 5, 2, 12, 0, 0),
                    "reviewCreatedVersion": "9.0.0",
                }
            )
        ],
    )
    monkeypatch.setattr(pipeline, "collect_reddit_feedback", lambda queries: [])
    monkeypatch.setattr(pipeline, "collect_app_store_reviews", lambda app_id: [])

    start_response = client.post("/analyze-feedback/start", json=valid_payload())
    run_id = start_response.json()["run_id"]

    latest_response = client.get("/runs/latest/status?wait_seconds=1")
    assert latest_response.status_code == 200
    latest_payload = latest_response.json()
    assert latest_payload["run_id"] == run_id
