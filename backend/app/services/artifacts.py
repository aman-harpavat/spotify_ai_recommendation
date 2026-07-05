from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from app.config import settings
from app.schemas import ArtifactItem, ArtifactManifest

ARTIFACT_DEFINITIONS: dict[str, tuple[str, str]] = {
    "all_feedback_raw.csv": (
        "text/csv",
        "All raw collected feedback records before cleaning and filtering.",
    ),
    "all_feedback_clean.csv": (
        "text/csv",
        "All cleaned feedback records after normalization and cleaning.",
    ),
    "all_clusters.csv": (
        "text/csv",
        "All generated feedback clusters with frequency, source distribution, representative quotes, and mapped research questions.",
    ),
    "all_clusters.json": (
        "application/json",
        "Full cluster payload for all generated clusters across all tiers.",
    ),
    "all_clusters_compact.json": (
        "application/json",
        "Index for GPT-safe compact cluster shard artifacts.",
    ),
    "opportunity_traceability_compact.json": (
        "application/json",
        "Index for GPT-safe compact opportunity traceability shard artifacts.",
    ),
    "opportunity_traceability_compact_part_1.json": (
        "application/json",
        "GPT-safe compact opportunity traceability shard 1.",
    ),
    "opportunity_traceability_compact_part_2.json": (
        "application/json",
        "GPT-safe compact opportunity traceability shard 2.",
    ),
    "opportunity_traceability_compact_part_3.json": (
        "application/json",
        "GPT-safe compact opportunity traceability shard 3.",
    ),
    "opportunity_traceability_compact_part_4.json": (
        "application/json",
        "GPT-safe compact opportunity traceability shard 4.",
    ),
    "opportunity_traceability_compact_part_5.json": (
        "application/json",
        "GPT-safe compact opportunity traceability shard 5.",
    ),
    "opportunity_traceability_compact_part_6.json": (
        "application/json",
        "GPT-safe compact opportunity traceability shard 6.",
    ),
    "success_criteria_impact_mapping_compact.json": (
        "application/json",
        "GPT-safe compact success-criteria impact summaries for all opportunities.",
    ),
    "source_summary.csv": (
        "text/csv",
        "Source-level collection and relevance summary for the run.",
    ),
    "charts_data.json": (
        "application/json",
        "Full chart-ready dataset for downstream visualization and reporting.",
    ),
    "quality_diagnostics.json": (
        "application/json",
        "Run-level evidence quality diagnostics, contamination warnings, and time-window purity checks.",
    ),
    "evidence_appendix.md": (
        "text/markdown",
        "Evidence appendix with cluster notes, quotes, and research-question mapping.",
    ),
    "compact_gpt_payload.json": (
        "application/json",
        "Compact payload intended for Custom GPT synthesis.",
    ),
    "research_question_coverage.json": (
        "application/json",
        "Research-question coverage analysis with evidence strength and gaps.",
    ),
    "opportunity_traceability.json": (
        "application/json",
        "Opportunity-to-question traceability and brief alignment evidence.",
    ),
    "segment_evidence.json": (
        "application/json",
        "Evidence-backed user segments derived from collected feedback.",
    ),
    "success_criteria_impact_mapping.json": (
        "application/json",
        "Success criteria impact mapping for opportunity candidates.",
    ),
    "processing_notes.md": (
        "text/markdown",
        "Processing notes, warnings, and source limitations recorded for the run.",
    ),
    "run.log": (
        "text/plain",
        "Run-specific execution log written fresh for this run only.",
    ),
    "final_report.md": (
        "text/markdown",
        "Final GPT-generated Markdown report for download.",
    ),
}

for tier_name in ("tier_1", "tier_2", "tier_3"):
    for part in range(1, settings.compact_cluster_artifact_max_parts_per_tier + 1):
        ARTIFACT_DEFINITIONS[
            f"all_clusters_compact_{tier_name}_part_{part}.json"
        ] = (
            "application/json",
            f"GPT-safe compact cluster shard for {tier_name} part {part}.",
        )


def ensure_run_dir(run_id: str) -> Path:
    run_dir = Path(settings.runs_dir_path) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def artifact_path(run_id: str, artifact_name: str) -> Path:
    if artifact_name not in ARTIFACT_DEFINITIONS:
        raise FileNotFoundError(artifact_name)
    return ensure_run_dir(run_id) / artifact_name


def build_artifact_manifest(run_id: str) -> ArtifactManifest:
    artifacts = [
        ArtifactItem(
            name=name,
            type=mime_type,
            description=description,
            url=f"/runs/{run_id}/artifact/{name}",
        )
        for name, (mime_type, description) in ARTIFACT_DEFINITIONS.items()
        if artifact_path(run_id, name).exists()
    ]
    return ArtifactManifest(run_id=run_id, artifacts=artifacts)


def write_json_artifact(run_id: str, artifact_name: str, payload: Any) -> None:
    path = artifact_path(run_id, artifact_name)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_markdown_artifact(run_id: str, artifact_name: str, content: str) -> None:
    path = artifact_path(run_id, artifact_name)
    path.write_text(content, encoding="utf-8")


def write_csv_artifact(
    run_id: str,
    artifact_name: str,
    rows: list[dict[str, Any]],
    fieldnames: list[str],
) -> None:
    path = artifact_path(run_id, artifact_name)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
