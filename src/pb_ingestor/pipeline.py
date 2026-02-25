from __future__ import annotations

from pathlib import Path

from .crosswalk import infer_base_template_path, infer_crosswalk_path, load_crosswalk
from .enrichment import enrich_csv
from .ingest import ingest_xlsx
from .mapper import map_rows
from .markup import MarkupProfile
from .output import write_manual_review_csv, write_normalized_csv, write_qa_json, write_template_workbook


def run_conversion(
    source: str,
    template_type: str,
    markup_profile_path: str,
    output_csv: str,
    output_workbook: str,
    qa_json: str,
    manual_review_csv: str,
    crosswalk_path: str | None = None,
    template_path: str | None = None,
    labor_cost_default: float | None = None,
    labor_rate_default: float | None = None,
) -> dict:
    crosswalk_file = Path(crosswalk_path) if crosswalk_path else infer_crosswalk_path(template_type)
    template_file = Path(template_path) if template_path else infer_base_template_path(template_type)

    crosswalk = load_crosswalk(crosswalk_file)
    markup = MarkupProfile.from_file(markup_profile_path)
    ingest_result = ingest_xlsx(source)

    mapped, counters = map_rows(
        ingest_result.rows,
        crosswalk,
        markup,
        labor_cost_default=labor_cost_default,
        labor_rate_default=labor_rate_default,
    )

    write_normalized_csv(mapped, output_csv)
    write_template_workbook(mapped, template_file, output_workbook, crosswalk)
    write_manual_review_csv(mapped, manual_review_csv)
    write_qa_json(
        qa_json,
        counters,
        ingest_result.mode,
        ingest_result.errors,
        source_file=Path(source).name,
        asset_refs=ingest_result.asset_refs,
    )

    return {
        "summary": counters,
        "ingest_mode": ingest_result.mode,
        "parser_stage": ingest_result.parser_stage,
        "errors": ingest_result.errors,
        "asset_refs": ingest_result.asset_refs,
        "output_csv": output_csv,
        "output_workbook": output_workbook,
        "qa_json": qa_json,
        "manual_review_csv": manual_review_csv,
    }


def run_enrichment(
    input_csv: str,
    output_csv: str,
    qa_json: str,
    domains_config: str,
    sleep_ms: int = 100,
) -> dict:
    return enrich_csv(
        input_csv=input_csv,
        output_csv=output_csv,
        qa_json=qa_json,
        domains_config=domains_config,
        sleep_ms=sleep_ms,
    )
