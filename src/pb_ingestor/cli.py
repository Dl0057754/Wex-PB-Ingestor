from __future__ import annotations

import argparse
import json
from pathlib import Path

from jsonschema import Draft202012Validator

from .crosswalk import load_manifest
from .ingest import ingest_xlsx
from .pipeline import run_conversion, run_enrichment
from .crosswalk import infer_base_template_path, infer_crosswalk_path, load_crosswalk, load_manifest
from .enrichment import enrich_csv
from .ingest import ingest_xlsx
from .mapper import map_rows
from .markup import MarkupProfile
from .output import write_manual_review_csv, write_normalized_csv, write_qa_json, write_template_workbook


def _cmd_analyze(args: argparse.Namespace) -> int:
    result = ingest_xlsx(args.source)
    print(f"ingest_mode={result.mode}")
    print(f"parser_stage={result.parser_stage}")
    print(f"rows_found={len(result.rows)}")
    print(f"asset_refs={len(result.asset_refs)}")
    if result.errors:
        print("errors=")
        for err in result.errors:
            print(f"- {err}")
    return 0


def _resolve_template(template_type: str, template_path_arg: str | None) -> Path:
    return Path(template_path_arg) if template_path_arg else infer_base_template_path(template_type)


def _run_single_conversion(
    source: str,
    template_type: str,
    markup_profile_path: str,
    output_csv: str,
    output_workbook: str,
    qa_json: str,
    manual_review_csv: str,
    crosswalk_arg: str | None = None,
    template_path_arg: str | None = None,
    labor_cost_default: float | None = None,
    labor_rate_default: float | None = None,
) -> dict:
    return run_conversion(
        source=source,
        template_type=template_type,
        markup_profile_path=markup_profile_path,
        output_csv=output_csv,
        output_workbook=output_workbook,
        qa_json=qa_json,
        manual_review_csv=manual_review_csv,
        crosswalk_path=crosswalk_arg,
        template_path=template_path_arg,
    crosswalk_path = Path(crosswalk_arg) if crosswalk_arg else infer_crosswalk_path(template_type)
    crosswalk = load_crosswalk(crosswalk_path)
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
    write_template_workbook(mapped, _resolve_template(template_type, template_path_arg), output_workbook, crosswalk)
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
        "source": source,
        "summary": counters,
        "ingest_mode": ingest_result.mode,
        "errors": ingest_result.errors,
        "qa_json": qa_json,
    }


def _cmd_convert(args: argparse.Namespace) -> int:
    result = _run_single_conversion(
        source=args.source,
        template_type=args.template_type,
        markup_profile_path=args.markup_profile,
        output_csv=args.output_csv,
        output_workbook=args.output_workbook,
        qa_json=args.qa_json,
        manual_review_csv=args.manual_review_csv,
        crosswalk_arg=args.crosswalk,
        template_path_arg=args.template_path,
        labor_cost_default=args.labor_cost_default,
        labor_rate_default=args.labor_rate_default,
    )

    counters = result["summary"]
    print(f"wrote_output_csv={args.output_csv}")
    print(f"wrote_output_workbook={args.output_workbook}")
    print(f"wrote_manual_review={args.manual_review_csv}")
    print(f"wrote_qa={args.qa_json}")
    print(f"summary={counters['rows_processed']} processed / {counters['rows_incomplete']} incomplete")
    return 0


def _cmd_convert_all(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    run_results = []
    aggregate = {
        "rows_total": 0,
        "rows_processed": 0,
        "rows_incomplete": 0,
        "rows_manual_review": 0,
        "rows_duplicates_ignored": 0,
    }

    for row in manifest:
        if row.output_type not in {"single_part", "bundle", "supplier_loader"}:
            print(f"skipping={row.customer_name} reason=output_type_not_set")
            continue
        source = row.source_file
        customer_key = row.customer_name.lower().replace(" ", "_")
        result = _run_single_conversion(
            source=source,
            template_type=row.output_type,
            markup_profile_path=args.markup_profile,
            output_csv=f"{args.out_dir}/converted/{customer_key}.csv",
            output_workbook=f"{args.out_dir}/converted/{customer_key}.xlsx",
            qa_json=f"{args.out_dir}/qa/{customer_key}.json",
            manual_review_csv=f"{args.out_dir}/qa/{customer_key}_manual_review.csv",
            template_path_arg=row.base_template or None,
            labor_cost_default=args.labor_cost_default,
            labor_rate_default=args.labor_rate_default,
        )
        run_results.append(result)
        for k in aggregate:
            aggregate[k] += result["summary"].get(k, 0)

    consolidated = {
        "manifest": args.manifest,
        "runs": run_results,
        "aggregate": aggregate,
        "summary_text": f"{aggregate['rows_processed']} processed / {aggregate['rows_incomplete']} incomplete",
    }
    output = Path(args.consolidated_qa)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(consolidated, indent=2))
    print(f"wrote_consolidated_qa={args.consolidated_qa}")
    print(consolidated["summary_text"])
    return 0


def _cmd_enrich(args: argparse.Namespace) -> int:
    qa = run_enrichment(
    qa = enrich_csv(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        qa_json=args.qa_json,
        domains_config=args.domains_config,
        sleep_ms=args.sleep_ms,
    )
    print(f"wrote_enriched_csv={args.output_csv}")
    print(f"wrote_enrichment_qa={args.qa_json}")
    print(f"summary={qa['summary']}")
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    schema = json.loads(Path(args.schema).read_text())
    payload = json.loads(Path(args.input).read_text())
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        msg = "; ".join(f"{'.'.join(map(str, e.path)) or '<root>'}:{e.message}" for e in errors[:10])
        raise SystemExit(f"validation_error={msg}")
    print("validation=ok")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pb-ingestor", description="Pricebook ingestion CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    analyze = sub.add_parser("analyze", help="Inspect source workbook and count extracted rows")
    analyze.add_argument("source")
    analyze.set_defaults(func=_cmd_analyze)

    convert = sub.add_parser("convert", help="Convert a source file to outputs (normalized CSV, template XLSX, QA JSON)")
    convert.add_argument("source")
    convert.add_argument("--template-type", choices=["single_part", "bundle", "supplier_loader"], required=True)
    convert.add_argument("--crosswalk", default=None)
    convert.add_argument("--template-path", default=None)
    convert.add_argument("--markup-profile", default="config/markup/default_global_tiered_markup.json")
    convert.add_argument("--labor-cost-default", type=float, default=None)
    convert.add_argument("--labor-rate-default", type=float, default=None)
    convert.add_argument("--output-csv", default="out/converted/output.csv")
    convert.add_argument("--output-workbook", default="out/converted/output.xlsx")
    convert.add_argument("--qa-json", default="out/qa/run_report.json")
    convert.add_argument("--manual-review-csv", default="out/qa/manual_review.csv")
    convert.set_defaults(func=_cmd_convert)

    convert_all = sub.add_parser("convert-all", help="Batch-convert files listed in a manifest")
    convert_all.add_argument("--manifest", default="config/mappings/customer_manifest.csv")
    convert_all.add_argument("--markup-profile", default="config/markup/default_global_tiered_markup.json")
    convert_all.add_argument("--labor-cost-default", type=float, default=None)
    convert_all.add_argument("--labor-rate-default", type=float, default=None)
    convert_all.add_argument("--out-dir", default="out")
    convert_all.add_argument("--consolidated-qa", default="out/qa/consolidated.json")
    convert_all.set_defaults(func=_cmd_convert_all)

    enrich = sub.add_parser("enrich", help="Enrich converted CSV with manufacturer website data")
    enrich.add_argument("input_csv")
    enrich.add_argument("--domains-config", default="config/enrichment/manufacturer_domains.json")
    enrich.add_argument("--output-csv", default="out/enriched/enriched.csv")
    enrich.add_argument("--qa-json", default="out/qa/enrichment.json")
    enrich.add_argument("--sleep-ms", type=int, default=100)
    enrich.set_defaults(func=_cmd_enrich)

    validate_cmd = sub.add_parser("validate", help="Validate JSON against JSON schema")
    validate_cmd.add_argument("input")
    validate_cmd.add_argument("--schema", required=True)
    validate_cmd.set_defaults(func=_cmd_validate)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
