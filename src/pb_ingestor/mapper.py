from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .crosswalk import CrosswalkRow
from .ingest import SourceRow
from .markup import MarkupProfile


@dataclass
class MappedRow:
    row: dict[str, Any]
    status: str
    status_reason: str


def _normalize_part_number(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper().replace(" ", "")


def _find_field(source: SourceRow, candidates: list[str]) -> Any:
    lower_map = {k.lower(): v for k, v in source.values.items()}
    for c in candidates:
        cl = c.lower()
        if cl in lower_map and str(lower_map[cl]).strip():
            return lower_map[cl]
    for header, value in lower_map.items():
        if value in (None, ""):
            continue
        for c in candidates:
            if c.lower() in header:
                return value
    return None


def _parse_cost(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _manufacturer_site_hint(manufacturer: str | None, part_number: str | None) -> str | None:
    if not manufacturer or not part_number:
        return None
    normalized = manufacturer.lower().replace("&", "and").replace(" ", "")
    return f"https://www.google.com/search?q=site:{normalized}.com+{part_number}"


def map_rows(
    source_rows: list[SourceRow],
    crosswalk: list[CrosswalkRow],
    markup_profile: MarkupProfile,
    labor_cost_default: float | None = None,
    labor_rate_default: float | None = None,
) -> tuple[list[MappedRow], dict[str, int]]:
    output: list[MappedRow] = []
    seen_part_numbers: set[str] = set()
    required_columns = [c.output_column for c in crosswalk if c.required]

    counters = {
        "rows_total": len(source_rows),
        "rows_processed": 0,
        "rows_incomplete": 0,
        "rows_manual_review": 0,
        "rows_duplicates_ignored": 0,
    }

    for source in source_rows:
        part_number = _find_field(source, ["manufacturer part number", "part number", "mfr part", "model", "item id", "item", "sku"])
        normalized = _normalize_part_number(part_number)
        if normalized and normalized in seen_part_numbers:
            counters["rows_duplicates_ignored"] += 1
            continue
        if normalized:
            seen_part_numbers.add(normalized)

        source_description = _find_field(source, ["description", "item description", "item desc", "name"])
        source_manufacturer = _find_field(source, ["manufacturer", "mfr", "brand"]) or source.source_sheet
        cost_val = _find_field(source, ["cost", "net cost", "price", "customer cost"])
        part_cost = _parse_cost(cost_val)

        out_row: dict[str, Any] = {
            "Manufacturer Part Number": part_number,
            "manufacturer_part_number_original": part_number,
            "manufacturer_part_number_normalized": normalized,
            "Part Name": source.family_context or source_description,
            "Description": source_description,
            "Manufacturer": source_manufacturer,
            "Category": source.source_sheet,
            "Part Cost": float(part_cost) if part_cost is not None else None,
            "Part Price": None,
            "Labor Cost": labor_cost_default,
            "Labor Rate": labor_rate_default,
            "Labor Hours": None,
            "Warranty": None,
            "Status": "processed",
            "Status Reason": "",
            "Enrichment URL Hint": _manufacturer_site_hint(str(source_manufacturer), str(part_number) if part_number else None),
            "source_file": source.source_file,
            "source_sheet": source.source_sheet,
            "source_row_number": source.source_row_number,
        }

        if part_cost is not None:
            try:
                out_row["Part Price"] = float(markup_profile.price_for_cost(part_cost))
            except Exception as exc:
                out_row["Status"] = "manual_review"
                out_row["Status Reason"] = f"markup_error:{exc}"
        else:
            out_row["Status"] = "manual_review"
            out_row["Status Reason"] = "missing_or_invalid_cost"

        if not part_number:
            out_row["Status"] = "manual_review"
            prior = out_row["Status Reason"]
            out_row["Status Reason"] = f"{prior};missing_part_number" if prior else "missing_part_number"

        missing_required = [col for col in required_columns if out_row.get(col) in (None, "")]
        if missing_required:
            out_row["Status"] = "manual_review"
            prior = out_row["Status Reason"]
            req_msg = "missing_required:" + ",".join(missing_required)
            out_row["Status Reason"] = f"{prior};{req_msg}" if prior else req_msg

        for cw in crosswalk:
            out_row.setdefault(cw.output_column, out_row.get(cw.output_column))

        status = out_row["Status"]
        if status == "processed":
            counters["rows_processed"] += 1
        else:
            counters["rows_manual_review"] += 1
            counters["rows_incomplete"] += 1

        output.append(MappedRow(row=out_row, status=out_row["Status"], status_reason=out_row["Status Reason"]))

    return output, counters
