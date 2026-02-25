from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CrosswalkRow:
    output_template: str
    output_sheet: str
    output_column: str
    required: bool
    source_priority: str
    transform_rule: str
    notes: str


@dataclass(frozen=True)
class ManifestRow:
    customer_name: str
    source_file: str
    completed_template: str
    base_template: str
    output_type: str
    notes: str


def load_crosswalk(path: str | Path) -> list[CrosswalkRow]:
    rows: list[CrosswalkRow] = []
    with Path(path).open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                CrosswalkRow(
                    output_template=row["output_template"],
                    output_sheet=row["output_sheet"],
                    output_column=row["output_column"],
                    required=row["required"].strip().lower() == "yes",
                    source_priority=row["source_priority"].strip(),
                    transform_rule=row["transform_rule"].strip(),
                    notes=row.get("notes", "").strip(),
                )
            )
    return rows


def load_manifest(path: str | Path) -> list[ManifestRow]:
    rows: list[ManifestRow] = []
    with Path(path).open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                ManifestRow(
                    customer_name=row["customer_name"].strip(),
                    source_file=row["source_file"].strip(),
                    completed_template=row.get("completed_template", "").strip(),
                    base_template=row.get("base_template", "").strip(),
                    output_type=row.get("output_type", "").strip(),
                    notes=row.get("notes", "").strip(),
                )
            )
    return rows


def infer_crosswalk_path(template_type: str) -> Path:
    mapping = {
        "single_part": Path("config/mappings/crosswalk_single_part.csv"),
        "bundle": Path("config/mappings/crosswalk_bundle_single_part_single_labor.csv"),
        "supplier_loader": Path("config/mappings/crosswalk_supplier_loader.csv"),
    }
    if template_type not in mapping:
        raise ValueError(f"Unknown template type: {template_type}")
    return mapping[template_type]


def infer_base_template_path(template_type: str) -> Path:
    mapping = {
        "single_part": Path("samples/base-templates/Single part template.xlsx"),
        "bundle": Path("samples/base-templates/Single part single labor template.xlsx"),
        "supplier_loader": Path("samples/base-templates/Supplier Loader Template JUN2024.xlsx"),
    }
    if template_type not in mapping:
        raise ValueError(f"Unknown template type: {template_type}")
    return mapping[template_type]
