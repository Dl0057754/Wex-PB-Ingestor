# Mapping Contract (v1)

This document captures the finalized mapping rules for converting distributor pricebooks into supported output templates.

## 1) Supported Output Templates

User selects output template at runtime:
- `Single part` (`samples/base-templates/Single part template.xlsx`)
- `Bundle (1 part, 1 labor)` (`samples/base-templates/Single part single labor template.xlsx`)
- `Supplier Loader` (`samples/base-templates/Supplier Loader Template JUN2024.xlsx`, sheets `Sheet1` + `FORM`)

### Template naming and required fields
- Preserve **exact sheet and column naming** already present in the target template.
- Any column marked `*Required` in the template is treated as required for data completeness checks.
- Goal is to populate as many fields as possible while retaining traceability for missing data.

## 2) Canonical Row Model

Each imported source row should be represented with normalized + original values:
- `source_file`
- `source_sheet`
- `source_row_number`
- `manufacturer_part_number_original`
- `manufacturer_part_number_normalized`
- `manufacturer`
- `brand`
- `product_family`
- `category`
- `description_source`
- `description_enriched`
- `warranty_raw`
- `warranty_aggregated`
- `part_cost`
- `part_price`
- `labor_cost`
- `labor_rate`
- `labor_hours`
- `status` (`processed`, `incomplete`, `manual_review`)
- `status_reason`

## 3) Part Number Rules

- Canonical part identity is **manufacturer part number**.
- Store both original and normalized values.
- Normalized matching is case-insensitive.
- Duplicate handling: dedupe by normalized manufacturer part number and **keep first occurrence**.

## 4) Source Inference Rules

### Manufacturer
Use this priority order:
1. Explicit source manufacturer/mfr column
2. Source sheet name
3. Pricebook filename/customer context

### Brand / Category / Product family
Use all three signals where available:
- Sheet name (typically brand)
- Merged-row headers (typically product family)
- Explicit category/product columns

## 5) Merged Cell Behavior

- Treat merged header rows as context that applies to subsequent detail rows.
- Support merged ranges that represent either:
  - Product family/category labels, or
  - Repeated part-number/grouped part context
- Propagate merged values to child/detail rows when safe.
- If intent is ambiguous, flag row for manual review.

## 6) Sheet Processing Rules

- Process all **visible** sheets.
- Do **not** exclude `Paste bid here`; it may contain customer cost/pricing context.
- If `Paste bid here` is currently blank, do not fail run; continue.

## 7) Cost and Price Rules

- Pricebooks provide **Part Cost** input.
- Apply global tiered markup profile to compute **Part Price**.
- Rounding mode: **nearest cent**.

Formula:
- `part_price = round(part_cost * (1 + markup_percent / 100), 2)`

## 8) Bundle Template Labor Rules

For `Bundle (1 part, 1 labor)` output:
- Include and populate columns for `Labor Cost` and `Labor Rate`.
- `Labor Hours` is intentionally left blank for customer manual entry.
- Labor values may come from user-supplied form fields/profile defaults.

Recommended derived values if labor hours is provided:
- `labor_price = labor_rate * labor_hours`
- `total_price = part_price + labor_price`

If labor hours is missing, keep row but flag as incomplete/manual-review as needed by QA policy.

## 9) Enrichment Rules (Manufacturer Website Scraping)

- Source priority:
  1. Source workbook description
  2. Manufacturer website scrape
  3. Completed-template reference

Description overwrite policy:
- Overwrite when source description is blank
- Overwrite when scrape confidence is high

Matching policy:
- Allow close/family matches (not exact-only), especially for model families with size variants.

Warranty policy:
- Pull and aggregate all available warranty values found.
- If none found, leave blank.

## 10) Malformed/Non-Standard Input Handling

For malformed `.xlsx` files (e.g., non-zip payloads):
- Attempt fallback extraction/parsing paths (see `docs/mapping/fallback_importer_spec.md`).
- Continue processing other files and report failures/incompletes.
- Capture embedded asset references where possible (jpg/png/pdf/docx).

## 11) Error and Review Policy

- Missing required output fields => set status to `manual_review` and include reason.
- Rows with unrecoverable parse/mapping ambiguity => `manual_review`.
- Duplicate part numbers after first kept occurrence can be silently ignored but counted in QA summary.

## 12) Run Acceptance Policy

- All runs are considered complete if pipeline finishes and reports outcomes.
- Emit run summary in the format:
  - `X rows processed`
  - `Y rows incomplete`
  - `Z rows manual review`
- Example: `400 processed / 100 incomplete`.
