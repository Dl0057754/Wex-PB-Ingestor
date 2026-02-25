# Wex Pricebook Ingestor (v0.3)

Runnable CLI for converting distributor files into:
- normalized CSV output,
- template-native workbook output (`.xlsx`),
- QA summary JSON,
- manual-review CSV,
- enrichment-enriched CSV from manufacturer websites.

## Install

```bash
python -m pip install -e .
```

## Commands

### 1) Analyze source file

```bash
pb-ingestor analyze "samples/distributor-books/Gallatin River All Pricing 9-15-25.xlsx"
```

### 2) Convert single source file

```bash
pb-ingestor convert "samples/distributor-books/Gallatin River All Pricing 9-15-25.xlsx" \
  --template-type bundle \
  --labor-rate-default 125 \
  --output-csv out/converted/gallatin.csv \
  --output-workbook out/converted/gallatin.xlsx \
  --qa-json out/qa/gallatin.json \
  --manual-review-csv out/qa/gallatin_manual_review.csv
```

### 3) Batch convert all files from manifest

```bash
pb-ingestor convert-all \
  --manifest config/mappings/customer_manifest.csv \
  --out-dir out \
  --consolidated-qa out/qa/consolidated.json
```

### 4) Enrich converted CSV from manufacturer websites

```bash
pb-ingestor enrich out/converted/gallatin.csv \
  --domains-config config/enrichment/manufacturer_domains.json \
  --output-csv out/enriched/gallatin_enriched.csv \
  --qa-json out/qa/gallatin_enrichment.json
```

### 5) Validate JSON against schema (full JSON Schema validation)

```bash
pb-ingestor validate out/qa/gallatin.json --schema schemas/qa_run_report.schema.json
```

## Current capabilities

- Standard `.xlsx` ingestion with visible-sheet processing and merged-cell value propagation.
- Hardened fallback parser for malformed files with multi-strategy parsing (`csv`, `tsv`, `;`, `|`, fixed-width).
- Embedded asset reference scanning (`jpg/png/pdf/docx`) surfaced in QA output.
- Manufacturer part-number dedupe (`keep first`).
- Global tiered markup profile support with nearest-cent rounding and overlap validation.
- Template workbook writer that fills matching columns by header name.
- Manual-review export for rows missing required data.
- Website enrichment flow with manufacturer-domain allowlist and confidence/status fields.

## Testing

```bash
pytest -q
```

## Notes

- This implementation intentionally avoids vendor APIs.
- Website enrichment is best-effort and should be reviewed in QA before final publishing.
