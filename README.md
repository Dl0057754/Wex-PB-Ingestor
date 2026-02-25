# Wex Pricebook Ingestor (v0.2)

Runnable CLI for converting distributor files into:
- normalized CSV output,
- template-native workbook output (`.xlsx`),
- QA summary JSON,
- manual-review CSV.

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

### 4) Validate QA JSON required keys

```bash
pb-ingestor validate out/qa/gallatin.json --schema schemas/qa_run_report.schema.json
```

## Current capabilities

- Standard `.xlsx` ingestion with visible-sheet processing and merged-cell value propagation.
- Fallback parser for malformed files (CSV-style recovery attempt).
- Basic embedded asset reference scanning (`jpg/png/pdf/docx`) surfaced in QA output.
- Manufacturer part-number dedupe (`keep first`).
- Global tiered markup profile support with nearest-cent rounding.
- Template workbook writer that fills matching columns by header name.
- Manual-review export for rows missing required data.

## Notes

- This implementation intentionally avoids vendor APIs.
- Enrichment currently provides manufacturer-site search URL hints (`Enrichment URL Hint`) as a manual/web-scrub starting point.
- Deep scraping and advanced confidence scoring are planned follow-up enhancements.
