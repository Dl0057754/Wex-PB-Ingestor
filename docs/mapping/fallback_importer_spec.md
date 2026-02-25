# Fallback Importer Spec for Malformed Distributor Files

This spec defines fallback behavior for source files that cannot be parsed as valid OpenXML `.xlsx` archives.

## Target examples
- `samples/distributor-books/Kinzer Air Glacier Supply Price Book 6-19-2025.xlsx`
- `samples/distributor-books/temp Control Glacier Supply Price Book 6-17-2025.xlsx`

## Detection

Treat input as malformed workbook when:
- Zip/OpenXML container parsing fails, or
- required workbook parts (`xl/workbook.xml`, worksheet parts) are missing.

## Fallback pipeline

### Stage 1: Byte-level file typing
- Detect likely format signatures for:
  - text/csv/tsv
  - xml/html
  - binary office
  - image/pdf/docx containers

### Stage 2: Tabular extraction attempts
Try in order until one succeeds:
1. CSV parser (`,` delimiter) + heuristic header detection
2. TSV parser (`\t` delimiter)
3. Delimiter sniffing parser (`,`, `;`, `|`, tab)
4. XML table extraction (if XML/HTML-like)
5. Fixed-width heuristics (last resort)

### Stage 3: Worksheet emulation
- Create synthetic sheet names when true worksheet metadata is absent:
  - `Recovered Sheet 1`, `Recovered Sheet 2`, etc.
- Preserve original row index for traceability.

### Stage 4: Asset detection and reference capture
Scan payload for and/or parse embedded references to:
- `.jpg`, `.jpeg`, `.png`
- `.pdf`
- `.docx`

For each discovered asset reference, capture:
- `asset_type`
- `asset_name_or_ref`
- `source_offset_or_context`
- `associated_row` (if inferable)

### Stage 5: Mapping handoff
- Emit canonical row objects with confidence tags:
  - `high`, `medium`, `low`
- Low-confidence rows route to manual review queue.

## Error handling

- Fallback importer should never abort entire batch.
- On per-file failure, emit structured file-level error:
  - `file_name`
  - `error_stage`
  - `error_message`
  - `rows_recovered`

## QA reporting hooks

Include counters in run summary:
- `files_processed_standard`
- `files_processed_fallback`
- `files_failed`
- `rows_recovered_fallback`
- `rows_manual_review_fallback`

## Notes

- Fallback extraction is best-effort and may not retain full workbook semantics.
- Manual review is expected for malformed inputs and should be visible in QA outputs.
