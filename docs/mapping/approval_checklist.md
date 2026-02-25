# Mapping Approval Checklist

Use this to approve pre-implementation mapping decisions.

## A. Template and field rules
- [ ] Preserve exact target template column names and sheet names.
- [ ] Treat all `*Required` columns as mandatory.
- [ ] Missing required values route row to manual review.

## B. Part identity and dedupe
- [ ] Canonical key = manufacturer part number.
- [ ] Store normalized + original part number values.
- [ ] Dedupe by normalized part number, keep first occurrence.

## C. Pricing and markup
- [ ] Pricebook cost maps to `Part Cost`.
- [ ] Global tiered markup profile computes `Part Price`.
- [ ] Rounding is nearest cent.

## D. Bundle behavior
- [ ] Include `Labor Cost` and `Labor Rate`.
- [ ] Leave `Labor Hours` blank for customer manual entry.
- [ ] Missing labor hours can be flagged as incomplete.

## E. Source extraction
- [ ] Process all visible sheets, including `Paste bid here`.
- [ ] Use merged rows as contextual headers when appropriate.
- [ ] Infer manufacturer/category from sheet names and file context when needed.

## F. Enrichment
- [ ] Allow close/family part matches for scraping.
- [ ] Overwrite description when source is blank or confidence is high.
- [ ] Aggregate all warranty values; blank if not found.

## G. Fallback and QA reporting
- [ ] Attempt fallback parsing for malformed workbook files.
- [ ] Capture asset references (jpg/png/pdf/docx) when possible.
- [ ] Emit run summary counts (processed/incomplete/manual review).
