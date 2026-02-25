from decimal import Decimal

from pb_ingestor.crosswalk import CrosswalkRow
from pb_ingestor.ingest import SourceRow
from pb_ingestor.mapper import map_rows
from pb_ingestor.markup import MarkupProfile, MarkupTier


def _profile():
    return MarkupProfile([MarkupTier(min_cost=Decimal("0.01"), max_cost=None, markup_percent=Decimal("100"), order=1)])


def test_mapper_dedupes_part_number():
    crosswalk = [
        CrosswalkRow("Single part", "Single part", "Manufacturer Part Number", True, "", "", ""),
        CrosswalkRow("Single part", "Single part", "Part Cost", True, "", "", ""),
    ]
    rows = [
        SourceRow("f.xlsx", "S", 2, {"part number": "ABC-1", "cost": "10"}),
        SourceRow("f.xlsx", "S", 3, {"part number": "ABC-1", "cost": "20"}),
    ]
    mapped, counters = map_rows(rows, crosswalk, _profile())
    assert len(mapped) == 1
    assert counters["rows_duplicates_ignored"] == 1


def test_mapper_required_fields_trigger_manual_review():
    crosswalk = [CrosswalkRow("Single part", "Single part", "Manufacturer Part Number", True, "", "", "")]
    rows = [SourceRow("f.xlsx", "S", 2, {"description": "x", "cost": "10"})]
    mapped, counters = map_rows(rows, crosswalk, _profile())
    assert mapped[0].status == "manual_review"
    assert counters["rows_incomplete"] == 1


def test_missing_cost_no_longer_blocks_processed_status():
    crosswalk = [
        CrosswalkRow("Single part", "Single part", "Manufacturer Part Number", True, "", "", ""),
        CrosswalkRow("Single part", "Single part", "Part Cost", True, "", "", ""),
        CrosswalkRow("Single part", "Single part", "Part Price", True, "", "", ""),
    ]
    rows = [SourceRow("Bryant File.xlsx", "GAS FURNACES", 2, {"part number": "ABC-1"})]
    mapped, counters = map_rows(rows, crosswalk, _profile())
    assert mapped[0].status == "processed"
    assert "warning_missing_cost" in mapped[0].status_reason
    assert counters["rows_processed"] == 1


def test_brand_inferred_from_filename_over_sheet_category():
    crosswalk = [CrosswalkRow("Single part", "Single part", "Manufacturer Part Number", True, "", "", "")]
    rows = [SourceRow("Bryant R454B.xlsx", "GAS FURNACES", 2, {"part number": "ABC-1"})]
    mapped, _ = map_rows(rows, crosswalk, _profile())
    assert mapped[0].row["Manufacturer"] == "Bryant"
    assert "site:bryant.com" in (mapped[0].row["Enrichment URL Hint"] or "")
