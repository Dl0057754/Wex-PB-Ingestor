from pathlib import Path

from pb_ingestor.ingest import ingest_xlsx


def test_fallback_for_non_xlsx(tmp_path: Path):
    p = tmp_path / "bad.xlsx"
    p.write_text("part number,cost\nABC-1,10\n")
    result = ingest_xlsx(p)
    assert result.mode == "fallback"
    assert len(result.rows) == 1
