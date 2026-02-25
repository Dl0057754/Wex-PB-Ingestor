from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup


@dataclass
class EnrichmentResult:
    part_name: str | None
    description: str | None
    warranty: str | None
    source_url: str | None
    confidence: str
    status: str


def load_domain_allowlist(path: str | Path) -> dict[str, list[str]]:
    p = Path(path)
    if not p.exists():
        return {}
    data = json.loads(p.read_text())
    return {k.lower(): v for k, v in data.items()}


def _extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for el in soup(["script", "style", "noscript"]):
        el.decompose()
    return re.sub(r"\s+", " ", soup.get_text(" ")).strip()


def _pick_warranty(text: str) -> str | None:
    matches = re.findall(r"([^.]{0,80}warrant[^.]{0,120}\.)", text, flags=re.IGNORECASE)
    if not matches:
        return None
    unique = []
    seen = set()
    for m in matches:
        mm = m.strip()
        if mm.lower() in seen:
            continue
        seen.add(mm.lower())
        unique.append(mm)
    return " | ".join(unique[:3])


def _confidence(part_number: str, text: str) -> str:
    p = part_number.strip().lower()
    t = text.lower()
    if p and p in t:
        return "high"
    if p and any(token in t for token in re.split(r"[-\s]+", p) if token):
        return "medium"
    return "low"


def enrich_part(
    part_number: str,
    manufacturer: str | None,
    domains_by_manufacturer: dict[str, list[str]],
    timeout_s: float = 8.0,
) -> EnrichmentResult:
    if not part_number:
        return EnrichmentResult(None, None, None, None, "low", "not_found")

    m_key = (manufacturer or "").strip().lower()
    domains = domains_by_manufacturer.get(m_key, [])
    if not domains and m_key:
        # fallback heuristic based on manufacturer token
        domains = [m_key.replace("&", "and").replace(" ", "") + ".com"]

    candidates = []
    for d in domains[:3]:
        candidates.extend(
            [
                f"https://{d}/search?q={quote_plus(part_number)}",
                f"https://{d}/?s={quote_plus(part_number)}",
                f"https://{d}/{quote_plus(part_number)}",
            ]
        )

    headers = {"User-Agent": "Mozilla/5.0 (compatible; pb-ingestor/0.1)"}

    for url in candidates:
        try:
            resp = requests.get(url, timeout=timeout_s, headers=headers)
        except Exception:
            continue
        if resp.status_code >= 400 or not resp.text:
            continue

        text = _extract_text(resp.text)
        conf = _confidence(part_number, text)
        if conf == "low":
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.title.get_text(" ", strip=True) if soup.title else None
        description = None
        md = soup.find("meta", attrs={"name": "description"})
        if md and md.get("content"):
            description = md["content"].strip()
        if not description:
            description = text[:350] if text else None

        warranty = _pick_warranty(text)
        return EnrichmentResult(title, description, warranty, url, conf, "enriched")

    return EnrichmentResult(None, None, None, None, "low", "not_found")


def enrich_csv(
    input_csv: str | Path,
    output_csv: str | Path,
    qa_json: str | Path,
    domains_config: str | Path,
    sleep_ms: int = 100,
) -> dict[str, Any]:
    allowlist = load_domain_allowlist(domains_config)
    rows = list(csv.DictReader(Path(input_csv).open(newline="")))

    counters = {"rows_total": len(rows), "enriched": 0, "not_found": 0, "ambiguous": 0, "blocked": 0}
    for row in rows:
        part_number = (row.get("Manufacturer Part Number") or row.get("manufacturer_part_number_original") or "").strip()
        manufacturer = (row.get("Manufacturer") or "").strip()
        result = enrich_part(part_number, manufacturer, allowlist)

        row["Enriched Part Name"] = result.part_name
        row["Enriched Description"] = result.description
        row["Enriched Warranty"] = result.warranty
        row["Enrichment Source URL"] = result.source_url
        row["Enrichment Confidence"] = result.confidence
        row["Enrichment Status"] = result.status

        if result.status in counters:
            counters[result.status] += 1
        elif result.status == "enriched":
            counters["enriched"] += 1
        else:
            counters["not_found"] += 1

        time.sleep(max(0, sleep_ms) / 1000)

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    qa = {"summary": counters, "domains_config": str(domains_config)}
    qa_out = Path(qa_json)
    qa_out.parent.mkdir(parents=True, exist_ok=True)
    qa_out.write_text(json.dumps(qa, indent=2))
    return qa
