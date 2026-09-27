#!/usr/bin/env python3
"""Extract weekly sentinel counts from an IDWR weekly report PDF.

Some cumulative ``teitenrui`` CSVs are incomplete upstream (for example 2016-W37
stops after Kyoto). The IDWR weekly report PDF for the same week contains the
per-prefecture sentinel tables, so the missing weekly values can be recovered
from an official source instead of being imputed.

The output rows are appended to ``data/supplements/sentinel_weekly_reports.csv``.
The dataset build only uses rows for prefecture/disease/weeks that are missing
from the cumulative files.

Usage (pypdf is only needed for this one-off extraction)::

    uv run --with pypdf python scripts/extract_weekly_report_sentinel.py 2016 37
"""

from __future__ import annotations

import argparse
import csv
import io
import re
from pathlib import Path

import httpx
from pypdf import PdfReader  # type: ignore[import-not-found]

from jp_idwr_db.config import get_config
from jp_idwr_db.utils import PREFECTURE_ISO_MAP

OUTPUT = Path(__file__).resolve().parents[1] / "data" / "supplements" / "sentinel_weekly_reports.csv"
REPORT_URL = "https://id-info.jihs.go.jp/surveillance/idwr/idwr/{year}/idwr{year}-{week:02d}.pdf"
FIELDS = ["year", "week", "prefecture", "disease", "count", "per_sentinel", "source_url"]

# Weekly sentinel tables, identified by a marker unique to each page.
# Columns are (dataset disease name, has a per-sentinel column).
TABLES: list[tuple[str, list[tuple[str, bool]]]] = [
    (
        "RSウイルス",
        [
            ("Influenza(excld. avian influenza and pandemic influenza)", True),
            ("Respiratory syncytial virus infection", False),
            ("Pharyngoconjunctival fever", True),
            ("Group A streptococcal pharyngitis", True),
            ("Infectious gastroenteritis", True),
            ("Chickenpox", True),
            ("Hand, foot and mouth disease", True),
            ("Erythema infection", True),
            ("Exanthem subitum", True),
        ],
    ),
    (
        "百日咳",
        [
            ("Pertussis", True),
            ("Herpangina", True),
            ("Mumps", True),
            ("Acute hemorrhagic conjunctivitis", True),
            ("Epidemic keratoconjunctivitis", True),
            ("Bacterial meningitis", True),
            ("Aseptic meningitis", True),
            ("Mycoplasma pneumonia", True),
            ("Chlamydial pneumonia(excluding psittacosis)", True),
        ],
    ),
    ("ロ タ ウ イ ル ス", [("Infectious gastroenteritis (only by Rotavirus)", True)]),
]
NUMBER = re.compile(r"^(-|\d+(\.\d+)?)$")


def _value(token: str) -> float:
    """IDWR tables write zero as ``-``."""
    return 0.0 if token == "-" else float(token)


def _numeric_rows(text: str, width: int) -> list[list[str]]:
    rows = []
    for line in text.splitlines():
        tokens = line.split()
        if len(tokens) >= width and all(NUMBER.match(token) for token in tokens):
            rows.append(tokens[:width])
    return rows


def extract(year: int, week: int) -> list[dict[str, object]]:
    """Download one weekly report and return its per-prefecture sentinel rows."""
    url = REPORT_URL.format(year=year, week=week)
    config = get_config()
    response = httpx.get(
        url, headers={"User-Agent": config.user_agent}, timeout=60, follow_redirects=True
    )
    response.raise_for_status()
    pages = [page.extract_text() or "" for page in PdfReader(io.BytesIO(response.content)).pages]
    prefectures = list(PREFECTURE_ISO_MAP)

    records: list[dict[str, object]] = []
    for marker, columns in TABLES:
        matches = [
            text for text in pages if marker in text and "北海道" in text and "定点当り" in text
        ]
        if len(matches) != 1:
            raise ValueError(f"Expected one table page with {marker!r}, found {len(matches)}")
        width = sum(2 if has_rate else 1 for _, has_rate in columns)
        rows = _numeric_rows(matches[0], width)
        if len(rows) != len(prefectures) + 1:
            raise ValueError(f"Table {marker!r}: expected 48 rows (total + 47), got {len(rows)}")

        total_row, prefecture_rows = rows[0], rows[1:]
        position = 0
        for disease, has_rate in columns:
            counts = [_value(row[position]) for row in prefecture_rows]
            if sum(counts) != _value(total_row[position]):
                raise ValueError(f"{disease}: prefecture counts do not add up to the total")
            for prefecture, row, count in zip(prefectures, prefecture_rows, counts, strict=True):
                records.append(
                    {
                        "year": year,
                        "week": week,
                        "prefecture": prefecture,
                        "disease": disease,
                        "count": count,
                        "per_sentinel": _value(row[position + 1]) if has_rate else "",
                        "source_url": url,
                    }
                )
            position += 2 if has_rate else 1
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("year", type=int)
    parser.add_argument("week", type=int)
    args = parser.parse_args()

    records = extract(args.year, args.week)
    existing: list[dict[str, str]] = []
    if OUTPUT.exists():
        with OUTPUT.open(encoding="utf-8", newline="") as handle:
            existing = [
                row
                for row in csv.DictReader(handle)
                if (int(row["year"]), int(row["week"])) != (args.year, args.week)
            ]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(existing)
        writer.writerows(records)
    print(f"Wrote {len(records)} rows for {args.year}-W{args.week:02d} to {OUTPUT}")


if __name__ == "__main__":
    main()
