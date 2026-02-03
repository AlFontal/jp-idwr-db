from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from .config import get_config
from .http import cached_head

BASE_KAKO = "https://idsc.niid.go.jp/idwr/CDROM/Kako/"
BASE_YDATA = "https://id-info.jihs.go.jp/niid/images/idwr/ydata/"
BASE_ANNUAL = "https://id-info.jihs.go.jp/surveillance/idwr/annual/"


@dataclass
class ConfirmedRule:
    start: int
    end: int
    base: str
    pattern: str


RULES_SEX = [
    ConfirmedRule(
        1999, 2000, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_11.xls"
    ),  # R uses Syu_11 for 99-00
    ConfirmedRule(2001, 2010, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_01_1.xls"),
    ConfirmedRule(2011, 2013, BASE_YDATA, "{year}/Syuukei/Syu_01_1.xls"),
    ConfirmedRule(2014, 2020, BASE_YDATA, "{year}/Syuukei/Syu_01_1.xlsx"),
    ConfirmedRule(2021, 9999, BASE_ANNUAL, "{year}/syulist/Syu_01_1.xlsx"),
]

RULES_PLACE = [
    ConfirmedRule(2001, 2010, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_02_1.xls"),
    ConfirmedRule(2011, 2013, BASE_YDATA, "{year}/Syuukei/Syu_02_1.xls"),
    ConfirmedRule(2014, 2020, BASE_YDATA, "{year}/Syuukei/Syu_02_1.xlsx"),
    ConfirmedRule(2021, 9999, BASE_ANNUAL, "{year}/syulist/Syu_02_1.xlsx"),
]


def url_confirmed(year: int, type: Literal["sex", "place"] = "sex") -> str:
    """
    Get the URL for the confirmed cases Excel file.
    """
    rules = RULES_SEX if type == "sex" else RULES_PLACE

    # Validation
    if type == "place" and year <= 2000:
        raise ValueError("Year must be >= 2001 for place data.")

    for rule in rules:
        if rule.start <= year <= rule.end:
            h_year = year - 1988
            path = rule.pattern.format(year=year, h_year=h_year)
            return f"{rule.base}{path}"

    # Fallback/Fail safe
    raise ValueError(f"No URL rule found for year {year} and type {type}")


def url_bullet(
    year: int,
    week: int | Iterable[int] | None = None,
    lang: Literal["en", "ja"] = "en",
) -> list[str]:
    if year <= 2023:
        raise ValueError("Year must be > 2023 for bullet data.")

    if week is None:
        weeks = list(range(1, 54))
    elif isinstance(week, int):
        weeks = [week]
    else:
        weeks = list(week)

    weeks = [w for w in weeks if 1 <= w <= 53]
    if not weeks:
        raise ValueError("Week must be between 1 and 53.")

    urls: list[str] = []
    config = get_config()

    for w in weeks:
        if lang == "en":
            base = "https://id-info.jihs.go.jp/surveillance/idwr/en/rapid/"
            path = f"{year}/{w:02d}/zensu{w:02d}.csv"
        else:
            if year >= 2025 and w >= 11:
                base = "https://id-info.jihs.go.jp/surveillance/idwr/jp/rapid/"
            else:
                base = "https://id-info.jihs.go.jp/surveillance/idwr/rapid/"
            path = f"{year}/{w:d}/{year}-{w:02d}-zensu.csv"

        url = f"{base}{path}"
        try:
            resp = cached_head(url, config)
            if resp.status_code == 200:
                content_length = resp.headers.get("content-length", "0")
                if int(content_length) > 0:
                    urls.append(url)
        except Exception:
            continue

    return urls
