"""URL generation for Japanese infectious disease surveillance data.

This module generates URLs for downloading data from the National Institute of
Infectious Diseases (NIID) surveillance system. It handles different URL patterns
across years and dataset types, reflecting changes in the official data structure.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from .config import get_config
from .http import cached_head

# Base URLs for different data repositories
BASE_KAKO = "https://idsc.niid.go.jp/idwr/CDROM/Kako/"
BASE_YDATA = "https://id-info.jihs.go.jp/niid/images/idwr/ydata/"
BASE_ANNUAL = "https://id-info.jihs.go.jp/surveillance/idwr/annual/"


@dataclass
class ConfirmedRule:
    """Rule for constructing confirmed cases data URLs.

    Different years use different base URLs and filename patterns. This class
    encapsulates that variation.

    Attributes:
        start: First year this rule applies to (inclusive).
        end: Last year this rule applies to (inclusive).
        base: Base URL for this time period.
        pattern: Filename pattern with placeholders for year/h_year.
    """

    start: int
    end: int
    base: str
    pattern: str


# Rules for sex-disaggregated data (available from 1999)
RULES_SEX = [
    ConfirmedRule(1999, 2000, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_11.xls"),
    ConfirmedRule(2001, 2010, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_01_1.xls"),
    ConfirmedRule(2011, 2013, BASE_YDATA, "{year}/Syuukei/Syu_01_1.xls"),
    ConfirmedRule(2014, 2020, BASE_YDATA, "{year}/Syuukei/Syu_01_1.xlsx"),
    ConfirmedRule(2021, 9999, BASE_ANNUAL, "{year}/syulist/Syu_01_1.xlsx"),
]

# Rules for place data (available from 2001)
RULES_PLACE = [
    ConfirmedRule(2001, 2010, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_02_1.xls"),
    ConfirmedRule(2011, 2013, BASE_YDATA, "{year}/Syuukei/Syu_02_1.xls"),
    ConfirmedRule(2014, 2020, BASE_YDATA, "{year}/Syuukei/Syu_02_1.xlsx"),
    ConfirmedRule(2021, 9999, BASE_ANNUAL, "{year}/syulist/Syu_02_1.xlsx"),
]

# Annual sentinel tables: weekly reported cases by sex, prefecture, and week
# (table 1-2 in 1999-2000, table 8-1 from 2001).
RULES_SENTINEL = [
    ConfirmedRule(1999, 2000, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_12.xls"),
    ConfirmedRule(2001, 2010, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_08_1.xls"),
    ConfirmedRule(2011, 2013, BASE_YDATA, "{year}/Syuukei/Syu_08_1.xls"),
    ConfirmedRule(2014, 2020, BASE_YDATA, "{year}/Syuukei/Syu_08_1.xlsx"),
    ConfirmedRule(2021, 9999, BASE_ANNUAL, "{year}/syulist/Syu_08_1.xlsx"),
]

# Annual sentinel tables: weekly cases per sentinel (table 8-2). The 1999-2000
# equivalent (table 4-3) is organised by week and is not parsed.
RULES_SENTINEL_RATE = [
    ConfirmedRule(2001, 2010, BASE_KAKO, "H{h_year:02d}/Syuukei/Syu_08_2.xls"),
    ConfirmedRule(2011, 2013, BASE_YDATA, "{year}/Syuukei/Syu_08_2.xls"),
    ConfirmedRule(2014, 2020, BASE_YDATA, "{year}/Syuukei/Syu_08_2.xlsx"),
    ConfirmedRule(2021, 9999, BASE_ANNUAL, "{year}/syulist/Syu_08_2.xlsx"),
]

AnnualTable = Literal["sex", "place", "sentinel", "sentinel_rate"]
_ANNUAL_RULES: dict[str, list[ConfirmedRule]] = {
    "sex": RULES_SEX,
    "place": RULES_PLACE,
    "sentinel": RULES_SENTINEL,
    "sentinel_rate": RULES_SENTINEL_RATE,
}


def url_annual(year: int, table: AnnualTable) -> str | None:
    """Get the URL of an annual IDWR table, or None if it does not exist for the year.

    Args:
        year: Surveillance year.
        table: "sex" and "place" (notifiable diseases), "sentinel" (weekly
            sentinel counts), or "sentinel_rate" (weekly cases per sentinel).
    """
    for rule in _ANNUAL_RULES[table]:
        if rule.start <= year <= rule.end:
            return f"{rule.base}{rule.pattern.format(year=year, h_year=year - 1988)}"
    return None


def url_confirmed(year: int, type: Literal["sex", "place"] = "sex") -> str:
    """Get the URL for confirmed cases Excel file.

    Constructs the URL for downloading sex-disaggregated or place-specific
    confirmed case data for a given year, accounting for historical URL
    structure changes.

    Args:
        year: Year of the data (e.g., 2023).
        type: Dataset type ("sex" or "place").

    Returns:
        Full URL for downloading the Excel file.

    Raises:
        ValueError: If year is invalid for the given type, or if no URL
            rule exists for the specified year and type.

    Example:
        >>> url_confirmed(2023, "sex")
        'https://id-info.jihs.go.jp/surveillance/idwr/annual/2023/syulist/Syu_01_1.xlsx'
    """
    rules = RULES_SEX if type == "sex" else RULES_PLACE

    # Validation: place data only available from 2001
    if type == "place" and year <= 2000:
        raise ValueError("Year must be >= 2001 for place data.")

    for rule in rules:
        if rule.start <= year <= rule.end:
            # h_year is Heisei year (year - 1988)
            h_year = year - 1988
            path = rule.pattern.format(year=year, h_year=h_year)
            return f"{rule.base}{path}"

    raise ValueError(f"No URL rule found for year {year} and type {type}")


def _require_ok(response: object, url: str) -> None:
    """Fail loudly on anything but 200: only a 404 means "not published".

    Treating a transient 5xx or 403 as "not published" would silently drop a
    week from the build.
    """
    status = getattr(response, "status_code", None)
    if status != 200:
        raise RuntimeError(f"Unexpected HTTP status {status} checking {url}")


def url_bullet(
    year: int,
    week: int | Iterable[int] | None = None,
) -> list[str]:
    """Get URLs for weekly bulletin data (rapid surveillance reports).

    Generates URLs for weekly CSV reports. Only available from 2024 onwards.
    This function checks URL availability using HEAD requests and only returns
    URLs that exist on the server.

    Args:
        year: Year of the data (must be > 2023).
        week: Week number(s) (1-53). If None, checks all weeks.

    Returns:
        List of valid URLs for available weeks.

    Raises:
        ValueError: If year <= 2023 or if week numbers are out of range.

    Note:
        Always uses English version of bulletins.

    Example:
        >>> url_bullet(2024, week=1)
        ['https://id-info.jihs.go.jp/en/surveillance/idwr/rapid/2024/01/zensu01.csv']
    """
    if year <= 2023:
        raise ValueError("Year must be > 2023 for bullet data.")

    # Normalize week parameter
    if week is None:
        weeks = list(range(1, 54))
    elif isinstance(week, int):
        weeks = [week]
    else:
        weeks = list(week)

    # Validate week range
    # ISO years with 53 weeks (e.g. 2026) publish a week-53 report.
    weeks = [w for w in weeks if 1 <= w <= 53]
    if not weeks:
        raise ValueError("Week must be between 1 and 53.")

    urls: list[str] = []
    config = get_config()

    for w in weeks:
        base = "https://id-info.jihs.go.jp/en/surveillance/idwr/rapid/"
        url = f"{base}{year}/{w:02d}/zensu{w:02d}.csv"
        resp = cached_head(url, config)
        if resp.status_code == 404:
            continue  # not published (yet)
        _require_ok(resp, url)
        content_length = resp.headers.get("content-length")
        if content_length is None or content_length == "" or int(content_length) > 0:
            urls.append(url)

    return urls


def url_sentinel(
    year: int,
    week: int | Iterable[int] | None = None,
) -> list[str]:
    r"""Get URLs for sentinel surveillance data (teitenrui).

    Generates URLs for sentinel (teitenrui) CSV reports across historical URL patterns.
    This function checks URL availability using HEAD requests and only returns URLs
    that exist on the server.

    Args:
        year: Year of the data (must be >= 1999).
        week: Week number(s) (1-53). If None, checks all weeks.

    Returns:
        List of valid URLs for available weeks.

    Raises:
        ValueError: If year < 1999 or if week numbers are out of range.

    Note:
        Sentinel data contains diseases like RSV, Influenza, HFMD monitored
        through ~3,000 designated sentinel clinics across Japan.
        Uses English version from /rapid/ path.

    Example:
        >>> url_sentinel(2025, week=4)
        ['https://id-info.jihs.go.jp/en/surveillance/idwr/rapid/2025/04/teitenrui04.csv']
    """
    if year < 1999:
        raise ValueError("Year must be >= 1999 for sentinel data.")

    # Normalize week parameter
    if week is None:
        weeks = list(range(1, 54))
    elif isinstance(week, int):
        weeks = [week]
    else:
        weeks = list(week)

    # Validate week range
    weeks = [w for w in weeks if 1 <= w <= 53]
    if not weeks:
        raise ValueError("Week must be between 1 and 53.")

    urls: list[str] = []

    for w in weeks:
        # URL patterns evolved over time.
        if year >= 2023:
            candidates = [
                f"https://id-info.jihs.go.jp/en/surveillance/idwr/rapid/{year}/{w:02d}/teitenrui{w:02d}.csv"
            ]
        elif year >= 2015:
            candidates = [
                f"https://id-info.jihs.go.jp/niid/images/idwr/data-e/idwr-e{year}/{year}{w:02d}/teitenrui{w:02d}.csv"
            ]
        else:
            yyww = f"{year % 100:02d}{w:02d}"
            candidates = [
                f"https://id-info.jihs.go.jp/niid/images/idwr/data-e/idwr-e{year}/{yyww}/teitenrui{w:02d}.csv",
                f"https://www.niid.go.jp/niid/images/idwr/data-e/idwr-e{year}/{yyww}/teitenrui{w:02d}.csv",
            ]

        for url in candidates:
            # Check if URL exists
            resp = cached_head(url, get_config())
            if resp.status_code == 404:
                continue  # not published (yet), or not on this host
            _require_ok(resp, url)
            content_length = resp.headers.get("content-length")
            if content_length is None or content_length == "" or int(content_length) > 0:
                urls.append(url)
                break

    return urls
