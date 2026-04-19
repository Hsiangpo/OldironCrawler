from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urljoin, urlparse

import httpx

from oldironcrawler.extractor.protocol_client import HtmlPage


_UMBRACO_ALIAS_RE = re.compile(r"media\.umbraco\.io/([a-z0-9_-]+)/", re.IGNORECASE)
_PEOPLE_PATH_HINTS = ("our-people", "/people", "leadership", "team", "executive")
_ALL_BIO_QUERY = """
query OldIronAllBio {
  allBio {
    items {
      name
      url
      jobTitle
      emailAddress
      department {
        name
      }
      location {
        name
      }
    }
  }
}
""".strip()


@dataclass
class UmbracoBio:
    name: str
    url: str
    job_title: str
    email_address: str
    departments: list[str]
    location: str


def maybe_build_umbraco_people_page(
    *,
    website: str,
    pages: list[HtmlPage],
    proxy_url: str,
    timeout_seconds: float,
) -> HtmlPage | None:
    people_page = _find_people_page(pages)
    if people_page is None:
        return None
    project_alias = _extract_project_alias(pages)
    if not project_alias:
        return None
    bios = _fetch_umbraco_bios(
        project_alias=project_alias,
        website=website,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    if not bios:
        return None
    return HtmlPage(url=people_page.url, html=_render_people_html(website, bios))


def _find_people_page(pages: list[HtmlPage]) -> HtmlPage | None:
    for page in pages:
        lowered = str(page.url or "").lower()
        if any(hint in lowered for hint in _PEOPLE_PATH_HINTS):
            return page
    return None


def _extract_project_alias(pages: list[HtmlPage]) -> str:
    for page in pages:
        match = _UMBRACO_ALIAS_RE.search(str(page.html or ""))
        if match is not None:
            return str(match.group(1) or "").strip()
    return ""


def _fetch_umbraco_bios(
    *,
    project_alias: str,
    website: str,
    proxy_url: str,
    timeout_seconds: float,
) -> list[UmbracoBio]:
    client_kwargs: dict[str, object] = {
        "follow_redirects": True,
        "headers": {"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        "timeout": timeout_seconds,
        "trust_env": False,
    }
    if proxy_url:
        client_kwargs["proxy"] = proxy_url
    origin = _origin(website)
    headers = {
        "umb-project-alias": project_alias,
        "content-type": "application/json",
        "origin": origin,
        "referer": origin + "/",
    }
    payload = {"operationName": "OldIronAllBio", "variables": {}, "query": _ALL_BIO_QUERY}
    try:
        with httpx.Client(**client_kwargs) as client:
            response = client.post("https://graphql.umbraco.io/", headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
    except Exception:  # noqa: BLE001
        return []
    items = (((data or {}).get("data") or {}).get("allBio") or {}).get("items") or []
    result: list[UmbracoBio] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "").strip()
        job_title = str(item.get("jobTitle", "") or "").strip()
        if not name or not job_title:
            continue
        result.append(
            UmbracoBio(
                name=name,
                url=urljoin(origin, str(item.get("url", "") or "").strip()),
                job_title=job_title,
                email_address=str(item.get("emailAddress", "") or "").strip(),
                departments=[
                    str(department.get("name", "") or "").strip()
                    for department in (item.get("department") or [])
                    if isinstance(department, dict) and str(department.get("name", "") or "").strip()
                ],
                location=str(((item.get("location") or {}).get("name", "") if isinstance(item.get("location"), dict) else "") or "").strip(),
            )
        )
    return _rank_bios(result)[:40]


def _rank_bios(bios: list[UmbracoBio]) -> list[UmbracoBio]:
    return sorted(bios, key=lambda bio: (-_bio_score(bio), bio.name.lower()))


def _bio_score(bio: UmbracoBio) -> int:
    lowered = f"{bio.job_title} {' '.join(bio.departments)}".lower()
    score = 0
    for phrase, value in (
        ("chief executive officer", 100),
        ("group chief executive", 96),
        ("managing director", 90),
        ("president", 88),
        ("chief operating officer", 80),
        ("chief financial officer", 80),
        ("chief commercial officer", 76),
        ("chief marketing officer", 76),
        ("non-executive chairman", 74),
        ("chairman", 72),
        ("founder", 70),
        ("owner", 68),
        ("director", 60),
        ("leadership", 24),
    ):
        if phrase in lowered:
            score += value
    return score


def _render_people_html(website: str, bios: list[UmbracoBio]) -> str:
    cards: list[str] = ["<main><h1>Public People API Results</h1>"]
    cards.append(f"<p>Homepage: {website}</p>")
    for bio in bios:
        cards.append("<article>")
        cards.append(f"<h2>{bio.name}</h2>")
        cards.append(f"<p>Title: {bio.job_title}</p>")
        if bio.departments:
            cards.append(f"<p>Department: {'; '.join(bio.departments)}</p>")
        if bio.location:
            cards.append(f"<p>Location: {bio.location}</p>")
        if bio.url:
            cards.append(f"<p>Profile URL: {bio.url}</p>")
        if bio.email_address:
            cards.append(f"<p>Email: {bio.email_address}</p>")
        cards.append("</article>")
    cards.append("</main>")
    return "".join(cards)


def _origin(website: str) -> str:
    parsed = urlparse(str(website or "").strip())
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return str(website or "").rstrip("/")
