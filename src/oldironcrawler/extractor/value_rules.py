from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import unquote, urlparse


_REP_WEIGHTS = {
    "about": 10,
    "contact": 9,
    "company": 10,
    "leadership": 16,
    "chairman": 14,
    "chief": 14,
    "impressum": 14,
    "imprint": 14,
    "team": 12,
    "partner": 10,
    "principal": 10,
    "management": 14,
    "board": 14,
    "governance": 14,
    "officers": 14,
    "founder": 16,
    "executive": 14,
    "people": 10,
    "director": 14,
    "president": 12,
    "owner": 12,
    "chair": 6,
    "profile": 8,
    "referral": 8,
    "referrals": 8,
    "solicitor": 10,
}
_EMAIL_WEIGHTS = {
    "contact": 20,
    "driver": 10,
    "drive": 8,
    "impressum": 10,
    "imprint": 10,
    "jobs": 8,
    "careers": 8,
    "career": 8,
    "recruit": 8,
    "join": 6,
    "support": 14,
    "help": 10,
    "customer": 12,
    "service": 10,
    "privacy": 8,
    "legal": 8,
    "terms": 8,
    "office": 10,
    "location": 8,
    "about": 5,
}
_NEGATIVE_TOKENS = {
    "blog", "discussion", "discussions", "event", "forum", "forums",
    "news", "post", "press-release", "sponsored", "tag",
    "author", "category",
}
_REP_SOURCE_NEGATIVE_TOKENS = {
    "forum",
    "forums",
    "news",
    "blog",
    "post",
    "posts",
    "article",
    "articles",
    "story",
    "stories",
    "charitable",
    "corporate",
    "media",
    "press",
    "release",
    "releases",
    "event",
    "events",
    "foundation",
    "fund",
    "funds",
    "award",
    "awards",
    "insight",
    "insights",
    "resource",
    "resources",
    "responsibility",
    "case",
    "study",
    "studies",
    "update",
    "updates",
    "announcement",
    "announcements",
    "discussion",
    "discussions",
    "sponsored",
    "career",
    "careers",
    "driver",
    "drivers",
    "guide",
    "guides",
    "job",
    "jobs",
    "position",
    "positions",
    "vehicle",
    "vehicles",
}
_REP_SOURCE_STRONG_TOKENS = {
    "about",
    "company",
    "contact",
    "impressum",
    "imprint",
    "leadership",
    "team",
    "leadership",
    "management",
    "board",
    "governance",
    "officers",
    "executive",
    "people",
    "director",
    "president",
    "owner",
    "founder",
}
_PERSON_DETAIL_CONTEXT_TOKENS = {
    "about",
    "bio",
    "board",
    "contact",
    "director",
    "executive",
    "founder",
    "leadership",
    "management",
    "member",
    "members",
    "officers",
    "partner",
    "partners",
    "people",
    "principal",
    "profile",
    "profiles",
    "referral",
    "referrals",
    "solicitor",
    "team",
}
_PERSON_DETAIL_NON_NAME_TOKENS = _PERSON_DETAIL_CONTEXT_TOKENS | {
    "and",
    "advice",
    "business",
    "certification",
    "charitable",
    "coach",
    "coaching",
    "company",
    "commercial",
    "corporate",
    "course",
    "courses",
    "cultures",
    "digital",
    "dispute",
    "employment",
    "executive",
    "faq",
    "faqs",
    "foundation",
    "fund",
    "funds",
    "group",
    "help",
    "home",
    "html",
    "index",
    "investment",
    "investments",
    "leaders",
    "management",
    "mentoring",
    "office",
    "our",
    "page",
    "pages",
    "payroll",
    "private",
    "resolution",
    "responsibility",
    "secretarial",
    "service",
    "services",
    "sessions",
    "sitemap",
    "start",
    "store",
    "support",
    "supervision",
    "taxation",
    "the",
    "training",
    "us",
    "wealth",
}
_EMAIL_STRONG_SCORE = 12
_EMAIL_STOP_SCORE = 8
_EMAIL_HARD_LIMIT = 32
_EMAIL_FAMILY_TARGET = 6
_REP_POSITIVE_LEARN_CAP = 10
_REP_PERSON_DETAIL_FINAL_BONUS = 8
_LEARNING_STOP_TOKENS = {
    "and",
    "for",
    "from",
    "our",
    "the",
    "with",
    "your",
}
_COMPOSITE_TOKEN_MAP = {
    "aboutus": ["about", "us"],
    "contactus": ["contact", "us"],
    "executiveteam": ["executive", "team"],
    "leadershipteam": ["leadership", "team"],
    "managementteam": ["management", "team"],
    "ourpeople": ["our", "people"],
    "ourteam": ["our", "team"],
    "privacypolicy": ["privacy", "policy"],
    "teammembers": ["team", "members"],
}
_FAMILY_PREFIX_SKIP_TOKENS = {
    "financial",
    "advisers",
    "individual",
    "individuals",
    "professional",
    "professionals",
}


@dataclass
class UrlCandidate:
    url: str
    tokens: list[str]
    family_key: str
    discovery_order: int
    depth: int
    rep_rule_score: int
    email_rule_score: int
    rep_learn_score: int
    email_learn_score: int
    rep_noise_penalty: int
    is_person_detail_page: bool

    @property
    def rep_final_score(self) -> int:
        capped_rep_learn_score = self.rep_learn_score
        if capped_rep_learn_score > _REP_POSITIVE_LEARN_CAP:
            capped_rep_learn_score = _REP_POSITIVE_LEARN_CAP
        person_detail_bonus = _REP_PERSON_DETAIL_FINAL_BONUS if self.is_person_detail_page else 0
        return self.rep_rule_score + capped_rep_learn_score + person_detail_bonus - self.rep_noise_penalty

    @property
    def email_final_score(self) -> int:
        return self.email_rule_score + self.email_learn_score


def build_candidates(
    start_url: str,
    discovered_urls: list[str],
    rep_learned: dict[str, int],
    email_learned: dict[str, int],
) -> list[UrlCandidate]:
    urls: list[str] = []
    for url in [start_url, *discovered_urls]:
        value = str(url or "").strip()
        if value and value not in urls:
            urls.append(value)
    candidates: list[UrlCandidate] = []
    for discovery_order, url in enumerate(urls):
        path_tokens = extract_path_tokens(url)
        trimmed_tokens = _trim_family_tokens(path_tokens)
        depth = max((urlparse(url).path or "").count("/"), 0)
        family_key = _family_key(path_tokens)
        is_person_detail_page = _looks_like_person_detail_page(trimmed_tokens)
        learning_tokens = _build_learning_tokens(path_tokens, is_person_detail_page=is_person_detail_page)
        rep_rule_score = _score_tokens(path_tokens, _REP_WEIGHTS)
        email_rule_score = _score_tokens(path_tokens, _EMAIL_WEIGHTS)
        rep_rule_score += _path_phrase_bonus(url, kind="representative")
        email_rule_score += _path_phrase_bonus(url, kind="email")
        rep_learn_score = _score_tokens(learning_tokens, rep_learned)
        email_learn_score = _score_tokens(learning_tokens, email_learned)
        if url == start_url:
            rep_rule_score += 30
            email_rule_score += 20
        if is_person_detail_page:
            rep_rule_score += 12
        locale_penalty = _locale_mismatch_penalty(start_url, url)
        rep_noise_penalty = _rep_noise_penalty(path_tokens, depth, rep_rule_score)
        candidates.append(
            UrlCandidate(
                url=url,
                tokens=learning_tokens,
                family_key=family_key,
                discovery_order=discovery_order,
                depth=depth,
                rep_rule_score=rep_rule_score - min(depth, 6) - locale_penalty,
                email_rule_score=email_rule_score - min(depth, 6) - locale_penalty,
                rep_learn_score=rep_learn_score,
                email_learn_score=email_learn_score,
                rep_noise_penalty=rep_noise_penalty,
                is_person_detail_page=is_person_detail_page,
            )
        )
    return candidates


def select_representative_urls(candidates: list[UrlCandidate], *, target_count: int = 5) -> tuple[list[str], list[str]]:
    ordered = sorted(candidates, key=lambda item: (-item.rep_final_score, item.depth, item.discovery_order, item.url))
    selected: list[str] = []
    used_families: set[str] = set()
    used_person_families: set[str] = set()
    for candidate in ordered:
        if candidate.rep_final_score <= 0 and candidate.url != ordered[0].url:
            continue
        if candidate.is_person_detail_page:
            if candidate.family_key in used_person_families:
                continue
            used_person_families.add(candidate.family_key)
        else:
            if candidate.family_key in used_families and len(selected) < target_count:
                continue
            used_families.add(candidate.family_key)
        selected.append(candidate.url)
        if len(selected) >= target_count:
            break
    teacher_pool = [candidate.url for candidate in ordered if candidate.url not in selected and _allow_teacher_candidate(candidate)][:40]
    return selected[:target_count], teacher_pool


def select_email_urls(candidates: list[UrlCandidate]) -> list[str]:
    ordered = sorted(candidates, key=lambda item: (-item.email_final_score, item.depth, item.url))
    urls: list[str] = []
    family_counts: dict[str, int] = {}
    strong_families: set[str] = set()
    for candidate in ordered:
        score = candidate.email_final_score
        if score <= 0:
            if urls:
                break
            continue
        family_key = candidate.family_key
        family_limit = 2 if _is_strong_email_candidate(candidate) else 1
        if family_counts.get(family_key, 0) >= family_limit:
            continue
        if _should_stop_email_selection(score, strong_families):
            break
        urls.append(candidate.url)
        family_counts[family_key] = family_counts.get(family_key, 0) + 1
        if _is_strong_email_candidate(candidate):
            strong_families.add(family_key)
        if len(urls) >= _EMAIL_HARD_LIMIT:
            break
    return urls


def build_fetch_plan(
    start_url: str,
    rep_urls: list[str],
    email_urls: list[str],
    *,
    rep_limit: int,
    email_soft_limit: int,
    email_hard_limit: int,
    total_hard_limit: int,
) -> dict[str, list[str]]:
    rep_cap = max(min(rep_limit, total_hard_limit), 0)
    selected_rep_urls = _take_unique_urls(rep_urls, limit=rep_cap, priority_url=start_url)
    homepage_primary_urls = _select_homepage_primary_urls(
        start_url,
        selected_rep_urls,
        total_hard_limit=total_hard_limit,
    )
    email_candidates = _exclude_existing_urls(
        _take_unique_urls(email_urls, limit=max(len(email_urls), 0), priority_url=start_url),
        [*selected_rep_urls, *homepage_primary_urls],
    )
    email_total_cap = max(
        min(email_hard_limit, total_hard_limit - len(selected_rep_urls) - len(homepage_primary_urls)),
        0,
    )
    email_primary_cap = max(min(email_soft_limit, email_total_cap), 0)
    email_primary_urls = email_candidates[:email_primary_cap]
    email_overflow_urls = email_candidates[email_primary_cap:email_total_cap]
    return {
        "rep_urls": selected_rep_urls,
        "homepage_primary_urls": homepage_primary_urls,
        "email_primary_urls": email_primary_urls,
        "email_overflow_urls": email_overflow_urls,
        "all_primary_urls": [*selected_rep_urls, *homepage_primary_urls, *email_primary_urls],
    }


def extract_learning_tokens(url: str) -> list[str]:
    path_tokens = extract_path_tokens(url)
    return _build_learning_tokens(
        path_tokens,
        is_person_detail_page=_looks_like_person_detail_page(_trim_family_tokens(path_tokens)),
    )


def merge_representative_urls(base_urls: list[str], learned_urls: list[str], *, limit: int = 5) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for url in [*base_urls, *learned_urls]:
        value = str(url or "").strip()
        canonical = _canonical_target_url(value)
        if value and canonical not in seen:
            seen.add(canonical)
            urls.append(value)
        if len(urls) >= limit:
            break
    return urls


def canonicalize_target_url(url: str) -> str:
    return _canonical_target_url(url)


def count_selected_families(candidates: list[UrlCandidate], urls: list[str]) -> int:
    candidate_map = {candidate.url: candidate for candidate in candidates}
    families = {
        candidate.family_key
        for url in urls
        if (candidate := candidate_map.get(url)) is not None
    }
    return len(families)


def extract_path_tokens(url: str) -> list[str]:
    parsed = urlparse(str(url or ""))
    parts = [segment for segment in parsed.path.split("/") if segment]
    tokens: list[str] = []
    for part in parts:
        decoded = re.sub(r"\.[a-z0-9]{2,5}$", "", unquote(part).strip().lower())
        for token in re.split(r"[\W_]+", decoded, flags=re.UNICODE):
            for clean in _expand_composite_token(token):
                if len(clean) < 3:
                    continue
                if clean not in tokens:
                    tokens.append(clean)
    return tokens


def _expand_composite_token(token: str) -> list[str]:
    clean = str(token or "").strip().lower()
    if not clean:
        return []
    expanded = _COMPOSITE_TOKEN_MAP.get(clean)
    if expanded:
        return expanded
    return [clean]


def _family_key(tokens: list[str]) -> str:
    trimmed = _trim_family_tokens(tokens)
    if not trimmed:
        return "root"
    return "/".join(trimmed[:2])


def _build_learning_tokens(path_tokens: list[str], *, is_person_detail_page: bool = False) -> list[str]:
    tokens = _filter_learning_tokens(path_tokens, is_person_detail_page=is_person_detail_page)
    family_feature = _family_feature(_family_key(tokens))
    if family_feature and family_feature not in tokens:
        tokens.append(family_feature)
    return tokens


def _family_feature(family_key: str) -> str:
    if family_key == "root":
        return ""
    return f"family:{family_key}"


def _filter_learning_tokens(path_tokens: list[str], *, is_person_detail_page: bool = False) -> list[str]:
    blocked_tokens = set()
    if is_person_detail_page:
        blocked_tokens = set(_extract_person_name_tokens(_trim_family_tokens(path_tokens)))
    tokens: list[str] = []
    for token in path_tokens:
        if token in blocked_tokens or token in _LEARNING_STOP_TOKENS or token in tokens:
            continue
        tokens.append(token)
    return tokens


def _score_tokens(tokens: list[str], weights: dict[str, int]) -> int:
    score = 0
    for token in tokens:
        if token in _NEGATIVE_TOKENS:
            score -= 6
        for weighted, value in weights.items():
            if token == weighted:
                score += int(value)
    return score


def _rep_noise_penalty(tokens: list[str], depth: int, rep_rule_score: int) -> int:
    penalty = 0
    has_strong_rep_token = any(token in _REP_SOURCE_STRONG_TOKENS for token in tokens)
    if any(token in _REP_SOURCE_NEGATIVE_TOKENS for token in tokens):
        penalty += 18
    if not has_strong_rep_token and len(tokens) >= 6:
        penalty += 18
    elif not has_strong_rep_token and len(tokens) >= 4:
        penalty += 10
    if not has_strong_rep_token and depth >= 2:
        penalty += 6
    if rep_rule_score <= 0 and not has_strong_rep_token:
        penalty += 10
    return penalty


def _allow_teacher_candidate(candidate: UrlCandidate) -> bool:
    if candidate.is_person_detail_page:
        return True
    if candidate.rep_noise_penalty >= 18:
        return False
    return True


def _is_strong_email_candidate(candidate: UrlCandidate) -> bool:
    return candidate.email_final_score >= _EMAIL_STRONG_SCORE


def _should_stop_email_selection(score: int, strong_families: set[str]) -> bool:
    if len(strong_families) < _EMAIL_FAMILY_TARGET:
        return False
    return score < _EMAIL_STOP_SCORE


def _path_phrase_bonus(url: str, *, kind: str) -> int:
    lowered = str(url or "").lower()
    score = 0
    for phrase, value in (
        ("discount-partners", -22),
        ("forums/", -28),
        ("/our-services/", -14),
        ("/services/", -8),
        ("sponsored_discussions", -28),
        ("member/email-options", -24),
    ):
        if phrase in lowered:
            score += value
    if kind == "representative":
        for phrase, value in (
            ("about-us/our-people", 24),
            ("about-us", 8),
            ("company-leadership", 18),
            ("executive-team", 18),
            ("leadership-team", 18),
            ("management-team", 16),
            ("our-people", 18),
            ("our-team", 12),
            ("team-members", 16),
            ("executive-team", 18),
        ):
            if phrase in lowered:
                score += value
    if kind == "email":
        for phrase, value in (
            ("contact-us", 14),
            ("contact/", 6),
            ("privacy-policy", 6),
        ):
            if phrase in lowered:
                score += value
    return score


def _locale_mismatch_penalty(start_url: str, candidate_url: str) -> int:
    start_locale = _extract_locale_token_from_url(start_url)
    candidate_locale = _extract_locale_token_from_url(candidate_url)
    if not start_locale or not candidate_locale:
        return 0
    if start_locale == candidate_locale:
        return 0
    return 12


def _extract_locale_token_from_url(url: str) -> str:
    path = str(urlparse(str(url or "")).path or "").strip("/")
    if not path:
        return ""
    first = path.split("/", 1)[0].strip().lower()
    if re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", first):
        return first
    return ""


def _take_unique_urls(urls: list[str], *, limit: int, priority_url: str) -> list[str]:
    ordered_urls = _promote_priority_url(urls, priority_url)
    result: list[str] = []
    seen: set[str] = set()
    for url in ordered_urls:
        value = str(url or "").strip()
        canonical = _canonical_target_url(value)
        if not value or canonical in seen:
            continue
        seen.add(canonical)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def _exclude_existing_urls(urls: list[str], existing_urls: list[str]) -> list[str]:
    blocked = {_canonical_target_url(str(url or "").strip()) for url in existing_urls if str(url or "").strip()}
    return [url for url in urls if _canonical_target_url(str(url or "").strip()) not in blocked]


def _promote_priority_url(urls: list[str], priority_url: str) -> list[str]:
    priority = str(priority_url or "").strip()
    ordered_urls = [str(url or "").strip() for url in urls if str(url or "").strip()]
    if not priority:
        return ordered_urls
    priority_key = _canonical_target_url(priority)
    matched = [url for url in ordered_urls if _canonical_target_url(url) == priority_key]
    if not matched:
        return ordered_urls
    first = matched[0]
    return [first, *[url for url in ordered_urls if _canonical_target_url(url) != priority_key]]


def _select_homepage_primary_urls(
    start_url: str,
    selected_rep_urls: list[str],
    *,
    total_hard_limit: int,
) -> list[str]:
    homepage_url = str(start_url or "").strip()
    if not homepage_url:
        return []
    if homepage_url in selected_rep_urls:
        return []
    if len(selected_rep_urls) >= total_hard_limit:
        return []
    return [homepage_url]


def _canonical_target_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return str(url or "").strip().lower().rstrip("/")
    host = str(parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    path = str(parsed.path or "").rstrip("/")
    query = str(parsed.query or "").strip()
    if path == "/":
        path = ""
    if query:
        return f"{parsed.scheme.lower()}://{host}{path}?{query}"
    return f"{parsed.scheme.lower()}://{host}{path}"


def _trim_family_tokens(tokens: list[str]) -> list[str]:
    trimmed = list(tokens)
    while trimmed and trimmed[0] in _FAMILY_PREFIX_SKIP_TOKENS:
        trimmed = trimmed[1:]
    return trimmed


def _looks_like_person_detail_page(tokens: list[str]) -> bool:
    if len(tokens) < 3 or len(tokens) > 6:
        return False
    if not any(token in _PERSON_DETAIL_CONTEXT_TOKENS for token in tokens):
        return False
    name_tokens = _extract_person_name_tokens(tokens)
    if len(name_tokens) < 2 or len(name_tokens) > 3:
        return False
    return all(token.isalpha() and len(token) >= 3 for token in name_tokens[:2])


def _extract_person_name_tokens(tokens: list[str]) -> list[str]:
    return [
        token for token in tokens
        if token not in _PERSON_DETAIL_NON_NAME_TOKENS and token not in _NEGATIVE_TOKENS
    ]
