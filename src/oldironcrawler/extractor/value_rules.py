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
    "media",
    "press",
    "release",
    "releases",
    "event",
    "events",
    "award",
    "awards",
    "insight",
    "insights",
    "resource",
    "resources",
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
    "coach",
    "coaching",
    "company",
    "commercial",
    "course",
    "courses",
    "cultures",
    "digital",
    "dispute",
    "employment",
    "executive",
    "faq",
    "faqs",
    "group",
    "help",
    "home",
    "html",
    "index",
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
}
_EMAIL_STRONG_SCORE = 12
_EMAIL_STOP_SCORE = 8
_EMAIL_HARD_LIMIT = 32
_EMAIL_FAMILY_TARGET = 6
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


@dataclass
class UrlCandidate:
    url: str
    tokens: list[str]
    family_key: str
    depth: int
    rep_rule_score: int
    email_rule_score: int
    rep_learn_score: int
    email_learn_score: int
    rep_noise_penalty: int
    is_person_detail_page: bool

    @property
    def rep_final_score(self) -> int:
        return self.rep_rule_score + self.rep_learn_score - self.rep_noise_penalty

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
    for url in urls:
        path_tokens = extract_path_tokens(url)
        depth = max((urlparse(url).path or "").count("/"), 0)
        family_key = _family_key(path_tokens)
        learning_tokens = _build_learning_tokens(path_tokens)
        is_person_detail_page = _looks_like_person_detail_page(path_tokens)
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
    ordered = sorted(candidates, key=lambda item: (-item.rep_final_score, item.depth, item.url))
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


def extract_learning_tokens(url: str) -> list[str]:
    return _build_learning_tokens(extract_path_tokens(url))


def merge_representative_urls(base_urls: list[str], learned_urls: list[str], *, limit: int = 5) -> list[str]:
    urls: list[str] = []
    for url in [*base_urls, *learned_urls]:
        value = str(url or "").strip()
        if value and value not in urls:
            urls.append(value)
        if len(urls) >= limit:
            break
    return urls


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
    if not tokens:
        return "root"
    return "/".join(tokens[:2])


def _build_learning_tokens(path_tokens: list[str]) -> list[str]:
    tokens = _filter_learning_tokens(path_tokens)
    family_feature = _family_feature(_family_key(tokens))
    if family_feature and family_feature not in tokens:
        tokens.append(family_feature)
    return tokens


def _family_feature(family_key: str) -> str:
    if family_key == "root":
        return ""
    return f"family:{family_key}"


def _filter_learning_tokens(path_tokens: list[str]) -> list[str]:
    tokens: list[str] = []
    for token in path_tokens:
        if token in _LEARNING_STOP_TOKENS or token in tokens:
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
        ("sponsored_discussions", -28),
        ("member/email-options", -24),
    ):
        if phrase in lowered:
            score += value
    if kind == "representative":
        for phrase, value in (
            ("about-us", 8),
            ("company-leadership", 18),
            ("executive-team", 18),
            ("leadership-team", 18),
            ("management-team", 16),
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


def _looks_like_person_detail_page(tokens: list[str]) -> bool:
    if len(tokens) < 3 or len(tokens) > 6:
        return False
    if not any(token in _PERSON_DETAIL_CONTEXT_TOKENS for token in tokens):
        return False
    name_tokens = [
        token for token in tokens
        if token not in _PERSON_DETAIL_NON_NAME_TOKENS and token not in _NEGATIVE_TOKENS
    ]
    if len(name_tokens) < 2 or len(name_tokens) > 3:
        return False
    return all(token.isalpha() and len(token) >= 3 for token in name_tokens[:2])
