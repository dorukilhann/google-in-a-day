from __future__ import annotations

import re
from collections import Counter
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin, urlsplit, urlunsplit


TOKEN_RE = re.compile(r"[A-Za-z0-9]+(?:'[A-Za-z0-9]+)?")
BLOCKED_SCHEMES = {"mailto", "javascript", "tel", "data"}


def tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall(text.lower())


def term_frequencies(text: str) -> Counter[str]:
    return Counter(tokenize(text))


def normalize_url(raw_url: str, base_url: str | None = None) -> str | None:
    if not raw_url:
        return None

    candidate = raw_url.strip()
    if not candidate:
        return None

    if base_url:
        candidate = urljoin(base_url, candidate)

    split = urlsplit(candidate)
    if split.scheme and split.scheme.lower() in BLOCKED_SCHEMES:
        return None
    if split.scheme.lower() not in {"http", "https"}:
        return None
    if not split.netloc:
        return None

    scheme = split.scheme.lower()
    hostname = split.hostname.lower() if split.hostname else ""
    try:
        port = split.port
    except ValueError:
        return None
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname

    path = split.path or "/"
    return urlunsplit((scheme, netloc, path, split.query, ""))


class PageParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.links: list[str] = []
        self._text_parts: list[str] = []
        self._title_parts: list[str] = []
        self._ignored_stack: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: Iterable[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        attrs_dict = dict(attrs)
        if lowered in {"script", "style", "noscript"}:
            self._ignored_stack.append(lowered)
            return
        if lowered == "title":
            self._in_title = True
            return
        if lowered == "a":
            href = attrs_dict.get("href")
            normalized = normalize_url(href or "", self.page_url)
            if normalized:
                self.links.append(normalized)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered == "title":
            self._in_title = False
            return
        if self._ignored_stack and self._ignored_stack[-1] == lowered:
            self._ignored_stack.pop()

    def handle_data(self, data: str) -> None:
        if not data or self._ignored_stack:
            return
        clean = " ".join(data.split())
        if not clean:
            return
        if self._in_title:
            self._title_parts.append(clean)
        else:
            self._text_parts.append(clean)

    @property
    def title(self) -> str:
        return " ".join(self._title_parts).strip()

    @property
    def text(self) -> str:
        return " ".join(self._text_parts).strip()
