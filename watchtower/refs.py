"""Stable ticket identities and their human-readable names.

Keep ``ref`` bare in stores, joins, receipts and transport keys. A display
name is decoration: even an obsolete suffix resolves to the same ticket.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

_REF = re.compile(r"^([A-Za-z0-9_]+(?:-[A-Za-z0-9_]+)*?-\d+)(?::.*|-.*)?$", re.S)
_HOST = re.compile(r"[a-z0-9][a-z0-9-]{0,62}", re.I | re.ASCII)


def canonical_ref(value: Any) -> str:
    text = str(value or "").strip()
    host, sep, ticket = text.partition("/")
    if sep and _HOST.fullmatch(host) and (match := _REF.fullmatch(ticket)):
        return f"{host.lower()}/{match.group(1).upper()}"
    match = _REF.fullmatch(text)
    return match.group(1).upper() if match else text


def ticket_slug(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode()
    words = re.findall(r"[a-z0-9]+", text.lower())[:4]
    kept = []
    for word in words:
        if len("-".join(kept + [word])) > 30:
            break
        kept.append(word)
    return "-".join(kept) or "ticket"


def slug_of(item: dict) -> str:
    return ticket_slug(item.get("slug") or item.get("title") or item.get("note"))


def display_ref(item: dict) -> str:
    ref = str(item.get("ref") or item.get("number") or "?")
    return f"{canonical_ref(ref)}:{slug_of(item)}"
