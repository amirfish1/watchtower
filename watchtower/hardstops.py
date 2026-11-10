"""Built-in hard-stop floor for auto-decisions.

A worker's minor, reversible decision may be approved automatically, but never
when its own words name one of the things below: sending to customers, a
production change, money or prices, ad spend or a public post, or anything
that cannot be undone. Those always go to a human.

This is a veto list, so being over-inclusive is correct here: a false hit only
means a human is asked. A miss would let an automatic approval through, so
when in doubt a pattern matches. It can never grant an approval.

The patterns are generic English. To add rules of your own, point
``WT_HARD_STOPS_FILE`` at a Python file that exposes ``hard_stops(text)`` and
returns a list of matched rule names (see ``autodecide.floor_hits``). Extra
rules add to this list; nothing can remove a rule from it.
"""

from __future__ import annotations

import re
from typing import List, Pattern, Tuple

_AUDIENCE = (r"(?:customers?|clients?|users?|subscribers?|leads?|prospects?|members?|patients?|"
             r"guests?|buyers?)")
_SOCIAL = (r"(?:x|twitter|linkedin|instagram|facebook|reddit|tiktok|youtube|threads|bluesky|mastodon|"
           r"hacker news|hn|product hunt|producthunt)")

HARD_STOPS: Tuple[Tuple[str, Pattern[str]], ...] = (
    ("sending to customers", re.compile(
        r"\b\w*send[_ -]?(?:switch|switches|enabled|flag|toggle)\b"
        r"|\boutreach\b|\bcold[- ]?(?:e-?mails?|calls?|calling|texts?|messages?)\b"
        r"|\bbroadcast(?:s|ed|ing)?\b|\bnewsletters?\b|\bmass[- ](?:e-?mails?|mailing|texts?|messages?)\b"
        r"|\b(?:e-?mail|sms|text|drip|marketing|messaging) (?:campaigns?|blasts?)\b|\bcampaign sends?\b"
        r"|\b(?:send|sends|sending|sent|e-?mail|e-?mails|e-?mailing|e-?mailed|sms|text|texts|texting|"
        r"dm|dms|message|messages|messaging|notify|notifies|notifying|notification|notifications|"
        r"reply to|replying to|call|calls|calling|phone|phoning|contact|contacting)\b"
        rf"[^.\n]{{0,50}}\b{_AUDIENCE}\b"
        rf"|\b{_AUDIENCE}\b[^.\n]{{0,30}}\b(?:e-?mails?|sms|dms?|texts?|notifications?)\b", re.I)),
    ("a production change, which reaches customers", re.compile(
        r"\bprod(?:uction)?\b|\blive (?:site|website|servers?|environment|traffic)\b", re.I)),
    ("money or prices", re.compile(
        r"\bstripe\b|\bpaypal\b|\brefund(?:s|ed|ing)?\b|\bcharge(?:s|d)?\b|\bcharging\b|\bchargebacks?\b"
        r"|\binvoic(?:e|es|ed|ing)\b|\bbilling\b|\bbilled\b|\bpric(?:e|es|ed|ing)\b"
        r"|\bpayments?\b|\bpayouts?\b|\bsubscriptions?\b|\bcoupons?\b|\bdiscount(?:s|ed|ing)?\b"
        r"|\bpromo codes?\b|\bcredit cards?\b|\bpaid (?:test|run|eval|evaluation|plan|tier)s?\b"
        r"|[$€£]\s?\d|\b\d+(?:\.\d+)?\s?(?:usd|eur|gbp|dollars?|euros?|pounds?)\b", re.I)),
    ("ad spend or a public post", re.compile(
        r"\bads\b|\bad (?:spend|budgets?|campaigns?|accounts?|sets?|creatives?|copy)\b"
        r"|\badvertis(?:e|es|ed|ing|ement|ements)\b|\b(?:campaign|marketing|advertising) budgets?\b"
        r"|\btweet(?:s|ed|ing)?\b|\bpress releases?\b|\bblog posts?\b"
        r"|\bpublic (?:\w+ )?(?:post|posts|page|pages|site|website|text|wording|announcements?|statements?)\b"
        r"|\blanding pages?\b|\bshow hn\b"
        r"|\b(?:post|posts|posted|posting|publish|publishes|published|publishing|share|shares|shared|"
        r"sharing|announce|announces|announced|announcing|launch|launches|launched|launching)\b"
        rf"[^.\n]{{0,40}}\b(?:on|to)\s+{_SOCIAL}\b", re.I)),
    ("cannot be undone", re.compile(
        r"\birreversibl[ey]\b|\birrevocabl[ey]\b"
        r"|\b(?:can ?not|can'?t|could ?not|couldn'?t|won'?t|will not) be "
        r"(?:undone|reversed|recalled|recovered|restored|rolled back)\b"
        r"|\bforce[- ]?push(?:es|ed|ing)?\b|\bpush(?:es|ed|ing)?\b[^.\n]{0,40}(?:--force|\s-f\b)"
        r"|\b(?:rewrite|rewrites|rewriting|rewrote|scrub|scrubs|scrubbing)\b[^.\n]{0,20}\bhistory\b"
        r"|\bfilter-(?:branch|repo)\b|\bdrop (?:table|database|schema)\b|\btruncat(?:e|es|ed|ing)\b"
        r"|\bprod(?:uction)? (?:database|db|migrations?)\b"
        r"|\b(?:delet(?:e|es|ed|ing)|wip(?:e|es|ed|ing)|eras(?:e|es|ed|ing)|purg(?:e|es|ed|ing)|"
        r"destroy(?:s|ed|ing)?)\b[^.\n]{0,30}\b(?:production|prod|live|customers?|clients?|users?)\b"
        r"|\brm\s+-[a-z]*(?:r[a-z]*f|f[a-z]*r)[a-z]*\b|\brm\s+(?:-r\s+-f|-f\s+-r)\b", re.I)),
)


def hard_stops(text: str) -> List[str]:
    """Names of the hard-stop rules ``text`` matches, in rule order, without duplicates."""
    text = str(text or "")
    hits: List[str] = []
    for name, pattern in HARD_STOPS:
        if name not in hits and pattern.search(text):
            hits.append(name)
    return hits
