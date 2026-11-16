#!/usr/bin/env python3
"""Route WatchTower human questions to Amir's iMessage and replies back.

Outbound: every newly blocked ticket in --queues is sent through
`hermes send --to bluebubbles` (isolated HERMES_HOME, same pattern as
bym_alert_bridge.py). Inbound: polls the BlueBubbles 1:1 chat; only messages
from ALLOWED_HANDLE's chat that are not isFromMe are acted on.

  "<REF> <answer>"   -> wt answer <REF> "<answer>"
  anything else      -> ignored (plain 1:1 texts to Hermes are never consumed
                        or replied to; an answer needs an explicit ref)
  "ticket: <text>"   -> wt add -q MAZKIR (mock/code-task intake)

Pre-send check (optional): when WT_IMESSAGE_PRESEND_CHECK names a program, every
question is shown to it before it is sent (`<program> check --ref REF --queues A,B`,
text on stdin; exit 0 = send). Anything else, including a check that cannot run,
means the question is NOT sent and is checked again after HOLD_RETRY_S. The send
itself, the wording and the reply contract do not change. Unset = no check.

Health: GET http://127.0.0.1:8854/status. Log: ~/.local/log/wt-imessage-router.log
Stdlib only.
"""
import argparse
import http.server
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

HERMES_BIN = os.path.expanduser("~/.hermes/hermes-agent/venv/bin/hermes")
BRIDGE_HERMES_HOME = os.path.expanduser("~/.hermes-bym-bridge")
BB_ENV = Path(os.path.expanduser("~/.hermes/.env"))
# The chat id identifies the owner's phone handle; it is machine-local config
# (BLUEBUBBLES_CHAT_GUID in BB_ENV), never a literal in this public file.
STATE = Path(os.path.expanduser("~/.local/state/wt-imessage-router.json"))
LOG_PATH = os.path.expanduser("~/.local/log/wt-imessage-router.log")
# Machine-local path of the owner-notification pre-send check; never a literal here.
PRESEND_CHECK = os.environ.get("WT_IMESSAGE_PRESEND_CHECK", "")
HOLD_RETRY_S = 900
REF_RE = re.compile(r"^\s*\[?([A-Za-z][A-Za-z0-9_]*(?:-[A-Za-z][A-Za-z0-9_]*)*-\d+)(?::[A-Za-z0-9_-]+|-[A-Za-z0-9_-]+)?\]?(?![\w-])[\s:,-]*(.*)$", re.S)
TICKET_RE = re.compile(r"^\s*ticket\s*:\s*(.+)$", re.I | re.S)

health = {"started": time.time(), "last_poll": None, "last_error": None,
          "pending": [], "answered": 0, "filed": 0, "held": []}


def log(line: str) -> None:
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a") as f:
            f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {line}\n")
    except OSError:
        pass


def parse_reply(text: str, pending: list[str]):
    """Return ('ticket', text) | ('answer', ref, text) | None.

    A text is consumed as an answer only when it starts with the ref of a
    currently pending question. Everything else is an ordinary message to
    Hermes and must be left alone (no consume, no reply)."""
    m = TICKET_RE.match(text)
    if m:
        return ("ticket", m.group(1).strip())
    m = REF_RE.match(text)
    if m and m.group(2).strip():
        ref = m.group(1).upper()
        if ref in pending:
            return ("answer", ref, m.group(2).strip())
    return None


def format_question(t: dict) -> str:
    return (f"[{t['ref']}] needs you: {t.get('title') or t.get('note') or ''}\n\n"
            f"{t.get('block_question', '')}\n\n"
            f"Reply \"{t['ref']} <answer>\" (the ref is required).")


def wt(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["wt", *args], capture_output=True, text=True, timeout=60)


def send(text: str) -> None:
    env = dict(os.environ, HERMES_HOME=BRIDGE_HERMES_HOME)
    r = subprocess.run([HERMES_BIN, "send", "--to", "bluebubbles", "--quiet", text],
                       capture_output=True, text=True, timeout=30, env=env)
    if r.returncode != 0:
        raise RuntimeError(f"hermes send rc={r.returncode}: {r.stderr.strip()[:200]}")


def presend(ref: str, text: str, queues: list[str]):
    """None when no check is configured, else (ok, check_id, why).

    Fail closed: a check that is missing, crashes, times out or answers
    anything but a clean yes is a no."""
    if not PRESEND_CHECK:
        return None
    try:
        r = subprocess.run([sys.executable, PRESEND_CHECK, "check", "--ref", ref,
                            "--queues", ",".join(queues)],
                           input=text, capture_output=True, text=True, timeout=90)
        data = json.loads(r.stdout or "{}")
        ok = r.returncode == 0 and data.get("ok") is True and bool(data.get("id"))
        return (ok, data.get("id"), str(data.get("code") or f"rc={r.returncode}")[:80])
    except Exception as e:
        return (False, None, type(e).__name__)


def report(*args: str) -> None:
    """Tell the check what became of a question. Best effort: never raises,
    never delays a send or an answer by more than the timeout."""
    if not PRESEND_CHECK:
        return
    try:
        subprocess.run([sys.executable, PRESEND_CHECK, *args],
                       capture_output=True, text=True, timeout=30)
    except Exception as e:
        log(f"REPORT FAIL {args[0]}: {type(e).__name__}")


def bb_recent() -> list:
    for line in BB_ENV.read_text().splitlines():
        if line.startswith("BLUEBUBBLES_") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v.strip().strip('"'))
    base = os.environ["BLUEBUBBLES_SERVER_URL"].rstrip("/")
    pw = urllib.parse.quote(os.environ["BLUEBUBBLES_PASSWORD"])
    chat_guid = os.environ.get("BLUEBUBBLES_CHAT_GUID", "")
    if not chat_guid:
        raise RuntimeError(f"BLUEBUBBLES_CHAT_GUID is not set in {BB_ENV}")
    guid = urllib.parse.quote(chat_guid, safe="")
    url = f"{base}/api/v1/chat/{guid}/message?limit=20&password={pw}"
    with urllib.request.urlopen(url, timeout=20) as resp:
        return json.loads(resp.read().decode()).get("data") or []


def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {"seeded": False, "notified": [], "seen": []}


def save_state(s: dict) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(s))


def tick(state: dict, queues: list[str], notify_queue: str) -> None:
    blocked = [t for t in json.loads(wt("blocked", "--json").stdout or "[]")
               if t.get("project") in queues]
    first = not state["seeded"]
    for t in blocked:
        if t["ref"] not in state["notified"]:
            if not first:  # first run records existing blocks without texting
                held = state.setdefault("held", {})
                if time.time() < held.get(t["ref"], 0):
                    continue  # refused by the pre-send check; looked at again later
                text = format_question(t)
                verdict = presend(t["ref"], text, queues)
                if verdict and not verdict[0]:
                    # Nothing was sent, so this is not "notified": the ticket
                    # may become a real owner question later and is re-checked.
                    held[t["ref"]] = time.time() + HOLD_RETRY_S
                    log(f"HELD {t['ref']}: {verdict[2]}")
                    continue
                held.pop(t["ref"], None)
                try:
                    send(text)
                    log(f"SENT {t['ref']}")
                    if verdict:
                        report("outcome", "--id", verdict[1], "--outcome", "accepted")
                except Exception as e:
                    # A failed send was still reaching the phone, then this
                    # loop retried every poll. One attempt, then stop.
                    log(f"SEND FAIL {t['ref']}: {e}")
                    if verdict:
                        report("outcome", "--id", verdict[1], "--outcome", "failed",
                               "--detail", type(e).__name__)
            state["notified"].append(t["ref"])
    live = {t["ref"] for t in blocked}
    state["notified"] = [r for r in state["notified"] if r in live]  # re-block re-notifies
    state["held"] = {r: at for r, at in state.get("held", {}).items() if r in live}
    health["held"] = sorted(state["held"])
    pending = [t["ref"] for t in blocked]
    health["pending"] = pending

    seen = set(state["seen"])
    for msg in reversed(bb_recent()):
        guid = msg.get("guid")
        if not guid or guid in seen or msg.get("isFromMe"):
            continue
        seen.add(guid)
        if first:
            continue
        act = parse_reply((msg.get("text") or ""), pending)
        if not act:
            continue
        if act[0] == "ticket":
            r = wt("add", "-q", notify_queue, "--title", act[1][:80], "--text", act[1],
                   "--submitter", "amir-imessage")
            send(f"Filed: {r.stdout.strip()[:200] or r.stderr.strip()[:200]}")
            health["filed"] += 1
            log(f"TICKET rc={r.returncode}")
        elif act[0] == "answer":
            r = wt("answer", act[1], act[2])
            ok = r.returncode == 0
            send(f"{'Answered' if ok else 'FAILED to answer'} {act[1]}"
                 + ("" if ok else f": {r.stderr.strip()[:150]}"))
            health["answered"] += ok
            log(f"ANSWER {act[1]} rc={r.returncode}")
            report("answer", "--ref", act[1], "--ok", "1" if ok else "0")
    state["seen"] = list(seen)[-200:]
    state["seeded"] = True
    save_state(state)
    health["last_poll"] = time.time()


class Status(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        age = None if health["last_poll"] is None else round(time.time() - health["last_poll"])
        ok = self.path == "/status"
        body = json.dumps({"ok": age is not None and age < 120, "last_poll_age_s": age,
                           **{k: health[k] for k in ("last_error", "pending", "answered", "filed", "held")}}
                          if ok else {"error": "not found"}).encode()
        self.send_response(200 if ok else 404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queues", default="PROJECTS,MAZKIR")
    ap.add_argument("--ticket-queue", default="MAZKIR")
    ap.add_argument("--port", type=int, default=8854)
    ap.add_argument("--poll", type=int, default=8)
    a = ap.parse_args()
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", a.port), Status)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    state = load_state()
    queues = a.queues.split(",")
    while True:
        try:
            tick(state, queues, a.ticket_queue)
            health["last_error"] = None
        except Exception as e:  # keep polling; surfaced via /status and the log
            health["last_error"] = f"{type(e).__name__}: {e}"[:200]
            log(f"ERROR {health['last_error']}")
        time.sleep(a.poll)


if __name__ == "__main__":
    main()
