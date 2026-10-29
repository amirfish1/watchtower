"""GitHub issue bodies travel on stdin, not argv (WT-40).

Production `wt answer` died in `_edit_body` with OSError errno 7 ("Argument
list too long"): the whole issue history went to `gh issue edit --body <...>`
as ONE argv string, and Linux caps a single argument at MAX_ARG_STRLEN
(32 pages, 128 KiB), so exec failed before gh ever started. These tests
drive the real `_run`/`_run_raw`/`subprocess.run` path against a temporary
fake `gh` executable that records the argv and stdin bytes it received. No
GitHub request is made; this is fake-transport evidence only. Real API
acceptance of `--body-file -` is gh's documented contract (`gh issue
create|edit|comment --help`: "use - to read from standard input").
"""

from __future__ import annotations

import errno
import json
import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

import pytest

import watchtower.github_backend as github_backend


FAKE_GH = r'''#!/usr/bin/env python3
import json, os, sys, time

out = os.environ["FAKE_GH_OUT"]
mode = os.environ.get("FAKE_GH_MODE", "ok")
args = sys.argv[1:]
tag = str(os.getpid())
with open(os.path.join(out, tag + ".json"), "w") as fh:
    json.dump({"argv": args}, fh)
# gh's (pflag's) option grammar for these commands: a value flag consumes
# the next token whatever it looks like ("--title --body" is a title, and
# "--label --" is a label); --flag=value is one token; a bare "--" that no
# flag consumed ends options.
VALUE = {"--repo", "-R", "--title", "-t", "--body", "-b", "--body-file", "-F",
         "--label", "-l", "--add-label", "--remove-label", "--assignee", "-a",
         "--add-assignee", "--remove-assignee", "--json", "--comment", "-c"}
opts, positional, i = {}, [], 0
while i < len(args):
    a = args[i]
    name, eq, val = a.partition("=")
    if a == "--":
        positional.extend(args[i + 1:])
        break
    if a.startswith("--") and eq and name in VALUE:
        opts.setdefault(name, []).append(val)
        i += 1
    elif a in VALUE and i + 1 < len(args):
        opts.setdefault(a, []).append(args[i + 1])
        i += 2
    else:
        if not a.startswith("-") or a == "-":
            positional.append(a)
        i += 1
opts["_positional"] = positional[2:]  # after the `issue <verb>` command path
with open(os.path.join(out, tag + ".opts.json"), "w") as fh:
    json.dump(opts, fh)
if mode == "hang":
    time.sleep(30)  # never reads stdin; the parent's timeout must still fire
body = None
if "--body-file" in opts:
    src = opts["--body-file"][-1]
    body = sys.stdin.buffer.read() if src == "-" else open(src, "rb").read()
    with open(os.path.join(out, tag + ".body"), "wb") as fh:
        fh.write(body)
if mode == "fail":
    print("HTTP 422: Validation Failed", file=sys.stderr)
    sys.exit(1)
if mode == "ratelimit":
    print("API rate limit exceeded for user ID 1", file=sys.stderr)
    sys.exit(1)
if mode == "auth_once":
    flag = os.path.join(out, "auth-failed-once")
    if not os.path.exists(flag):
        open(flag, "w").close()
        print("To get started with GitHub CLI, please run:  gh auth login",
              file=sys.stderr)
        sys.exit(4)
state = os.path.join(out, "issue-body")
if args[:2] == ["issue", "edit"] and body is not None:
    with open(state, "wb") as fh:
        fh.write(body)
if args[:2] == ["issue", "view"]:
    with open(state, "rb") as fh:
        print(json.dumps({"body": fh.read().decode("utf-8")}))
if args[:2] == ["issue", "create"]:
    print("https://github.com/acme/body/issues/7")
sys.exit(0)
'''

REPO = "acme/body"


def _arg_limit() -> int:
    # Linux MAX_ARG_STRLEN: one argv string may not exceed 32 pages.
    return 32 * os.sysconf("SC_PAGE_SIZE")


def _tricky(n: int, salt: str = "") -> str:
    """A body of at least ``n`` characters that a lossy transport would
    mangle: multi-byte Unicode, both quote kinds, backslashes, CRLF and bare
    CR, shell metacharacters, a trailing newline with no strip."""
    unit = (f"{salt}line é中\U0001F600 \"dq\" 'sq' `bt` $HOME \\n\\\\ "
            "<!-- watchtower:meta --> \t\r\n-- bare cr\r|\n")
    return (unit * (n // len(unit) + 1)) + "\n\n"


@pytest.fixture
def gh(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    exe = bin_dir / "gh"
    exe.write_text(FAKE_GH)
    exe.chmod(0o755)
    out = tmp_path / "gh-out"
    out.mkdir()
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_GH_OUT", str(out))
    monkeypatch.setenv("FAKE_GH_MODE", "ok")
    monkeypatch.setenv("WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "conn.json"))
    monkeypatch.setenv("WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "cache.json"))
    monkeypatch.setattr(github_backend, "_GH_SCRUB_ENV", False)

    class Gh:
        exe_path = exe
        out_dir = out

        @staticmethod
        def calls():
            recs = []
            for f in sorted(out.glob("*[0-9].json")):
                rec = json.loads(f.read_text())
                rec["opts"] = json.loads(f.with_suffix(".opts.json").read_text())
                body = f.with_suffix(".body")
                rec["body"] = body.read_bytes() if body.exists() else None
                rec["pid"] = int(f.stem)
                recs.append(rec)
            return recs

    return Gh


@pytest.fixture
def backend():
    return github_backend.GitHubIssuesBackend("BODY", repo=REPO)


@pytest.fixture
def no_tempfiles(monkeypatch):
    """The transport is stdin: nothing to leak, so any temp file is a bug."""
    def boom(*a, **k):
        raise AssertionError("body transport must not create temp files")
    for name in ("mkstemp", "NamedTemporaryFile", "TemporaryFile", "mkdtemp"):
        monkeypatch.setattr(tempfile, name, boom)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="MAX_ARG_STRLEN is Linux")
def test_body_over_the_single_argument_limit_reaches_gh_byte_exact(gh, backend, no_tempfiles):
    big = _tricky(2 * _arg_limit() + 4096)
    # The production failure, reproduced on the old transport: exec refuses it.
    with pytest.raises(OSError) as exc:
        subprocess.run([str(gh.exe_path), "issue", "edit", "1", "--body", big],
                       capture_output=True)
    assert exc.value.errno == errno.E2BIG

    for verb in (["issue", "edit", "12"], ["issue", "comment", "12"], ["issue", "create"]):
        for f in gh.out_dir.iterdir():
            f.unlink()
        backend._run([*verb, "--repo", REPO, "--title", "T é"], body=big)
        (call,) = gh.calls()
        assert call["body"] == big.encode("utf-8"), verb
        assert big not in call["argv"]
        assert call["argv"] == [*verb[:2], "--body-file", "-", *verb[2:],
                                "--repo", REPO, "--title", "T é"]


def test_edit_body_round_trip_keeps_extra_args_and_full_history(gh, backend):
    """The exact production path: `_edit_body` writes, re-reads via view, and
    finds nothing to re-apply because every byte landed."""
    history = _tricky(3 * _arg_limit())
    item = {"number": 12, "_github_body": "", "text": ""}
    conflicts = backend._edit_body(item, history, {"status": "open"}, "--title", "New")
    assert conflicts == []
    edits = [c for c in gh.calls() if c["argv"][:2] == ["issue", "edit"]]
    assert len(edits) == 1, "no re-apply: the fresh read matched what we wrote"
    sent = edits[0]["body"].decode("utf-8")
    assert sent == github_backend._body_with_metadata(history, {"status": "open"})
    assert history.rstrip() in sent  # _body_with_metadata trims the tail itself
    assert edits[0]["argv"] == ["issue", "edit", "--body-file", "-", "12",
                                "--repo", REPO, "--title", "New"]


def test_short_body_uses_the_same_transport(gh, backend):
    backend._run(["issue", "comment", "3", "--repo", REPO], body="ok")
    (call,) = gh.calls()
    assert call["body"] == b"ok"
    assert call["argv"] == ["issue", "comment", "--body-file", "-", "3", "--repo", REPO]


def test_empty_body_is_sent_not_dropped(gh, backend):
    backend._run(["issue", "edit", "3", "--repo", REPO], body="")
    (call,) = gh.calls()
    assert call["body"] == b""


def test_commands_without_a_body_keep_argv_and_stdin_untouched(gh, backend):
    backend._run(["issue", "edit", "3", "--repo", REPO, "--add-label", "x"])
    backend._run(["issue", "close", "3", "--repo", REPO, "--comment", "done"])
    calls = gh.calls()
    assert [c["argv"][:2] for c in calls] == [["issue", "edit"], ["issue", "close"]]
    assert all(c["body"] is None for c in calls)
    assert all("--body-file" not in c["argv"] for c in calls)


def test_concurrent_writes_each_deliver_their_own_body(gh, backend):
    bodies = [_tricky(_arg_limit() + 1000 * i, salt=f"writer{i} ") for i in range(6)]
    errors = []

    def write(i):
        try:
            backend._run(["issue", "comment", str(i), "--repo", REPO], body=bodies[i])
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(len(bodies))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert not errors
    got = {c["opts"]["_positional"][0]: c["body"] for c in gh.calls()}
    assert got == {str(i): b.encode("utf-8") for i, b in enumerate(bodies)}


def test_failure_raises_without_echoing_the_body(gh, backend, monkeypatch):
    monkeypatch.setenv("FAKE_GH_MODE", "fail")
    secretish = "BODY-CONTENT-" + "x" * (_arg_limit() + 10)
    with pytest.raises(github_backend.GitHubBackendError) as exc:
        backend._run(["issue", "edit", "4", "--repo", REPO], body=secretish)
    msg = str(exc.value)
    assert "HTTP 422" in msg and "--body-file -" in msg
    assert "BODY-CONTENT" not in msg
    (call,) = gh.calls()
    assert call["body"] == secretish.encode()


def test_rate_limit_failure_still_recorded(gh, backend, monkeypatch):
    monkeypatch.setenv("FAKE_GH_MODE", "ratelimit")
    seen = []
    monkeypatch.setattr(github_backend, "_record_gh_rate_limited", seen.append)
    with pytest.raises(github_backend.GitHubBackendError):
        backend._run(["issue", "comment", "4", "--repo", REPO], body="hi")
    assert seen and "rate limit" in seen[0]


def test_auth_heal_retry_resends_the_full_body(gh, backend, monkeypatch):
    monkeypatch.setenv("FAKE_GH_MODE", "auth_once")
    monkeypatch.setattr(github_backend, "_attempt_auth_self_heal", lambda: True)
    big = _tricky(_arg_limit() + 1)
    backend._run(["issue", "edit", "5", "--repo", REPO], body=big)
    calls = gh.calls()
    assert len(calls) == 2
    assert [c["body"] for c in calls] == [big.encode("utf-8")] * 2


def test_timeout_is_bounded_kills_gh_and_hides_the_body(gh, backend, monkeypatch, no_tempfiles):
    """A gh that never reads stdin (the pipe fills after 64 KiB) must not
    wedge the caller: the bounded timeout still fires and the child is reaped."""
    monkeypatch.setenv("FAKE_GH_MODE", "hang")
    real_run = subprocess.run
    seen_timeout = []

    def short_run(*a, **k):
        seen_timeout.append(k.get("timeout"))
        k["timeout"] = 1.5
        return real_run(*a, **k)

    monkeypatch.setattr(github_backend.subprocess, "run", short_run)
    big = "SECRET-" + _tricky(_arg_limit())
    with pytest.raises(github_backend.GitHubBackendError) as exc:
        backend._run(["issue", "edit", "6", "--repo", REPO], body=big)
    assert seen_timeout == [60], "the production bound is unchanged"
    assert "timed out" in str(exc.value)
    assert "SECRET-" not in str(exc.value)
    (call,) = gh.calls()
    with pytest.raises(ProcessLookupError):
        os.kill(call["pid"], 0)




# Values that look like gh flags. gh's parser gives each one to the option
# before it, so it must reach gh verbatim and must never move the body.
_FLAG_SHAPED = ["--body", "--body-file", "-F", "-b", "--body=x", "--", "-", "--repo"]
_SENTINEL = "PRIVATE-BODY-SENTINEL"


def test_flag_shaped_values_reach_gh_verbatim_after_the_body_flag(gh, backend):
    """No argv token is inspected or rewritten: --body-file - sits right after
    `issue <verb>`, where no caller value can consume it, and every caller
    token follows unchanged in its original position (WT-40 verify:4/5)."""
    for value in _FLAG_SHAPED:
        for f in gh.out_dir.iterdir():
            f.unlink()
        args = ["issue", "edit", "1", "--repo", REPO, "--title", value,
                "--add-label", value, "--remove-label", value,
                "--add-assignee", value, f"--add-label={value}"]
        backend._run(args, body=_SENTINEL)
        (call,) = gh.calls()
        assert call["argv"] == ["issue", "edit", "--body-file", "-", *args[2:]], value
        assert call["body"] == _SENTINEL.encode()
        assert call["opts"]["--body-file"] == ["-"]
        assert call["opts"]["--title"] == [value]
        assert call["opts"]["--add-label"] == [value, value]
        assert call["opts"]["--remove-label"] == [value]
        assert call["opts"]["--add-assignee"] == [value]
        assert call["opts"]["_positional"] == ["1"]
        assert not any(_SENTINEL in a for a in call["argv"])


def test_body_on_a_non_body_command_is_refused_before_gh(gh, backend):
    """The transport fixes its own position from the two leading tokens only;
    a body handed to any other command is a caller bug, refused unspawned."""
    for args in (["issue", "close", "1"], ["issue", "view", "1"], ["label", "create", "x"],
                 ["issue"], []):
        with pytest.raises(github_backend.GitHubBackendError) as exc:
            backend._run(args, body=_SENTINEL)
        assert _SENTINEL not in str(exc.value)
    assert gh.calls() == []


def test_no_body_writer_builds_a_body_flag_on_argv():
    """Bodies reach gh only through body=; no production argv literal names a
    body flag (the structural half of the invariant)."""
    import ast
    tree = ast.parse(Path(github_backend.__file__).read_text())
    flags = {"--body", "-b", "--body-file", "-F"}
    # _body_argv is the one place --body-file - is placed; nothing else may.
    owner = next(n for n in ast.walk(tree)
                 if isinstance(n, ast.FunctionDef) and n.name == "_body_argv")
    sanctioned = {id(n) for n in ast.walk(owner)}
    offenders = [
        (node.lineno, elt.value) for node in ast.walk(tree)
        if id(node) not in sanctioned
        if isinstance(node, ast.List)
        for elt in node.elts
        if isinstance(elt, ast.Constant) and elt.value in flags
    ]
    assert offenders == []
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "attr", "") in ("_run", "_run_raw")
             and any(k.arg == "body" for k in n.keywords)]
    assert len(calls) >= 7, "every body writer passes body= explicitly"


_LABELS = ["--body", "--body-file", "-b", "-F", "--", "--body=x"]


@pytest.mark.parametrize("label", _LABELS + ["-l", "--title", "-", "bug", "queue:é"])
@pytest.mark.parametrize("size", ["empty", "short", "oversized"])
def test_enqueue_with_flag_shaped_queue_label_reaches_gh(gh, monkeypatch, label, size):
    """Verifier witness (WT-40 verify:5): a valid configured queue label equal
    to a body flag or "--" was refused before gh spawned. It must reach gh as
    the --label value, the title unchanged, the body exactly on stdin."""
    backend = github_backend.GitHubIssuesBackend("BODY", repo=REPO, queue_label=label)
    assert backend.queue_label == label and not backend.catch_all
    monkeypatch.setattr(backend, "_ensure_labels", lambda: None)
    monkeypatch.setattr(backend, "get", lambda ident: {"number": 7})
    text = {"empty": "", "short": f"{_SENTINEL} t \"q\"\r\n",
            "oversized": _SENTINEL + "\U0001F600" * 23000}[size]
    note = f"{_SENTINEL} n" if size != "oversized" else _SENTINEL + "\U0001F600" * 3970

    backend.enqueue(title="--label", note=note, text=text)

    (call,) = gh.calls()
    assert call["argv"] == ["issue", "create", "--body-file", "-", "--repo", REPO,
                            "--title", "--label", "--label", label]
    assert call["opts"]["--label"] == [label]
    assert call["opts"]["--title"] == ["--label"]
    assert call["opts"]["--body-file"] == ["-"]
    sent = call["body"].decode("utf-8")
    assert sent == github_backend._body_with_metadata(text or note, _sent_meta(sent))
    assert not any(_SENTINEL in a for a in call["argv"])
    if size == "oversized":
        assert len(call["body"]) > _arg_limit()


@pytest.mark.parametrize("label", _LABELS)
def test_flag_shaped_label_failure_diagnostics_stay_private(gh, monkeypatch, label):
    backend = github_backend.GitHubIssuesBackend("BODY", repo=REPO, queue_label=label)
    monkeypatch.setattr(backend, "_ensure_labels", lambda: None)
    monkeypatch.setenv("FAKE_GH_MODE", "fail")
    with pytest.raises(github_backend.GitHubBackendError) as exc:
        backend.enqueue(title=_SENTINEL, note=f"{_SENTINEL} note", text=f"{_SENTINEL} body")
    msg = str(exc.value)
    assert "HTTP 422" in msg and "--body-file -" in msg
    assert f"--label {label}" in msg
    assert _SENTINEL not in msg
    (call,) = gh.calls()
    assert call["opts"]["--label"] == [label]


@pytest.mark.parametrize("label", _LABELS)
def test_edit_body_and_claim_extras_keep_flag_shaped_values(gh, backend, label):
    """_edit_body's extra args (claim's --add-assignee/--add-label, release's
    --remove-label) carry values gh must see verbatim."""
    item = {"number": 12, "_github_body": "", "text": ""}
    backend._edit_body(item, "body é", {}, "--add-assignee", label,
                       "--add-label", label, "--remove-label", label)
    edit = next(c for c in gh.calls() if c["argv"][:2] == ["issue", "edit"])
    assert edit["argv"] == ["issue", "edit", "--body-file", "-", "12", "--repo", REPO,
                            "--add-assignee", label, "--add-label", label,
                            "--remove-label", label]
    assert edit["opts"]["--add-label"] == [label]
    assert edit["opts"]["_positional"] == ["12"]
    assert edit["body"].decode("utf-8") == github_backend._body_with_metadata("body é", {})


@pytest.mark.parametrize("title", _FLAG_SHAPED + ["normal title é"])
@pytest.mark.parametrize("size", ["empty", "short", "oversized"])
def test_enqueue_with_flag_shaped_title_keeps_title_and_body(gh, backend, monkeypatch, title, size):
    """Verifier witness (WT-40 verify:4): enqueue(title='--body') turned the
    title into --body-file and left the generated body on argv."""
    monkeypatch.setattr(backend, "_ensure_labels", lambda: None)
    monkeypatch.setattr(backend, "get", lambda ident: {"number": 7})
    # Within enqueue's own clipping bounds (note 4000, text 24000 chars).
    text = {"empty": "", "short": f"{_SENTINEL} t \"q\"\r\n",
            "oversized": _SENTINEL + "\U0001F600" * 23000}[size]
    note = f"{_SENTINEL} n" if size != "oversized" else _SENTINEL + "\U0001F600" * 3970

    backend.enqueue(title=title, note=note, text=text)

    (call,) = gh.calls()
    assert call["opts"]["--title"] == [title]
    assert call["opts"]["--body-file"] == ["-"]
    assert "--body" not in call["opts"]
    assert call["argv"][:2] == ["issue", "create"]
    sent = call["body"].decode("utf-8")
    assert sent == github_backend._body_with_metadata(text or note, _sent_meta(sent))
    assert not any(_SENTINEL in a for a in call["argv"]), "no body or note text on argv"
    if size == "oversized":
        assert len(call["body"]) > _arg_limit()


def _sent_meta(sent):
    return github_backend._split_body(sent)[1]


@pytest.mark.parametrize("title", _FLAG_SHAPED)
def test_failure_diagnostics_never_show_body_note_or_title(gh, backend, monkeypatch, title):
    monkeypatch.setattr(backend, "_ensure_labels", lambda: None)
    monkeypatch.setenv("FAKE_GH_MODE", "fail")
    with pytest.raises(github_backend.GitHubBackendError) as exc:
        backend.enqueue(title=title + _SENTINEL, note=f"{_SENTINEL} note",
                        text=f"{_SENTINEL} body")
    msg = str(exc.value)
    assert "HTTP 422" in msg and "--body-file -" in msg
    assert _SENTINEL not in msg
    assert f"<text:{len(title + _SENTINEL)} chars>" in msg


def test_timeout_diagnostics_hide_title_and_body(gh, backend, monkeypatch):
    monkeypatch.setenv("FAKE_GH_MODE", "hang")
    real_run = subprocess.run
    monkeypatch.setattr(github_backend.subprocess, "run",
                        lambda *a, **k: real_run(*a, **{**k, "timeout": 1.5}))
    with pytest.raises(github_backend.GitHubBackendError) as exc:
        backend._run(["issue", "edit", "6", "--repo", REPO, "--title",
                      github_backend._Text(_SENTINEL)], body=_SENTINEL)
    assert "timed out" in str(exc.value) and _SENTINEL not in str(exc.value)


def test_update_with_flag_shaped_title_edits_both_exactly(gh, backend):
    item = {"number": 12, "_github_body": "", "text": ""}
    backend._edit_body(item, "body é", {}, "--title", github_backend._Text("--body"))
    edit = next(c for c in gh.calls() if c["argv"][:2] == ["issue", "edit"])
    assert edit["opts"]["--title"] == ["--body"]
    assert edit["body"].decode("utf-8") == github_backend._body_with_metadata("body é", {})
