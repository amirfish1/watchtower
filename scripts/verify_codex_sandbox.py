#!/usr/bin/env python3
"""Opt-in real Codex acceptance against a disposable queue and checkout.

Run from the repository: python3 scripts/verify_codex_sandbox.py --transport exec
Each invocation starts one model turn on the existing Codex account. It never
uses the real queue, changes live settings, or copies authentication material.
Evidence is retained in the printed directory for review.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=("exec", "app-server"), required=True)
    parser.add_argument("--codex-home", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--model", default="")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    auth_home = args.codex_home.expanduser().resolve(strict=True)
    package = Path(__file__).resolve().parent.parent
    codex = shutil.which("codex")
    if not codex:
        parser.error("codex is not installed")
    # /tmp can be a shared-memory mount on fleet hosts. Keep the fixture on
    # the ordinary filesystem so a denied shared-memory mount cannot hide it.
    base = Path(tempfile.mkdtemp(prefix="wt-sandbox-acceptance-", dir=Path.home()))
    repo, trusted = base / "workspace", base / "controller"
    repo.mkdir()
    trusted.mkdir(mode=0o700)
    for key in list(os.environ):
        if key.startswith(("WATCHTOWER_", "WT_", "CCC_")) or key in (
                "CODEX_THREAD_ID", "CLAUDE_CODE_SESSION_ID", "CLAUDE_SESSION_ID"):
            os.environ.pop(key)
    files = {
        "WATCHTOWER_STORE": "queue.json", "WATCHTOWER_CONFIG_FILE": "queue-config.json",
        "WATCHTOWER_WORKERS_FILE": "workers.json", "WATCHTOWER_ACTIVITY_LOG": "activity.log",
        "WATCHTOWER_WORKER_SESSIONS_FILE": "worker-sessions.json",
        "WATCHTOWER_WORKER_IDS_FILE": "worker-ids.json",
        "WATCHTOWER_SESSION_ORIGINS_FILE": "origins.json",
        "WATCHTOWER_OUTBOX_FILE": "outbox.json", "WATCHTOWER_AGENTS_FILE": "agents.json",
        "WATCHTOWER_DELIVERIES_FILE": "deliveries.json",
        "WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE": "absent-spawn-defaults.json",
        "WATCHTOWER_LAUNCH_FAILURES_FILE": "failures.json",
        "WATCHTOWER_CODEX_THREAD_REGISTRY": "codex-threads.json",
        "WATCHTOWER_DAEMON_PID": "daemon.pid",
    }
    os.environ.update({key: str(trusted / name) for key, name in files.items()})
    os.environ.update(WATCHTOWER_DATA_DIR=str(trusted / "data"), WT_NO_AUTOSTART="1",
                      WT_NO_SELF_UPDATE="1", WATCHTOWER_DELEGATE_URL="off",
                      WATCHTOWER_CODEX_BIN=codex)
    sys.path.insert(0, str(package))
    from watchtower import config, queue, sandbox_bridge, sandbox_policy, sandbox_handoff, workers

    shim_dir = trusted / "bin"
    shim_dir.mkdir()
    entry = ("import sys; sys.path.insert(0, " + repr(str(package)) + "); "
             "from watchtower.cli import main; sys.exit(main())")
    shim = shim_dir / "wt"
    shim.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + " -I -c "
                    + shlex.quote(entry) + ' "$@"\n')
    shim.chmod(0o700)
    os.environ["PATH"] = str(shim_dir) + os.pathsep + os.environ["PATH"]
    queue_name, worker = "ACCEPTANCE", "sandbox-acceptance-" + args.transport
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    (repo / "arithmetic.py").write_text("def add(a, b):\n    return a - b\n")
    (repo / "test_arithmetic.py").write_text(
        "import unittest\nfrom arithmetic import add\n"
        "class Addition(unittest.TestCase):\n"
        "    def test_addition(self):\n        self.assertEqual(add(2, 3), 5)\n")
    subprocess.run(["git", "-C", str(repo), "add", "arithmetic.py", "test_arithmetic.py"], check=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=Sandbox Acceptance",
                    "-c", "user.email=acceptance@localhost", "-c", "core.hooksPath=/dev/null",
                    "commit", "-qm", "acceptance fixture"], check=True)
    initial_commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    integration = base / "integration"
    # Clone the controller's known fixture BEFORE granting workspace writes.
    subprocess.run(["git", "clone", "--no-local", "-q", str(repo), str(integration)], check=True)
    config.set_codex_sandbox(queue_name, "workspace-write", str(base / "isolated"))
    config.set_codex_transport(queue_name, args.transport)
    config.set_repo_path(queue_name, str(repo))
    # Prevent a manual-run-only fixture from offering zero tickets.
    config.set_auto_drain(queue_name, True)
    item = queue.enqueue(project=queue_name, note="Fix fixture addition", repo_path=str(repo))
    store = sandbox_bridge.register(queue_name, worker, str(repo))
    workers.codex_sandbox_prepare(queue_name, worker, str(repo))
    # A worker can plant any executable in its writable PATH. Trusted proof
    # and export must use system Git and reject a fictional commit id.
    planted = store / "bin" / "git"
    fake_sha = "ab" * 20
    planted.write_text("#!/bin/sh\nprintf '%s\\n' " + fake_sha + "\n")
    planted.chmod(0o700)
    forged = sandbox_policy.verify_worker_commit(worker, fake_sha)
    export_refused = False
    try:
        sandbox_policy.export_patch(str(repo), str(store), fake_sha, initial_commit)
    except sandbox_policy.SandboxPolicyError:
        export_refused = True
    finally:
        planted.unlink()
    if forged.get("ok") or not export_refused:
        raise RuntimeError("worker-controlled PATH forged commit or patch proof")
    forbidden, shared = trusted / "forbidden", Path("/dev/shm") / (base.name + "-forbidden")
    boundary_program = f'''import json, pathlib, socket
out={{}}
for name, path in [("outside", {str(forbidden)!r}), ("shared_memory", {str(shared)!r})]:
    try:
        pathlib.Path(path).write_text("forbidden")
        out[name]=(name == "outside")
    except OSError:
        out[name]=True
for name, family in [("tcp_denied", socket.AF_INET), ("unix_denied", socket.AF_UNIX)]:
    try:
        s=socket.socket(family);s.settimeout(1)
        s.connect(("127.0.0.1", 9) if family==socket.AF_INET else {str(trusted / 'absent.sock')!r})
        out[name]=False
    except PermissionError:
        out[name]=True
    except OSError:
        out[name]=False
pathlib.Path("boundary.json").write_text(json.dumps(out))
assert all(out.values()), out
'''
    # Fixed trusted fixture content; the worker only runs it inside its policy.
    (repo / "check_boundary.py").write_text(boundary_program)
    goal = (
        f"This is a disposable sandbox acceptance fixture. Work only in {repo}. "
        f"Claim the single ticket with wt claim -q {queue_name} --worker {worker} --json. "
        "Change arithmetic.py to return a + b. Run python3 -m unittest -v > unit-result.txt 2>&1 and "
        "python3 check_boundary.py; both must pass, without approval or policy escalation. "
        "Commit only arithmetic.py with git add arithmetic.py then "
        "git -c user.name='Sandbox Acceptance' -c user.email=acceptance@localhost "
        "-c core.hooksPath=/dev/null commit -m 'Fix addition'. "
        f"Then wt close {item['ref']} --worker {worker} --summary 'Fixed fixture addition.' "
        "--commit <your SHA>. Do not push, create goals, access credentials, or operate on "
        "other tickets. The close exports a handoff and parks the ticket for trusted "
        "integration; do not claim the store commit integrated. Stop after that handoff, "
        "or explain the exact failure."
    )
    argv = workers.build_drain_command(queue_name, "codex", worker, str(repo),
                                        model=args.model, effort="medium", goal=goal)
    env = workers._spawn_env(worker, queue=queue_name, engine="codex", codex_home=str(auth_home))
    log_path = trusted / "worker.log"
    stop = threading.Event()
    broker = threading.Thread(target=sandbox_bridge.serve, args=(stop,), daemon=True)
    broker.start()
    try:
        with log_path.open("wb") as log:
            proc = subprocess.Popen(argv, cwd="/", env=env, stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            workers.record_worker(proc.pid, queue_name, "codex", worker, str(repo), str(log_path),
                                   codex_home=str(auth_home))
            try:
                code = proc.wait(args.timeout)
            except subprocess.TimeoutExpired:
                import signal
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(15)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(5)
                code = 124
    finally:
        stop.set()
        broker.join(5)
    current = queue.get(item["ref"]) or {}
    handoff_status = current.get("status")
    integrated_commit = ""
    handoffs = list((sandbox_bridge.authority_dir() / "handoffs").glob(worker + "-*.json"))
    if handoff_status == "awaiting_answer" and len(handoffs) == 1:
        record = json.loads(handoffs[0].read_text())
        adopted = sandbox_handoff.adopt(worker, record["commit"], integration)
        integrated_commit = adopted["commit"]
        # Fixture controller reviews known arithmetic bytes before executing.
        if (integration / "arithmetic.py").read_text() != "def add(a, b):\n    return a + b\n":
            raise RuntimeError("adopted fixture patch failed source review")
        subprocess.run([sys.executable, "-m", "unittest", "-v"], cwd=integration, check=True)
        # This disposable fixture has a local release ref; real integrations
        # must push and check the configured remote release branch instead.
        subprocess.run(["git", "-C", str(integration), "update-ref", "refs/remotes/origin/main",
                        integrated_commit], check=True)
        subprocess.run(["git", "-C", str(integration), "merge-base", "--is-ancestor",
                        integrated_commit, "origin/main"], check=True)
        resolution = dict(record["resolution"], commit=integrated_commit)
        current = queue.close(item["ref"], resolution=resolution) or {}
    boundary_file = repo / "boundary.json"
    boundary = json.loads(boundary_file.read_text()) if boundary_file.exists() else {}
    result = {"transport": args.transport, "exit_code": code, "evidence": str(base),
              "status": current.get("status"), "worker": current.get("claimed_by"),
              "commit": (current.get("resolution") or {}).get("commit"), "boundary": boundary,
              "handoff_status": handoff_status, "integrated_commit": integrated_commit,
              "forged_commit_rejected": not forged.get("ok"), "forged_export_rejected": export_refused,
              "forbidden_write_absent": not forbidden.exists() and not shared.exists()}
    unit_result = repo / "unit-result.txt"
    result["tests_passed"] = unit_result.exists() and "\nOK\n" in unit_result.read_text()
    result["source_fixed"] = (repo / "arithmetic.py").read_text() == "def add(a, b):\n    return a + b\n"
    result["passed"] = bool(code == 0 and current.get("status") == "closed"
                             and handoff_status == "awaiting_answer" and integrated_commit and result["commit"]
                             and result["commit"] != initial_commit
                             and result["tests_passed"] and result["source_fixed"]
                             and len(boundary) == 4 and all(boundary.values())
                             and result["forbidden_write_absent"])
    (trusted / "verdict.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
