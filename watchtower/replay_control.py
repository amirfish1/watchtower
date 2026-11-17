"""Cooperative cancellation for capped replay runners, never an approval.

Runners must hold request() around the entire upstream call. Cancellation
waits for that call to finish, then prevents every subsequent request. Existing
legacy ledgers are deliberately unsupported and must never be converted/reset.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import time


SCHEMA = "watchtower-replay-control-v1"


class ReplayControl:
    def __init__(self, path, decision_id):
        self.path = Path(path).resolve()
        self.decision_id = decision_id

    @contextmanager
    def locked(self, timeout=None):
        # Unlike the queue's best-effort lock this boundary must fail closed.
        import fcntl
        with self.path.with_suffix(self.path.suffix + ".lock").open("a") as lock:
            if timeout is None:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            else:
                deadline = time.monotonic() + timeout
                while True:
                    try:
                        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("runner request still in flight; cancellation NOT acknowledged; retry later")
                        time.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _read(self):
        state = json.loads(self.path.read_text())
        if (state.get("schema") != SCHEMA
                or state.get("decision_id") != self.decision_id):
            raise ValueError("unsupported runner or decision mismatch; legacy evidence is unchanged")
        count, cap = state.get("forwarded"), state.get("cap")
        if (type(count) is not int or type(cap) is not int or not 0 <= count <= cap
                or cap < 1 or type(state.get("terminal")) is not bool
                or type(state.get("cancelled")) is not bool):
            raise ValueError("invalid replay control ledger")
        return state

    def _write(self, state):
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w") as stream:
            json.dump(state, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        tmp.replace(self.path)

    def create(self, cap):
        """Initialize only after separately obtaining approval for this cap.

        Exclusive creation retains terminal budgets across process restarts.
        This API grants no authority to make model requests.
        """
        if type(cap) is not int or cap < 1 or not self.decision_id:
            raise ValueError("a decision ID and positive request cap are required")
        with self.locked():
            with self.path.open("x") as stream:
                json.dump(dict(schema=SCHEMA, decision_id=self.decision_id,
                               forwarded=0, cap=cap, terminal=False, cancelled=False), stream)

    @contextmanager
    def request(self):
        with self.locked():
            state = self._read()
            if state["terminal"] or state["forwarded"] >= state["cap"]:
                raise ValueError("replay is terminal or request cap exhausted")
            state["forwarded"] += 1
            self._write(state)  # charge before sending, including failed attempts
            yield

    def finish(self):
        with self.locked():
            state = self._read()
            if not state["terminal"]:
                state["terminal"] = True
                self._write(state)

    def cancel(self):
        self._read()  # reject legacy files before even creating a lock beside them
        with self.locked(timeout=30):
            state = self._read()
            if not state["terminal"]:
                state.update(terminal=True, cancelled=True)
                self._write(state)
            # A naturally completed runner is consumed, not newly cancelled.
            if not state["cancelled"]:
                raise ValueError("runner already completed; use --consumed for spent requests")
            return dict(path=str(self.path), sha256=hashlib.sha256(self.path.read_bytes()).hexdigest(),
                        requests=state["forwarded"], cap=state["cap"], terminal=True,
                        runner_schema=SCHEMA)
