"""Credential-free Antigravity routing, interoperable with CCC when installed.

Only account ids and log/session identities are persisted. No CCC import or
running server is needed; both tools use the same local binding schema.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3

from . import config

_IDS = ("primary", "secondary")
_CLOUD_ENV = frozenset((
    "AGY_ADC_AUTH", "GOOGLE_APPLICATION_CREDENTIALS", "GEMINI_API_KEY",
    "GOOGLE_API_KEY", "GOOGLE_GENAI_USE_VERTEXAI", "GOOGLE_CLOUD_PROJECT",
    "GOOGLE_CLOUD_LOCATION", "GOOGLE_CLOUD_QUOTA_PROJECT", "GCLOUD_PROJECT",
    "GCP_PROJECT", "CLOUDSDK_CORE_PROJECT", "GOOGLE_GEMINI_BASE_URL",
    "GOOGLE_VERTEX_BASE_URL", "ANTHROPIC_API_KEY", "OPENAI_API_KEY",
    "AGY_CDE_ENTERPRISE_AUTH", "AGY_CDE_SEAMLESS_AUTH", "AGY_CLI_CDE_AUTH_ACTION",
    "CLOUDSDK_AUTH_ACCESS_TOKEN", "GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_CREDENTIALS",
    "GOOGLE_SERVICE_ACCOUNT_AUTH", "GOOGLE_GENAI_USE_ENTERPRISE", "AGY_ACP_ENABLE_GATEWAY_AUTH",
))


def validate(account_id):
    if account_id not in _IDS:
        raise ValueError("Unknown Antigravity account; choose primary or secondary")
    return account_id


def worker_account():
    value = config._ccc_spawn_defaults().get("worker_antigravity_account_id", "primary")
    return validate(value or "primary")


def _state_root():
    return Path(os.environ.get("WATCHTOWER_ANTIGRAVITY_STATE_DIR")
                or Path(config.CCC_SPAWN_DEFAULTS_FILE).parent / "antigravity-accounts")


@contextmanager
def _db():
    root = _state_root()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    con = sqlite3.connect(root / "bindings.sqlite3", timeout=10)
    con.execute("CREATE TABLE IF NOT EXISTS bindings (sid TEXT PRIMARY KEY, account_id TEXT NOT NULL)")
    con.execute("CREATE TABLE IF NOT EXISTS pending (log TEXT PRIMARY KEY, account_id TEXT NOT NULL)")
    try:
        with con:
            yield con
    finally:
        con.close()


def session_account(sid):
    if not sid:
        return "primary"
    with _db() as con:
        row = con.execute("SELECT account_id FROM bindings WHERE sid=?", (str(sid),)).fetchone()
    if row:
        return validate(row[0])
    # Pending logs may reveal a native ID before worker discovery runs.
    from .workers import resolve_antigravity_session_id
    with _db() as con:
        pending = con.execute("SELECT log FROM pending").fetchall()
    for (log,) in pending:
        if resolve_antigravity_session_id(log) == sid:
            finalize_pending(log, sid)
            return session_account(sid)
    # Legacy sessions predate selection and must never follow a new default.
    return "primary"


def bind(sid, account_id):
    validate(account_id)
    if not sid:
        return
    with _db() as con:
        old = con.execute("SELECT account_id FROM bindings WHERE sid=?", (str(sid),)).fetchone()
        if old and old[0] != account_id:
            raise ValueError("Antigravity session is already bound to another account")
        con.execute("INSERT OR IGNORE INTO bindings VALUES (?,?)", (str(sid), account_id))


def bind_pending(log, account_id):
    validate(account_id)
    with _db() as con:
        con.execute("INSERT OR REPLACE INTO pending VALUES (?,?)", (str(log), account_id))


def finalize_pending(log, sid):
    if not log or not sid:
        return
    with _db() as con:
        row = con.execute("SELECT account_id FROM pending WHERE log=?", (str(log),)).fetchone()
        if row:
            old = con.execute("SELECT account_id FROM bindings WHERE sid=?", (str(sid),)).fetchone()
            if old and old[0] != row[0]:
                raise ValueError("Antigravity session is already bound to another account")
            con.execute("INSERT OR IGNORE INTO bindings VALUES (?,?)", (str(sid), row[0]))
            con.execute("DELETE FROM pending WHERE log=?", (str(log),))


def launch_settings(account_id, model=None):
    validate(account_id)
    if account_id == "secondary":
        overlay = {"gcp": {"project": "", "location": ""}, "modelProvider": ""}
    else:
        home = Path(os.environ.get("WATCHTOWER_ANTIGRAVITY_HOME")
                    or Path(os.environ.get("CCC_ANTIGRAVITY_PRIMARY_HOME") or Path.home()) / ".gemini" / "antigravity-cli")
        try:
            settings = json.loads((home / "settings.json").read_text())
        except FileNotFoundError:
            settings = {}
        except (OSError, ValueError):
            raise ValueError("Cannot read Antigravity authentication settings") from None
        if not isinstance(settings, dict):
            raise ValueError("Cannot read Antigravity authentication settings")
        gcp = settings.get("gcp") or {}
        if not isinstance(gcp, dict):
            raise ValueError("Cannot read Antigravity authentication settings")
        overlay = {"gcp": {key: gcp.get(key, "") for key in ("project", "location")},
                   "modelProvider": settings.get("modelProvider") or ""}
    if model:
        overlay["model"] = model
    return overlay


def _share_directory(link, target):
    target = target.resolve()
    try:
        link.symlink_to(target, target_is_directory=True)
    except FileExistsError:
        if not link.is_symlink() or link.resolve() != target:
            raise ValueError("Antigravity secondary storage conflicts")


def _secondary_home(env):
    root = _state_root() / "secondary-home"
    cli = root / ".gemini" / "antigravity-cli"
    cli.mkdir(mode=0o700, parents=True, exist_ok=True)
    primary_home = Path(env["CCC_ANTIGRAVITY_PRIMARY_HOME"])
    native = Path(os.environ.get("WATCHTOWER_ANTIGRAVITY_HOME")
                  or primary_home / ".gemini" / "antigravity-cli")
    # CLI 1.5.8 ignores CONFIG_REL_DIR in normal startup. Isolate HOME and
    # keyring scope; share conversation data only, never settings or profiles.
    for name in ("conversations", "brain"):
        target = (native / name).resolve()
        target.mkdir(mode=0o700, parents=True, exist_ok=True)
        _share_directory(cli / name, target)
    # Queue workers must retain their WT registry, legacy config and stop
    # sentinels. This does not expose any native Antigravity auth profiles.
    _share_directory(root / ".watchtower", primary_home / ".watchtower")
    return root


# Ephemeral selectors only: never tokens, API keys, or credential contents.
# ADC is an absolute file reference, not the file's material.
_PRIMARY_SELECTOR_MARKER = "CCC_ANTIGRAVITY_PRIMARY_SELECTORS"
_PRIMARY_REQUIRED_MARKER = "CCC_ANTIGRAVITY_PRIMARY_REQUIRED_ENV"
_CREDENTIAL_ENV = frozenset((
    "GEMINI_API_KEY", "GOOGLE_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY",
    "GOOGLE_CREDENTIALS", "GOOGLE_SERVICE_ACCOUNT_AUTH", "CLOUDSDK_AUTH_ACCESS_TOKEN",
    "GOOGLE_OAUTH_ACCESS_TOKEN",
))
_PRIMARY_SELECTORS = frozenset((
    "ANTIGRAVITY_CREDENTIAL_SCOPE", "ANTIGRAVITY_CONFIG_REL_DIR",
    "ANTIGRAVITY_GEMINI_DIR", "GEMINI_DIR", "XDG_CONFIG_HOME", "AGY_ADC_AUTH",
    "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_GENAI_USE_VERTEXAI",
    "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION", "GOOGLE_CLOUD_QUOTA_PROJECT",
    "GCLOUD_PROJECT", "GCP_PROJECT", "CLOUDSDK_CORE_PROJECT",
))


def _safe_selectors(env):
    saved = {}
    for key in _PRIMARY_SELECTORS:
        value = env.get(key)
        if value is None:
            continue
        if not isinstance(value, str):
            continue
        if key == "GOOGLE_APPLICATION_CREDENTIALS" and not Path(value).is_absolute():
            continue
        if key in ("AGY_ADC_AUTH", "GOOGLE_GENAI_USE_VERTEXAI") and value.lower() not in (
                "1", "true", "yes", "0", "false", "no"):
            continue
        saved[key] = value
    return saved


def _is_ccc_secondary(env):
    home = env.get("HOME")
    primary = env.get("CCC_ANTIGRAVITY_PRIMARY_HOME")
    return bool(
        env.get("CCC_ANTIGRAVITY_ACCOUNT_ID") == "secondary"
        and env.get("ANTIGRAVITY_CREDENTIAL_SCOPE") == "ccc-antigravity-secondary"
        and home and primary and Path(primary).is_absolute()
        and Path(home).resolve() == (_state_root() / "secondary-home").resolve()
        and Path(primary).resolve() != Path(home).resolve())


def _restore_primary(env):
    try:
        required = json.loads(env.pop(_PRIMARY_REQUIRED_MARKER, "[]"))
    except (ValueError, TypeError):
        required = []
    if isinstance(required, list) and any(
            isinstance(key, str) and (key in _CREDENTIAL_ENV or key.startswith("AGY_LLM_GATEWAY_"))
            and not env.get(key) for key in required):
        raise ValueError("Primary Antigravity authentication requires the original launcher environment")
    env["HOME"] = env["CCC_ANTIGRAVITY_PRIMARY_HOME"]
    # Older CCC launches have no snapshot. Remove only overrides known to
    # belong to this namespace so native primary config can select auth again.
    for key, forced in (("ANTIGRAVITY_CREDENTIAL_SCOPE", "ccc-antigravity-secondary"),
                        ("ANTIGRAVITY_CONFIG_REL_DIR", "ccc-antigravity-secondary/config"),
                        ("AGY_ADC_AUTH", "false")):
        if env.get(key) == forced:
            env.pop(key, None)
    try:
        saved = json.loads(env.pop(_PRIMARY_SELECTOR_MARKER, "{}"))
    except (ValueError, TypeError):
        saved = {}
    if isinstance(saved, dict):
        env.update(_safe_selectors(saved))


def launch_env(account_id, env=None):
    validate(account_id)
    result = dict(os.environ if env is None else env)
    was_secondary = _is_ccc_secondary(result)
    if account_id == "primary" and was_secondary:
        _restore_primary(result)
    elif account_id == "secondary" and not was_secondary:
        result[_PRIMARY_SELECTOR_MARKER] = json.dumps(_safe_selectors(result), separators=(",", ":"))
        result[_PRIMARY_REQUIRED_MARKER] = json.dumps(sorted(
            key for key in result if result[key] and (
                key in _CREDENTIAL_ENV or key.startswith("AGY_LLM_GATEWAY_"))))
    result["CCC_ANTIGRAVITY_ACCOUNT_ID"] = account_id
    result.setdefault("CCC_ANTIGRAVITY_PRIMARY_HOME", result.get("HOME") or str(Path.home()))
    result["CCC_ANTIGRAVITY_LS_DIR"] = str(Path(config.CCC_SPAWN_DEFAULTS_FILE).parent / "agy-ls")
    if account_id == "secondary":
        from . import storage
        for key in list(result):
            if key in _CLOUD_ENV or key.startswith("AGY_LLM_GATEWAY_"):
                result.pop(key, None)
        result["AGY_ADC_AUTH"] = "false"
        result["ANTIGRAVITY_CREDENTIAL_SCOPE"] = "ccc-antigravity-secondary"
        result["ANTIGRAVITY_CONFIG_REL_DIR"] = "ccc-antigravity-secondary/config"
        result["HOME"] = str(_secondary_home(result))
        result.pop("ANTIGRAVITY_GEMINI_DIR", None)
        result.pop("GEMINI_DIR", None)
        result.pop("XDG_CONFIG_HOME", None)
        result.setdefault("WATCHTOWER_DATA_DIR", str(storage.data_dir()))
        result.setdefault("WATCHTOWER_CONFIG_FILE", str(config.config_path()))
        result.setdefault("WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", str(config.CCC_SPAWN_DEFAULTS_FILE))
        result.setdefault("WATCHTOWER_MODEL_POLICY_FILE", str(config.CCC_MODEL_POLICY_FILE))
        if not result.get("CCC_SERVER"):
            try:
                server = (Path(config.CCC_SPAWN_DEFAULTS_FILE).parent / "port.txt").read_text().strip()
            except OSError:
                server = ""
            if server.startswith(("http://", "https://")):
                result["CCC_SERVER"] = server
        if result.get("CCC_SERVER"):
            result.setdefault("WATCHTOWER_DELEGATE_URL", result["CCC_SERVER"])
    return result


def configure_command(cmd, account_id, model=None):
    if any(word == "--settings" or word.startswith("--settings=") for word in cmd[1:]):
        raise ValueError("Use account selection instead of custom Antigravity --settings")
    return list(cmd) + ["--settings", json.dumps(launch_settings(account_id, model), separators=(",", ":"))]
