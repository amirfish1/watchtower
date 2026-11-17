"""Lazy, machine-configured credentials for explicit semantic triage only."""
import json
import os
import subprocess

from . import storage


def triage_key():
    key = os.environ.get("TYPESAFE_API_KEY") or os.environ.get("JEV_API_KEY")
    if key:
        return key
    path = storage.data_dir() / "related-credentials.json"
    try:
        text = path.read_text()
    except FileNotFoundError:
        raise ValueError("Jev triage needs TYPESAFE_API_KEY or JEV_API_KEY, or a configured "
                         "related credential command; local retrieval still works") from None
    except OSError:
        raise ValueError("Cannot read related credential command configuration") from None
    try:
        settings = json.loads(text)
        command = settings.get("command") if isinstance(settings, dict) else None
        if (not isinstance(command, list) or not command
                or not all(isinstance(arg, str) and arg and "\0" not in arg for arg in command)
                or not os.path.isabs(command[0])):
            raise ValueError
    except ValueError:
        raise ValueError("Related credential command must be a JSON argv array with an absolute executable") from None
    try:
        result = subprocess.run(command, cwd="/", stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                text=True, timeout=10, check=True)
    except (OSError, ValueError, subprocess.SubprocessError):
        # Command output may contain credentials, including on failure.
        raise ValueError("Related credential command failed; local retrieval still works") from None
    key = result.stdout.strip()
    if not key or len(key) > 16384 or any(c.isspace() for c in key):
        raise ValueError("Related credential command did not return one nonempty key")
    return key
