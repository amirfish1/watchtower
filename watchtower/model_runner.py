"""Bounded, tool-free structured model execution under CCC capability profiles.

No queue claims, delivery actions, provider credentials or caller model selection.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time

from . import config, workers


class ModelRunError(ValueError):
    """Safe diagnostic: never contains input or provider response text."""


def _check_schema(schema):
    if not isinstance(schema, dict):
        raise ModelRunError('invalid JSON schema')
    allowed = {'type', 'properties', 'required', 'additionalProperties', 'items',
               'enum', 'const', 'anyOf', 'description', 'title', 'default',
               'minimum', 'maximum', 'minItems', 'maxItems', 'minLength', 'maxLength', 'pattern'}
    if set(schema) - allowed:
        raise ModelRunError('unsupported JSON schema keyword')
    for nested in schema.get('properties', {}).values():
        _check_schema(nested)
    for nested in schema.get('anyOf', []):
        _check_schema(nested)
    for key in ('items', 'additionalProperties'):
        if isinstance(schema.get(key), dict):
            _check_schema(schema[key])


def validate_schema(value, schema, path='$'):
    """Validate the bounded JSON Schema vocabulary used by model consumers."""
    if not isinstance(schema, dict):
        raise ModelRunError('invalid JSON schema')
    allowed = {'type', 'properties', 'required', 'additionalProperties', 'items',
               'enum', 'const', 'anyOf', 'description', 'title', 'default',
               'minimum', 'maximum', 'minItems', 'maxItems', 'minLength', 'maxLength', 'pattern'}
    if set(schema) - allowed:
        raise ModelRunError('unsupported JSON schema keyword')
    for branch in schema.get('anyOf', []):
        try:
            validate_schema(value, branch, path)
            break
        except ModelRunError:
            continue
    else:
        if 'anyOf' in schema:
            raise ModelRunError(f'schema mismatch at {path}')
    kind = schema.get('type')
    kinds = kind if isinstance(kind, list) else [kind]
    checks = {'object': isinstance(value, dict), 'array': isinstance(value, list),
              'string': isinstance(value, str), 'integer': type(value) is int,
              'number': type(value) in (int, float) and math.isfinite(value),
              'boolean': type(value) is bool, 'null': value is None}
    if kind is not None and not any(checks.get(t, False) for t in kinds):
        raise ModelRunError(f'schema mismatch at {path}')
    if 'enum' in schema and value not in schema['enum']:
        raise ModelRunError(f'schema enum mismatch at {path}')
    if 'const' in schema and value != schema['const']:
        raise ModelRunError(f'schema const mismatch at {path}')
    if isinstance(value, dict):
        props = schema.get('properties', {})
        if any(key not in value for key in schema.get('required', [])):
            raise ModelRunError(f'schema missing required field at {path}')
        for key, item in value.items():
            if key in props:
                validate_schema(item, props[key], path + '.' + key)
            elif schema.get('additionalProperties') is False:
                raise ModelRunError(f'schema extra field at {path}')
            elif isinstance(schema.get('additionalProperties'), dict):
                validate_schema(item, schema['additionalProperties'], path)
    if isinstance(value, list):
        for item in value:
            validate_schema(item, schema.get('items', {}), path + '[]')
        if len(value) < schema.get('minItems', 0) or len(value) > schema.get('maxItems', float('inf')):
            raise ModelRunError(f'schema array length mismatch at {path}')
    if isinstance(value, str):
        if len(value) < schema.get('minLength', 0) or len(value) > schema.get('maxLength', float('inf')):
            raise ModelRunError(f'schema string length mismatch at {path}')
        if 'pattern' in schema and re.search(schema['pattern'], value) is None:
            raise ModelRunError(f'schema pattern mismatch at {path}')
    if type(value) in (int, float):
        if not math.isfinite(value) or value < schema.get('minimum', -float('inf')) or value > schema.get('maximum', float('inf')):
            raise ModelRunError(f'schema number mismatch at {path}')


def _capture(argv, prompt, cwd, timeout, limit, env):
    """File-backed capture, one process group, hard elapsed/output limits."""
    with tempfile.TemporaryFile() as inp, tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        inp.write(prompt.encode()); inp.seek(0)
        proc = subprocess.Popen(argv, stdin=inp, stdout=out, stderr=err,
                                cwd=cwd, env=env, start_new_session=True)
        deadline = time.monotonic() + timeout
        try:
            while proc.poll() is None:
                if time.monotonic() >= deadline:
                    raise ModelRunError('model execution timed out')
                if os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > limit:
                    raise ModelRunError('model output limit exceeded')
                time.sleep(.05)
            if os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > limit:
                raise ModelRunError('model output limit exceeded')
            out.seek(0); err.seek(0)
            return proc.returncode, out.read().decode(errors='replace'), err.read().decode(errors='replace')
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()


def _attempt(candidate, request, directory, timeout, limit):
    engine, model = candidate['engine'], candidate['model']
    effort = candidate.get('effort') or 'medium'
    env = dict(os.environ)
    if engine == 'claude':
        argv = [workers._resolve_engine_bin('claude'), '-p', '--output-format', 'json',
                '--tools', '', '--strict-mcp-config', '--setting-sources', '',
                '--no-session-persistence', '--model', model, '--effort', effort,
                '--max-budget-usd', str(request['max_budget_usd']),
                '--system-prompt', request['system'],
                '--json-schema', json.dumps(request['json_schema'])]
        prompt = request['prompt']
    elif engine == 'devin':
        # Devin print mode returns plain text, so the schema is carried in the
        # prompt and the reply is parsed as JSON. A deny-all config keeps the
        # tools:none contract: the run must answer from the prompt alone.
        deny_config = os.path.join(directory, 'devin-deny-all.json')
        with open(deny_config, 'w') as fh:
            json.dump({'permissions': {'deny': [
                'read', 'edit', 'write', 'grep', 'glob', 'exec',
                'webfetch', 'websearch', 'mcp__*']}}, fh)
        schema_hint = json.dumps(request['json_schema'])
        combined = (request['system'] + '\n\n' + request['prompt'] +
                    '\n\nRespond with a single JSON object matching this JSON '
                    'Schema. Output only the JSON, no prose, no code fences:\n' +
                    schema_hint)
        argv = [workers._resolve_engine_bin('devin') or 'devin', '-p',
                '--config', deny_config, '--model', model,
                '--respect-workspace-trust', 'false', '--', combined]
        prompt = ''
    elif engine == 'codex':
        raise ModelRunError('Codex tools:none execution is unavailable in the installed runtime; profile cannot safely fall back')
    else:
        raise ModelRunError('profile engine does not support tool-free structured execution')
    rc, out, err = _capture(argv, prompt, directory, timeout, limit, env)
    if engine == 'devin':
        if rc != 0:
            return None, workers._quota_exhausted(out + '\n' + err)
        # The CLI prepends a login banner (with ANSI styling) to the reply;
        # the answer is the JSON object span within it.
        text = re.sub(r'\x1b\[[0-9;]*m', '', out)
        start, end = text.find('{'), text.rfind('}')
        try:
            structured = json.loads(text[start:end + 1] if 0 <= start < end else text.strip())
        except ValueError:
            return None, False
        return {'subtype': 'success', 'is_error': False, 'result': text.strip(),
                'structured_output': structured, 'modelUsage': {}}, False
    if engine == 'claude':
        try:
            result = json.loads(out)
        except ValueError:
            result = {}
        failed = rc != 0 or result.get('is_error') or result.get('subtype') != 'success'
        if failed:
            return None, workers._quota_exhausted(out + '\n' + err)
        return result, False


def run(request):
    if not isinstance(request, dict):
        raise ModelRunError('request must be an object')
    if request.get('profile') not in ('fast', 'standard', 'deep'):
        raise ModelRunError('a configured capability profile is required')
    if any(k in request for k in ('primary', 'model', 'engine', 'effort')):
        raise ModelRunError('caller model selection is unsupported; use a profile')
    if request.get('tools') not in ('none', []):
        raise ModelRunError('only tools:none is supported')
    for key in ('system', 'prompt'):
        if not isinstance(request.get(key), str) or not request[key].strip():
            raise ModelRunError(f'{key} must be nonempty text')
    if not isinstance(request.get('json_schema'), dict):
        raise ModelRunError('json_schema must be an object')
    _check_schema(request['json_schema'])
    request = dict(request)
    request.setdefault('max_budget_usd', 5)
    timeout = request.get('timeout_seconds', 480)
    limit = request.get('max_output_bytes', 2097152)
    if type(timeout) not in (int, float) or not 0 < timeout <= 480:
        raise ModelRunError('timeout must be positive and at most 480 seconds')
    if type(limit) is not int or not 1024 <= limit <= 2097152:
        raise ModelRunError('output limit must be between 1024 and 2097152 bytes')
    if type(request['max_budget_usd']) not in (int, float) or not 0 < request['max_budget_usd'] <= 5:
        raise ModelRunError('budget must be positive and at most 5 USD')
    try:
        profile = config.model_profile(request['profile'])
    except ValueError as exc:
        raise ModelRunError(str(exc)) from None
    candidates = profile.get('models', [])
    if not candidates:
        raise ModelRunError('capability profile has no configured models')
    if not (config.worker_fallback_policy() or {}).get('enabled'):
        candidates = candidates[:1]
    for candidate in candidates:
        if candidate['engine'] not in ('claude', 'devin'):
            raise ModelRunError('profile_capability_unavailable: ' + candidate['engine'] + ' has no enforceable tools:none structured runner')
    trace = []
    deadline = time.monotonic() + timeout
    with tempfile.TemporaryDirectory(prefix='wt-model-') as directory:
        for candidate in candidates:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ModelRunError('model execution timed out')
            result, quota = _attempt(candidate, request, directory, remaining, limit)
            trace.append({**candidate, 'status': 'success' if result else ('quota_exhausted' if quota else 'failed')})
            if result:
                validate_schema(result.get('structured_output'), request['json_schema'])
                model_usage = result.get('modelUsage') or {}
                observed = None
                if isinstance(model_usage, dict) and model_usage:
                    observed = max(model_usage, key=lambda name: sum(
                        v for k, v in (model_usage[name] or {}).items()
                        if k in ('inputTokens', 'outputTokens', 'input_tokens', 'output_tokens')
                        and type(v) in (int, float)))
                result['execution'] = {'profile': request['profile'], **candidate,
                                       'configured_model': candidate['model'],
                                       'actual_model': observed, 'attempts': trace}
                return result
            if not quota:
                raise ModelRunError('model execution failed; fallback only applies to quota exhaustion')
    raise ModelRunError('all configured profile models exhausted')


def command(args):
    import sys
    try:
        raw = sys.stdin.buffer.read(2097153)
        if len(raw) > 2097152:
            raise ModelRunError('request input limit exceeded')
        result = run(json.loads(raw))
        print(json.dumps(result))
        return 0
    except (ModelRunError, ValueError, OSError, KeyError, TypeError):
        # Preserve safe explicit ModelRunError while never echoing provider/input text.
        exc = sys.exc_info()[1]
        message = str(exc) if isinstance(exc, ModelRunError) else 'invalid model request or unavailable runtime'
        print(json.dumps({'subtype': 'error', 'is_error': True, 'error': message}), file=sys.stderr)
        return 1
