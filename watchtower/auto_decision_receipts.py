"""Content proof for a system answer's own GitHub transport mutation.

This proves delivery, not approval. Only a separately persisted auto_decision
can be consumed as authority. Every other comment/source change remains hashed.
"""
import copy
import hashlib
from . import autodecide, github_backend as gh


def answer_content_proof(item, expected_text):
    history = item.get('history') or []
    answers = [h for h in history if h.get('event') == 'answer']
    if not answers:
        raise ValueError('system answer missing')
    answer = answers[-1]
    kinds = [k for k in (answer.get('kind'), answer.get('by', {}).get('kind')) if k]
    if not kinds or any(k != 'system' for k in kinds):
        raise ValueError('latest answer is not the system receipt')
    matches = []
    for i, comment in enumerate(item.get('_github_comments') or []):
        text, key = gh._strip_entry_marker(str(comment.get('body') or ''))
        ref = gh._comment_ref(comment)
        if not key or not ref or key != answer.get('entry_key') or text != expected_text:
            continue
        sha = hashlib.sha256(text.encode()).hexdigest()
        if answer.get('text_ref'):
            if (answer['text_ref'] != ref or answer.get('text_len') != len(text)
                    or answer.get('text_sha256') != sha):
                continue
        elif answer.get('text') != text:
            continue
        matches.append((i, {'answer_entry_key': key, 'answer_comment_ref': ref,
                            'answer_comment_id': str(comment.get('id') or ref),
                            'answer_sha256': sha, 'answer_length': len(text)}))
    if len(matches) != 1:
        raise ValueError('missing, edited or ambiguous exact system answer link')
    excluded, proof = matches[0]
    body, _ = gh._split_body(item['_github_body'])
    original = copy.deepcopy(item)
    original['text'] = gh._issue_text(body, item.get('note'), item.get('title'),
                                     [c for i, c in enumerate(item['_github_comments']) if i != excluded])
    return autodecide.ticket_revision(original), proof


def verify_decision_content(item, event):
    if event.get('event') != 'auto_decision' or event.get('by', {}).get('kind') != 'system' or event.get('kind', 'system') != 'system':
        raise ValueError('actual system auto_decision required')
    blocks = [h for h in item.get('history', []) if h.get('event') == 'block']
    if not blocks or blocks[-1].get('at') != event.get('blocked_at') or item.get('needs_input'):
        raise ValueError('receipt block generation was superseded or held')
    revision, proof = answer_content_proof(item, event.get('text'))
    if any(event.get(key) != value for key, value in proof.items()):
        raise ValueError('stored system receipt ID/key/hash/ref does not match delivery')
    return revision
