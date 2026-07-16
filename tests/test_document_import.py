from __future__ import annotations

import importlib
import json
import subprocess
from pathlib import Path

import pytest

from watchtower.document_import import (
    ReasoningError,
    _call_claude,
    extract_document,
    plan_import,
)


FIXTURES = Path(__file__).parent / "fixtures" / "doc_import"


def ticket(
    title: str,
    body: str,
    anchor: str,
    *,
    item_type: str = "feature",
    depends_on: list[str] | None = None,
) -> dict:
    return {
        "title": title,
        "body": body,
        "type": item_type,
        "depends_on": depends_on or [],
        "source_anchor": anchor,
    }


def response(*tickets: dict) -> dict:
    return {"tickets": list(tickets)}


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))

    import watchtower.queue as q
    import watchtower.cli as cli
    import watchtower.document_import as document_import

    importlib.reload(q)
    importlib.reload(cli)

    class Namespace:
        pass

    namespace = Namespace()
    namespace.q = q
    namespace.cli = cli
    namespace.document_import = document_import
    return namespace


@pytest.mark.parametrize(
    ("fixture_name", "model_response", "expected_titles"),
    [
        (
            "checkboxes.md",
            response(
                ticket("Add login tests", "Cover login validation and expired sessions.", "L5-L7"),
                ticket("Update docs", "Update the release documentation.", "L9"),
            ),
            ["Add login tests", "Update docs"],
        ),
        (
            "headings.md",
            response(
                ticket("Implement document parser", "Implement the parser with source locations.", "L5-L10"),
                ticket(
                    "Verify import safety",
                    "Prove preview mode cannot mutate queue state.",
                    "L12-L18",
                    depends_on=["Implement document parser"],
                ),
            ),
            ["Implement document parser", "Verify import safety"],
        ),
        (
            "numbered.md",
            response(
                ticket("Create migration tool", "Build the migration tool with a dry-run path.", "L3-L6"),
                ticket(
                    "Verify production behavior",
                    "Verify the migration and activity log in production.",
                    "L6-L7",
                    depends_on=["Create migration tool"],
                ),
            ),
            ["Create migration tool", "Verify production behavior"],
        ),
        (
            "messy.md",
            response(
                ticket(
                    "Tighten checkout validation",
                    "Reject unsupported card states, add coverage, and preserve provider behavior.",
                    "L6-L8",
                    item_type="bug",
                ),
                ticket(
                    "Create and verify rollback runbook",
                    "Turn the meeting notes into an operator runbook and verify it in staging.",
                    "L10-L11",
                ),
            ),
            ["Tighten checkout validation", "Create and verify rollback runbook"],
        ),
    ],
)
def test_whole_document_reasoning_handles_fixture_shapes(
    fixture_name, model_response, expected_titles
):
    calls = []

    def reasoner(prompt, schema):
        calls.append((prompt, schema))
        return model_response

    candidates = extract_document(FIXTURES / fixture_name, reasoner=reasoner)

    assert [candidate.title for candidate in candidates] == expected_titles
    assert len(calls) == 1
    assert calls[0][1]["required"] == ["tickets"]
    assert "one focused worker session" in calls[0][0]
    assert "L1 |" in calls[0][0]


def test_reasoning_finds_implicit_prose_work_and_ignores_decisions_and_code():
    prompt_seen = []

    candidates = extract_document(
        FIXTURES / "messy.md",
        reasoner=lambda prompt, schema: prompt_seen.append(prompt) or response(
            ticket(
                "Tighten checkout validation",
                "Reject unsupported states and preserve existing provider behavior.",
                "L6-L8",
                item_type="bug",
            ),
            ticket(
                "Create and verify rollback runbook",
                "Write the operator runbook and test it in staging.",
                "L10-L11",
            ),
        ),
    )

    assert [candidate.title for candidate in candidates] == [
        "Tighten checkout validation",
        "Create and verify rollback runbook",
    ]
    assert "unsupported card states still pass" in prompt_seen[0]
    assert "Example only" in prompt_seen[0]
    assert "omit" in prompt_seen[0].lower()


def test_candidate_contains_model_anchor_provenance_and_v3_stable_key(tmp_path):
    source = tmp_path / "plan.md"
    source.write_text("Context.\n\nWe need safer login validation.\n", encoding="utf-8")

    first = extract_document(
        source,
        reasoner=lambda *_: response(
            ticket("Harden login validation", "Cover the unsafe login path.", "L3", item_type="bug")
        ),
    )[0]
    source.write_text(
        "Edited context.\n\nMore context.\n\nWe need safer login validation.\n",
        encoding="utf-8",
    )
    moved = extract_document(
        source,
        reasoner=lambda *_: response(
            ticket("Improve login safety", "Cover the unsafe login path.", "L5", item_type="bug")
        ),
    )[0]

    assert first.source_path == str(source.resolve())
    assert first.anchor == "L3"
    assert first.source_ref == f"{source.resolve()}#L3"
    assert f"Imported from: {source.resolve()}#L3" in first.body
    assert moved.import_key == first.import_key
    assert first.import_key.startswith("doc-import:v3:")


def test_distinct_tickets_supported_by_same_passage_get_distinct_keys(tmp_path):
    source = tmp_path / "plan.md"
    source.write_text("Build the API and document its operator workflow.\n", encoding="utf-8")

    candidates = extract_document(
        source,
        reasoner=lambda *_: response(
            ticket("Build API", "Implement and test the API.", "L1"),
            ticket("Document operations", "Write the operator workflow.", "L1"),
        ),
    )

    assert candidates[0].import_key != candidates[1].import_key
    assert all(candidate.import_key.startswith("doc-import:v3:") for candidate in candidates)


@pytest.mark.parametrize(
    ("bad_response", "message"),
    [
        ({"tickets": "not a list"}, "tickets must be an array"),
        (response({"title": "missing fields"}), "exactly these fields"),
        (response(ticket("", "body", "L1")), "title must"),
        (response(ticket("Task", "", "L1")), "body must"),
        (response(ticket("Task", "body", "L99")), "outside the document"),
        (response(ticket("Task", "body", "L2-L1")), "ascending"),
        (
            response(
                ticket("First", "body", "L1", depends_on=["Later"]),
                ticket("Later", "body", "L1"),
            ),
            "earlier ticket",
        ),
        (
            response(ticket("Same", "one", "L1"), ticket("Same", "two", "L1")),
            "unique",
        ),
    ],
)
def test_malformed_reasoning_batch_fails_validation(tmp_path, bad_response, message):
    source = tmp_path / "plan.md"
    source.write_text("A document line.\n", encoding="utf-8")

    with pytest.raises(ReasoningError, match=message):
        extract_document(source, reasoner=lambda *_: bad_response)


def test_plan_import_skips_existing_ids_in_any_ticket_state(tmp_path):
    source = tmp_path / "plan.md"
    source.write_text("First.\nSecond.\n", encoding="utf-8")
    candidates = extract_document(
        source,
        reasoner=lambda *_: response(
            ticket("First task", "Do first.", "L1"),
            ticket("Second task", "Do second.", "L2", depends_on=["First task"]),
        ),
    )

    plan = plan_import(candidates, [{"id": candidates[0].import_key, "status": "closed"}])

    assert plan.new == (candidates[1],)
    assert plan.existing == (candidates[0],)


def test_claude_adapter_invokes_one_tool_free_structured_call(monkeypatch):
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps({"structured_output": response(ticket("Task", "Body", "L1"))}),
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = _call_claude("prompt", {"type": "object"})

    assert payload["tickets"][0]["title"] == "Task"
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv[0] == "claude"
    assert argv.count("-p") == 1
    assert "--json-schema" in argv
    assert "--tools" in argv
    assert "--safe-mode" in argv
    assert "--no-session-persistence" in argv
    assert argv[argv.index("--model") + 1] == "sonnet"
    assert "prompt" not in argv
    assert kwargs["input"] == "prompt"
    assert kwargs["timeout"] > 0


def test_claude_adapter_fails_loudly_for_unavailable_binary(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError()))

    with pytest.raises(ReasoningError, match="not available"):
        _call_claude("prompt", {"type": "object"})


def test_claude_adapter_fails_loudly_for_nonzero_or_malformed_output(monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 1, stdout="", stderr="auth failed"),
    )
    with pytest.raises(ReasoningError, match="auth failed"):
        _call_claude("prompt", {"type": "object"})

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, stdout="not json", stderr=""),
    )
    with pytest.raises(ReasoningError, match="invalid JSON"):
        _call_claude("prompt", {"type": "object"})


def _install_reasoner(monkeypatch, wt, payloads):
    calls = []
    iterator = iter(payloads)

    def fake_call(prompt, schema):
        calls.append((prompt, schema))
        return next(iterator)

    monkeypatch.setattr(wt.document_import, "_call_claude", fake_call)
    return calls


def test_import_defaults_to_dry_run_and_shows_full_reasoning(wt, monkeypatch, capsys):
    calls = _install_reasoner(
        monkeypatch,
        wt,
        [response(ticket("Add login tests", "Cover validation and expired sessions.", "L5-L7"))],
    )

    rc = wt.cli.main(["import", str(FIXTURES / "checkboxes.md"), "-q", "DOCS"])

    assert rc == 0
    assert len(calls) == 1
    assert wt.q.list_items(project="DOCS") == []
    output = capsys.readouterr().out
    assert "WOULD FILE: [feature] Add login tests" in output
    assert "Cover validation and expired sessions." in output
    assert "depends_on: none" in output
    assert "dry-run" in output


def test_apply_records_provenance_type_and_resolved_dependencies(wt, monkeypatch, capsys):
    _install_reasoner(
        monkeypatch,
        wt,
        [
            response(
                ticket("Build importer", "Build the reasoning adapter.", "L5-L10"),
                ticket(
                    "Verify importer",
                    "Test preview and apply behavior.",
                    "L12-L18",
                    depends_on=["Build importer"],
                ),
            )
        ],
    )

    rc = wt.cli.main([
        "import", str(FIXTURES / "headings.md"), "-q", "DOCS", "--apply", "--type", "bug",
    ])

    assert rc == 0
    items = wt.q.list_items(project="DOCS")
    assert [item["type"] for item in items] == ["bug", "bug"]
    assert all(item["source"] == "doc-import" for item in items)
    assert all(item["id"].startswith("doc-import:v3:") for item in items)
    assert items[0]["url"].endswith("headings.md#L5-L10")
    assert "Depends on:\n- DOCS-1: Build importer" in items[1]["text"]
    assert "FILED: DOCS-2" in capsys.readouterr().out


def test_invalid_model_batch_files_nothing_even_with_apply(wt, monkeypatch, capsys):
    _install_reasoner(monkeypatch, wt, [{"tickets": [{"title": "bad"}]}])

    rc = wt.cli.main(["import", str(FIXTURES / "messy.md"), "-q", "DOCS", "--apply"])

    assert rc == 1
    assert wt.q.list_items(project="DOCS") == []
    assert "error:" in capsys.readouterr().err


def test_reimport_after_edit_calls_reasoner_once_and_files_only_new_item(
    wt, monkeypatch, tmp_path, capsys
):
    source = tmp_path / "plan.md"
    source.write_text("Login safety needs work.\n", encoding="utf-8")
    calls = _install_reasoner(
        monkeypatch,
        wt,
        [
            response(ticket("Harden login", "Cover the unsafe path.", "L1", item_type="bug")),
            response(
                ticket("Harden login", "Cover the unsafe path with more context.", "L3", item_type="bug"),
                ticket("Document login recovery", "Add operator recovery steps.", "L4"),
            ),
            response(
                ticket("Harden login", "Cover the unsafe path with more context.", "L3", item_type="bug"),
                ticket("Document login recovery", "Add operator recovery steps.", "L4"),
            ),
        ],
    )

    assert wt.cli.main(["import", str(source), "-q", "DOCS", "--apply"]) == 0
    capsys.readouterr()
    source.write_text("Edited context.\n\nLogin safety needs work.\nDocument recovery too.\n", encoding="utf-8")
    assert wt.cli.main(["import", str(source), "-q", "DOCS", "--apply"]) == 0
    second_output = capsys.readouterr().out
    assert "created=1" in second_output
    assert "existing=1" in second_output

    assert wt.cli.main(["import", str(source), "-q", "DOCS", "--apply"]) == 0
    third_output = capsys.readouterr().out
    assert len(calls) == 3
    assert len(wt.q.list_items(project="DOCS")) == 2
    assert "created=0" in third_output


def test_import_missing_file_fails_before_reasoning_or_queue_mutation(
    wt, monkeypatch, tmp_path, capsys
):
    monkeypatch.setattr(
        wt.document_import,
        "_call_claude",
        lambda *_: pytest.fail("missing files must not call the reasoner"),
    )

    rc = wt.cli.main(["import", str(tmp_path / "missing.md"), "-q", "DOCS", "--apply"])

    assert rc == 1
    assert wt.q.list_items(project="DOCS") == []
    assert "error:" in capsys.readouterr().err


def test_import_help_explains_whole_document_reasoning_and_preview(wt, capsys):
    with pytest.raises(SystemExit) as exc:
        wt.cli.main(["import", "--help"])

    assert exc.value.code == 0
    output = capsys.readouterr().out
    assert "whole document" in output
    assert "one Claude reasoning call" in output
    assert "default: dry-run" in output
