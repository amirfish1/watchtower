# WT-86 offline gate runtime — source-only candidate

Status: implementation in progress; WT-61 review/main/live-policy hold remains.

Measured starting point: system Python lacks pytest. OPS-643's copied-runtime
proof on 36f728d ran all 425 mandated tests: 415 passed, 10 failed in 86.52s.
Eight failures exercise denied sockets; two depend on host model defaults.
Those are original full-gate acceptance failures, not permission to skip tests,
change network policy, substitute suites or run adopted bytes on the host.

Options considered before implementation:

1. System install: requires host privilege and modifies the shared machine;
   outside this repair's supported offline, disposable-root scope.
2. Copy the controller's installed distributions on every gate: matches the
   OPS proof, but mutable environments and metadata become runtime authority;
   repeated copying complicates provenance and permits accidental upgrades.
3. Ship a pinned pure-Python zip plus per-file provenance manifest in the
   trusted controller package, validate a compiled-in digest, then copy verified
   bytes into the existing fresh store. Chosen: offline, bounded, reproducible,
   independent of controller interpreter/venv and adopted source metadata.
4. Product option: refuse pytest gates on unsupported hosts and require another
   already-capable host. Safe fallback, but leaves this host's required workflow
   unusable; retain refusal when the pinned archive is absent/untrusted.

Goal: the normal sandbox_test_commit launcher imports pinned pytest without
network or additional roots; injected assertion failures still block, inherited
Python/plugin variables are absent, and native filesystem/socket boundaries
remain enforced. Run the unchanged four-suite command and report its actual
result. No passing/full-repair claim until original integration/adoption and
combined review requirements are met.

Security contract: only bytes pinned by trusted controller code are provisioned;
never discover packages, plugins or sitecustomize from adopted repositories or
host PYTHONPATH. Runtime is copied to a newly allocated store before gate code
runs, with plugin auto-discovery disabled and user site disabled. Repository
code/conftest still executes only inside the native sandbox. No archive is
extracted and no dependency loader executes on the host. Existing release and
security holds are unchanged.

Implementation: `watchtower/sandbox_runtime.py` pins both archive and manifest
SHA256, rejects linked/nonregular/group-writable/oversized assets, checks each
archive member against its manifest, then exclusively writes verified zip and
manifest inside the existing disposable store. `run_cmd_gate` uses that zip as
its only PYTHONPATH and reports runtime provenance. Both ordinary command gates
and `sandbox_test_commit.py` share this path. No command or suite is replaced.
The package-data list includes both assets for installed controllers.

Provenance/rebuild: run `scripts/build_sandbox_pytest_runtime.py` with the trusted
development venv offline. It reads (never imports) the five fixed distributions,
checks copied files against installed wheel RECORD SHA256, rejects symlinks and
non-pure-Python payloads, retains package metadata/licenses, and emits deterministic
zip bytes and per-file hashes/versions. A reviewed update must deliberately change
the controller pins. The pytest distribution includes the required top-level
`py.py` compatibility shim as well as `_pytest` and `pytest`.

Native acceptance: `tests/test_sandbox_runtime.py` passed 7/7 with real Codex
kernel sandboxes (no skips). System Python imports pinned pytest with a clean
environment; the regular sandbox_test entry point passes a real pytest test,
rejects an injected assertion, denies IPv4/IPv6 and Unix bind plus shared-memory
writes, hides inherited Python/plugin/secret variables, leaves host sentinels
untouched and cleans fresh runtime stores. Missing, altered, linked and unsafe
controller assets refuse setup without launching the gate.
