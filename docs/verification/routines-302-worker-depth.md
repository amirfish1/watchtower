# ROUTINES-302: thorough work at every depth

Source: `wt find ROUTINES-302 --json --worker routines-f3f0ba98` on
2026-11-14. This is an owner report about worker guidance, not a studio
booking/payment incident. No production customer records were changed, so
booking audit logs and studio-local event times are not applicable. No later
clarifications or greetings were present at verification time.

## Original report and sentence coverage

> Not sure if this is a watchtower or a CC issue but I’ve noticed that the guidance workers has become something that emphasizes doing quick fixes and I think we started it when I introduced the deep parameter, but I actually don’t like the fact that by default, we are optimizing for something quick. I never said that tickets should be quick. I want them to be thorough always and the only goal that I had with deep is to further encourage workers to take on multi hours and multi days work, but I feel we could’ve reached the opposite result where tickets are being resolved even quicker and with lower quality the regular ones.

Independent claims within the three original sentences are numbered below.
Statements about past worker quality are owner observations/hypotheses; this
change verifies the instructions delivered to workers, not historical fleet
quality or a guarantee of future agent compliance.

| Claim | Fact, hypothesis or unknown | Immediate repair and long-term prevention | Possible confusion and delivered help | Evidence/status |
|---|---|---|---|---|
| 1a. “Not sure if this is a watchtower or a CC issue” | Ownership uncertainty. | Located and corrected the canonical WatchTower prompts shared by drain and single-ticket launches. | Both launch modes now communicate the same quality standard. No CCC-specific workaround is needed for these prompts. | `watchtower/workers.py`: `DEPTH_CONTRACT`, `drain_goal`, `run_once_goal`; rendered-goal regression tests across five engines. VERIFIED. |
| 1b. “I’ve noticed that the guidance workers has become something that emphasizes doing quick fixes” | Confirmed in the previous `fix-and-look`, `no design step`, and `a quick look is enough` instructions. | Removed those instructions from both prompts, the deep module and runbook. Regression checks reject their return in rendered goals. | The worker is explicitly told to investigate, design and verify against the failure, consequences and regression risks. | `workers.py`, `deep.py`, `docs/worker-runbook.md`; `test_rendered_worker_goals_require_thorough_work_at_every_depth`. VERIFIED. |
| 1c. “I think we started it when I introduced the deep parameter” | Historical timing is an owner hypothesis; the association with depth is confirmed in the old depth contract. Exact introduction chronology is not needed to repair the current guidance. | Revised the default and deep guidance together rather than correcting only one launch mode. | Deep now adds sustained-work structure to the shared standard. | `DEPTH_CONTRACT`, `deep_brief`, runbook Ticket depth section. VERIFIED current source; historical chronology UNKNOWN. |
| 1d. “I actually don’t like the fact that by default, we are optimizing for something quick” | Owner preference and confirmed prior prompt behavior. | Made thoroughness the explicit default while preserving the existing `quick` storage/API value. | CLI help and the reference page explain that `quick` is a legacy name, not a speed target. | `watchtower/cli.py`, `site/cli.html`; add/take/edit help tests, local/GitHub depth compatibility tests and independent desktop/mobile browser check. VERIFIED. |
| 2. “I never said that tickets should be quick.” | Authoritative owner correction; no contrary authorization inferred. | Removed the claim that the owner will catch mistakes within minutes and the ban on design. | Instructions explicitly reject relying on owner testing and allow authorized structural work without a depth-promotion question. | Both rendered goal variants and runbook; regression checks reject automatic promotion language. VERIFIED. |
| 3a. “I want them to be thorough always” | Explicit quality requirement. | Require a complete solution and verified outcomes at every depth; retain the existing definition of done and symptom-based close rules. | Default depth cannot excuse a token fix, incomplete repair or weak verification. | `DEPTH_CONTRACT`, runbook Definition of done and Ticket depth; launcher and product-gate regression checks. VERIFIED instruction delivery, not a guarantee of agent compliance. |
| 3b. “the only goal that I had with deep is to further encourage workers to take on multi hours and multi days work” | Explicit intended role for deep. | Deep now supports hours/days, repeated progress checkpoints, evidence-led continuation, data deferral and resumable notes. The 90-minute default is a checkpoint interval; explicit owner/ticket caps still require approval to exceed. | A checkpoint ending is never a reason to close or automatically ask the owner. No-gain attempts require reassessment rather than endless repetition. | `deep.py`, runbook and CLI help; sustained-work test with a 240-minute checkpoint, defer/resume tests. VERIFIED. |
| 3c. “I feel we could’ve reached the opposite result where tickets are being resolved even quicker and with lower quality the regular ones.” | Owner concern; fleet-wide speed/quality causality is unmeasured. The offending instructions are directly evidenced. | Removed the instruction-level cause across both launch modes and corrected connected documentation. Tests protect the quality standard and preserve completion/approval rules. | Shared quality language makes deep a work-structure setting rather than a quality tier. | 28 depth/local/GitHub tests plus 14 selected launcher/product-gate tests passed. Historical fleet-wide quality improvement is not claimed. VERIFIED guidance repair. |

## Customer/owner action coverage

No booking/edit/cancellation/rebooking/recovery sequence was reported. These
are the owner's described actions; their historical times/order are unknown
except that the report followed introduction/observation. Intent is labelled
as reported or inferred. Each row independently considers the safeguard after
the earlier misunderstanding has already happened.

| Step / action and intended outcome | Immediate repair | Long-term prevention at this step | Possible confusion and delivered help | Evidence/status |
|---|---|---|---|---|
| 1. Introduced `deep`; reported intent: enable work spanning hours/days. Historical time unknown. | Corrected the deep brief and its relationship to the default. | Checkpoints support continued authorized work, deferral and cross-session handoff; explicit caps remain binding. | The worker is told that checkpoint/session completion does not complete the ticket. | `deep_brief`; sustained-work and defer/resume tests. VERIFIED. |
| 2. Observed quick-fix guidance and possibly worse ordinary resolutions; historical ordering of individual observations unknown. Inferred goal: identify why quality fell. | Traced the actual prompt text to WatchTower and removed the shortcuts. | Both launch variants use the same tested contract; regression coverage includes all engine lifecycle variants. | The surviving `quick` enum is explained in worker prompts, CLI help and reference documentation to prevent reintroducing the speed interpretation. | Rendered-goal tests, help tests, independent browser verification. VERIFIED instructions; historical resolution-quality measurements UNKNOWN. |
| 3. Filed this correction. Logged system filing: 2026-11-14 15:13:52 UTC (studio timezone N/A). Reported goal: thoroughness always. | Implemented the explicit correction in the canonical repo, tested and pushed it; runtime refresh evidence is attached to ticket progress. | Definition-of-done and approval limits remain in place. No structural-work promotion is required solely because of duration/complexity. | Resolution distinguishes delivered prompt changes from unmeasured downstream agent behavior; existing sessions retain already-delivered initial messages. | Commit and rollout proof in ROUTINES-302 progress/resolution. No customer repair, external reply or money action was required. |

## Verification

- `python3 -m pytest -q tests/test_deep_work.py tests/test_github_depth.py`:
  **28 passed**. Includes local/GitHub depth compatibility, default/custom
  checkpoint values, defer/resume, rendered goals for Claude/Codex/Kimi/Devin/
  Antigravity, and add/take/edit help.
- `python3 -m pytest -q tests/test_smoke.py tests/test_product_gate.py
  tests/test_workers_lifecycle.py -k 'goal or depth or brief or run_once'`:
  **14 passed, 386 deselected**. Confirms launcher behavior and product-gate
  instructions remain intact.
- Independent fleet verifier `f83f61cf-773f-4919-a0db-87718948fa8a`:
  **VERIFIED** fresh headless Chromium at desktop 1440 and mobile 390.
  The `--depth / --budget-min` row contains the new standard and checkpoint
  explanation; document widths equal viewport widths with no horizontal
  overflow. The verifier also reviewed the prompt/deep contract and found no
  remaining speed-first or automatic complexity-block instruction.
- Browser evidence (host-local):
  `/home/hermes/dev/scratch/ROUTINES-302/verdict.json`, `raw.json`, `verify.js`,
  `row-desktop.png`, `row-mobile.png`, `table-desktop.png`, `table-mobile.png`,
  `viewport-desktop.png`, `viewport-mobile.png`. A fixed-header overlap in
  the mobile table element capture is a screenshot artifact; the row capture
  is unobstructed.
- This is a worker-guidance change. It does not itself prove the quality of
  future ticket resolutions or rewrite messages already delivered to running
  workers. The local static-site render was checked; no public-site deployment
  is claimed. Exact commit/push and reconciler refresh proof are recorded in
  ticket progress so they can be attached after committing this artifact.
