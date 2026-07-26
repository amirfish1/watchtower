#!/usr/bin/env bash
# cut-release.sh — cut a WatchTower release: bump, tag, publish.
#
# WatchTower ships as a stdlib-only CLI, so a release is only three things: the
# two version strings agreeing, an annotated tag, and a GitHub release. None of
# what CCC's cut-release.sh does beyond that (DMG, notarization, Sparkle
# appcast, Homebrew formula) has an analogue here, so this stays short on
# purpose — a release script nobody trusts is a release script nobody runs, and
# WT's version sat at 0.1.0 for 257 commits because there was no script at all.
#
# NO PyPI PUBLISHING. There are no PyPI credentials on this machine, so the
# `watchtower-cli` distribution is not uploaded here; users install from the
# GitHub tag. If credentials ever land, add the build+twine step after the
# GitHub release and verify the sdist version matches ${VERSION} first.
#
# Usage:
#   ./scripts/cut-release.sh 0.2.0                # explicit version
#   ./scripts/cut-release.sh minor                # auto-bump from pyproject.toml
#   ./scripts/cut-release.sh patch --dry-run      # print every step, change nothing
#   ./scripts/cut-release.sh 0.2.0 --notes-file notes.md
#
# Prereqs: clean tree on main, `gh` logged in, push rights on origin.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

VERSION=""
BUMP=""
DRY_RUN=0
NOTES_FILE=""
# Releases are cut from main. Overridable only so this script can be exercised
# end-to-end from a git worktree; leave WT_RELEASE_BRANCH unset for real releases.
RELEASE_BRANCH="${WT_RELEASE_BRANCH:-main}"

usage() {
  echo "usage: ./scripts/cut-release.sh <X.Y.Z|patch|minor|major> [--dry-run] [--notes-file F]" >&2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --notes-file=*) NOTES_FILE="${1#*=}" ;;
    --notes-file) shift; NOTES_FILE="${1:-}" ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "cut-release: unknown flag $1" >&2; usage; exit 2 ;;
    patch|minor|major) [ -z "$VERSION$BUMP" ] && BUMP="$1" ;;
    *) [ -z "$VERSION$BUMP" ] && VERSION="$1" ;;
  esac
  shift
done

GREEN=$'\033[32m'; YEL=$'\033[33m'; RED=$'\033[31m'; NC=$'\033[0m'
step() { echo "${GREEN}==> $*${NC}"; }
warn() { echo "${YEL}!  $*${NC}"; }
die()  { echo "${RED}cut-release: $*${NC}" >&2; exit 1; }
run()  { if [ "$DRY_RUN" = 1 ]; then echo "   ${YEL}[dry-run]${NC} $*"; else eval "$@"; fi; }

# ── Resolve the target version ──────────────────────────────────────────────
# pyproject.toml is the source of truth for "where we are now"; __init__.py is
# kept in lockstep with it (tests/test_release_version.py asserts that).
CURRENT="$(sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml | head -1)"
printf '%s' "$CURRENT" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' \
  || die "cannot parse current version from pyproject.toml (got '${CURRENT}')"

if [ -n "$BUMP" ]; then
  IFS=. read -r MAJ MIN PAT <<<"$CURRENT"
  case "$BUMP" in
    major) MAJ=$((MAJ + 1)); MIN=0; PAT=0 ;;
    minor) MIN=$((MIN + 1)); PAT=0 ;;
    patch) PAT=$((PAT + 1)) ;;
  esac
  VERSION="${MAJ}.${MIN}.${PAT}"
fi

if ! printf '%s' "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "cut-release: version must be X.Y.Z or patch|minor|major, got '${VERSION:-<none>}'" >&2
  usage
  exit 2
fi

step "Cutting v${VERSION}  (current: ${CURRENT})  dry-run=${DRY_RUN}"

# ── Preflight ───────────────────────────────────────────────────────────────
step "Preflight checks"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
[ "$BRANCH" = "$RELEASE_BRANCH" ] \
  || die "on branch '${BRANCH}', releases are cut from '${RELEASE_BRANCH}'"
# Tracked modifications only: a dirty tree means the tag would point at code
# that differs from what anyone can check out. Untracked files ride along
# harmlessly (the commit below stages by explicit path), so they only warn.
[ -z "$(git status --porcelain --untracked-files=no)" ] \
  || die "working tree has uncommitted changes; commit or stash them first"
[ -z "$(git status --porcelain --untracked-files=normal | grep '^??' || true)" ] \
  || warn "untracked files present — they will NOT be part of the release"
git rev-parse "v${VERSION}" >/dev/null 2>&1 && die "tag v${VERSION} already exists" || true
command -v gh >/dev/null || die "gh not found"
if ! gh auth status >/dev/null 2>&1; then
  [ "$DRY_RUN" = 1 ] && warn "gh is not authenticated (fine for --dry-run)" || die "gh not authenticated"
fi

# ── 1. Version bump (lockstep) ──────────────────────────────────────────────
step "1/5  Bump version in pyproject.toml + watchtower/__init__.py"
# BSD sed (this repo is developed on macOS); the -i '' is deliberate.
run "sed -i '' 's/^version = \".*\"/version = \"${VERSION}\"/' pyproject.toml"
run "sed -i '' 's/^__version__ = \".*\"/__version__ = \"${VERSION}\"/' watchtower/__init__.py"
if [ "$DRY_RUN" = 0 ]; then
  grep -q "^version = \"${VERSION}\"" pyproject.toml \
    && grep -q "^__version__ = \"${VERSION}\"" watchtower/__init__.py \
    || die "version bump verification failed — pyproject.toml and __init__.py disagree"
fi

# ── 2. Tests ────────────────────────────────────────────────────────────────
# A tag is permanent; a red suite at tag time is not worth the five minutes saved.
step "2/5  Run the test suite"
run "python3 -m pytest -q"

# ── 3. Commit ───────────────────────────────────────────────────────────────
step "3/5  Commit the version bump"
# --only <paths>: this clone is shared with sibling agent sessions, so never
# commit whatever happens to be staged. If the files are already at ${VERSION}
# (bumped by hand in an earlier commit) there is nothing to commit and that is
# fine — the tag below is then the only new thing.
if [ "$DRY_RUN" = 0 ] && git diff --quiet -- pyproject.toml watchtower/__init__.py; then
  warn "pyproject.toml and __init__.py already say ${VERSION} — nothing to commit"
else
  run "git commit --only pyproject.toml watchtower/__init__.py -m 'chore(release): v${VERSION}'"
fi

# ── 4. Tag + push ───────────────────────────────────────────────────────────
step "4/5  Tag and push ${RELEASE_BRANCH} + v${VERSION}"
run "git tag -a v${VERSION} -m 'v${VERSION}'"
run "git push origin ${RELEASE_BRANCH}"
run "git push origin v${VERSION}"

# ── 5. GitHub release ───────────────────────────────────────────────────────
step "5/5  Create the GitHub release"
# No CHANGELOG.md in this repo, so --generate-notes (commit log since the last
# tag) is the notes source unless the caller supplies better ones.
if [ -n "$NOTES_FILE" ]; then
  [ -f "$NOTES_FILE" ] || die "--notes-file '${NOTES_FILE}' does not exist"
  run "gh release create v${VERSION} --title 'v${VERSION}' --notes-file '${NOTES_FILE}'"
else
  run "gh release create v${VERSION} --title 'v${VERSION}' --generate-notes"
fi

if [ "$DRY_RUN" = 0 ]; then
  echo "   release: $(gh release view "v${VERSION}" --json url --jq .url 2>/dev/null || echo '?')"
  echo "   wt --version: $(python3 -c 'import watchtower; print(watchtower.__version__)')"
fi
step "Done — v${VERSION} tagged and released (not published to PyPI)."
