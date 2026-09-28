#!/usr/bin/env python3
"""Cut a version: changelog heading, VERSION, commit, tag — and refuse when something is off.

    python3 tools/release.py --check              # what CI runs: version, changelog, tags, tree
    python3 tools/release.py 0.3.0 --dry-run      # show what a cut would do
    python3 tools/release.py 0.3.0                # cut it: CHANGELOG.md, VERSION, commit, tag v0.3.0
    python3 tools/release.py 0.3.0 --push         # …and push main and the tag; the release workflow
                                                  # then publishes the changelog section as the note

What a cut does, in order, stopping at the first thing that is wrong:

1. The working tree is clean and on `main`.
2. `CHANGELOG.md` has a non-empty `[Unreleased]` section: a release with nothing to say is not
   a release.
3. The version is higher than the newest tag.
4. Nothing that must stay local is tracked: no `*.log`, nothing under `runs/`, no `.env`.
5. No forbidden string is in the tree or in the commits since the last tag (messages and diffs).
   The patterns come from `.release-denylist` next to this repository's root, one per line,
   case-insensitive, and that file is git-ignored on purpose: the names it guards against must
   not enter history through the guard itself. Absent file: the check is skipped, loudly.
6. `[Unreleased]` becomes `[X.Y.Z] — <today>` with a fresh empty `[Unreleased]` above it, and
   `VERSION = "X.Y.Z"` in the script moves with it.
7. One commit, `Release X.Y.Z`, and an annotated tag `vX.Y.Z` whose message is the first
   bullet of the section. `--push` pushes both; `.github/workflows/release.yml` then publishes
   the section as the GitHub release through `tools/release_notes.py`.

`--check` runs steps 4 and 5 plus the consistency rules CI enforces on every push: `VERSION`
equals the newest changelog version, and every tag has a changelog section.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHANGELOG = ROOT / "CHANGELOG.md"
SCRIPT = ROOT / "zpod-vcf-deployer.py"
DENYLIST = ROOT / ".release-denylist"
MUST_STAY_LOCAL = re.compile(r"(^|/)\.env$|\.log$|^runs/")


def git(*args: str, check: bool = True) -> str:
    return subprocess.run(("git", *args), cwd=ROOT, capture_output=True, text=True,
                          check=check).stdout.strip()


def fail(message: str) -> None:
    print(f"✗ {message}", file=sys.stderr)
    raise SystemExit(1)


def ok(message: str) -> None:
    print(f"✓ {message}")


# ── the file and the script ──────────────────────────────────────────────────────────


def changelog_versions() -> list[str]:
    """Versions with a section, newest first, `[Unreleased]` excluded."""
    return [m.group(1) for m in re.finditer(r"^## \[(\d[^\]]*)\]", CHANGELOG.read_text(), re.M)]


def unreleased_body() -> str:
    text = CHANGELOG.read_text()
    m = re.search(r"^## \[Unreleased\]\n(.*?)(?=^## \[|\Z)", text, re.M | re.S)
    return (m.group(1) if m else "").strip()


def script_version() -> str:
    m = re.search(r'^VERSION = "([^"]+)"$', SCRIPT.read_text(), re.M)
    return m.group(1) if m else ""


def tags() -> list[str]:
    listing = git("tag", "-l", "v[0-9]*", "--sort=-v:refname")
    return [t for t in listing.splitlines() if t]


def vkey(version: str) -> tuple[int, ...]:
    return tuple(int(p) for p in re.findall(r"\d+", version))


# ── the checks ───────────────────────────────────────────────────────────────────────


def check_tracked_files() -> None:
    tracked = git("ls-files").splitlines()
    leaked = [f for f in tracked if MUST_STAY_LOCAL.search(f)]
    if leaked:
        fail("tracked but must stay local: " + ", ".join(leaked))
    ok(f"nothing local is tracked ({len(tracked)} files)")


def check_denylist(since: str | None) -> None:
    if not DENYLIST.is_file():
        print("! no .release-denylist next to the repository: the forbidden-string check is skipped",
              file=sys.stderr)
        return
    patterns = [p.strip() for p in DENYLIST.read_text().splitlines() if p.strip() and not p.startswith("#")]
    if not patterns:
        ok("denylist is empty")
        return
    regex = re.compile("|".join(re.escape(p) for p in patterns), re.I)
    hits = []
    for path in git("ls-files").splitlines():
        try:
            text = (ROOT / path).read_text(errors="replace")
        except OSError:
            continue
        for n, line in enumerate(text.splitlines(), 1):
            if regex.search(line):
                hits.append(f"{path}:{n}")
    scope = f"{since}..HEAD" if since else "HEAD"
    log = git("log", "-p", "--format=%H %s%n%b", scope, check=False)
    for line in log.splitlines():
        if regex.search(line):
            hits.append(f"history {scope}: {line.strip()[:80]}")
            if len(hits) > 20:
                break
    if hits:
        fail(f"forbidden string(s) from .release-denylist found:\n  " + "\n  ".join(hits[:20]))
    ok(f"no forbidden string in the tree or in {scope} ({len(patterns)} pattern(s))")


def check_consistency() -> None:
    versions = changelog_versions()
    if not versions:
        fail("CHANGELOG.md has no released section")
    newest, current = versions[0], script_version()
    if current != newest:
        fail(f'VERSION = "{current}" in the script, but the newest changelog section is [{newest}]')
    ok(f"VERSION {current} matches the newest changelog section")
    missing = [t for t in tags() if t.removeprefix("v") not in versions]
    if missing:
        fail("tagged, but not in CHANGELOG.md: " + ", ".join(missing))
    ok(f"every tag has a changelog section ({len(tags())} tags)")


def check_all() -> None:
    check_tracked_files()
    check_denylist(tags()[0] if tags() else None)
    check_consistency()


# ── the cut ──────────────────────────────────────────────────────────────────────────


def cut(version: str, *, dry_run: bool, push: bool) -> None:
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        fail(f"'{version}' is not X.Y.Z")
    if git("status", "--porcelain"):
        fail("the working tree is not clean; commit or stash first")
    branch = git("rev-parse", "--abbrev-ref", "HEAD")
    if branch != "main":
        fail(f"on branch {branch}, releases are cut from main")
    body = unreleased_body()
    if not body:
        fail("CHANGELOG.md has an empty [Unreleased] section: nothing to release")
    latest = tags()[0] if tags() else ""
    if latest and vkey(version) <= vkey(latest):
        fail(f"{version} is not above the newest tag {latest}")
    if version in changelog_versions():
        fail(f"CHANGELOG.md already has a [{version}] section")
    check_tracked_files()
    check_denylist(latest or None)

    today = dt.date.today().isoformat()
    heading = f"## [{version}] — {today}"
    first_bullet = next((line.lstrip("- ").strip() for line in body.splitlines() if line.startswith("- ")), "")
    tag_message = f"zpod-wld-deployer {version}" + (f": {re.sub(r'[*`]', '', first_bullet)[:100]}" if first_bullet else "")

    print(f"\n{heading}\n{body[:400]}{'…' if len(body) > 400 else ''}\n")
    if dry_run:
        print(f"dry run: would write {heading}, set VERSION = \"{version}\", commit 'Release {version}', tag v{version}"
              + (", push main and the tag" if push else ""))
        return

    text = CHANGELOG.read_text()
    text = text.replace("## [Unreleased]\n", f"## [Unreleased]\n\n{heading}\n", 1)
    CHANGELOG.write_text(re.sub(r"\n{3,}", "\n\n", text))
    script = SCRIPT.read_text()
    SCRIPT.write_text(re.sub(r'^VERSION = "[^"]+"$', f'VERSION = "{version}"', script, count=1, flags=re.M))
    check_consistency()

    git("add", str(CHANGELOG), str(SCRIPT))
    git("commit", "-q", "-m", f"Release {version}")
    git("tag", "-a", f"v{version}", "-m", tag_message)
    ok(f"committed 'Release {version}' and tagged v{version}")
    if push:
        git("push", "origin", "main")
        git("push", "origin", f"v{version}")
        ok("pushed main and the tag; the release workflow publishes the note")
    else:
        print(f"next: git push origin main v{version}   (or re-run with --push)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("version", nargs="?", help="X.Y.Z to cut")
    parser.add_argument("--check", action="store_true", help="run the release-time checks and exit")
    parser.add_argument("--dry-run", action="store_true", help="show what a cut would do")
    parser.add_argument("--push", action="store_true", help="push main and the tag after cutting")
    args = parser.parse_args(argv)
    if args.check:
        check_all()
        return 0
    if not args.version:
        parser.error("name a version, or pass --check")
    cut(args.version, dry_run=args.dry_run, push=args.push)
    return 0


if __name__ == "__main__":
    sys.exit(main())
