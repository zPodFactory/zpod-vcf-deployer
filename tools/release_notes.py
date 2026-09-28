#!/usr/bin/env python
"""The changelog section a version already has, published as the release note GitHub shows.

The Releases tab was listing six tags with nothing under any of them, while the changelog
sat in the repository, written, long, and read by the website. A tag is not a release note:
it is a name for a commit. Somebody arriving from `pip install -U`, or from the tab itself,
got a version number and no answer to "what changed" — and the one place that answers it
was two clicks away in a file they had no reason to open.

So the notes are read from `CHANGELOG.md` rather than written a second time. A changelog
that exists twice disagrees with itself by the next release, and the copy people actually
edit is the file.

    python3 tools/release_notes.py 0.3.0             # print what would be published
    python3 tools/release_notes.py 0.3.0 --publish   # create it, or update it
    python3 tools/release_notes.py --all --publish   # every tag that has a section
    python3 tools/release_notes.py --check           # every tag has one

Tagging stays a human action: `--publish` passes `--verify-tag`, so this can fill in a
release for a tag that exists and can never invent the tag itself. `.github/workflows/
release.yml` runs the same command when a tag is pushed, which is what makes the rule hold
without anybody remembering it.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHANGELOG = ROOT / "CHANGELOG.md"

_TOKEN_FALLBACK_SAID = False


# ── the file ─────────────────────────────────────────────────────────────────────────


def sections() -> dict[str, tuple[str, str]]:
    """version → (date, body), newest first, skipping `[Unreleased]` and empty ones."""
    out: dict[str, tuple[str, str]] = {}
    text = CHANGELOG.read_text()
    heads = list(re.finditer(r"^## \[([^\]]+)\](?:\s*—\s*(.*))?$", text, re.M))
    for index, head in enumerate(heads):
        end = heads[index + 1].start() if index + 1 < len(heads) else len(text)
        version, date = head.group(1), (head.group(2) or "").strip()
        body = text[head.end():end].strip()
        if not re.match(r"^\d", version) or not body:
            continue
        out[version] = (date, body)
    return out


def released_versions() -> list[str]:
    """Every version with a section, newest first — the order the file is written in."""
    return list(sections())


# ── git and GitHub ───────────────────────────────────────────────────────────────────


def _git(*args: str) -> str:
    return subprocess.run(("git", *args), cwd=ROOT, capture_output=True, text=True,
                          check=True).stdout.strip()


def tags() -> list[str]:
    """Release tags, oldest first. Empty when there is no git or no tags."""
    try:
        listing = _git("tag", "-l", "v[0-9]*", "--sort=v:refname")
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [line for line in listing.splitlines() if line]


def repo_slug() -> str:
    """`owner/name`, read from the remote rather than asked of the network."""
    url = _git("remote", "get-url", "origin")
    match = re.search(r"github\.com[:/](?P<slug>[^/]+/[^/]+?)(?:\.git)?$", url)
    return match.group("slug") if match else ""


def _gh(*args: str) -> subprocess.CompletedProcess:
    """`gh`, with one workaround: an invalid GITHUB_TOKEN shadows a working login.

    `gh` prefers `GITHUB_TOKEN`/`GH_TOKEN` over the credential it stored at `gh auth
    login`, so an expired token exported in a shell — or inherited from a tool that sets
    one — turns every call into `401 Bad credentials` while `gh auth status` cheerfully
    reports a logged-in account underneath it. The retry is announced rather than silent:
    a workaround nobody sees is a token nobody fixes.
    """
    global _TOKEN_FALLBACK_SAID

    first = subprocess.run(("gh", *args), cwd=ROOT, capture_output=True, text=True)
    if first.returncode == 0:
        return first

    said = (first.stderr or "") + (first.stdout or "")
    shadowed = any(name in os.environ for name in ("GITHUB_TOKEN", "GH_TOKEN"))
    if not shadowed or not re.search(r"Bad credentials|401|gh auth login", said):
        return first

    env = {key: value for key, value in os.environ.items()
           if key not in ("GITHUB_TOKEN", "GH_TOKEN")}
    retry = subprocess.run(("gh", *args), cwd=ROOT, capture_output=True, text=True, env=env)
    if retry.returncode == 0 and not _TOKEN_FALLBACK_SAID:
        _TOKEN_FALLBACK_SAID = True
        print("note: the GITHUB_TOKEN in this environment is not valid — used the "
              "stored `gh auth login` credential instead", file=sys.stderr)
    return retry


def release_exists(tag: str) -> bool:
    return _gh("release", "view", tag, "--json", "tagName").returncode == 0


# ── what gets published ──────────────────────────────────────────────────────────────


def title_for(tag: str) -> str:
    """The version, and nothing else.

    A headline here was tried and dropped. The tagged commit's subject made a good sentence
    and a bad title: the release page then said the same thing twice in two voices, once as
    a heading somebody wrote for a commit and once as the changelog section directly below
    it, which is the copy that is actually maintained. A list of releases is read as a list
    of versions — `v0.2.4`, `v0.2.5`, `v0.3.0` — and the body is where the explaining goes.
    """
    return tag


def notes_for(version: str, *, slug: str = "") -> str:
    """The release body: the section as written, plus a link to the whole file."""
    found = sections()
    if version not in found:
        raise SystemExit(f"CHANGELOG.md has no section for {version} — write it first")
    _date, body = found[version]

    slug = slug or repo_slug()
    footer = []
    if slug:
        older = [tag for tag in tags() if tag != f"v{version}"]
        previous = _previous_tag(version, older)
        if previous:
            footer.append(f"**Full changelog**: "
                          f"https://github.com/{slug}/compare/{previous}...v{version}")
        footer.append(f"**Every release**: https://github.com/{slug}/blob/main/CHANGELOG.md")

    # No date line: GitHub prints the release date beside the tag already, and the body is
    # for what the changelog says rather than for restating the page around it.
    #
    # Blank lines between the footer links: GitHub renders a release body as markdown, and
    # two lines with one newline between them are one paragraph.
    return "\n\n".join(part for part in (body, "\n\n".join(footer)) if part) + "\n"


def _previous_tag(version: str, candidates: list[str]) -> str:
    """The newest tag older than this version, by the order the tags sort in."""
    def key(tag: str) -> tuple:
        return tuple(int(part) for part in re.findall(r"\d+", tag))

    older = [tag for tag in candidates if key(tag) < key(f"v{version}")]
    return max(older, key=key) if older else ""


def publish(version: str, *, newest: bool = True) -> str:
    """Create the release, or bring an existing one's notes back in line with the file."""
    tag = f"v{version}"
    if tag not in tags():
        raise SystemExit(f"{tag} is not a tag — tag the release first, then publish it")

    notes = notes_for(version)
    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False) as handle:
        handle.write(notes)
        path = handle.name

    try:
        if release_exists(tag):
            done = _gh("release", "edit", tag, "--notes-file", path,
                       "--title", title_for(tag))
            verb = "updated"
        else:
            done = _gh("release", "create", tag,
                       # Never as a side effect: a missing tag is a mistake to report,
                       # not a tag to create.
                       "--verify-tag",
                       "--title", title_for(tag),
                       "--notes-file", path,
                       "--latest" if newest else "--latest=false")
            verb = "created"
    finally:
        os.unlink(path)

    if done.returncode != 0:
        raise SystemExit(f"gh failed for {tag}:\n{done.stderr.strip()}")
    return f"{verb} {tag} — {done.stdout.strip() or title_for(tag)}"


# ── the check the test runs ──────────────────────────────────────────────────────────


def missing_sections() -> list[str]:
    """Tags with no changelog section. The rule, in one function, so a test can call it."""
    found = sections()
    return [tag for tag in tags() if tag.removeprefix("v") not in found]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("version", nargs="?", help="e.g. 0.3.0 (or v0.3.0)")
    parser.add_argument("--all", action="store_true",
                        help="every tag that has a changelog section")
    parser.add_argument("--publish", action="store_true",
                        help="write it to GitHub with gh, instead of printing it")
    parser.add_argument("--check", action="store_true",
                        help="exit non-zero if any tag has no changelog section")
    args = parser.parse_args(argv)

    if args.check:
        gaps = missing_sections()
        if gaps:
            print("tagged, but not in CHANGELOG.md: " + ", ".join(gaps), file=sys.stderr)
            return 1
        print(f"every tag has a changelog section ({len(tags())} tags)")
        return 0

    if args.all:
        wanted = [tag.removeprefix("v") for tag in tags()
                  if tag.removeprefix("v") in sections()]
    elif args.version:
        wanted = [args.version.removeprefix("v")]
    else:
        parser.error("name a version, or pass --all or --check")

    newest = released_versions()[0] if released_versions() else ""
    for version in wanted:
        if not args.publish:
            print(f"── v{version} — {title_for(f'v{version}')}\n")
            print(notes_for(version))
            continue
        print(publish(version, newest=(version == newest)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
