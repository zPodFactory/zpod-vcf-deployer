# Working on zpod-vcf-deployer

One-file CLI (`zpod-vcf-deployer.py`, PEP 723, run with `uv run`) that deploys VCF 9.x on zPodFactory
infrastructure. Read `README.md` first; `CHANGELOG.md` says what shipped.

## Releases

- Every change gets a line under `[Unreleased]` in `CHANGELOG.md`: what changed for the person
  using the tool, and why in a clause.
- A release is one command: `python3 tools/release.py X.Y.Z --push` (`--dry-run` first). It moves
  the heading and the shipped version, commits, tags `vX.Y.Z`, pushes; the tag then publishes the
  section as the GitHub release through `.github/workflows/release.yml`.
- `python3 tools/release.py --check` is what CI runs on every push: shipped version equals the
  newest section, every tag has a section, nothing local is tracked. Keep the forbidden-string
  list in `.release-denylist` (git-ignored) for the names that must never enter the history.
- Never tag by hand, never edit a release on GitHub: fix the changelog and re-run the workflow
  (`workflow_dispatch`, blank version republishes every tag).
