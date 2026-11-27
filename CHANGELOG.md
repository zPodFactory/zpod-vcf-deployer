# Changelog

Notable changes, newest first. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Entries say what changed for the person using the tool, and why in a clause. Every change
lands under `[Unreleased]` as it is made.

**Cutting a release.** `python3 tools/release.py X.Y.Z --push` does the rest: `[Unreleased]`
becomes `[X.Y.Z] — date`, the shipped version moves with it, the commit is tagged `vX.Y.Z` and
pushed, and the tag publishes this file's section as the GitHub release note
(`.github/workflows/release.yml`, `tools/release_notes.py`). The script refuses a dirty tree,
an empty `[Unreleased]`, a version not above the last tag, a tracked log or `.env`, and any
string from the local `.release-denylist`; `--check` runs the same rules, and CI runs it on
every push. Preview a note with `python3 tools/release_notes.py X.Y.Z`.

The `0.x` line stays pre-1.0 while the shape of the tool can still move; a change that breaks
a flag or an output someone may parse is named in a **Breaking** section, which is what warns
a reader, not the digit.

## [Unreleased]

## [0.2.0] — 2026-11-27

### Added

- **`--balloon-driver-vib PATH` installs the zNestedBalloon driver on every nested ESXi host.**
  Off unless given. In phase 3, before VCF bring-up, it copies the `zballoon` VIB (community
  acceptance, one build per ESXi release), installs it, loads the `zballoon` module and fails
  the deploy if the module or its `running` log line is missing. Why: lets the physical host
  reclaim memory that sits free inside nested hosts.
- **`config/v9.1-latest_std_3hosts-ftt0.json`**, the `v9.1-latest` template with vSAN
  `failuresToTolerate: 0`, so the deployer's vSAN ESA FTT=0 workaround applies. Why: a nested
  lab asks for FTT=0, and this saves editing the template by hand.

### Fixed

- **The vSAN ESA FTT=0 fix is re-applied whatever the deploy outcome.** It used to run again
  only after a successful deploy; it now also runs before each resume and on failure, so
  appliances deployed in between don't stay on vSAN's AutoRAID policy.
- **The NSX VDR MAC fix follows the template's vCenter and NSX hostnames.** It used a
  hardcoded `vcsa` and `nsx`, so a template renaming either got a false "vcsa unreachable"
  warning, or an NSX MAC fix that never found its manager. It now reads `vcenterHostname`
  and the NSX `vipFqdn`.
- **zPod deploy waits through `CONFIG_SCRIPTS` and `POST_SCRIPTS`.** It stopped with
  "Unexpected status: CONFIG_SCRIPTS" as soon as zPodFactory began running its scripts. Why: the
  wait loop only knew `PENDING` and `BUILDING`, while zPodFactory has two more in-progress phases.

### Changed

- **Releases follow the shared zPodFactory standard.** `tools/release.py` (cut, `--check`,
  `--draft`, `--from-commits`) and `tools/release_notes.py` are the same files as in every other
  repository, with a configuration block at the top; `tools/README.md` explains the release in a
  page. The workflow re-checks before publishing. Why: six repositories had four variants of the
  same idea.
- **CI checks out with `actions/checkout@v7`**, up from `v4`. Why: `v4` was just what got copied
  in from the sibling repo's workflow; nothing pins it there — GitHub-hosted runners already
  clear `v5`'s Node 24 requirement, and neither workflow checks out a fork PR, which is the one
  behavior `v7` changes.
- **`v9.1-latest` targets VCF 9.1.2.0**, up from 9.1.1.0. Why: 9.1.2 is the newest
  maintenance release of the 9.1.x line, and the template's top-level `version` is not
  auto-discovered.

## [0.1.0] — 2026-09-28

### Added

- **End-to-end VCF deployment on zPodFactory infrastructure.** A single PEP 723 script drives
  the full pipeline — zPod provisioning, DNS configuration, VCF depot setup (online token or
  offline depot server), automated bundle downloads, SDDC validation, and SDDC deployment —
  with live terminal progress tracking throughout. No manual Python environment: `uv run`
  installs everything from the script's inline metadata.
- **Both VCF 9.0.x and 9.1.x supported.** The deployer auto-detects the version family from the
  template's top-level `version` field and adapts DNS records, depot components, ESXi
  preparation and progress display accordingly. GA templates pin every component to its full,
  build-qualified version; `-latest` templates omit `version` so the Installer resolves the
  newest available per component, including express patches (9.1+ only — 9.0.x rejects patch
  versions at bring-up).
- **Per-template hostname/IP overrides via a `.ips.json` sidecar.** A template can ship a
  sidecar named after it (e.g. `v9.1.0.0_std_3hosts.json` → `v9.1.0.0_std_3hosts.ips.json`), a
  flat `{"<short hostname>": <last octet>}` object merged on top of the built-in DNS mapping —
  renaming or adding a role without touching the script. Unreadable sidecars, non-integer/
  out-of-range octets, or octets colliding with the zPod gateway/zbox/DHCP range abort or warn
  as appropriate.
- **Nested NSX-T VDR MAC collision fix.** The nested NSX-T deployed inside a zPod and
  zPodFactory's own outer NSX-T both default their virtual distributed router MAC to the same
  hardcoded value, silently breaking routed traffic through a nested T1 once hosts are
  prepared. Fixed per Broadcom's documented procedure (GET/PUT the nested NSX Manager's
  `vdr_mac` via its Policy API), applied proactively as soon as the "Deploy and configure NSX"
  milestone starts and again idempotently once the SDDC completes, run over SSH to zcore since
  the script's own network path crosses the same broken boundary.
- **FTT=0 restored on vSAN ESA.** VCF 9.1's vSAN ESA ignores
  `datastoreSpec.vsanSpec.failuresToTolerate` and auto-manages its own AutoRAID policy instead,
  landing every appliance on RAID-1/5/6 rather than the FTT=0 a nested lab has capacity for.
  Applied against vCenter with pyvmomi, both as a background task started as early as inventory
  allows and as an end-of-deploy sweep for anything deployed in between; failures warn rather
  than fail the run.
- **Live terminal UI**: Rich-based progress tables and spinners, resume support, per-phase
  timing, and `--debug-log` transcripts.

**Full history**: this release folds together everything from the initial commit through the
VCF 9.1.1.0 template bump; see `git log` for the individual commits.
