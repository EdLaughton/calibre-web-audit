# Operator Guide

This guide is for running `hardcover_tools` against a real Calibre library safely.

## Install

Editable install:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip setuptools wheel
python -m pip install -e .
```

Editable install with development dependencies:

```bash
python -m pip install -e '.[dev]'
```

Under `zsh`, keep `'.[dev]'` quoted so the extras spec is not expanded as a glob.

Non-editable install:

```bash
python -m pip install .
```

Installed console scripts:

```bash
hardcover-audit --help
hardcover-discovery --help
hardcover-apply --help
```

Module entrypoints remain valid:

```bash
python3 -m hardcover_tools.cli.audit --help
python3 -m hardcover_tools.cli.discovery --help
python3 -m hardcover_tools.cli.apply --help
```

## Environment

`audit` and `discovery` require:

```bash
export HARDCOVER_TOKEN='your_raw_token_here'
```

Use the raw token only. Do not prefix it with `Bearer `.

Optional but useful:

- `--author-aliases-json` for author alias normalization
- `--ebook-meta-command` to point at a host `ebook-meta`
- `--docker-ebook-meta-container` plus `--container-library-root` if `ebook-meta` lives in Docker

If `ebook-meta` is unavailable, the tools still run, but metadata probing falls back to weaker EPUB/content extraction. That is acceptable for smoke checks, but for a real library you usually want a working `ebook-meta`.

## Path Defaults

Given `--library-root /path/to/calibre-library`:

- metadata DB default: `/path/to/calibre-library/metadata.db`
- cache DB default: `/path/to/calibre-library/hardcover_cache.sqlite`
- output dir default: `/path/to/calibre-library/audit_output_YYYYMMDD_HHMMSS`
- apply write plan default:
  - first `/path/to/calibre-library/audit/write_plan.csv`
  - then `/path/to/calibre-library/write_plan.csv`

Each run writes `run.log` at the root of the chosen output directory.

If a legacy `/path/to/calibre-library/hardcover_cache.json` exists, it is imported into the SQLite cache automatically on first use.

## Recommended Workflow

1. Run `audit`.
2. Review `audit/summary.md`, `audit/actions.csv`, and `audit/write_plan.csv`.
   `actions_operator.csv` is the short triage view.
   `actions.csv` is the full forensic action/review sheet.
   `write_plan.csv` is the full-library apply sheet and may include `keep_hardcover_id` confirmation rows plus rows that are intentionally unsafe to apply.
3. Optionally run `discovery` and review `discovery/candidates.csv`.
4. Run `apply --dry-run` first.
5. Only rerun `apply` without `--dry-run` after checking the dry-run output.
6. Only add `--include-calibre-title-author` when you intentionally want Calibre title/author mutation in addition to identifier writes.
7. Only add file-write flags when you explicitly want the library files to mirror the DB update as well.

## Example Commands

Audit:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-audit \
  --library-root /path/to/calibre-library
```

Default audit logging is concise and progress-oriented. Use `--verbose` when you want per-book decision lines, and reserve `--debug-hardcover` for low-level Hardcover/cache troubleshooting.

Discovery:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library
```

Apply dry-run:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --dry-run
```

Apply DB + sidecar OPF:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --write-sidecar-opf
```

Apply DB + internal EPUB OPF:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --write-epub-opf
```

Dry-run file writes without DB mutation:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --files-only \
  --write-sidecar-opf \
  --dry-run
```

## File Metadata Write Modes

Default apply behavior remains:

- mutate `metadata.db` only
- write canonical identifiers only
- skip manual-review style rows
- skip unsafe rows by default

Opt-in file-write targets in this stage:

- Calibre sidecar OPF:
  - `metadata.opf`
  - or a single `.opf` file in the book folder when that is the only available OPF target
- internal OPF metadata in EPUB-family files:
  - `EPUB`
  - `KEPUB`
  - `OEBZIP`

Unsupported file types such as `PDF` are not rewritten. They are skipped cleanly and recorded in `apply/apply_log.csv`.

Target selection rules:

- `--write-sidecar-opf` enables sidecar OPF writing
- `--write-epub-opf` enables internal EPUB OPF writing
- `--write-ebook-metadata` enables both and chooses one target per row
- when both are available, sidecar OPF is preferred by default
- `--prefer-internal-epub-opf` flips that preference

What gets written:

- canonical identifiers only:
  - `hardcover-id`
  - `hardcover-slug`
  - `hardcover-edition`
- Calibre title/author equivalents only when `--include-calibre-title-author` is also set

What does not change in this stage:

- no audit/discovery behavior
- no write-plan row inclusion rules
- no new identifier names
- no safe-but-fake support for MOBI/PDF/AZW3 sidecar or internal metadata rewriting

## Safety Model

- `metadata.db` remains the primary source of truth
- DB mutation stays the default and safest path
- file writes are always explicit opt-in
- `--dry-run` resolves file targets and logs would-apply results without persisting DB or file changes
- DB writes still use the existing transaction model
- file writes use best-effort backup/restore and are rolled back if a later row fails during the same run
- there is no single atomic transaction spanning `metadata.db` plus multiple on-disk files

For real-library runs:

1. run `apply --dry-run` first
2. inspect `apply/apply_log.csv` for `db_write_status`, `file_write_target`, `file_write_status`, and skip reasons
3. keep your normal library backup before enabling sidecar/EPUB write modes

Apply safe actions only:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --apply-actions replace_hardcover_id,safe_auto_fix,update_calibre_metadata
```

Apply safe actions plus title/author updates:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --apply-actions replace_hardcover_id,safe_auto_fix,update_calibre_metadata \
  --include-calibre-title-author
```

## Safety Rules To Remember

- Canonical identifier names remain exactly:
  - `hardcover-id`
  - `hardcover-slug`
  - `hardcover-edition`
- `apply` skips manual-review style rows by default.
- `apply` restricts itself to `safe_to_apply_boolean=True` rows by default.
- Identifier-only apply remains the default.
- Title/author updates happen only when you explicitly request them.
- `metadata.db`-only apply remains the default.
- File writes are best-effort and explicit, not silent or implicit.
