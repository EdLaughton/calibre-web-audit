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
3. Optionally run `discovery` and review `discovery/candidates.csv`.
4. Run `apply --dry-run` first.
5. Only rerun `apply` without `--dry-run` after checking the dry-run output.
6. Only add `--include-calibre-title-author` when you intentionally want Calibre title/author mutation in addition to identifier writes.

## Example Commands

Audit:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-audit \
  --library-root /path/to/calibre-library
```

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
