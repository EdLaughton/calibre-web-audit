# Hardcover Tools Rewrite

`hardcover_tools` is a maintainable rewrite of a messy but working monolithic Calibre/Hardcover audit script.

The repo keeps the current script as the behavioural source of truth, but restructures the project around:

- thin CLI entrypoints
- reusable shared core modules
- conservative mutation boundaries
- parity-focused tests around the fragile ranking and routing rules

## Installation

Editable install:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip setuptools wheel
python -m pip install -e .
```

Editable install with test dependencies:

```bash
python -m pip install -e '.[dev]'
```

Quote `'.[dev]'` under `zsh` so the extras spec is not treated as a glob.

Non-editable install:

```bash
python -m pip install .
```

After installation the console scripts are:

```bash
hardcover-audit --help
hardcover-discovery --help
hardcover-apply --help
```

You can still use the module entrypoints directly:

```bash
python3 -m hardcover_tools.cli.audit --help
python3 -m hardcover_tools.cli.discovery --help
python3 -m hardcover_tools.cli.apply --help
```

## Entry Points

The project exposes three workflows:

1. `audit`
   Audits books already in Calibre against Hardcover and local ebook metadata.

2. `discovery`
   Finds missing series entries and related author books, then classifies them into shortlist/review/suppressed buckets.

3. `apply`
   Reads `audit/write_plan.csv` and applies approved identifier updates, with optional Calibre title/author updates.

## Required Inputs

### Common runtime inputs

- a Calibre library root
- a `metadata.db` inside that library root, or an explicit `--metadata-db`
- the working ebook files referenced by Calibre

### Hardcover token

`audit` and `discovery` require `HARDCOVER_TOKEN` as a raw token:

```bash
export HARDCOVER_TOKEN='your_raw_token_here'
```

Use the raw token only. Do not prefix it with `Bearer `.

### Optional runtime inputs

- `--cache-path` for the Hardcover cache database
- `--author-aliases-json` for author normalization aliases
- `--ebook-meta-command` or `--docker-ebook-meta-container` when ebook metadata extraction needs an explicit tool path/container

If `ebook-meta` is unavailable, the tools still run, but metadata probing falls back to weaker EPUB/ZIP/content extraction paths. For real-library audit quality, a working `ebook-meta` on the host or in the configured Docker container is strongly preferred.

## Runtime Defaults

Unless you override them explicitly:

- `--metadata-db` defaults to `<library-root>/metadata.db`
- `--cache-path` defaults to `<library-root>/hardcover_cache.sqlite`
- `--output-dir` defaults to `<library-root>/audit_output_YYYYMMDD_HHMMSS`
- `apply --write-plan` first looks for `<library-root>/audit/write_plan.csv`, then falls back to `<library-root>/write_plan.csv`
- `run.log` is written at the root of each output directory

If an older `<library-root>/hardcover_cache.json` exists, it is detected and imported into the SQLite cache on first use.

## Outputs

Each command writes a timestamped output directory by default unless you pass `--output-dir`.

### Audit outputs

- `audit/actions.csv`
- `audit/write_plan.csv`
- `audit/summary.md`
- `run.log`

### Discovery outputs

- `discovery/candidates.csv`
- `discovery/summary.md`
- `run.log`

### Apply outputs

- `apply/apply_log.csv`
- `apply/summary.md`
- `run.log`

## Real-Library Workflow

Recommended end-to-end flow:

1. Export `HARDCOVER_TOKEN`.
2. Run `audit` against the Calibre library root.
3. Review `audit/summary.md`, `audit/actions.csv`, and `audit/write_plan.csv`.
4. Optionally run `discovery` to generate shortlist/review/suppressed discovery candidates.
5. Run `apply --dry-run` against the chosen `write_plan.csv`.
6. Only rerun `apply` without `--dry-run` once the dry-run output looks correct.

Example audit:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-audit \
  --library-root /path/to/calibre-library
```

Example discovery:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library
```

## Safe Apply Workflow

`apply` is intentionally conservative.

Default behavior:

- consumes `audit/write_plan.csv` only
- skips manual-review style rows by default
- applies only rows marked `safe_to_apply_boolean=True`
- updates Hardcover identifiers only
- does not update Calibre title/author unless you explicitly ask for it
- uses a transaction and rolls back on failure

Recommended workflow:

1. Run `audit` and inspect `audit/actions.csv`, `audit/write_plan.csv`, and `audit/summary.md`.
2. Run `apply` with `--dry-run` first.
3. If the dry-run looks correct, rerun without `--dry-run`.
4. Only add `--include-calibre-title-author` when you explicitly want Calibre title/author rewrites as well as identifier updates.

Example dry-run:

```bash
python3 -m hardcover_tools.cli.apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --dry-run
```

Example safe apply with selected actions:

```bash
python3 -m hardcover_tools.cli.apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --apply-actions replace_hardcover_id,safe_auto_fix,update_calibre_metadata
```

Example safe apply including Calibre title/author changes:

```bash
python3 -m hardcover_tools.cli.apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --apply-actions replace_hardcover_id,safe_auto_fix,update_calibre_metadata \
  --include-calibre-title-author
```

Console-script equivalents:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --dry-run
```

## Identifier Conventions

The canonical identifier contract is fixed.

Write-back and internal canonical names are exactly:

- `hardcover-id`
- `hardcover-slug`
- `hardcover-edition`

Legacy aliases may still be accepted when reading older metadata, but new writes are normalized back to those canonical names.

## Testing

Run the current parity-focused test set with:

```bash
pytest -q
```

## Operator Docs

For a more explicit runbook, see [`docs/operator_guide.md`](docs/operator_guide.md).
