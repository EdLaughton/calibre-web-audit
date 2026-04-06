# Hardcover Tools Rewrite

`hardcover_tools` is a maintainable rewrite of a messy but working monolithic Calibre/Hardcover audit script.

The repo keeps the current script as the behavioural source of truth, but restructures the project around:

- thin CLI entrypoints
- reusable shared core modules
- conservative mutation boundaries
- parity-focused tests around the fragile ranking and routing rules

## Entry Points

The project exposes three workflows:

1. `audit`
   Audits books already in Calibre against Hardcover and local ebook metadata.

2. `discovery`
   Finds missing series entries and related author books, then classifies them into shortlist/review/suppressed buckets.

3. `apply`
   Reads `audit/write_plan.csv` and applies approved identifier updates, with optional Calibre title/author updates.

You can run them either as Python modules:

```bash
python3 -m hardcover_tools.cli.audit --help
python3 -m hardcover_tools.cli.discovery --help
python3 -m hardcover_tools.cli.apply --help
```

Or through console scripts after installation:

```bash
hardcover-audit --help
hardcover-discovery --help
hardcover-apply --help
```

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

## Identifier Conventions

The canonical identifier contract is fixed.

Write-back and internal canonical names are exactly:

- `hardcover-id`
- `hardcover-slug`
- `hardcover-edition`

Legacy aliases may still be accepted when reading older metadata, but new writes are normalized back to those canonical names.

## Example Commands

Audit:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
python3 -m hardcover_tools.cli.audit \
  --library-root /path/to/calibre-library
```

Discovery:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
python3 -m hardcover_tools.cli.discovery \
  --library-root /path/to/calibre-library
```

Apply:

```bash
python3 -m hardcover_tools.cli.apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --dry-run
```

Console script equivalents:

```bash
hardcover-audit --library-root /path/to/calibre-library
hardcover-discovery --library-root /path/to/calibre-library
hardcover-apply --library-root /path/to/calibre-library --write-plan /path/to/output/audit/write_plan.csv --dry-run
```

## Testing

Run the current parity-focused test set with:

```bash
PYTHONPATH=. pytest -q
```

## Caveats And TODOs

- Core audit and discovery behavior still relies on the legacy runtime module for the heaviest matching/search/orchestration paths; the rewrite has improved structure first, not fully replaced every legacy implementation yet.
- The current fixture `write_plan.csv` did not originally carry slug columns. The rewritten audit output now enriches the write plan with slug fields for future runs.
- When `apply` replaces a Hardcover work id and the write plan does not include a replacement slug, it clears the stale slug rather than preserving mismatched identifier data.
- The local SQLite `title_sort()` helper used during explicit Calibre title updates is a conservative approximation of Calibre's article-handling behavior, not a full Calibre implementation.
