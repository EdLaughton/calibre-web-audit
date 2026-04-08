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
   Finds missing series entries and related author books, then classifies them into shortlist/review/suppressed buckets. It can also optionally export or push conservative downstream queues for Bookshelf and Shelfmark from those already-classified rows.

3. `apply`
   Reads `audit/write_plan.csv` and applies approved identifier updates, with optional Calibre title/author updates and optional file-level metadata write-back.

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
- `--cwa-app-db` and/or `--cwa-dirs-json` for opt-in Calibre-Web-Automated runtime resolution

If `ebook-meta` is unavailable, the tools still run, but metadata probing falls back to weaker EPUB/ZIP/content extraction paths. For real-library audit quality, a working `ebook-meta` on the host or in the configured Docker container is strongly preferred.

### Optional CWA compatibility

`audit`, `discovery`, and `apply` can optionally resolve their effective `--library-root` and `--metadata-db` from Calibre-Web-Automated runtime files.

- `--cwa-app-db /config/app.db`
- `--cwa-dirs-json /app/calibre-web-automated/dirs.json`
- the flags are opt-in and do nothing unless you pass them
- explicit `--library-root` and `--metadata-db` still win
- when CWA split-library mode is enabled in `app.db`, the tools use:
  - `config_calibre_split_dir` as the library root
  - `<config_calibre_dir>/metadata.db` as the metadata DB
- when split-library mode is not enabled:
  - `dirs.json.calibre_library_dir` is preferred for the library root when available
  - otherwise `app.db.config_calibre_dir` is used
  - the metadata DB defaults to `<library-root>/metadata.db`

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

- `audit/actions_operator.csv`
- `audit/actions.csv`
- `audit/write_plan.csv`
- `audit/summary.md`
- `run.log`

### Discovery outputs

- `discovery/candidates.csv`
- `discovery/summary.md`
- `discovery/bookshelf_queue.csv` when `--export-bookshelf` or `--push-bookshelf` is used
- `discovery/bookshelf_queue.json` when `--export-bookshelf` or `--push-bookshelf` is used
- `discovery/bookshelf_push_log.csv` when `--export-bookshelf` or `--push-bookshelf` is used
- `discovery/bookshelf_summary.md` when `--export-bookshelf` or `--push-bookshelf` is used
- `discovery/shelfmark_queue.csv` when `--export-shelfmark` or `--push-shelfmark` is used
- `discovery/shelfmark_queue.json` when `--export-shelfmark` or `--push-shelfmark` is used
- `discovery/shelfmark_push_log.csv` when `--export-shelfmark` or `--push-shelfmark` is used
- `discovery/shelfmark_release_candidates.csv` when `--export-shelfmark-releases` or `--push-shelfmark-download` is used
- `discovery/shelfmark_release_candidates.json` when `--export-shelfmark-releases` or `--push-shelfmark-download` is used
- `discovery/shelfmark_selected_releases.csv` when `--export-shelfmark-releases` or `--push-shelfmark-download` is used
- `discovery/shelfmark_download_log.csv` when `--export-shelfmark-releases` or `--push-shelfmark-download` is used
- `discovery/shelfmark_summary.md` when any Shelfmark integration flag is used
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
   `actions_operator.csv` is the compact triage sheet.
   `actions.csv` is the full forensic non-keep/review sheet.
   `write_plan.csv` is the full-library apply sheet and can include `keep_hardcover_id` confirmation rows as well as unsafe/manual-review rows.
4. Optionally run `discovery` to generate shortlist/review/suppressed discovery candidates.
5. Run `apply --dry-run` against the chosen `write_plan.csv`.
6. Only rerun `apply` without `--dry-run` once the dry-run output looks correct.

Example audit:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-audit \
  --library-root /path/to/calibre-library
```

Example audit against a standard CWA deployment:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-audit \
  --cwa-app-db /config/app.db \
  --cwa-dirs-json /app/calibre-web-automated/dirs.json
```

Example apply against a CWA split-library deployment:

```bash
hardcover-apply \
  --cwa-app-db /config/app.db \
  --cwa-dirs-json /app/calibre-web-automated/dirs.json \
  --write-plan /path/to/output/audit/write_plan.csv \
  --dry-run
```

Audit logging defaults to compact phase/progress summaries. Add `--verbose` for per-book decision lines, and add `--debug-hardcover` only when you need low-level Hardcover HTTP/cache chatter.

Example discovery:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library
```

## Bookshelf Discovery Integration

The discovery workflow can optionally turn conservative discovery rows into a native Bookshelf queue, and can optionally push that queue into a Bookshelf instance.

Safety model:

- fully opt-in: nothing is exported or pushed unless you pass `--export-bookshelf` or `--push-bookshelf`
- default approval is `--bookshelf-approval shortlist-only`
- `shortlist-only` exports only `eligible_for_shortlist_boolean=True` rows
- `safe-only` is stricter and keeps only the plain `shortlist` bucket
- `all-approved` keeps all non-suppressed discovery rows
- suppressed rows are never exported or pushed
- live search triggering is off by default
- `--dry-run` resolves lookup strategy, records queue/log outputs, and skips add/search mutations

Bookshelf connection and add settings:

- `--bookshelf-url`
- `--bookshelf-api-key`
- `--bookshelf-root-folder`
- `--bookshelf-quality-profile-id`
- `--bookshelf-metadata-profile-id`
- optional `--bookshelf-trigger-search`
- optional `--bookshelf-mode book|author|auto` with default `book`

Lookup routing:

1. `discovery` calls `GET /api/v1/config/development` and inspects `metadataSource`.
2. If `metadataSource` is clearly Hardcover-backed, the integration allows provider-neutral direct ID lookups:
   - `edition:<hardcover-edition>`
   - `work:<hardcover-id>`
3. If `metadataSource` is not clearly Hardcover-backed, raw Hardcover IDs are treated as unsafe and lookup falls back to:
   - `isbn:<isbn13>`
   - `asin:<asin>`
   - title + author text search
4. Ambiguous matches are skipped and logged instead of forced.

Bookshelf add behavior:

- uses `GET /api/v1/search?term=...` for lookup
- uses the returned `BookResource` or `AuthorResource` as the add payload base
- fills required Bookshelf settings conservatively
- disables silent post-add searching unless `--bookshelf-trigger-search` is explicitly set
- uses explicit command enqueueing for post-add search when requested
- writes queue and push trace files under `discovery/`

Example queue export only:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-bookshelf
```

Example dry-run push:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --push-bookshelf \
  --bookshelf-url http://bookshelf.local:8787 \
  --bookshelf-api-key YOUR_BOOKSHELF_API_KEY \
  --bookshelf-root-folder /library/books \
  --bookshelf-quality-profile-id 1 \
  --bookshelf-metadata-profile-id 1 \
  --dry-run
```

Example live push with explicit post-add search:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --push-bookshelf \
  --bookshelf-url http://bookshelf.local:8787 \
  --bookshelf-api-key YOUR_BOOKSHELF_API_KEY \
  --bookshelf-root-folder /library/books \
  --bookshelf-quality-profile-id 1 \
  --bookshelf-metadata-profile-id 1 \
  --bookshelf-trigger-search
```

Example Hardcover-backed direct-ID mode:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --push-bookshelf \
  --bookshelf-url http://bookshelf-hardcover.local:8787 \
  --bookshelf-api-key YOUR_BOOKSHELF_API_KEY \
  --bookshelf-root-folder /library/books \
  --bookshelf-quality-profile-id 1 \
  --bookshelf-metadata-profile-id 1
```

This mode only uses raw Hardcover IDs when the target instance reports a clearly Hardcover-backed `metadataSource`.

Example non-Hardcover fallback mode:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --push-bookshelf \
  --bookshelf-url http://bookshelf-nonhardcover.local:8787 \
  --bookshelf-api-key YOUR_BOOKSHELF_API_KEY \
  --bookshelf-root-folder /library/books \
  --bookshelf-quality-profile-id 1 \
  --bookshelf-metadata-profile-id 1
```

If that Bookshelf instance reports a non-Hardcover `metadataSource`, the integration falls back to ISBN, ASIN, and title+author lookup only.

## Shelfmark Discovery Integration

Shelfmark now has two separate discovery-side workflows in `hardcover-discovery`:

- a conservative metadata request workflow
- a concrete release search workflow with optional explicit download queueing

Both workflows are entirely opt-in and preserve the existing discovery ranking and classification behavior.

Safety model:

- fully opt-in: nothing is exported or pushed unless you pass a Shelfmark flag
- default approval is `--shelfmark-approval shortlist-only`
- `shortlist-only` exports only `eligible_for_shortlist_boolean=True` rows
- `safe-only` is stricter and keeps only the plain `shortlist` bucket
- `all-approved` keeps all non-suppressed discovery rows
- suppressed rows are never exported or pushed
- source selection for concrete releases is always explicit via `--shelfmark-source`
- queue/download is never attempted unless you pass `--push-shelfmark-download`
- `--dry-run` writes the same queue/log artifacts and skips live request or download mutations

Shelfmark connection and control flags:

- `--shelfmark-url`
- optional `--shelfmark-username`
- optional `--shelfmark-password`
- optional `--shelfmark-note`
- optional `--shelfmark-approval shortlist-only|safe-only|all-approved`
- optional `--export-shelfmark`
- optional `--push-shelfmark`
- optional `--export-shelfmark-releases`
- optional `--push-shelfmark-download`
- required for release workflows: `--shelfmark-source`
- optional for release workflows: `--shelfmark-content-type`
- optional for release workflows: `--shelfmark-selection best|most_seeders|first|largest|preferred-format`
- optional for release workflows: `--shelfmark-format-keywords epub,kepub,pdf,...`
- optional for release workflows: `--shelfmark-min-seeders N`
- optional for release workflows: `--shelfmark-allowed-indexers idx1,idx2,...`
- optional for release workflows: `--shelfmark-blocked-indexers idx1,idx2,...`
- optional for release workflows: `--shelfmark-require-protocol http|torrent|nzb|dcc`
- optional for release workflows: `--shelfmark-timeout-seconds N`
- optional for release workflows: `--shelfmark-min-interval-ms N`
- optional for release workflows: `--shelfmark-max-retries N`
- optional for release workflows: `--shelfmark-retry-backoff-seconds N`

Shelfmark request workflow:

1. `discovery` builds a request queue from already-classified discovery rows.
2. Live request push logs in with `POST /api/auth/login`.
3. It checks `GET /api/request-policy`.
4. Live request submission only proceeds when:
   - `requests_enabled=true`
   - the ebook request policy resolves to `request_book`
5. Discovery rows are submitted as Hardcover-backed book requests:
   - `provider=hardcover`
   - `provider_id=<hardcover-id>`
   - `content_type=ebook`
   - `request_level=book`
   - `source=*`
6. Duplicate pending requests are logged as explicit duplicate skips.

Shelfmark release workflow:

1. `discovery` searches Shelfmark releases for eligible rows.
2. It prefers the metadata-backed route first:
   - `GET /api/releases?provider=hardcover&book_id=<hardcover-id>&source=<explicit-source>&content_type=<content-type>`
3. If that yields no releases, it falls back conservatively to a title+author search against the same explicit source.
4. Returned releases are filtered locally for:
   - explicit source match
   - requested content type when the release advertises one
   - requested format keywords when provided
   - minimum seeders when provided
   - allowed / blocked underlying indexers when configured
   - required protocol when configured
   - presence of concrete `source` and `source_id`
5. One accepted release is selected with an explicit deterministic rule:
   - `first`
   - `most_seeders`
   - `largest`
   - `preferred-format`
   - `best`
6. Only `--push-shelfmark-download` queues the selected release to `POST /api/releases/download`.
   Without that flag, `discovery` exports the chosen release and logs what would have been queued.
7. Release searches are paced serially with a conservative default request interval.
8. Transient failures such as timeouts, `429`, and `503` can be retried with explicit backoff controls.
9. Per-row search failures are logged and skipped without aborting the whole discovery run unless the setup itself is invalid.

Selection rules:

- `first`: first accepted release in Shelfmark response order
- `most_seeders`: highest seeders, then larger size, then stable response order
- `largest`: largest `size_bytes`, then higher seeders, then stable response order
- `preferred-format`: earliest matching keyword in `--shelfmark-format-keywords`, then higher seeders, then larger size
- `best`: if format keywords are provided, earliest matching keyword wins; otherwise a conservative built-in format priority is used, then higher seeders, then larger size

Release hardening behavior:

- `--shelfmark-source` must always name the top-level Shelfmark source, such as `direct_download` or `prowlarr`
- when `--shelfmark-source prowlarr` is used, allow/block filtering applies to the underlying `release.indexer` values returned by Shelfmark
- allow/block indexer matching is exact after case-folding and whitespace normalization
- `--shelfmark-allowed-indexers` is applied before selection and may also be passed through to Shelfmark’s `indexers=` search parameter for `prowlarr`
- `--shelfmark-blocked-indexers` is applied locally before selection
- `--shelfmark-require-protocol` filters releases by the returned `protocol` field before selection
- bad sources, timeouts, and upstream/provider errors are logged per row and do not stall the rest of the run
- `shelfmark_release_candidates.csv`, `shelfmark_selected_releases.csv`, and `shelfmark_download_log.csv` record candidate counts before and after filtering, retry counts, HTTP status, error kind, error message/body, and the final row action

Auth model:

- the request workflow requires username/password because it depends on Shelfmark’s authenticated request API
- the release workflow can run anonymously against a no-auth Shelfmark instance, or with username/password when the instance requires login
- no dedicated Shelfmark API-key flow was found in the inspected Shelfmark codebase, so this integration uses the real username/password auth surface when auth is needed

Artifacts:

- `discovery/shelfmark_queue.csv`
- `discovery/shelfmark_queue.json`
- `discovery/shelfmark_push_log.csv`
- `discovery/shelfmark_release_candidates.csv`
- `discovery/shelfmark_release_candidates.json`
- `discovery/shelfmark_selected_releases.csv`
- `discovery/shelfmark_download_log.csv`
- `discovery/shelfmark_summary.md`

Example request queue export only:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark
```

Example request dry-run validation:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --push-shelfmark \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-username YOUR_SHELFMARK_USERNAME \
  --shelfmark-password YOUR_SHELFMARK_PASSWORD \
  --dry-run
```

Example release export only:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark-releases \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source libgen
```

Example dry-run release search:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark-releases \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source libgen \
  --shelfmark-content-type ebook \
  --dry-run
```

Example dry-run with `prowlarr` and ebook-only filtering:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark-releases \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source prowlarr \
  --shelfmark-content-type ebook \
  --dry-run
```

Example allowlisted indexers only:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark-releases \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source prowlarr \
  --shelfmark-allowed-indexers "MyAnonamouse,Anna's Archive"
```

Example blocked indexers:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark-releases \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source prowlarr \
  --shelfmark-blocked-indexers "Indexer To Skip,Other Indexer"
```

Example dry-run queue/download:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --push-shelfmark-download \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source libgen \
  --shelfmark-content-type ebook \
  --dry-run
```

Example timeout override:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark-releases \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source prowlarr \
  --shelfmark-timeout-seconds 60
```

Example live queue/download with explicit source selection:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --push-shelfmark-download \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source libgen \
  --shelfmark-content-type ebook \
  --shelfmark-selection best
```

Example ebook-only mode:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark-releases \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source libgen \
  --shelfmark-content-type ebook \
  --shelfmark-format-keywords epub,kepub,pdf
```

Example most-seeders mode:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --push-shelfmark-download \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source libgen \
  --shelfmark-selection most_seeders \
  --shelfmark-min-seeders 10
```

Example slower pacing with retries for rate-limited providers:

```bash
HARDCOVER_TOKEN='your_raw_token_here' \
hardcover-discovery \
  --library-root /path/to/calibre-library \
  --export-shelfmark-releases \
  --shelfmark-url http://shelfmark.local:8084 \
  --shelfmark-source prowlarr \
  --shelfmark-min-interval-ms 3000 \
  --shelfmark-max-retries 2 \
  --shelfmark-retry-backoff-seconds 4
```

Limitations:

- direct queue/download still depends on Shelfmark returning at least one concrete release with `source` and `source_id`
- if all returned releases fail source/content-type/format/indexer/protocol/min-seeder checks, the row is exported and logged as filtered out
- the request workflow and the release workflow are separate by design; request policy does not control direct release downloads
- the release workflow does not invent or guess a release when multiple results exist; it only applies the explicit operator-selected rule and logs why that rule chose the winner
- retries stay serial and conservative in this pass; there is no concurrent release search fan-out

## Safe Apply Workflow

`apply` is intentionally conservative.

Default behavior:

- consumes `audit/write_plan.csv` only
- skips manual-review style rows by default
- applies only rows marked `safe_to_apply_boolean=True`
- updates Hardcover identifiers only
- updates `metadata.db` only
- does not write sidecar OPF or internal EPUB metadata unless you explicitly request it
- does not update Calibre title/author unless you explicitly ask for it
- uses a transaction and rolls back on failure

Recommended workflow:

1. Run `audit` and inspect `audit/actions.csv`, `audit/write_plan.csv`, and `audit/summary.md`.
   Treat `actions_operator.csv` as the short review queue.
   Treat `actions.csv` as the full forensic audit artifact.
   Treat `write_plan.csv` as the apply sheet, and filter it using `safe_to_apply_boolean`, `safe_to_apply_reason`, and `action_type`.
2. Run `apply` with `--dry-run` first.
3. If the dry-run looks correct, rerun without `--dry-run`.
4. Only add `--include-calibre-title-author` when you explicitly want Calibre title/author rewrites as well as identifier updates.
5. Only add file-write flags after a dry-run and only when you want Calibre-sidecar or EPUB metadata to mirror the DB update.

Supported opt-in file-write targets:

- Calibre sidecar OPF files:
  - `metadata.opf`
  - or a single `.opf` file in the book folder when that is the only available OPF target
- internal OPF metadata in EPUB-family files:
  - `EPUB`
  - `KEPUB`
  - `OEBZIP`

Unsupported formats are skipped explicitly and recorded in `apply/apply_log.csv`.

Safety model for file writes:

- `metadata.db` remains the primary source of truth
- DB-only apply remains the default
- file writes are opt-in
- dry-run resolves and logs file targets without mutating files
- live file writes use best-effort backup/restore, but there is no single atomic transaction spanning both `metadata.db` and multiple files
- for real libraries, keep normal Calibre/library backups before enabling file-write modes

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

Example DB + sidecar OPF apply:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --write-sidecar-opf
```

Example DB + internal EPUB metadata apply:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --write-epub-opf
```

Example dry-run for file metadata writes:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --write-ebook-metadata \
  --dry-run
```

Example files-only sidecar dry-run:

```bash
hardcover-apply \
  --library-root /path/to/calibre-library \
  --write-plan /path/to/output/audit/write_plan.csv \
  --files-only \
  --write-sidecar-opf \
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
