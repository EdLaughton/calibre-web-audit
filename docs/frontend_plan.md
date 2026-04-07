# Frontend plan

## Objective
Add a web frontend to `calibre-web-audit` that makes audit, discovery, and apply easier to review and operate.

The frontend should:
- make audit outputs easier to browse and approve
- make discovery outputs easier to browse and route to downstream systems
- keep apply safe and explicit
- preserve the existing command-line workflows as the source of truth
- start as a thin layer over the existing engine/runtime architecture rather than replacing core logic

## Non-goals for the first implementation
- no broad rewrite of audit/discovery/apply logic
- no behavioural drift in matching, ranking, blank-language routing, edition selection, or apply safety rules
- no multi-user collaboration features in the first version
- no direct editing of raw CSV artifacts as the primary persistence model
- no attempt to replace all CLI workflows at once

## Design principles
1. Keep generated outputs immutable per run.
2. Store operator review/approval state separately from generated outputs.
3. Generate derived apply/export plans from:
   - original run artifacts
   - review-state decisions
4. Preserve dry-run-first and explicit confirmation for mutations.
5. Build the UI as an operator layer over existing command/runtime/engine code.
6. Prefer a thin backend and progressive rollout.

## Recommended architecture

### Backend
Use Python and keep it in this repo.

Preferred stack:
- FastAPI backend
- Jinja2 templates + HTMX for the first version
- lightweight SQLite database for UI review state and run index

Why:
- fits the existing Python codebase
- easier to integrate with local command/runtime execution
- much lower complexity than introducing a separate frontend app immediately
- can later grow into a richer API if needed

### Frontend
Stage 1 should use:
- server-rendered pages
- HTMX for dynamic filtering/details/actions
- minimal JavaScript

Later, if needed:
- a richer React/Vue frontend can be layered on top of the same backend API

## Data model

### Generated run artifacts
These remain the source outputs from audit/discovery/apply runs.

Examples:
- `audit/actions_operator.csv`
- `audit/actions.csv`
- `audit/write_plan.csv`
- `audit/summary.md`
- `discovery/candidates.csv`
- `discovery/bookshelf_queue.csv`
- `discovery/bookshelf_push_log.csv`
- `apply/apply_log.csv`
- `apply/summary.md`

These files should be treated as immutable run outputs.

### UI review state
Store separately in a lightweight app database.

Suggested entities:
- `runs`
  - id
  - run_type (`audit`, `discovery`, `apply`)
  - created_at
  - output_dir
  - status
  - summary_path
- `audit_row_reviews`
  - id
  - run_id
  - source_row_key
  - decision (`approved`, `rejected`, `deferred`, `edited`)
  - edited_action
  - edited_selected_edition
  - include_title_author_write
  - include_file_write
  - notes
- `discovery_row_reviews`
  - id
  - run_id
  - source_row_key
  - decision (`approved`, `rejected`, `deferred`)
  - downstream_target (`bookshelf`, `shelfmark`, `none`)
  - notes

### Derived plans
The backend should be able to build:
- approved audit apply plan
- approved Bookshelf export/push plan
- approved Shelfmark export/push plan

These should be generated from original artifacts + review state, not by mutating original CSVs.

## User workflows

### Audit workflow
1. Run audit from UI or import an existing run directory.
2. Show summary.
3. Show operator-friendly table.
4. Allow filters by:
   - action
   - reason family
   - safe_to_apply
   - manual review family
   - metadata cleanup family
5. Show a detail panel for a row.
6. Allow approve/reject/defer/edit-safe-fields.
7. Generate a derived approved apply plan.
8. Allow dry-run apply.
9. Allow confirmed apply.

### Discovery workflow
1. Run discovery from UI or import an existing run directory.
2. Show shortlist/review/suppressed tabs.
3. Show detailed metadata panel.
4. Allow approve/reject/defer.
5. Route approved items to:
   - Bookshelf
   - Shelfmark
6. Export or push via explicit action.
7. Show push/export logs.

### Apply workflow
1. Show the approved derived plan.
2. Show exactly what would change.
3. Separate:
   - DB changes
   - sidecar OPF changes
   - EPUB metadata changes
4. Allow dry-run first.
5. Require explicit confirmation for real apply.
6. Show apply log and summary after execution.

## Safety model
- all mutation actions must support dry-run where practical
- original run outputs remain unchanged
- review state is separate
- derived plans are explicit artifacts
- real apply/push actions require clear confirmation
- logs must clearly distinguish:
  - preview
  - dry-run
  - real execution

## Backend responsibilities
The web app should:
- index runs
- load artifacts
- normalize CSV rows into typed view models
- persist review decisions
- generate derived plans
- invoke existing audit/discovery/apply logic or commands
- expose operator-friendly pages and lightweight JSON/HTMX endpoints

## Proposed module layout
Suggested additions under `hardcover_tools/`:

- `web/`
  - `app.py`
  - `routes_runs.py`
  - `routes_audit.py`
  - `routes_discovery.py`
  - `routes_apply.py`
  - `db.py`
  - `models.py`
  - `services/`
    - `run_index.py`
    - `artifact_loader.py`
    - `review_state.py`
    - `derived_plans.py`
    - `job_runner.py`
  - `templates/`
    - `base.html`
    - `runs.html`
    - `audit_run.html`
    - `audit_row_detail.html`
    - `discovery_run.html`
    - `apply_run.html`
  - `static/`

Exact names can change if better names emerge.

## Execution model
Stage 1 should avoid deep async complexity.

Preferred first approach:
- backend launches existing command functions or subprocess jobs
- store run metadata + output locations
- poll for job completion in a simple way
- keep it single-user/local-first

Later enhancements can add:
- background task queue
- websocket/live progress
- richer job control

## Stages

### Stage 1: backend skeleton + read-only audit browser
Implement:
- FastAPI app skeleton
- run index
- ability to register/import existing audit runs
- audit run page
- render `actions_operator.csv` in a filterable table
- row detail view
- basic summary rendering
- minimal templates and styling
- tests for artifact loading / run indexing / basic routes

Do not implement editing or apply actions yet.

### Stage 2: review-state persistence for audit
Implement:
- review-state DB
- approve/reject/defer/edit-safe-fields
- detail panel actions
- approved-row filtering
- derived approved apply plan generation

### Stage 3: apply UI
Implement:
- dry-run apply from derived plan
- confirmed apply
- apply result pages/logs
- file-write mode controls where already supported

### Stage 4: discovery UI
Implement:
- discovery browser
- shortlist/review/suppressed tabs
- detail panels
- review-state persistence
- downstream routing targets

### Stage 5: downstream integrations in UI
Implement:
- Bookshelf export/push from approved discovery rows
- Shelfmark export/push/search from approved discovery rows
- logs and summaries

### Stage 6: polish
Implement:
- better navigation
- saved filters
- richer summaries
- run comparison helpers
- optional API hardening for future richer frontend

## CLI relationship
The CLI commands remain first-class.
The frontend is an additional operator layer, not a replacement.

The app should reuse existing runtime/engine logic where practical.

## Documentation requirements
When Stage 1 lands, update:
- `README.md`
- `docs/operator_guide.md`

to explain:
- how to run the web app
- what Stage 1 supports
- what it does not yet support
- how it relates to CLI workflows

## Acceptance criteria for Stage 1
Stage 1 is successful if:
1. the app can start locally
2. existing audit runs can be loaded/imported
3. an audit run page clearly shows:
   - run metadata
   - summary
   - actions_operator table
   - row detail panel/page
4. no audit/discovery/apply decision logic changes
5. tests cover artifact loading and basic web routes
