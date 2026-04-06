# Rewrite plan

## Purpose of this file
This file is the task-specific companion to `AGENTS.md`.

- `AGENTS.md` contains the durable repo rules that should apply on every run.
- This file contains the detailed rewrite brief, stage plan, output expectations, and acceptance criteria for the full migration.

Use both together:
1. read `AGENTS.md`
2. read this file
3. inspect the working-directory source/fixture files
4. implement the requested stage only

---

## Objective
Redesign a messy but working monolithic Python script into a maintainable Python project.

This is **not** a light-touch refactor.
A substantial rewrite/restructure is allowed and preferred if that produces a cleaner result.

However, the new project must preserve the important current behaviour and outputs of the patched runtime script.

Optimise for **maintainability and testability over minimal diff**.

---

## Working-directory files
The working directory contains these files:

1. `hardcover_calibre_audit.txt`
   - this is actually the current patched runtime Python script, just stored as `.txt`
   - treat it as Python source
   - this is the **primary behavioural source of truth**

2. `hardcover-schema.txt`
   - Hardcover API schema / SQL / query-shape reference
   - use it to improve the new shared Hardcover client and data models
   - do not assume the current monolithic script fully exploits the schema

3. `actions.csv`
   - latest audit action output

4. `write_plan.csv`
   - latest audit write-plan output

5. `candidates.csv`
   - latest discovery output

6. `audit_summary.md`
   - latest audit summary

7. `discovery_summary.md`
   - latest discovery summary

### How to use these files
- Use the patched runtime script for **behaviour**, not structure.
- Use the CSVs and summaries as **parity fixtures**.
- Use the Hardcover schema to improve the new shared Hardcover client and data models.

---

## Primary source of truth
Use the patched runtime script as the **behavioural source of truth**.

You do **not** need to preserve its exact structure.

You **may**:
- rename files, functions, modules, and classes
- rewrite large sections from scratch
- substantially redesign the internal architecture

You must preserve the important current behaviour, outputs, identifier contract, and safety rules.

---

## Goal
Split the current monolithic script into a Python project with 3 main entrypoints:

1. **audit**
   - audits existing Calibre books against Hardcover
   - verifies or replaces the Hardcover work link
   - selects the preferred Hardcover edition
   - outputs:
     - `audit/actions.csv`
     - `audit/write_plan.csv`
     - `audit/summary.md`

2. **discovery**
   - discovers missing books in started series
   - discovers standalone / related books by authors already owned
   - selects preferred editions
   - classifies discovery rows into shortlist / review / suppressed buckets
   - outputs:
     - `discovery/candidates.csv`
     - `discovery/summary.md`

3. **apply**
   - reads the audit write plan
   - applies approved changes back to Calibre `metadata.db`
   - supports dry-run and selective application
   - outputs:
     - `apply/apply_log.csv`
     - `apply/summary.md`

---

## Design intent
The current script is too large and mixed-purpose.

The target is a small Python project with reusable shared modules.

Key design intent:
- discovery should not be tangled into audit internals
- apply should be a separate, conservative mutation step
- shared matching / normalization / edition-selection / client code should live in reusable core modules
- use the current script only as a behavioural reference, not as a structural template

---

## Target project structure
Create something broadly like:

```text
hardcover_tools/
  __init__.py
  cli/
    audit.py
    discovery.py
    apply.py
  core/
    models.py
    config.py
    logging_utils.py
    calibre_db.py
    hardcover_client.py
    cache.py
    ebook_meta.py
    text_normalization.py
    matching.py
    edition_selection.py
    audit_engine.py
    discovery_engine.py
    apply_engine.py
    output.py
  tests/
    test_identifier_names.py
    test_text_cleaning.py
    test_matching.py
    test_canonical_clean_replacement.py
    test_collectionish_penalties.py
    test_blank_language_buckets.py
    test_edition_selection.py
```

The exact module names may change if that produces a better design, but keep the architectural intent:
- thin CLI entrypoints
- reusable shared core modules
- testable business logic separated from CLI and I/O

---

## Critical compatibility constraints

### Identifier contract
Preserve compatibility with the current Calibre-Web-Automated identifier contract:

- `hardcover-id` = Hardcover work/book id
- `hardcover-slug` = Hardcover slug
- `hardcover-edition` = Hardcover edition id

When reading existing metadata, you may accept legacy aliases if useful.

However, the canonical internal and write-back names must be exactly:
- `hardcover-id`
- `hardcover-slug`
- `hardcover-edition`

Do **not** invent new canonical identifier names such as:
- `hardcover-edition-id`
- `hardcover_edition_id`

---

## Behaviour to preserve from the current script

### Audit / matching
Preserve or faithfully reimplement:
- verification of existing Hardcover links against actual ebook file metadata
- current retry / search-hint behaviour where useful
- strong title cleaning / normalization
- canonical title similarity
- canonical clean replacement relink logic
- the under-80 `canonical_clean_replacement` carve-out
- confidence boost for canonical clean replacement relinks
- conservative relink safety rules
- collectionish / omnibus / boxed-set demotion so those do not win incorrectly
- strong handling of marketing-fluff titles
- acceptance of partial-author cases where appropriate, without weakening safety generally

### Edition selection
Preserve:
- English ebook first preference
- if no suitable English ebook, then use an English reserve pool
- avoid audiobook preference by default
- preserve current blank-language handling logic as refined in the current script

### Discovery
Preserve:
- missing-series discovery
- owned-author discovery
- discovery prefilter / suppression logic
- blank-language sub-buckets, not one giant monolithic bucket
- current shortlist / review / suppressed style outputs

### Logging
Preserve:
- compact readable progress logging
- low-level Hardcover HTTP / CACHE HIT / CACHE STORE logs hidden by default
- low-level logs only shown by an explicit debug flag

### Caching
Preserve the newer hybrid per-book cache approach where it exists.
Do **not** regress back to purely batch-exact cache behaviour for core fetch paths.

---

## Output expectations
The rewritten project should still generate outputs similar in purpose to the current ones.

### Audit outputs
- `actions.csv`
- `write_plan.csv`
- `summary.md`

### Discovery outputs
- `candidates.csv`
- `summary.md`

### Apply outputs
- `apply_log.csv`
- `summary.md`

The exact column order can improve if helpful, but do **not** remove important information.

---

## Blank-language discovery buckets
Preserve the newer split so the old giant “blank language but plausibly English” bucket stays broken into meaningful sub-buckets, including equivalents of:

- promoted shortlist likely-English blank-language
- core series review
- side material low priority
- cold single-edition stub low priority
- generic blank-language review
- metadata-junk suppressed
- weak-English-signal suppressed

---

## Apply CLI requirements
Implement a separate apply CLI that mutates Calibre `metadata.db` from `audit/write_plan.csv`.

### Minimum required flags
- `--dry-run`
- `--apply-safe-only`
- `--apply-actions replace_hardcover_id,safe_auto_fix,update_calibre_metadata`
- `--include-calibre-title-author`
- `--include-identifiers-only`
- `--limit N`

### Apply rules
- by default, do not apply manual-review rows
- support dry-run parity with detailed logs
- support selective application by action type
- update identifiers cleanly without duplicates
- update title / author only when requested
- use transactions safely
- rollback on failure
- produce an apply log with one row per attempted operation

---

## Implementation philosophy
Prefer a clean redesign over preserving messy internal structure.

But do **not** casually change matching thresholds or safety rules.
Be conservative when behaviour is ambiguous.

Where possible:
- move pure logic into testable functions
- isolate Calibre DB writes
- isolate Hardcover API calls
- isolate CSV/summary rendering
- avoid circular imports
- avoid giant god modules

---

## Development plan

### Stage 1
- create the project skeleton
- move shared dataclasses / helpers / normalization / matching into core modules
- get the audit CLI working first
- reproduce audit outputs with near parity

### Stage 2
- move discovery logic into a separate discovery engine
- get the discovery CLI working with near parity
- preserve the blank-language split

### Stage 3
- implement the apply CLI
- support dry-run and safe-only operation
- correctly update `metadata.db` from `write_plan.csv`

### Stage 4
- add tests
- add README usage docs
- expose runnable entrypoints

---

## CLI expectations
Support:
- `python -m hardcover_tools.cli.audit ...`
- `python -m hardcover_tools.cli.discovery ...`
- `python -m hardcover_tools.cli.apply ...`

If useful, also add console scripts in `pyproject.toml`:
- `hardcover-audit`
- `hardcover-discovery`
- `hardcover-apply`

---

## Suggested data flow
- `audit` is the authoritative tool for books already in Calibre
- `discovery` is a separate recommendation pipeline
- `apply` consumes `audit/write_plan.csv` only
- `discovery` should not re-run the full audit engine internally unless absolutely required
- if useful, `discovery` may optionally consume audit outputs as inputs

---

## Use of Hardcover schema
Use the attached schema/reference to improve the new design where appropriate, especially:
- strongly typed / structured query helpers
- shared response parsing
- cleaner book / edition / series models
- explicit selection of the fields needed for audit vs discovery vs apply
- avoiding duplicated ad hoc query shapes across the codebase

Do **not** introduce speculative behaviour that is not grounded in the current script or schema.

---

## Testing requirements
Add tests for the most fragile logic:
- marketing fluff is stripped from titles correctly
- canonical title similarity behaves as expected
- canonical clean replacement works, including under-80 carve-out
- collectionish / boxed-set / omnibus candidates are demoted correctly
- blank-language rows route to the right sub-buckets
- English ebook first logic works
- identifier normalization preserves canonical names:
  - `hardcover-id`
  - `hardcover-slug`
  - `hardcover-edition`

---

## README requirements
Write a README that explains:
- project purpose
- the three entrypoints
- required inputs
- expected outputs
- safe apply workflow
- example commands
- identifier conventions
- caveats / TODOs

---

## Acceptance criteria
The rewrite is successful if:

1. `audit` runs and produces the same class of outputs as the current script
2. `discovery` runs separately and preserves the important discovery logic
3. `apply` can safely mutate `metadata.db` from `write_plan.csv`
4. the code is materially cleaner and more modular than the current script
5. the identifier contract remains:
   - `hardcover-id`
   - `hardcover-slug`
   - `hardcover-edition`

---

## What to return at the end
Return:
1. the new project structure
2. a concise summary of major design choices
3. any intentional behavioural differences from the current script
4. any remaining TODOs
5. example commands for audit, discovery, and apply

---

## Stage-by-stage use with Codex

### Stage 1 prompt
Read `AGENTS.md` and `docs/rewrite_plan.md`. Inspect the working-directory files. Start with **Stage 1** only. Before coding, briefly summarize the behavioural contract you infer from the current script and fixtures. Then implement Stage 1.

### Stage 2 prompt
Read `AGENTS.md` and `docs/rewrite_plan.md`. Inspect the current project state and the working-directory fixtures. Continue with **Stage 2** only. Preserve the behavioural contract, identifier contract, output meaning, and blank-language split.

### Stage 3 prompt
Read `AGENTS.md` and `docs/rewrite_plan.md`. Inspect the current project state and the working-directory fixtures. Continue with **Stage 3** only. Implement the apply path conservatively with dry-run, selective application, transactional safety, and rollback on failure.

### Stage 4 prompt
Read `AGENTS.md` and `docs/rewrite_plan.md`. Inspect the current project state and the working-directory fixtures. Continue with **Stage 4** only. Add the fragile-logic tests, README, and runnable entrypoints without weakening earlier behaviour or contracts.
