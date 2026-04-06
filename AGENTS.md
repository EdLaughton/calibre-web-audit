# AGENTS.md

## Project purpose
This repository is a clean rewrite of a messy but working monolithic Python script into a maintainable Python project.

This is not a light-touch refactor.
A substantial rewrite/restructure is allowed and preferred if it produces a cleaner result.

Optimise for maintainability and testability over minimal diff.

## Durable repo rules
Preserve the important current behaviour and outputs of the patched runtime script, but do not preserve its structure.
Treat the current script as the behavioural source of truth, not a structural template.

The project should converge on 3 entrypoints:
- audit
- discovery
- apply

Keep the architecture aligned to:
- thin CLI entrypoints
- reusable shared core modules
- testable business logic separated from CLI and I/O

## Identifier contract
Preserve compatibility with the Calibre-Web-Automated identifier contract.

Canonical names are:
- hardcover-id
- hardcover-slug
- hardcover-edition

You may accept legacy aliases when reading existing metadata if useful.
But internal canonical naming and all write-back must use exactly:
- hardcover-id
- hardcover-slug
- hardcover-edition

## Behaviour that must not drift

### Audit / matching
Preserve or faithfully reimplement:
- verification of existing Hardcover links against actual ebook file metadata
- strong title cleaning / normalization
- canonical title similarity
- canonical clean replacement relink logic
- the under-80 canonical_clean_replacement carve-out
- confidence boost for canonical clean replacement relinks
- conservative relink safety rules
- collectionish / omnibus / boxed-set demotion
- strong handling of marketing-fluff titles
- acceptance of partial-author cases where appropriate without weakening safety generally

### Edition selection
Preserve:
- English ebook first preference
- if no suitable English ebook, use an English reserve pool
- avoid audiobook preference by default
- blank-language handling logic

### Discovery
Preserve:
- missing-series discovery
- owned-author discovery
- discovery prefilter / suppression logic
- blank-language sub-buckets, not one giant bucket
- shortlist / review / suppressed style outputs

### Logging
Preserve:
- compact readable progress logging
- low-level Hardcover HTTP / CACHE HIT / CACHE STORE logs hidden by default
- low-level logs only under explicit debug mode

### Caching
Do not regress the newer hybrid per-book cache approach into purely batch-exact cache behaviour for core fetch paths.

## Apply rules
Apply is a separate conservative mutation step.
It consumes audit/write_plan.csv and mutates metadata.db safely.

By default, do not apply manual-review rows.
Update title/author only when explicitly requested.
Update identifiers cleanly without duplicates.

## Testing priorities
Add tests for the most fragile logic:
- marketing fluff stripping
- canonical title similarity
- canonical clean replacement, including the under-80 carve-out
- collectionish / boxed-set / omnibus demotion
- blank-language routing
- English ebook first logic
- identifier normalization preserving:
  - hardcover-id
  - hardcover-slug
  - hardcover-edition

## Working style in this repo
Before editing, understand the current behaviour and identify non-negotiable rules.
Preserve behaviour, not legacy structure.

Prefer:
- explicit names
- deterministic outputs
- safe defaults
- clean module boundaries
- concise summaries of changes

Avoid:
- silent threshold changes
- monolithic rewrites with no parity checks
- changing output meaning without documenting it
- DB mutations without clear dry-run support

## Done means
A task is done when:
- the requested code is implemented
- relevant tests pass or the remaining gaps are stated clearly
- behaviour-sensitive changes are verified against available fixtures or prior outputs
- any intentional behavioural differences are called out explicitly
- exact commands to run are provided at the end
