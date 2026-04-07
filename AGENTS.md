# AGENTS.md

## Project purpose
This repository is the owned project in this workspace.

Its purpose is to:
- audit an existing Calibre library against Hardcover
- discover missing books / related books
- apply approved metadata changes back to Calibre
- provide safe, operator-friendly outputs and integration paths to adjacent systems

This is a high-agency engineering repo.
Optimise for:
1. correctness
2. maintainability
3. testability
4. operator usefulness
5. safe mutation paths

Do not optimise primarily for minimal diff unless explicitly asked.

## Scope and ownership
You may inspect other repos and systems as needed for integration work, but you should only modify this repository by default.

Do not edit non-owned repos unless the user explicitly approves that.

When comparing this repo against another repo/system/API:
- inspect the external side as needed
- identify the best worthwhile change(s) that can be made inside this repo
- implement them directly unless the user explicitly asked for recommendations only

## Execution bias
Default to implementing improvements, not merely describing them.

If there is a clear, practical improvement that can be made safely inside this repo:
- make the change
- add tests where appropriate
- update docs when needed
- return the result

Do not stop at a recommendation list if a worthwhile implementation can be completed in this repo.

Recommendation-only mode should be used only when:
- the user explicitly asks for review/recommendations only
- the required change would need edits in a non-owned repo
- the external contract is too unclear to implement safely without confirmation

## Refactor policy
Large changes, structural refactors, module replacement, code deletion, and code rewrites are allowed and encouraged when they are the best path forward.

Do not preserve awkward or legacy structure merely because it already exists.

Prefer:
- replacing poor code with better code
- extracting coherent modules
- deleting obsolete code
- consolidating duplicated logic
- redesigning internal boundaries when it materially improves the repo
- rewriting messy implementations when that is the clearest route

Do not:
- make speculative rewrites with no clear benefit
- change important behaviour silently
- replace code without preserving important contracts, outputs, and safety rules unless the task explicitly allows that

## Durable repo contracts

### Canonical identifier contract
Preserve these canonical names exactly:
- `hardcover-id`
- `hardcover-slug`
- `hardcover-edition`

You may accept legacy aliases when reading data if useful.
But internal canonical naming and write-back naming must remain exactly the above.

Do not invent new canonical names such as:
- `hardcover-edition-id`
- `hardcover_edition_id`
- `hardcover-work-id`

### Core behaviour that must not drift without explicit intent
Preserve unless the task explicitly calls for behavioural change:

#### Audit / matching
- strong title cleaning / normalization
- canonical title similarity
- canonical clean replacement relink logic
- under-80 canonical_clean_replacement carve-out
- confidence boost for canonical clean replacement relinks
- conservative relink safety rules
- collectionish / omnibus / boxed-set demotion
- strong handling of marketing-fluff titles
- acceptance of partial-author cases where appropriate without generally weakening safety

#### Edition selection
- English ebook first preference
- English reserve pool if no suitable English ebook exists
- avoid audiobook preference by default
- preserve blank-language handling logic

#### Discovery
- missing-series discovery
- owned-author discovery
- discovery prefilter / suppression logic
- blank-language sub-buckets, not one giant bucket
- shortlist / review / suppressed style outputs

#### Apply / mutation
- conservative defaults
- DB-first safety model unless explicitly extended
- dry-run support
- selective application by action type
- no casual weakening of guardrails

## Working style
Prefer:
- decisive implementation
- clear module boundaries
- explicit, readable names
- reusable core modules
- thin CLI layers
- testable business logic
- additive, opt-in integration features
- concise operator-facing outputs and logs
- strong verification before declaring work done

Avoid:
- analysis-only responses when implementation is possible
- giant god modules
- hidden coupling
- unnecessary framework creep
- mixing I/O, decision logic, formatting, and persistence in one place
- silent output-contract drift
- changing thresholds/rules during output-only or integration-only tasks unless necessary

## Cross-repo and external integration work
This repo may integrate with systems such as:
- Bookshelf
- Calibre-Web-Automated
- shelfmark
- Calibre metadata.db and library files
- Hardcover-backed metadata services

When implementing integrations:
1. inspect the external system first
2. treat the external system as an interface/contract
3. implement the integration on this repo’s side only unless explicitly approved otherwise
4. prefer opt-in flags and dry-run support
5. log assumptions and per-row/per-item outcomes clearly
6. avoid assuming undocumented APIs or behaviour when code/docs can be inspected

## Output and operator UX bias
This repo should be pleasant for a real operator to use.

Prefer:
- compact, readable default console output
- more detailed output behind explicit verbose/debug modes
- summaries that tell the operator what to do next
- derived operator-friendly files when full forensic outputs are too dense
- explicit distinctions between forensic outputs, operator review outputs, and apply outputs

Do not add noise unless it meaningfully helps the operator.

## Mutation and file-writing safety
For anything that mutates DBs, OPFs, EPUBs, sidecars, or external systems:
- keep dry-run available where practical
- keep destination explicit
- log intended and actual mutations clearly
- avoid silent destructive actions
- document rollback limitations honestly
- preserve conservative defaults
- prefer additive/opt-in write modes
- do not assume atomicity across DB and file writes unless it actually exists

## Testing expectations
Add or update tests whenever the change has meaningful risk.

Prioritise tests for:
- matching/ranking rules
- normalization and cleanup
- blank-language routing
- edition selection
- identifier handling
- apply/mutation behaviour
- export/integration routing
- operator-facing output derivation when it has logic
- packaging/CLI behaviour when touched

Prefer deterministic fixture-based tests.
Do not rely on live external systems unless the task explicitly calls for that.

## Documentation expectations
Update docs when user-facing behaviour, flags, outputs, workflows, or integrations change.

At minimum consider:
- `README.md`
- `docs/operator_guide.md`

Docs should explain:
- what changed
- how to use it
- safe/default workflow
- limitations / caveats
- example commands where useful

## CLI expectations
CLI entrypoints should remain thin wrappers over reusable core logic.

Prefer:
- explicit flags
- conservative defaults
- dry-run support
- clear help text
- output paths and next steps shown clearly at the end

## Packaging and install expectations
When touching packaging/install flows:
- keep module entrypoints working
- keep console scripts working if present
- document editable and normal install flows if changed
- add packaging smoke tests where practical and deterministic

## What to do when comparing against another repo
When asked to compare this repo against another repo and improve things:
- inspect both repos
- identify the highest-value improvement(s) that can be implemented entirely here
- implement them
- only fall back to recommendations if implementation is blocked or would require editing the non-owned repo

When multiple worthwhile improvements exist:
- prefer one or two high-value changes over returning only a long list
- large refactors are acceptable if they are clearly the best route

## What to avoid
- do not change canonical identifier names
- do not silently change matching thresholds or safety rules
- do not weaken default mutation safety
- do not edit non-owned repos without explicit approval
- do not stop at recommendations when a safe, useful change can be made here
- do not preserve bad code just because it already works

## Done means
A task is done when:
- the requested or implied improvement is implemented
- relevant tests pass, or any remaining gaps are stated clearly
- docs are updated if needed
- behaviour-sensitive changes are verified against existing tests, fixtures, or outputs
- any intentional behavioural differences are called out explicitly
- exact commands to run or verify are provided

## Return format
At the end of substantial work, return:
1. what was changed
2. why it was changed
3. any intentional behavioural differences
4. remaining TODOs or limitations
5. exact verification commands
6. example usage commands when relevant
