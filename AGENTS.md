# AGENTS.md — coHound

Instructions for agents and developers working in this repository.

## 1. What this is

**coHound** builds a **comparative technical capability map** of software products from
publicly provable functionality. Not "the truth about a product": a reproducible map of what
is provable under a fixed protocol.

- **Source of truth: `cohound-spec-v2.md`** (methodology regulation + technical design,
  v2.1). Read the relevant section before any substantive edit.
- **Document index: `docs/architecture/README.md`** — what each architecture document is,
  whether what it describes works, whether it is worth keeping. Module-level contracts live
  in `01-modules-and-contracts.md` and `02-subsystems.md`; this file does **not** re-enumerate
  modules, because such lists rot faster than the code they describe (and nothing gates them).
- "Passport" here always means a **product passport** (`passports/{product_id}/`), never the
  regulation itself.

### 1.1 Target scale — and why a pilot corpus is not a constraint

The only profile with a real corpus today is `ru-pg-dbms` (`packages/ru-pg-dbms/`), and it is
a **pilot**. Until the owner says otherwise, no design decision may take its corpus as an
absolute input or as a bound: numbers measured on it are prices, not ceilings.

Target shape (owner, 03.11.2026):

| | target |
|---|---|
| refresh | **weekly**, per profile |
| profiles | **≥ 100**, each with its own corpus |
| cohort | **10–15 products** per profile, compared over **100–200 parameters** |
| hosts per product | **3–5** |
| content languages | **English, Russian, Chinese** |
| markdown dialects | **CommonMark, GFM, Pandoc Markdown** — all three, professionally |

Two consequences that are easy to forget: a rule that works for one product on one host is
not yet a rule (per-product, per-host and per-language behaviour is the default assumption),
and "no such construct in the corpus" is a statement about the pilot slice, not about the
target — which is exactly why a construct is declared out of scope only after a census with a
named instrument (`scripts/measure_md_chunking.py --census`; границы грамматик —
`18-markdown-chunker-plan.md`), never because one regex found nothing.

**A sample is a measurement; "the full corpus" is not the default.** The owner's rule
(16.11.2026): a measurement does not have to run over the whole corpus, or a large part of it,
to be evidence — a **statistically significant sample** is enough, and a run's wall-clock
duration is illustrative of *this host on this corpus*, not a property of the product or of a
future profile. What replaces the full-corpus reflex is not a smaller number but a **stated
uncertainty**: publish the interval (bootstrap CI) and the **paired** comparison (per-cell
discordance, exact McNemar), because a difference of two aggregates on a slice mixes the effect
of the axis with the composition of the sample. Measured 16.11.2026: the O19 instrument
published `hit_ci` while calling `bootstrap=1` — the interval was the point estimate, so the
field carried no information and "run everything" looked like the only way to be sure.

Consequence for review: "we did not measure the whole corpus" is not by itself a defect;
"we published a point without an interval, or compared two aggregates as if they were paired"
is. The reverse also holds: a sample does not excuse a claim about the **population** — the
slice bounds the claim, and a construct's absence in a slice is still a statement about that
slice (§1.1 above).

**Speed has measurement conditions too, and they are declared, not inferred.** On this host
(and on any laptop) the numbers move with the **power supply**: measured 16.11.2026 on the
reranker grid, `bge L=512 B=8` went 23.92 → 34.02 pairs/s (+42 %) after switching from USB-C to
the supplied AC adapter, while three repeats in one mode varied by 2.16 %. The mode cannot be
detected in software (`power.limit` = `[N/A]`, `cpufreq` not exposed in WSL2, `BAT1 power_now`
= 0), so instruments take it as an **operator-declared** argument (`--power-mode`, default
`unknown` with a visible warning) and publish the observed counters (`ac_online`,
`battery_status`, SM clock, watts, °C, `SW Power Capping`) **beside** it as control, never as
the source. Two things not to do: do not present a difference measured across two modes as the
price of one condition (the split "power vs host contention" is **not** measured — publish
`+14…+42 %, split not performed`), and do not read the **idle** clock as a mode indicator
(1057 MHz is idle; under load on AC the card runs 1747–1777 MHz).

**Refactoring is open.** Until the owner revisits it, architecture may be reworked wherever
that is reasonable, and backward compatibility is not required: the DB schema gate rejects
foreign versions visibly (`SchemaVersionMismatch`), packages/formats may be re-shaped. What is
**not** negotiable is the methodology (axioms §2), `run_id = f(declared inputs)`, I1–I23, and
category neutrality of `core/`.

### 1.2 Documentation lags the code — verify before citing

Documentation here is written as a record; the code, the host and the corpora keep moving. A
stale sentence is therefore the **normal** state of an old document, not an exception: a claim
read in `docs/` is a **hypothesis**, not a fact.

- **Numbers age fastest.** Row counts, document/chunk counts, timings, memory/VRAM peaks and
  verdicts like "unusable"/"rejected" describe **one measurement, on one date, on one host,
  against one corpus**. Before reusing one: re-measure it, or point to the published
  measurement file (`docs/reports/measurements/`), or quote it **with its date**. A number
  without a date is not evidence.
- **The artifact outranks the document.** Measured 06.11.2026: three documents still called
  `packages/ru-pg-dbms/golden.csv` "236 rows, unusable" long after the owner had rebuilt it to
  905 rows = 181 params × 5 products (`25cf2ab`) — the file was fine, the documents were not.
  Read the file, the DB row, or `git log` before repeating what a document says about it.
- **Code drift is silent.** Anchors in `docs/`, `README.md`, `AGENTS.md` are gated
  (`scripts/check_doc_anchors.py`), but the gate checks that `path:line` **resolves**, not that
  the sentence still means what the code does. Read the cited lines.
- **"Not done" / "rejected" / "open" carry a date and a reason.** A rejection is revisited when
  its condition changes (A3), not cited forever; an "open" item may have been closed since.
- **Correct a stale statement in the same commit** in which you find it. The next reader
  inherits what you leave behind — propagating it costs more than fixing it.

Owner note (06.11.2026): the project moves fast, so reusing old estimates and conclusions —
about the environment (keys, devices, caches, corpora) as well as about the code (throughput,
memory, document counts) — is where trust is lost: usually not by being wrong, but by being
outdated.

## 2. Axioms (spec §0) — do not violate

| | Axiom |
|---|---|
| **A1** | Prove value with minimal means first; optimise afterwards. |
| **A2** | The moat is the **measurement methodology**, not the AI pipeline. If an implementation requires changing the methodology, the implementation is wrong. |
| **A3** | Epistemology is uniform: defaults are assumptions/candidates; a default is replaced by **measurement**, never by foresight. |

Owner rule that follows from A3: **do not cap — measure.** A ceiling invented for safety is
an admission that cost was not measured. Prefer "measure the cost and publish the number"
over "refuse above N". Code that silently degrades instead of reporting is the worst option.

## 3. Repository map

| Path | What lives there |
|---|---|
| `cohound-spec-v2.md` | Regulation v2.1: semantics, invariants I1–I23, architecture. |
| `docs/status-dictionary.yaml` | Statuses and every enum value. Single source for all `StrEnum`s; schemas mirror it. |
| `docs/architecture/` | As-built design, plans, ADR registry, document index (`README.md`). |
| `docs/templates/registries/` | Registry specification templates. |
| `docs/user-guide/` | The guide a user **acts on**: `runbook.md` (environment → config → run → diagnosis → experiments), `package-profile.md` (what a category package is), `package-files.md` (file/field reference). Gated by `scripts/check_runbook.py`: commands resolve against the live argparse tree, flags against CLI + instrument `add_argument`s, `COHOUND_*` against `.env.example`/`core/`, repo paths and internal links against the tree. Prose commands are a warning, not a failure — "run this" and "this command was removed" are indistinguishable by syntax. |
| `core/` | The engine. **Category-neutral** (§4, rule 1). |
| `core/domain/` | Enums, models, invariants, scoring, R-2, anchors, dictionary. Pure Python, no I/O. |
| `core/ports/` | `Protocol` contracts only (LLM, crawler, extractor, chunker, store, gateway, state, …). |
| `core/pipeline/` | Scenarios Ш1–Ш9 (numbered as in the spec): orchestrator + 15 phases, gates, crawl, corpus, retrieve/evaluate, review/compute/report, incremental, experiments, package authoring. |
| `core/store/` | SQLite DDL + triggers, `StorePort` implementations, row converters, run backup. |
| `core/adapters/` | Concrete engines behind ports: extractors (html/text/epub/docling/pdfium), container readers, lexical+vector indexes, embedders, LLMs, search, crawlers, remote-* (seam). |
| `core/util/` | Text, security/SSRF, sanitize, retrieval eval, agreement (κ), feed, format probing, container policy, memory, robots/sitemap/rate limit, cache, ids/hashing/logging, `embed_input` (состав входа эмбеддера — объявление, а не аргумент, O19). |
| `core/config/` | `run.yaml`/package loading, `deploy.yaml` (zone profile), path layout, domain profiles, crawl sources, passports. |
| `core/cli/` | Composition root Z0: thin entry, parser, DI context, errors, output, adapter wiring, `doctor`, `commands/*`, `package/*`. |
| `core/edge/` | Composition root Z1/DMZ: seam server, auth, audit, seam schema, crawl scheduler, poller, crawler factory, runtime config, launcher. |
| `scripts/ci.sh` | Single CI entry point (spec §11.6). `scripts/check_doc_anchors.py` gates `path:line` anchors in `docs/`, `README.md`, `AGENTS.md`; `--check-drift BASE` reports anchors whose target line **moved** since `BASE` (a non-empty line at the old address proves nothing — measured 16.11.2026: 55 anchors drifted silently past the non-emptiness check). `scripts/check_runbook.py` gates the user-facing guide the same way anchors gate prose: a command the reader copies must exist. |
| `tests/selfcheck.py` | Autonomous foundation check (no pytest): dictionary, DDL, invariants, import graph §1, neutrality §1.3. |
| `packages/{case_id}/` | Category package (data, not code): `package.yaml`, `catalog.csv`(+`.lock`), `baselines.csv`, `cohort.yaml`, `golden.csv`, `derived_fields.csv`, `domain_profiles.yaml`, `scope_attestation.yaml`, `run.yaml`; `sources/` — authored inputs (seeds, corpus URLs, render modes, `manual_artifacts.yaml` — declared bytes the crawler did not take, written by `hound artifacts fetch`; its `manual_artifacts_digest` is a contract field, so adding a file is a new `run_id`; `reference_products.yaml` — which upstream lines the profile measures and at which versions; `baseline_facts.csv` — observations about the ancestor's documentation, each with a verbatim quote, text hash and date); optional `crawl_sources.yaml` (feed/channel routing) and `registries/` (package-scoped registry specs — no package has them yet, `_template` included). |
| `passports/{product_id}/` | Product passports. |
| `contracts/seam/` | Published JSON schemas of the DMZ seam (`/v1/*`). |
| `config/deploy.yaml` · `config/paths.example.yaml` | Zone profile (edge) · example directory layout (`COHOUND_HOME`/`COHOUND_*_DIR`). |
| `docs/reports/` | Published evidence: measurement reports (`measurements/`, indexed by its `README.md` — a published number lives there with its date and instrument) and incident traces. Per §1.2: the artifact outranks the document, and a number cited in `docs/architecture/` **without** an entry here is a hypothesis, not a proof. Re-measured 2026-11-16 with `rg -n 'замер\w*\s+(от\s+)?[0-9]{2}\.[0-9]{2}' docs/architecture/*.md \| grep -v reports/ \| wc -l` → **40** lines (the 15.11 count was 34): reading one of them means re-measuring or treating it as dated hearsay. |
| `scripts/measure_*.py`, `scripts/conformance_*.py`, `scripts/check_pdf_page_map.py` | **Named instruments** of measurements and censuses (A3): a claim is out of scope only after a census with one of these, and a re-measurement cites the script + date. `check_pdf_page_map.py` — инвариант карты страниц PDF (`page_map`: покрытие, разрывы, обратная сборка) на реальном корпусе, а не на фикстурах; exit 1 при дефекте. |
| `scripts/prepare_*.py` | **Артефакты для человека**, а не замеры: `prepare_retrieval_golden_docs.py` (документный эталон O14), `prepare_answer_sets.py` (набор отвечающих документов O42), `prepare_evidence_claims.py` (доказательная сила цитаты O18), `prepare_importance_pairs.py` (парные сравнения O20), `prepare_extraction_golden.py` (PDF-эталон извлечения O38б/D10 — 30–50 документов × пять направлений, двухколоночность определяет **прибор** `measure_reading_order.py`), `prepare_parameter_semantics.py` (семантика и правило зачёта каждого параметра каталога: `semantics`/`rule`/`evidence`/`absence_rule`/`concept_terms`; XLSX с подсветкой строк, в семантике которых автор не уверен, `apply` собирает `catalog.csv` только по заполненному `review`). Каждый пишет XLSX-лист на разметку и собирает результат обратно (`compile`/`validate`), отказывая на неразмеченном или чужом значении. Разметку не выдумывают: `candidate` в имени файла и `status: candidate` в наборе — это состояние «человек ещё не подтвердил». **Исключение по входу, а не по роли:** `prepare_retrieval_golden_chunks.py` разметки не требует — он выводит чанковый адрес из **уже размеченного** документного эталона и `cell_evidence` (`citation_quote`), поэтому пересобирается скриптом при каждой пере-нарезке, а `validate` отказывает на нерезолвимом `chunk_id`. |
| `.env.example` | Environment variables and external keys (LLM, scout, search adapters), no secrets. |

### 3.1 Artifacts are data, not repository files

`runs/`, `index_store/`, `corpus_raw/`, `cache/`, `extract_cache/`, `eval_cache/`,
`embed_cache/`, `state/` are **data** (03 §A.1.2). They appear in the tree only when no
layout is configured; on a configured host they live outside the repo under `COHOUND_HOME`.
Connection: `COHOUND_HOME` + `COHOUND_PATHS_FILE` in `./.env`; `paths.yaml` itself stays
outside git (paths are host-specific).

**Trap:** `COHOUND_HOME` shifts **all 12** `HARNESS_FIELDS`, including `packages` and
`passports` — those two must be **absolute** in `paths.yaml`, otherwise the CLI will not find
packages. Verify with `./hound.py doctor`, group `layout` (it prints the effective path **and
its source**). Tool artifacts (mypy/ruff/coverage/hypothesis/pycache/pytest) are addressed by
env vars in `~/.cohound/env.sh`. Tests do not depend on host state (`COHOUND_NO_DOTENV=1` in
`tests/conftest.py`); live-run checks read `COHOUND_LIVE_RUNS_DIR` and skip without it.

**Trap (re-measured 16.11.2026, and the earlier wording here was wrong).** Sourcing `.env`
(`set -a; . ./.env`) does **not** by itself change `run_id` — measured on `ru-pg-dbms`:
`plan` gives `ru-pg-dbms-3b82d5c0a132` with and without it, because `COHOUND_LLM_SLOT` is
empty and `COHOUND_SERP` is absent. What it does change is **which tree is measured**: the
file pins `COHOUND_HOME` + `COHOUND_PATHS_FILE`, and the `paths.yaml` it names carries
**absolute** `packages`/`passports` — the main checkout's. A run started from a worktree then
silently measures the main repo's package while the operator believes it measures the
worktree's. Read `doctor` group `layout` (`источник: file|home|env|cli`) before trusting a
number, and pass `PYTHONPATH` to the worktree instead of sourcing `.env`. The same shape
applies to the stand: pointing at it needs **both** `COHOUND_HOME` and `COHOUND_PATHS_FILE`,
otherwise one of them keeps the canonical layout.

### 3.2 Trusted zones

Web-facing work (crawl, search, registries, extraction of untrusted content) belongs in the
**DMZ** (`core/edge/`); non-web work stays in the core with no web egress; the only link is an
application-layer firewall over `/v1/*`. Profile `split` is an MVP option (default is `local`
single process). This strengthens I18 (the evaluator has no route to the internet) and makes
the I19 boundary explicit. Seam contracts are designed to be transparent to corporate
IDP/DLP/IDS (L7 JSON, published schemas, no pinning, OIDC/mTLS); attesting against specific
products is out of scope. `split-hardened` is accepted for config compatibility but loudly
warns that it adds nothing over `split` (alias, not another profile). `extractor_zone` is
declared but **not wired**: the extractor always runs in the core (D-9). See
`docs/architecture/07-deployment-topology.md`.

## 4. Hard rules

1. **Category neutrality of `core/`.** No product, vendor or domain terms; all specifics are
   data in `packages/`. Gated on three layers — `core/domain`, `core/ports`,
   `core/pipeline` (`tests/selfcheck.py::_neutrality`, same set in CI). The remaining layers
   (`adapters/`, `store/`, `util/`, `config/`, `cli/`, `edge/`) are held by the **rule**, not
   by a gate: a wide grep over `core/` is a hint, and a hit there is read by a human.
   Narrowing the gate does not weaken the rule — product knowledge in any layer is a defect.
2. **Invariants are code, not comments.** I1–I23 are enforced by Pydantic validators
   (`core/domain/`) and SQL triggers (`core/store/sqlite.py`). Every new domain entity ships
   with its invariant checks and a `selfcheck` entry.
3. **`failed` is never a status** (I4: `status = null`). **N/A only by a human** (I6/I7).
   **`❓` is never read as `❌`** (I12). Never auto-derive N/A.
4. **Evidence chain.** External content becomes evidence only through
   `corpus_raw → extract → chunk → content_hash → index` (I19). Retrieval is product-scoped
   (I17). The evaluator never calls external search (I18). Direct evidence requires a
   non-empty `citation_quote` (I22; `source_kind=manual` is the only exception).
5. **Crawl scope = declared hosts only** (§6).
6. **Index builds are immutable.** A new round/SERP produces a **new** build; in-place edits
   are forbidden.
7. **The evaluator model is a declared input**, never environment (§8).
8. **Declared axes are declared.** No axis value `auto`; no deriving a measurement condition
   from the host (§9).
9. **Comments and docstrings in Russian** (as in the regulation); identifiers in English.
10. **When moving code, re-point tests by address** (imports, `monkeypatch` targets,
    construction kwargs). Never delete an assertion, change an expected value, add a `skip`
    or widen an `except` to make a move pass. If a test only checks the thing being removed,
    replace it with an equivalent check on the new carrier.

### 4.1 Layering

`domain` (pure) ← `ports` (protocols) ← `pipeline` (scenarios) ← composition roots
(`cli/`, `edge/`). Adapters depend on ports, never the reverse; the import graph is gated by
`tests/selfcheck.py::_import_graph` (allow-list + relative-import resolution), so
`import-linter` is redundant.

## 5. Invariants that are easy to break

| | Rule |
|---|---|
| **I1** | evaluated ✅/⚠️ → at least one evidence item. |
| **I2/I2b** | evaluated ❌ → absence-evidence. `parameter_reference`/`distribution_absence` are provable only as `bounded`, which requires `scope_acceptance` (a human accepting the risk, `scope_attestation.yaml` per product) **and** a declared package procedure (`check_absence_attestation`); without either the cell is `❓` (`not_stated`), not `❌`. `registry_absence` derives its class from the declared rule (`EvidenceScope.for_registry_absence` → `denial_strength=authoritative`, `enumerated=True`) and needs no attestation — a registry is an authoritative enumeration. `explicit` needs no scope (but see I2c). The pre-`9ec346e` scalar `source_scope: exhaustive` and the checker `_has_exhaustive_scope` no longer exist (legacy rows are still readable). |
| **I2d** | `❌` → **датированный носитель**, и дата публикуется: `explicit`/`authoritative` — запись наблюдения **каждого** цитируемого документа (`corpus_artifact.observed_at`; `adopted` допускается, дата не обязана быть в этом цикле); `bounded` — снятие **каждого** объявленного корня `complete=1` с датой и совпадающим `policy_digest` (отпечаток **пространства**: объявленные режимы + `crawl_config_digest`, без решения по часам; `adopted` исчерпание **не** удостоверяет); `registry_absence` — как I2b. `absence_as_of = min(дат носителей)` → `cells.absence_as_of` + `cell_freshness.csv`. Носителя нет → `❓` + токен `absence_undated` (`evidence_scope.reason` и `validation_notes`), протокол отсутствия не публикуется. Перенесённый `❌` проходит то же правило. |
| **I3** | strict `❓` → **full** exhaustion protocol (`corpus` + `serp` + `registry` + `inheritance`) **or** `reviewed_by ≠ null`. An incomplete protocol is a report anomaly, not a status change. The protocol is written from **observed** facts and names only circuits that actually ran — a run without the SERP circuit keeps `❓` as an anomaly; that is a correct signal, not a defect to silence. |
| **I4** | `failed` → `status = null`, never a status. |
| **I5** | `failed` **is** in the Coverage denominator. Universe mode: denominator = `Σ w(scope)`; `pending`/missing counts as unresolved. `preliminary` (I13) and `N/A` (I7) are **outside** it; `universe = 0` → Coverage `defined=False`/UNDEFINED and Capability is not published (I11/G4). |
| **I6/I7** | N/A only by a human (`reviewed_by`). |
| **I12/I13** | `❓` is never downgraded to `❌`; `preliminary` is excluded from every calculation and has `status = null`. |
| **I16–I21** | Retrieval profile identical for every product in a cycle; product-scoped retrieval; no web for the evaluator; evidence only after hash+index; golden cut-off = pipeline `retrieval_cutoff_K`; model→scenario routing is decided by phase, not by the LLM. |
| **I22** | `evidence_type=direct` requires a non-empty `citation_quote` — the most frequent evidence rejection (`check_evidence`). There is **no** SQL trigger for I22 (`cell_evidence.citation_quote` has no `CHECK`); the gate is domain-only. |
| **I23** | Search budget fixed and logged per product. |

**I2c (subtle, and it changes cells).** A `❌` under **incomplete** corpus coverage
(`coverage.complete is False`) depends on the **negation class**, not on crawl completeness
alone (`check_absence_coverage`, mirrored in the report; executed in Ш6 via
`evaluate._enforce_absence_coverage`):

- `explicit` (verbatim vendor quote) and `authoritative` (authoritative enumeration) are
  **not** cancelled by incomplete coverage — they are testimony, not an inference from
  corpus completeness;
- `bounded` depends on the space, so it requires `scope_acceptance` (a human accepting the
  risk); without it the `❌` is not published;
- `unobserved`/`not_stated` never yield `❌` at all (I2).

A violation turns the cell into **`❓`** (`not_stated`, I12), not `failed`: an absent risk
acceptance is a procedural condition, not instrument failure (I4), and the cell must stay on
the map. The withdrawn `❌` publishes no absence protocol either (`absence_evidence=[]`) —
"we looked and did not find" is a claim that is not proven here. The reason goes out as the
machine token `absence_coverage_unsatisfied` in `evidence_scope.reason` **and**
`cells.validation_notes`, naming the missing component and the culprit
(`{product_id}/{param_id}`). `coverage=None` (no coverage record) → the check does not apply.
The signature lives in `packages/{case_id}/scope_attestation.yaml:attestations` (**per
product**), written by `hound package scope attest --package … --product … --actor …`; the
run reads it and passes it into Ш6, and used signatures are visible in the manifest
(`scope_attestations`). It is **not** in `EXECUTION_CONTRACT_FIELDS`: authorization, not
measurement.

**I2d (methodology 0.3.0) — dated absence, and `complete` is a fact of the corpus.**
`❌` is no longer an assertion about *today*: it is an assertion about a **dated event**
(«in the space exhausted by capture C of date D, this is absent»). The carrier depends on the
negation class (`check_absence_dated`): for `explicit`/`authoritative` — the observation
record of **every** cited document (`corpus_artifact.observed_at`, two address forms
`doc_version_id`/`doc_id`); for `bounded` — a capture of **every** declared root with
`complete=1` and a date (`CorpusCoverage.roots`), where an `adopted` capture attests `explicit`
but **not** `bounded`; `registry_absence` is untouched (I2b). No carrier ⇒ `❓` with token
`absence_undated` and no absence protocol — same discipline as I2c.

This **replaces** the owner decision of 29.10.2026 («`❌` требует `live|revalidated` в этом
цикле»): `_enforce_reuse_confirmed`, its two levels and the token `reuse_unconfirmed` are gone
(the token stays readable in old reports). The old rule was right for its carrier — «one run =
one snapshot» — and F1 separated the two facts it conflated: «не смотрели» (no record) and
«смотрели в марте» (record with an old date).

Two consequences that are easy to miss:

- **`complete` is now decided by captures, not by session items** (`derive_corpus_coverage`,
  `CorpusCoverage.roots`). Session counters (`truncated`, unreadable origin, blocking `FAILED`,
  `partial_extraction`) still decide — but **only when a session exists**; a no-network pass has
  none, and «unknown» is not «failed». `observed_sources == 0` no longer decides anything
  (measured 05.11.2026: `frozen` lost all four `❌` as `failed`/`status=null` on the very same
  bytes, composition and `complete=1` capture — `2026-11-05-absence-dated-demo.md`).
- **`scope_acceptance` stops rescuing `bounded`**: a risk signature does not replace a dated
  carrier, so `bounded` + incomplete capture ⇒ `❓` even with a signed acceptance. That is a
  tightening of I2c's branch, not a new gate: I2c itself is untouched.

`policy_digest` of the capture is the **fingerprint of the space the walk exhausted**
(declared policy modes + `crawl_config_digest`), not of the decision surface: the clock-derived
`decision`/`network_mode`/`corpus_source` decide *when* to look, not *what* was exhausted, and
including them would make a complete capture stop attesting on the next run whose `scheduled`
decision flipped. It is compared with the same space digest declared by the run: a **known**
mismatch forces `complete=False`; an unknown digest (either side) does not — absence of a fact
is not a fact of absence (A3). The decision surface stays `crawl_policy_digest`, the `run_id`
term. No staleness threshold is introduced (A3): `absence_as_of` is published, and «верить ли
`❌` от марта» is the reader's judgement, not a verdict.
`SCHEMA_VERSION` 3.26.0 (`corpus_coverage.roots_json`, `cells.absence_as_of` at 3.20.0;
`corpus_docs.page_map` at 3.21.0 — the page map, from which a chunk's page number is a
**function**, not a column: a copy would have to be maintained on every chunk-write path and
one missed path would give a silently wrong page; `corpus_docs.ocr_probe`/`pages_ocr`/
`pages_failed`, `runs.ocr_probe`, `index_manifest.ocr_probe` at 3.22.0 — recognition
provenance, §10; `runs.page_provenance_json` at 3.23.0 — the page of a **quote**: how many
citations name a page and why the others cannot (`mapped`/`no_page_map`/`no_char_span`/
`no_chunk`/`no_doc`), a run observation rather than a measurement condition; `corpus_docs.tables`
+ `runs.tables_json` at 3.24.0 — a table as an **object** (`table_id`/`page_no`/`caption`/
`header`/`rows`; `NULL` = the reader gives no tables, `[]` = looked and found none), not a
string in `context_prefix`; chunking does not depend on it; `corpus_docs.blocks` +
`runs.blocks_json` at 3.25.0 — document **structure** (`kind`/`char_start`/`char_end`/`order`/
`text_source` `read|reconstructed`/`page_no`/`bbox`/`parent_id`; `NULL` = the reader gives no
blocks, `()` = looked and found none), and a chunk's `block_id` is likewise a **function**
(`block_id_for`, same rule as the page number), not a column; `structured`/docling gives `None`
because its only exact per-page export is quadratic — measured 0.56 s for the whole document
vs 63.2 s for 200 page exports); `corpus_chunks.table_ref` at 3.26.0 — the **address of a
chunk inside a table** (`{table_id, row_start, row_end}`, contract
`core/domain/doc_tables.py:TableRef`; `NULL` = address unknown, which is **not** "the chunk is
not from a table"). A column here, unlike the page number and the block id, because a
table-object carries no offsets into the text (docling does not report them), so only the
chunker knows the address — at slicing time. `text`/`content_hash` do not change (measured
16.11.2026: `chunk_text` with and without tables gives byte-identical texts), but
`chunker_version` → `policy-6` because a reused chunk-set from `state.db` would otherwise come
back without the address — the same class as `pdfium-text-5`; `chunker_version` is a contract
field (`digests.py:116`), so this is an `execution_change` with an unchanged corpus text. Why
phase 2 is an address and not a re-slicing, with numbers: plan `26` §O39 and
`docs/reports/measurements/2026-11-16-tables.md` (24 documents / 181 tables: no table row is
longer than the chunk budget, so the cut already falls between rows). The address never reaches
retrieval — index body is `chunk.text` alone, embed input is `("text",)`, `embed_cache` key
carries no chunker version — and the price of `policy-6` was **measured** 17.11.2026 rather
than argued: the whole corpus re-chunked to 253 306 of 253 306 identical chunks, and
re-embedding changed the ranking on **0** of 15 golden cells (McNemar p=1.0)
(`2026-11-17-policy6-retrieval.md`; `postgrespro`/`pangolin`, 117 of 132 cells, not re-embedded
— ≈5 h GPU). STATE `13` —
`chunk_meta.table_ref`: the durable chunk-set carries the address too, and a file of an older
STATE is rejected by the version gate (the same rule that made `capture.policy_digest` STATE
`12`).

## 6. Crawl scope: declared hosts

A URL is queued only if it (1) passes SSRF validation, (2) belongs to a **declared** host,
(3) fits the root path-prefix on a segment boundary, and (4) is not blacklisted. The SSRF
check is **one** check — it is not a scope check.

SSRF has two layers. `validate_url` is pure and rejects any **literal** IP that is not
globally routable — including alternate IPv4 notations (`2130706433`, `0x7f000001`,
`0177.0.0.1`, `127.1`), trailing-dot/percent-encoded forms, unspecified, link-local,
CGNAT, multicast/reserved, and IPv4-mapped IPv6 (`::ffff:169.254.169.254`). The fetchers
additionally call `resolve_and_validate` — production wiring injects the system resolver, so
**every** resolved address must be global and a name pointing at loopback is refused; a
test-only `resolver=None` keeps the literal-only behaviour. Connection pinning to the checked
IP is not implemented; the residual TOCTOU/DNS-rebinding is risk **S9g**, named in
`core/util/security.py`. Redirects are checked hop by hop with the same function.

- The root host is always in scope. A subdomain is in scope **only if declared** (product
  seed root, or a `domain_profiles.yaml` key). `www.` is equivalent to the bare host on both
  sides.
- The predicate is `Frontier.host_in_scope` (`core/pipeline/crawl_frontier.py:1491`). This is
  **different** semantics from `fetch_plan._host_matches`: there a subdomain **inherits
  profile axes** (`fetch`/`extract`/limits), which is correct and needed. Inheriting axes ≠
  the right to spend another seed root's budget.
- Refusals are **visible**: `out_of_scope_subdomains` (count per host + examples) and
  `accepted_foreign_hosts` (how much of a root's budget went to a declared foreign host).
- A terminal artifact (document/container) is exempt from **path-prefix only**: it does not
  continue navigation, but SSRF/host-scope/blacklist still apply.

Measured cost of getting this wrong: `edu.postgrespro.ru` produced 563 of 682 items on a root
that declared only the parent host, while the product documentation root got zero of its own
items. Widening scope is an `execution_change` and a re-crawl, never a config tweak.

### 6.1 Refresh policy (plan 19)

Crawl policy is **per scope** — `product` | `host` | `root` | `path` (host + prefix inside a
root) — stored in the operator `state.db` (`crawl_policy`, append-only via `superseded_by`),
with package defaults in `crawl_policy.yaml`. Resolution is **most specific wins**
(`path` > `root` > `host` > `product`; longest literal prefix inside a kind); equal
specificity with different modes is a **conflict** (`crawl_policy_conflict`, `EXIT_USAGE`),
never a tie-break by clock/insertion order. A record that matches no declared root is
**visible** (`crawl_policy_unmatched`) and, like `note`/`actor`/`created_at`, does **not**
enter the digest: a no-op edit must not give every product an `execution_change`.

Modes: `auto` (as before), `on_request` (first discovery automatic, update only when the
root is named by `--refresh-scope`), `scheduled` (update when `now - observed_at >=
interval_hours`, resolved to `refresh`/`reuse` — the decision, not the intent, enters the
digest), `network_denied` (**no** requests for that root). `refresh_policy` (run level:
`auto|revalidate|refresh|offline|frozen`) composes **inside** the resolved policy:
`offline` reuses chunks (no requests), `frozen` reuses `corpus_raw` bytes with current
extraction/chunking and `frozen_source ∈ bytes|chunks` (canonicalised to `n/a` outside
`frozen`).

A no-network pass replays **durable known documents**, not just author roots: under `full`
mode there is no BFS to discover them, so `url_state` *is* the work set (measured
04.11.2026: roots-only lost 7 of 9 documents). It also extracts with the **declared**
per-URL `document_level`/`pdf_pipeline` (`extractor_factory`), not the composition default —
otherwise `extractor_version` names a condition the corpus never had. Container **members**
are replayed too (plan 19 §E.7): a container is not a document, so its retained-bytes path
and hash live in `container_state` (`STATE 10`, written write-through by the live descent);
a no-network pass descends those bytes with the same `ArchiveDescentWalker` inline (no pool,
no `crawl_session_items`) and names what it could not replay (`frozen_container_unreadable`/
`frozen_container_empty`, or `stored_bytes_missing`).

`network_denied` is a **network policy, not product exclusion**: the product stays on the
map, the corpus comes from what is already stored (`chunks`/`bytes`), coverage is incomplete
and is named (`crawl_policy_skipped` + `crawl_network_denied`), never `failed` and never
"no seeds".

### 6.2 Crawl width, retries and what `fetch_ms` measures

Three facts that are cheap to state and expensive to rediscover:

- **Width belongs to the node, not to the package.** `crawl.max_workers` and
  `crawl.host_concurrency` are declared in `config/deploy.yaml` (`CrawlLimits.max_workers`,
  `core/config/deploy.py:490`) and a package does **not** declare them — they are a property of
  the machine (memory, network), not of the category. `lanes_limit = 1` when
  `host_concurrency is None`. Measured 07.11.2026 on this host: 1207 documents in 209 s =
  **5.78 req/s**, p50/p95 151/113 ms. That number is a **price of this host**, not a ceiling:
  re-measure before quoting it.
- **Retries are not in the engine.** One cycle serves both `local` and `split`
  (`core/util/fetch_retry.py`, `FETCH_RETRY_TIMES = 2` at `:55`, its own slot per attempt,
  transient = 5xx/408/429/network), and Scrapy is configured with `RETRY_ENABLED=False`
  (`core/adapters/crawl_scrapy.py:168`) on purpose — otherwise two retry layers would double
  the load on a paced host and hide which one acted. Do not "fix" missing retries by enabling
  the engine's.
- **`fetch_ms` is not the host's response time.** It is measured around the fetch call
  (`core/pipeline/crawl_pacing.py:259`), so it includes everything the adapter did before the
  response arrived: measured 16.11.2026, Scrapy reported **1312 ms** on a page whose own
  response was ~100 ms. For "how fast is the host" read the pacing/pacing-vs-minimum numbers in
  `crawl status --per-host`, not `fetch_ms`. `COHOUND_SCOUT_CRAWLER=scrapy` is for
  browser-profile hosts only — there the fetch is serialised by design.

## 7. Gates G1–G4

- **What blocks a run.** `NON_BLOCKING_GATES = {G1, G2, G3}` (`core/pipeline/gates.py:79`,
  owner decision 2026-10-26): with no active decision the phase proceeds and a policy row is
  written to `gate_decisions` (`actor=policy`,
  `external_ref=waiver:policy:nonblocking:{gate}`), read in the manifest trail as
  `state: not_required`. Only **G4** and an **explicit rejection** (`verdict=rejected`) on the
  current subject block anything: a rejection is a decision, and silence of an external system
  is never recorded as rejection or approval.
- **G1/G2 are internal attestation.** A human approves the anchor package profile (cohort,
  roles, volume, passports) and the role×product matrix. The durable path
  (`profile_attestation` in `state/{package_id}/state.db`) is declared, checked
  (`make_profile_gate`, `core/pipeline/profile_attestation.py:313`, wired through
  `_profile_gate_resolver` in `core/cli/context.py`) and **has a producer — a human**:
  `hound package profile attest --gate G1|G2 --actor …` (`--dry-run` writes nothing and says
  so). A run **never writes it**, only reads it. Measured 2026-10-30 and re-checked
  2026-11-15: **zero rows** in every real `state.db` (three files, incl. the stand) — the
  mechanism exists, nothing is signed. Until rows exist the only working path is the per-run
  `gate_decisions` row, and it takes precedence.
- **G3/G4 are integration points.** coHound does not decide review or publication: on a block
  it writes `runs/{run_id}/gates/{gate}-request.json` (deterministic subject +
  `subject_digest`), and the decision arrives from outside and is only recorded.
- `gate_decisions` carries `subject_digest`, `source` (`internal|external`), `external_ref`,
  `verdict` (`approved|rejected`). A stale approval (the subject changed) does not pass, and
  the message names the component that changed.
- `run.yaml: gates.{g3,g4}: external|waived` (default `external`). `waived` → phase `skipped`
  with `skip_reason=gate_delegated_external`, a waiver row in `gate_decisions`, and
  `gate_waivers` in the run manifest (the manifest is a **row in `cohound.db`**;
  `report` exports `run_manifest.yaml` separately). An invisible waiver is forbidden.
- `hound gate` needs a subject digest: `--subject-digest`, else the one in
  `runs/{run_id}/gates/{gate}-request.json`; with neither it fails `EXIT_USAGE`
  (`gate_subject`). `--source external` also requires `--external-ref` and `--notes`.
- **Gate policy is process, not a measurement condition**: it is not in
  `EXECUTION_CONTRACT_FIELDS` (otherwise changing the procedure would change `run_id` and
  break the cross-cycle diff).
- Decisions are **per run**: a new `run_id` needs new decisions; a row from a previous run
  does not apply.
- `PHASES` (15): `normalize`, `lint`, `plan`, `g1`, `discovery`, `g2`, `registry`,
  `inheritance`, `corpus`, `evaluate`, `review`, `g3`, `compute`, `g4`, `report`.

## 8. LLM circuits (spec §7)

- **evaluator** (Ш6): models via an OpenAI-compatible API **inside the corporate perimeter**
  (no external LLM hosts or keys), **no web tool**, works only on the chunks it is given,
  strict JSON through constrained decoding.
  **The evaluator model is a declared input, not environment**: `--llm-model` >
  `deploy.llm.<slot>.model_id` > **error** (`EXIT_USAGE`, code `llm`). There is no default
  model: a model the operator did not declare is a false provenance record.
  `COHOUND_LLM_MODEL`/`COHOUND_LLM_DECODER` are **not read** by the evaluator (the fact is
  reported via `llm_env_notices`); endpoint/key/timeouts are operational and come from env.
  Reason: `llm_scenarios` feeds `execution_contract_digest` → `run_id`, so the model must be a
  function of declared inputs (I21, spec §7.4). An empty `base_url` in the slot plus a non-empty
  `COHOUND_LLM_BASE_URL` in env produces a **visible** warning: the evaluator was lifted
  outside the contract and `run_id` does not describe the model.
- **search adapter** (Ш5): external search as a content source, normalized into the corpus.
- **discovery / concept scout**: search is allowed; the output is candidates/hypotheses (G2).
  Here `COHOUND_LLM_*` env **is** read (args > env > slot) — a **deliberate asymmetry**:
  reconnaissance output is not evidence, so the model is an operational setting rather than a
  measurement condition. Merging "candidates" and "evidence" into one policy would let the
  environment rewrite verdict provenance.
- Model selection is a **measurement** task (A3), not a predetermination:
  `hound experiment evaluator-benchmark --run-id … --golden … --model …` computes JSON
  validity, κ, confusion, abstention, `fabricated_support`, bootstrap CI and a determinism
  probe (`--repeats`), persists `llm_scenarios`; `model_id` comes **only** from the flag.

## 9. Run identity, declared axes, versioning

- `run_id` is a function of **declared inputs only**: 35 fields in
  `EXECUTION_CONTRACT_FIELDS` (`core/pipeline/digests.py:105`). Host-observed values
  (`extractor_version`, `embed_model_id`, engine versions, index build) are **provenance**,
  not identity — they surface via `pb`/`index_build_id` and belong to `content_change`.
  `embed_spec` (`model_id@revision`) is the declared twin of `embed_model_id`, exactly as
  `extractor_stack` is of `extractor_version`: the weights are a measurement condition, the
  observed string is provenance. No declared model ⇒ **no dense branch** (`embed_spec: null`,
  visible wiring notice, R0 lexical) — the model is never picked from what an extra happened
  to install; a declared model that cannot be built is `EXIT_USAGE`, not a fallback (O6).
- **Anything that changes the corpus or the verdicts must be a declared input.** Precedents
  that were once silently outside the contract: `--max-depth`/`--max-documents`,
  `chunk_size`, `prompt_template`, `evaluation_policy`, `render_modes`, `passports`.
- **The token limiter is a declared condition, not a tuning knob.** `TOKEN_LIMIT` in
  `core/adapters/chunker_family.py:80` is `(limit, "model_id@revision")` or `None`; at `None`
  it is not applied and neither `chunker_version` nor `chunking_spec` changes (so switching it
  on/off cannot fake an `execution_change`). Declared-but-no-counter is `ValueError`, a counter
  without a declaration is a visible `token_limit_undeclared` warning, and every application is
  logged. Turning it on is an `execution_change` with a full re-embed; the cost is published
  (`docs/reports/measurements/2026-11-15-chunk-token-limit.md`: 253 306 → 262 876 chunks,
  >512-token share 5.457 % → 1.626 %, 426.9 s per corpus).
- **Crawl axes:** `crawl_strategy`, `crawl.rate_limit` and `crawl_config_digest` are contract
  fields; `max_depth`/`max_documents`/`exclude_*`/`robots`/`document_level`/`pdf_pipeline`/
  `pdf_max_pages`/`pdf_max_extracted_chars`
  enter through `crawl_config_digest` (`CrawlLimits.as_dict()`), and so does the **resolved**
  `rate_limit` (site > run, same formula as execution, key only when declared) — a per-site
  value overrides the run-level axis, so leaving it out let a host be re-paced without a new
  `run_id`. Add a new operator axis to the single inventory `CRAWL_AXIS_DESTS`
  (`core/cli/execution_axes.py:195`) so `plan` and `run` pick it up together — flag parity is
  tested. Changing an axis gives a **new `run_id`** (`execution_change`), not an edit of the
  current run; otherwise two different corpora become one measurement.
- **Declared defaults live in `deploy.yaml`, not in code.** `retrieval.profile` (I16) and
  `lexical.profile` are measurement conditions: they are resolved **before** the contract and
  the same value feeds the phases (`--retrieval-profile` > `deploy.retrieval.profile` > `R0`;
  `lexical.profile` → `lexical_spec` → `run_id` **and** the index seed), so "declared" and
  "executed" cannot diverge. `extract.cache_max_bytes` is the opposite case on purpose: an
  operational budget, deliberately **not** in `EXECUTION_CONTRACT_FIELDS` — it changes the
  price of re-extraction, not the corpus or the verdicts.
- **Crawl policy is identity, its execution is provenance.** `crawl_policy_digest`
  (plan 19 §6.1) is in `EXECUTION_CONTRACT_FIELDS`; `refresh_policy` is a separate `run_id`
  term and `frozen_source` enters canonicalised. Only those three describe the measurement
  conditions. `crawl_policy_unmatched`, `policy_skips`, `policy_zero_reasons`, per-root
  `network_mode`/`corpus_source` and `captured_at` (time of the capture record for `frozen`,
  `None` otherwise) are **provenance**: visible in the manifest (`runs.crawl_policy_json`),
  never in `run_id` — otherwise an operator's bookkeeping would rewrite identity. The digest
  is persisted (`runs.crawl_policy_digest`), so `hound diff` **names** it instead of printing
  "not recoverable from the manifest".
- **Observation dates are provenance, not identity** (plan 20 §F1). Because parts of a
  corpus refresh asynchronously, dates per artifact *are* different by definition. A
  **capture** is one walk of one root (`state.db: capture`, STATE 12: `capture_id` from
  `(product, root, run_id)`, `complete` = the walk was exhausted) and an **observation** is
  `url_state.observed_at`/`observed_how`/`capture_id` (COALESCE-upsert: a pass without
  network never erases a live observation). The run's **corpus snapshot**
  (`corpus_snapshot_id`, `captures[]`, `corpus_as_of`) lands in `runs.corpus_snapshot_json`
  and, for `pb`, in `IndexMeta.extra` — **never** in `_derive_partition_build_id` or
  `run_id`: it is known only after the crawl, while `plan` must compute `run_id` first. Same
  declared inputs over a shifted `state.db` therefore give a different snapshot; that is
  the definition of asynchronous refresh, not a changed measurement condition.
- `deploy_profile` is in the contract because `split` executes web work in the DMZ: it ignores
  `document_level`/`pdf_pipeline`, double-fetches (different observed URL set and `complete`),
  and does not extract container members. Same `run_id` over two executions would be a lie
  about measurement conditions.
- `embed_device` (`cpu|cuda`) is a contract field: the place of computation is a measurement
  condition, and without it two hosts could compute vectors on different devices under one
  `run_id`. `auto` does not exist (R5).
- **Version classes** — pick by semantics, not by the file that carries the enum:
  - **`methodology_change`** (`methodology_version`): rule semantics — status dictionary,
    scoring policy, absence/satellite/evidence model, definitions of
    Coverage/Comparability/sensitivity. Requires `golden-full` **and** golden re-annotation.
  - **`execution_change`** (`execution_versions`): measurement conditions — evaluator/embed
    model, cohort, crawl axes/strategy, deploy profile, prompt template, chunking spec,
    evaluation policy, lexical spec. Blocks the cross-cycle diff with an `execution_change`
    mark; `golden-full` required.
  - **`content_change`**: observed host/dependency facts. Visible in provenance, `pb`,
    `index_build_id`.
- **Схема БД не мигрирует.** Три БД (`runs/{run_id}/cohound.db`, `state/{package_id}/state.db`,
  `edge_jobs.db`) версионируются одинаково: версия читается **до** DDL, несовпадение —
  `SchemaVersionMismatch` с путём файла (`EXIT_USAGE`), файл не меняется. Аддитивных
  `ALTER TABLE ADD COLUMN` при открытии нет ни одного; колонки и индексы объявлены в
  `CREATE TABLE`. Колонка, которой там нет, — дефект DDL, а не повод досоздать её на открытии:
  тихий догон объявляет файл текущим, не будучи им (решение владельца 2026-11-01).
- Slugs (`param_id`, `product_id`) are immutable; retirement is `deprecated` +
  `replaced_by` (spec §13).

## 10. Extraction: formats, mechanisms, containers

- **HTML** is the primary source. **Plain text/markdown** (all variants) and **EPUB** are
  first-class, with their own readers (`extract_text`, `extract_epub`) and chunkers — not
  docling. **Office** (docx/pptx/xlsx) goes through docling; **PDF** goes through whichever
  mechanism the `crawl.pdf_pipeline` axis declares.
- **The HTML input is pre-cleaned, not allow-listed.** `strip_active_html`
  (`core/util/sanitize.py:296`) removes active content (`script`/`style`/`noscript`/`iframe`/
  `object`/`embed`/`svg`/`template`), `on*` handlers and `javascript:`/`vbscript:`/
  `data:text/html` URLs **together with their bodies**, but keeps `class`/`id`/`role` — the
  attributes `trafilatura` uses to tell an article from chrome. The `nh3` allow-list
  (`sanitize_html`) stripped exactly those, so the input reached the library as undifferentiated
  text: measured 02.11.2026 on 500 saved pages, text ×1.75 on one host (`chrome_share` 0.284
  vs 0.010) and **all** ATX headings lost on another (0.000 vs 0.727). The allow-list survives
  only on the seam validator (`gateway_validate.py:300`), where the question is "is this safe
  to forward", not "what is the article". Pre-clean output is byte-identical to not cleaning
  at all (500/500 pages), so the cost of the old path is a measured number, not a guess:
  `docs/reports/measurements/2026-11-02-html-preclean-vs-allowlist.md`.
- **The page, not the reader, declares the main region.** `trafilatura` finds the article by
  container (`article`/`main`/`role=main`/`#content`/`itemprop=articleBody`). DocBook layouts
  (headings live in `div.refsect1 > h2`, content in `div.container`) have no such container, so
  the rule path is skipped, extraction goes flat, and `<h2>` arrives as plain text while chrome
  arrives as text too. `_declare_main_region` (`core/adapters/extract_html.py`) wraps the body
  in `<main>` **only when** the page declares no marker; marked pages pass through byte for
  byte. Measured 02.11.2026, 40 pages per host: heading retention `database.diasoft.ru`
  0.000 → 0.845, `edu.postgrespro.ru` 0.024 → 0.939, `tantorlabs.ru` 0.000 → 0.782; five
  marker-bearing hosts unchanged to the last digit
  (`docs/reports/measurements/2026-11-02-html-headings-main-region.md`). Reader version is
  `trafilatura-html-8` (17.11.2026: a leading XHTML prolog — BOM, `<?xml … ?>`, `<!DOCTYPE …>` —
  is stripped before parsing, otherwise `lxml`'s "Unicode strings with encoding declaration"
  error sent pre-clean to the allow-list and the structure was lost; on the declared corpus that
  hit 1 file of 26 206, and it is `sitemap.xml`); D1–D4 are all `execution_change` because the
  corpus text changes.
- **A definition list is not a section.** `trafilatura` 2.3.1 rewrites `<dl>/<ol>/<ul>` into
  its own `<list>` and `dt`/`dd`/`li` into `<item>`, which the markdown serializer prints as
  **one line**: a `<h3>` inside a `<dd>` arrives as `- текст ### Примечание текст`, so the
  heading stops being a heading and `heading_path` carries a whole paragraph.
  `_neutralize_definition_lists` (`core/adapters/extract_html.py`) turns `dl`→`div`,
  `dt`→`p` (the term stays a paragraph, never a section), `dd`→`div` — but only
  for lists that contain a heading (46 of 2 379 HTML pages). Replacing **every** `<dl>`
  touches 809 pages, drops headings on **7** of them (29 heading lines; all seven lose every
  heading) and adds **+258 787** characters instead of +406 — navigation `<dl>`s stop being
  pruned by link density once their `<a>` is unwrapped. The narrow rule is also applied to
  unwrapping `<a>` so link-density pruning does not eat definition text (measured −61 % on
  the postgrespro acronyms page without it). Measured 03.11.2026 on 2 379 pages: defect lines
  88 → 59, headings +47 with zero losses, longest heading 1 076 → 381 characters, characters
  +406 (`docs/reports/measurements/2026-11-03-html-definition-lists.md`).
- **A pseudo-tag is text, not markup.** `trafilatura` 2.3.1 (`main_extractor.handle_quotes`)
  creates a new element named after a tag it found, and `lxml` refuses a colon in that name
  (`ValueError: Invalid tag name 'cluster_id:14841639068965178418'`). The exception is caught
  in `core.bare_extraction` as "junk", so the **whole document** is discarded:
  `extract()` → `None` → `parser_failed`, on a page holding 140 KB of text. `_escape_pseudo_tags`
  (`core/adapters/extract_html.py`) escapes `<` before a name containing a colon — nothing
  else: prose `<`, closing tags, comments and ordinary elements are untouched, so a document
  without such a construct passes **byte for byte**. It runs **last** among the input fixes
  because D2/D3 round-trip through `lxml` and would put the pseudo-tag back. Measured
  14.11.2026 over all 44 626 `corpus_raw` files (12 GB): the construct occurs in 248 files,
  10 of them HTML — **4** gave 0 characters and now give 150 596 (a docs page whose `<pre>`
  holds `header:<cluster_id:…>` log output), **6** had their code samples torn apart
  (`<my:a>` inside `SELECT xpath(…)` became an element and split the sample across two code
  fences) and now come out whole (32 215 → 32 843 characters).
- PDF has **one axis and two executable mechanisms**: `crawl.pdf_pipeline` names the
  **mechanism**, not the library (`PdfPipeline`, `core/domain/enums.py:519`).
  - `structured` (default) — layout models, table reconstruction, OCR (today: docling). The
    only mechanism where `crawl.document_level` (`off|text|layout|layout_tables|full`)
    applies.
  - `text_layer` — the file's own text layer, no models (`pypdfium2`). Page order as stored,
    no tables, no OCR, no levels to declare.
  `pypdfium2` is a **base dependency** (torch-free, `pyproject.toml` `dependencies`), so both
  mechanisms work out of the box and choosing one is a config decision, not an install step.
  Where it is missing or broken, an explicit `text_layer` raises `WiringError` →
  `EXIT_USAGE`: silent substitution is forbidden, because `extractor_version` would name an
  engine that never ran. Any mechanism change is `execution_change` and changes the
  `extract_cache` key `(content_hash, extractor_version, level)`.
- **The price of `text_layer` is measured, not extrapolated**: `PDFIUM_TEXT_PEAK_MB` = 134 MB
  for the whole anchor PDF corpus (240 documents, 15 929 pages, 20.9 M characters, 26.8 s,
  0 errors) and **0 VRAM** — a measured fact, not "unknown", since a native parser loads no
  models. That number drives the worker window, so `text_layer` gets the workers that
  docling's peak would deny. Its honest limits: tables are not rebuilt, scans are not read —
  **182 of 15 929 pages (1.14 %) carry no text layer** (measured 25.10.2026,
  `2026-10-25-pdf-backend-alternatives.md`; another 473 pages carry 1–39 characters).
  Reading order — the one limit that was named but never measured — **measured 16.11.2026**:
  0 two-column **prose** pages in 19 331 (149 PDFs); the 13 pages with a real gutter are
  version-comparison **tables** (fill 0.034–0.60 vs 0.89–0.97 for prose), which are read by rows
  and are not a defect (`2026-11-16-reading-order.md`). So "reading order is not guaranteed"
  stays a named limit about *other* corpora, not a defect of this one.
  `error="no_text_layer"` is raised only when the **whole document** has no text: an individual
  empty page currently vanishes without a mark and does not set `partial` — the visibility fix
  is plan `26` O26. A failing page does not kill the document (`partial=True`); control
  characters and `\r\n` are normalized, without which the
  paragraph chunker matches nothing and the document becomes one chunk.
- **Recognition provenance is a tri-state, never a zero placeholder.** `ExtractedDoc.pages_ocr`/
  `pages_failed` (`None` = not applicable or not measured — never `0` as a stand-in) and the
  document-level `ocr_probe` (`ok|unsupported|unavailable|unknown`, canon in
  `core/domain/ocr_probe.py`) are persisted as `corpus_docs.ocr_probe`/`pages_ocr`/`pages_failed`
  under the same **COALESCE** rule as `page_map` (a writer without extraction must not erase a
  measured outcome), aggregated into `runs.ocr_probe`/`index_manifest.ocr_probe` as the **worst**
  outcome (`unavailable > unsupported > unknown > ok` — degradation must not drown in a majority
  of `ok`) and published as `outputs/ocr_provenance.csv`. `pages_ocr` counts pages whose
  `confidence.pages[p].ocr_score` is a non-NaN number, read with `.get()` because
  `ConfidenceReport.pages` is a `defaultdict` (indexing would invent a page); `pages_failed`
  counts `ErrorItem`s carrying a `page_no` (document-level errors carry none). Two facts are
  deliberately **not** measured: "the page's text came from OCR" (`TextCell.from_ocr` needs
  `generate_parsed_pages=True`, which changes provenance and RSS — a separate axis and
  measurement, A3) and "OCR looked and produced no text" (the library does not report it; only a
  log warning).
- **Page provenance is stored once, derived everywhere else.** The `text_layer` reader fills
  `ExtractedDoc.page_map` (`(page_no 1-based, char_start, char_end)`, half-open, sorted,
  non-overlapping; `"\n\n"` between pages is a **gap**, not part of a segment; offsets are in
  the chunker's space, i.e. without a leading BOM, so the reader shifts by −1 on a BOM
  document). It is persisted as `corpus_docs.page_map` (JSON, `NULL` = the reader gave none —
  never `''`) with a **COALESCE** upsert: a writer without a map (HTML, docling, seam, reuse)
  must not erase a measured map of the same text. A chunk's page number is **not** a column
  but `page_number_for(char_start, page_map)` (`core/domain/page_map.py`): a copy would have to
  be maintained on every chunk-write path and one missed path gives a silently wrong page.
  Stamp `pdfium-text-5` is an `execution_change` on purpose: a warmed `extract_cache` entry
  without the map would hand back a document that lost provenance (and, since 3.25.0, without
  the page blocks either — the text is byte-identical, identity is not). Measured 15.11.2026 on
  the whole PDF corpus (307 documents / 21 636 pages): 0 defects, `text` and `content_hash`
  identical to the previous reader, cost +2.8…+6.1 % extraction time
  (`docs/reports/measurements/2026-11-15-pdf-page-map-corpus.md`, instrument
  `scripts/check_pdf_page_map.py`). Under `deploy_profile: split` the map is `None` — the seam
  (`SEAM "1"`) does not carry it yet; docling (`structured`) does not fill it either.
- **There is no hybrid PDF pipeline, and that is a measurement decision, not an unfinished
  job**: pdfium lost 6.7 % of docling identifiers on a real book, and the only table detector
  with recall 1.0 flags 90 % of pages (`docs/reports/measurements/2026-10-28-hybrid-quality-gate.md`).
  Materials and detectors are preserved there for further experiments; the revision condition
  is labelled pages, or a recall-1.0 classifier with real savings.
- **`crawl.pdf_device`** (`cpu|cuda`, no `auto`): device is part of `extractor_version`
  (`docling-doc-4:<level>/<device>/docling-<build>/<policy>/<finalize>`, plus
  `/ocr-cyrillic-v5` at `full` only; Office carries no
  level/device at all — `docling-office-1:<policy>/docling-<build>/<finalize>`) and `extractor_stack`, so it changes the
  `extract_cache` key and `run_id` once, explicitly. `cuda` with no usable device is
  `EXIT_USAGE` with numbers, **never** a silent CPU fallback. Measured 8–12× on document
  extraction (`docs/reports/measurements/2026-10-28-gpu-vs-cpu-extraction.md`); a second
  worker on one card gave 1.10× at +2.3 GB RSS, hence window 1 there — a consequence of
  measurement, not a ceiling set on faith. **The OCR alphabet is part of the same key**: at
  `full` docling's auto-OCR picks `lang=['ch']` and Cyrillic arrives as Latin
  transliteration (measured 14.11.2026: 176 chars, 0 Cyrillic) while the page is reported as
  read (`error=None`). `docling-doc-4` declares `cyrillic` + the rec model executed on
  `onnxruntime`; a missing model is a visible `OcrEngineUnavailableError`, never a silent
  fallback to `ch`. **That model is not in the `rapidocr` wheel** — measured 15.11.2026:
  `RECORD` 3.9.2 lists three models and the Cyrillic one is not among them, so it is
  downloaded into the package's `models/` dir by `scripts/fetch_ocr_models.py` (pinned URL
  + SHA-256, `hound doctor` group `deps:extract-pdf`, check `ocr:cyrillic`). Without it a
  clean install fails **15** tests that pass on a host where the file was once fetched —
  the exact shape of "green locally, red in CI" (CI runs the fetch as its own step). Text that is nothing but
  `<!-- image -->` markers is `EMPTY_TEXT_REASON` (`FailureKind.EMPTY_DOCUMENT`), not a
  document.
- **Containers** (`zip`/`7z`/`rar`) are opened by `archive_descent.py`. There is **no depth
  ceiling**, and that is held by **mechanics, not budget alone**: descent is iterative (an
  explicit stack of frame generators), so depth never hits the interpreter stack. Finiteness
  comes from a content-hash cycle check (`archive_cycle`) and a cumulative per-root budget
  (`archive_max_total_uncompressed_bytes`; the `archive_max_*` axes bound **one** container).
  Exhaustion is a visible `archive_budget_exhausted` record, never a quiet stop. Backends:
  `zip` via stdlib; `7z`/`rar` via the `extract-archives` extra (`py7zr`/`rarfile`, compressed
  rar members need an external `bsdtar`/`unrar`/`7z`; AES-zip is unsupported). Without a
  backend a container does **not** open visibly (`available()=False`,
  `backend_unavailable`, `container_reader_unavailable`) — never "silently empty". Regression
  test for the measured failure: `tests/pipeline/test_corpus_descent.py::test_chain_of_5000_distinct_archives_reaches_the_end`
  (before the explicit stack, a chain of *distinct* small archives died at ~990 levels and
  surfaced as the falsely named `container_reader_unavailable`).
- **Memory:** after each document a worker returns free glibc arenas to the OS
  (`core/util/memory.py`, `malloc_trim`); without it RSS ratchets to OOM (measurements in the
  module docstring).
- VRAM peak for `structured` level `full` is **not measured**, so under `cuda` the VRAM term
  of the scheduling window is not applied — visible as a preflight warning
  (`core/cli/wiring.py` `preflight_extraction_budget`). `text_layer` is not affected: its
  VRAM term is a measured 0 (`vram_peak_mb`).

## 11. Commands

### 11.1 Runbook

`./hound.py` re-execs into `.venv` and fixes `sys.path`; `python3 -m core.cli` is the same
entry point.

```bash
# 0. Plan: normalization + lint + run_id. Prints only; writes no draft run.yaml/cohort.yaml.
./hound.py plan --run-yaml packages/{case_id}/run.yaml --package packages/{case_id}

# 1. Run. --urls is the operator corpus input; without it the URL corpus is empty.
#    --follow streams crawl session progress to a terminal status.
./hound.py run --run-yaml packages/{case_id}/run.yaml \
  --package packages/{case_id} --urls packages/{case_id}/sources/corpus_urls.yaml --follow

# 2. A fresh run does NOT stop at g1: G1/G2/G3 do not block. Phase g1 goes on with
#    skip_reason=gate_not_blocking and a waiver row (source=external,
#    external_ref=waiver:policy:nonblocking:g1). Only G4 and an explicit rejection block.

# 3. Gate decisions are per run. On a block the request file carries subject_digest.
./hound.py gate --runs-dir runs --run-id {run_id} \
  --gate G4 --actor {who} --target publication --notes "{why}"

# 4. Report artifacts from the DB (writes {out}/{run_id}/outputs/); review queue separately.
./hound.py report --runs-dir runs --run-id {run_id}
./hound.py review --runs-dir runs --run-id {run_id}

# 5. Evaluator model selection is a measurement task (A3), not a predetermination.
./hound.py experiment evaluator-benchmark --runs-dir runs --run-id {run_id} \
  --golden packages/{case_id}/golden.csv --model {model_id}

# 5b. Extraction mechanisms A/B on a **human-annotated** set (O38б/D10): the set is built by
#     scripts/prepare_extraction_golden.py (candidates → XLSX → human → compile → validate);
#     AI never fills the decisions. auto_switch is always false — the choice is a human's.
./hound.py experiment extraction --runs-dir runs --run-id {run_id} \
  --golden packages/{case_id}/sources/extraction_golden.json \
  --mechanism-a structured --mechanism-b text_layer --level layout_tables --device cpu

# 6. Finish an interrupted Ш6 on the SAME run_id: retry only cells without a verdict.
#    --dry-run = selector only (no ports built). run_id does not change; per-attempt log
#    is runs/{run_id}/logs/retry.jsonl (08.11.2026, plan 25).
./hound.py retry --runs-dir runs --run-id {run_id} --dry-run
./hound.py retry --runs-dir runs --run-id {run_id} --llm-concurrency 4
```

Resume/force: `--until {phase}` stops after a phase; `--reset` repeats the phases bypassing
the phase journal but **keeps** the durable crawl session (`corpus` reopens it and does not
re-fetch URLs that have retained bytes); only `--from-scratch` wipes the crawl session and
adds a live re-fetch bypassing the R7 cache.

Crawl sessions: `./hound.py crawl run|status|list|resume|restart|cancel` (`resume` is
idempotent: same `session_id`, already-`FETCHED` items are not re-fetched). Refresh policy
per scope is set with `./hound.py crawl policy set|list|clear` and consumed per run via
`--refresh`/`--frozen-source`/`--refresh-scope` (semantics §6.1; operator reference —
`03-data-config-cli.md` §D.2.2). Liveness
(`crawl_sessions.owner`/`heartbeat_at`): a stale `running` becomes `interrupted` on session
open — not `failed`/`cancelled`, because the crawl body is not finalized; `crawl cancel` demotes
`running` to `interrupted` with `cancelled_by_operator`. `interrupted` **must** stay in
`_TERMINAL_SESSION_STATUSES` (`core/cli/context.py`) or `--follow` waits forever. This is not a
lease: there is no cross-node coordination. `--follow` prints `crawl_progress` (session
aggregate) and `crawl_product` (product boundaries — the aggregate cannot distinguish "still
queued" from "lost"). "Why does this product have zero sources" is answered by
`results[].notes` in `run --json`, along with `product_start`/`product_end`/`product_skipped`;
for a live crawl use `./hound.py crawl status --per-url`.

**Reference products (plan 27).** Inheritance from an upstream line is a **measurement**, not
an assumption: `sources/reference_products.yaml` declares which lines the profile measures and
at which versions, `sources/baseline_facts.csv` records observations about the ancestor's
documentation (verbatim quote + text hash + date + `review`). `./hound.py baseline
{build|list|facts|report|review|refresh|recon|map}` reads and writes them; none of it touches
`runs/`. The commands form a **chain** — `build → recon → review → report → map` — and a command
whose predecessor is missing refuses and names it (`list` prints the same next command).
Only `status=present` **and** `review=accepted` is inherited — anything else means "no
acceleration", and the parameter is evaluated normally (neither `❓` nor `❌` follows from it).
A fact is looked up on the **major** version of the declared line (`18.4.1` → `18`), never
"≤ V": otherwise an upstream regression is papered over. The reference corpus lives under
`COHOUND_HOME/reference/{package}/{product}/{version}/` and is **per-build**:
`builds/{build_id}/{reference.db,manifest.json,maps/{map_id}/}`, shared content-addressed
`raw/`, and a `current.json` pointer. Identity is **content** (`build_id`, `map_id`), never path
or `mtime`; `current.json` only says which build the operator asked for last. `manifest.json`
publishes the walk's provenance (`complete`, `incomplete_reasons`, `skipped`, `errors`), and
`absent` in facts is allowed **only** at `complete=true` (I2/I2c) — an incomplete walk is
`unclear`, never "not documented". `hound baseline build` resolves its axes as declared input:
`--max-depth` is unbounded by default (the bound is the declared root's path prefix, not an
invented ceiling), `--max-documents` has no cap; links are always extracted so that an explicit
`--max-depth 0` is visibly `complete=false` rather than a one-page corpus claiming exhaustion.
`hound baseline recon` is deterministic (quote must be a substring of the chunk text) and picks
candidates through `LexicalSearcherPort` (in-process BM25), not a linear scan; the map is the
evaluator's verdict and therefore carries its own identity, including the model.

Blocked bytes the crawler was not allowed to take (robots, auth) enter the corpus as a
**declared** input, never by editing `corpus_raw`: `./hound.py artifacts list …` shows the
refusals, `./hound.py artifacts fetch --mask '*.pdf' --actor {who} [--reason robots_disallow]
[--dry-run]` downloads and appends to `sources/manual_artifacts.yaml`. That file's
`manual_artifacts_digest` is a contract field, so fetching a file is a **new `run_id`**
(`13-manual-artifacts.md` §3.5).

### 11.2 Development and CI

```bash
.venv/bin/python tests/selfcheck.py             # foundation check, no pytest
PYTHON=.venv/bin/python bash scripts/ci.sh      # ruff → mypy → anchors → selfcheck → pytest+cov

.venv/bin/ruff check core tests scripts
.venv/bin/python scripts/mypy_gate.py           # domain must be clean; core is baselined
.venv/bin/python scripts/mypy_gate.py --update  # regenerate baseline — deliberate act only
.venv/bin/python scripts/check_doc_anchors.py --strict  # path:line anchors in docs/, README.md, AGENTS.md
```

**Anchor trap:** removing lines from a tracked module breaks anchors in **other** tracked
documents (not just the one you edited). Always run the anchor gate after `core/` edits, and
re-point a moved symbol **by content**, not by arithmetic.

```bash
# Full suite. Parallelism is bounded by MEMORY, not by cores: -n auto takes nproc and OOMs
# on a 7 GiB host (~0.6 GiB RSS per worker; observed test_edge_launcher[forkserver] flake
# under load, passes in isolation). Check `free -m` first; use -n 3..4.
.venv/bin/python -m pytest -q -n 3 --dist worksteal
.venv/bin/python -m pytest -q -n 3 --dist worksteal -m "not slow"
```

CI runs workers as `min(4, nproc)`; `COHOUND_CI_WORKERS=N` overrides, `COHOUND_CI_WORKERS=auto`
restores `-n auto` (meaningful only with RAM headroom). Spot-run the tests you touched first
(`.venv/bin/python -m pytest tests/pipeline/test_x.py::test_name` or `-k pattern`) — the full
suite runs once, after all lanes are reconciled.

Browser smoke test (separate environment with browser binaries, exit codes 0 ok / 1 fail /
2 skip when deps are absent): `python3 tests/smoke_scrapy_playwright.py`.

**Environment.** Python **3.14** (pinned baseline; greenfield, controlled deployment, all stack
deps ship cp314/abi3 wheels — `05-dependency-stack.md` §1). Base `dependencies` — пять:
`pydantic`, `pyyaml`, `pypdfium2` (механизм `text_layer`, ось обязана быть исполнимой без
extra), `markdown-it-py`+`mdit-py-plugins` (чанкер, `18`). `numpy` живёт в `retrieval`/
`embed-lite` и импортируется лениво.

```bash
# Extras are split on purpose: `extract` is torch-free (HTML + Office),
# `extract-pdf` pulls torch/torchvision for layout models.
uv sync --frozen --extra dev --extra retrieval --extra crawl --extra extract --extra extract-pdf
#   + --extra extract-archives for 7z/rar, --extra embed / embed-lite for embeddings,
#     --extra safety (prompt-injection guards), --extra ops (retry/structured logs).
# A REAL RUN also needs a DECLARED embedder model: embed.model_id + embed.revision in
# deploy.yaml (revision = HF sha, D3 15.11.2026) and the extra that can load it (embed or
# embed-lite). Without a declared model the dense branch is NOT built — embedder=None, no
# index_manifest/index_store, empty embed_model_id, embed_spec=null, R0 runs lexically with a
# visible wiring notice. The run is NOT refused for that (wiring.py, evaluate.py); it IS
# refused when the declared model cannot be built (missing extra/snapshot, embed.device=cuda
# without a device, embed.precision=fp16 without the torch path) — WiringError, never a
# silent substitution of another model or of fp32. The README quick-start includes the extra.
./hound.py doctor      # groups: runtime + deps:core/retrieval/crawl/extract/extract-pdf/extract-archives/safety/ops/embed/embed-lite/ann/pdf-measure/dev + env/config/layout/dirs/cli/foundation
```

`uv lock --check` passes; if you change dependencies, update the lock in the same commit or
`--frozen` refuses. **Editing the lock by hand is legitimate** — move the requirement where it
belongs (`dependencies` / extra / `requires-dist` marker) and confirm with `uv lock --check`.
A bare `uv lock` on this host (uv 0.11.16) re-resolves the whole graph and upgrades unrelated
packages (measured: 1886 changed lines, `aiohttp` 3.14.4 → 3.14.5 and ~10 more) — a dependency
upgrade nobody asked for, hidden inside a one-line change.

Third-party selection, licences and rejections live in
`docs/architecture/05-dependency-stack.md` (LangChain/LlamaIndex and all hosted parsers were
rejected); implementation status W1–W14, M0–M3 in `06-implementation-strategy.md`; zones and
the DMZ seam in `07-deployment-topology.md`; seam schemas in `contracts/seam/`. Source routing
(`crawl_sources.yaml`) expands `feed`/`channel` sources through `PollPort` (local
`LocalFeedPoller` ‖ split `RemoteFeedPoller` → seam `crawl/poll`) — `03-data-config-cli.md`
§D.2.1, `02-subsystems.md` §B.1. Env vars and external keys: `.env.example` + `03` §D.6.

## 12. Tooling

Host utilities, chosen by task rather than by habit. Behaviour measured 2026-10-31; versions
re-checked 2026-11-15 (`rg` 15.2.0, `py-spy` 0.4.3, `bat` 0.26.1 — the host upgrades them).

| Task | Tool | Invocation |
|---|---|---|
| Exact text / literal / non-code (yaml, sql, md) | `rg` 15.2.0 | `rg -n -t py 'pattern' core/` · `rg -F 'literal'` · `rg -e 'a|b'` |
| Files by extension/name/content | `fdfind` 8.6.0 (`fd` is a symlink) | `fdfind -e py -t f root_drain core/` · `fdfind -x rg -n 'pat' {}` |
| Code **structure** (callers, decorators, rewrites) | `ast-grep` 0.45.3 | `ast-grep run -p '$OBJ.insert($$$)' -l python core/` · `ast-grep outline -l python file.py` |
| Literal / regex substitution with a reviewable diff | `sd` 1.0.0 | `sd -p -F 'old' 'new' $(rg -l -F 'old' -t py core/)` |
| JSON | `jq` 1.6 | `jq -r '.results[].notes' run.json` |
| YAML / XML | `yq` 4.54.1 | `yq '.crawl' config/deploy.yaml` · `yq -o json …` |
| SQLite (runs, state) | `litecli` 1.17.1 | `litecli db.sqlite -e "select …"` |
| GitHub CI status / logs (repo is public) | `gh` 2.103.3 | `gh run list` · `gh run view --log-failed` — CI debugging without webfetch |
| Profiling a live process | `py-spy` 0.4.3 | `py-spy record -d 3 -r 50 -f speedscope -o /tmp/p.json -- .venv/bin/python …` |
| Structural diff (did the move change behaviour?) | `difftastic` 0.71.0 | `difft --color never --display inline /tmp/old new` |
| Symbols / usages / diff effects (structure, not text) | `brokk` (bifrost MCP; measured 2026-11-14) | `scan_usages_by_location` (target = the declaration **token**) · `search_symbols(patterns=[…])` · `get_summaries` / `get_symbol_sources` (file outline without reading 5 000-line monoliths) · `analyze_diff` / `cyclomatic_complexity` (delta base→worktree) · `blast_radius` (tests reachable from changed files, ~123 ms — first stop for "what does this patch touch") |
| Readable unified diff (presentation only) | `delta` 0.20.1 | `git diff \| delta --paging=never` |
| Reading files with syntax highlighting | `bat` 0.26.1 | `bat -p --line-range 100:160 file.py` |

Traps that cost time:

- `rg` and `fdfind` **honour `.gitignore`** — searching `runs/`, `cache/`, `extract_cache/`,
  `eval_cache/`, `corpus_raw/`, `index_store/` returns nothing and reads as "no such files".
  Use `rg -uu` / `fdfind -I` (and `-H` for hidden, e.g. `~/.cohound`).
- Never isolate call sites with `grep … | grep -v "def "` — it loses hits on wrapped
  definitions, lambdas and method references. "Who calls it / where is the decorator / rewrite
  every node" is `ast-grep`; "where does the word appear" is `rg`.
- `ast-grep`'s `sg` alias is deprecated; `sg` without arguments prints a banner. Use
  `ast-grep`. `ast-grep` rewrites write files in place and know nothing about parallel editing
  lanes — do not run rewrite mode over `core/` while other writers work there.
- `sd` is **regex by default** (`-F` for a literal pass) and rewrites **in place** — use
  `-p` first. It knows nothing about `.gitignore` or lanes either: feed it an explicit file
  list (`$(rg -l …)` or `$(fdfind …)`) instead of a bare `**/*` glob, or it will happily
  rewrite `runs/`, caches and generated artifacts.
- `difft` cannot read stdin: `git diff | difft` is useless. Materialize both sides
  (`git show HEAD:path > /tmp/old`, copy the working file to `/tmp/new`). `delta` is the
  opposite — it reads stdin and only *presents* a diff; it compares nothing, so it never
  replaces `difft` when the question is "did this move change behaviour".
- `py-spy` attach needs `sudo` on this host (`kernel.yama.ptrace_scope=1`). As a parent it
  needs no privileges. For a hung run, `sudo py-spy dump --pid <pid>` beats guessing from
  logs.
- `litecli` argument order matters: options must precede the database path.
- Counting and locating are `rg -c` / `rg -l` (or `-m N` to cap): a bare grep that dumps
  every match fills the tool-output cap, and the truncated tail is the part you never see.
- `pgrep -f 'core.cli run'` matches **its own** command line — a false ALIVE. Use
  `pgrep -f '[c]ore.cli run'`. Likewise `pkill -f "python -m pytest"` kills the wrapping shell
  command too.
- `brokk` answers are verified-or-honest: it either returns exact results or names its
  incompleteness (`forward_reference_incomplete`, `unresolvable_import_boundary`) — verified
  against `rg` on 2026-11-14. Re-measured 2026-11-15 (`timeout.execution: 180000` in place):
  - **`usage_graph` is not dead, but scope decides.** With `paths` naming one file it answers
    (`core/pipeline/gates.py`, depth 1: **52 edges / 22 nodes in 213 ms**); the **whole
    workspace** (`paths` omitted) still dies — `usage_graph exhausted its 60s request budget`.
    That budget is the **server's**, not the client's: raising `timeout.execution` cannot help,
    so "3/3 attempts failed" was a scoping defect as much as a budget one. The first call of a
    session is also cold and can hit a **4.5 s** internal budget (measured: `gates.py` failed
    at 4 638 ms, then answered in 213 ms) — a retry after `refresh` is the cheap fix.
  - `missing_tests` answers too: **49.6 s**, `complete`, 0 candidates on a docs-only diff
    (nothing to analyse — so this is a proof it runs, not that it scales to a code diff).
  - `blast_radius` (file-level test reachability from changed files) is **123 ms** and needs no
    warm-up; use it instead of `usage_graph` when the question is "which tests are in reach".
  - "Who calls X" is still `scan_usages_by_location`, and its target must be the **declaration
    token** (any other line/column → `invalid_location` with a recovery hint). Measured 15.11:
    `host_in_scope` → `no_external_usages` in 24 s, which matches the code (only
    `scope_admission` inside the same class calls it).
  - `rename_symbol` and rewrite tools **write files** and know nothing about parallel lanes —
    do not run them over the shared tree (same rule as `ast-grep` rewrite mode).
- Heredocs **work** in this shell tool: `cat <<'EOF'`, `bash -s <<'EOF'` and
  `python - <<'PY'` all ran (measured 2026-11-15, opencode 2.2.7, including nested `$()`,
  single and double quotes). An earlier note here claimed the opposite ("Portable shell
  scanner cannot analyze command: heredoc") — it is **not reproducible** on this host and
  was removed rather than kept as a superstition. Heredocs are fine for one-off scripts;
  files you intend to keep belong in `write`/`edit`.
- A code-graph MCP server is not part of the toolchain: its call-graph and dead-code answers
  were measured as wrong on this repo, while its complexity ranking and raw cypher were fine.
  It was removed from the opencode config on 2026-11-15; its niche is taken by `brokk` (above),
  whose usage answers must still be verified against `rg` when a whole-module claim is made.

## 13. Code conventions

- **Category neutrality of `core/`** — hard rule #1: no product/vendor/domain terms; all
  specifics are data in `packages/`. Gated on `core/domain`/`core/ports`/`core/pipeline`;
  the other layers are read by a human on a hint. Violating it blocks the case (spec §11.4).
- **Invariants are code**: Pydantic validators + SQL triggers, each new entity with its own
  checks and a `selfcheck` entry.
- **Strict typing**: Pydantic models with `extra="forbid"`; enum classes (`Status`, `Origin`,
  …) instead of string literals.
- **`StrEnum` everywhere**; source of truth is `docs/status-dictionary.yaml`, schemas mirror
  it. That includes new enums (`ranking_policy`, `gate`, `capability_policy`,
  `unknown_policy`, `pdf_pipeline`).
- **Copied third-party code** keeps its licence/copyright header; port by **copying**, not by
  depending on the external project; never copy code with no explicit licence.
- **Comments and docstrings in Russian**, identifiers in English.

## 14. Git conventions

- Atomic, meaningful commits; imperative subject with an area prefix: `core: …`, `docs: …`,
  `spec: …`, `tests: …`. Cite paths explicitly — never `git add -A` (data dirs and
  deliberately deleted files live in the working tree).
- No secrets. `runs/`, `index_store/`, `corpus_raw/`, `cache/`-family dirs, `__pycache__/` are
  ignored.
- Version boundaries per §9; do not move a field between classes to make a diff green.

## 15. Anti-patterns

- No domain terminology in `core/`.
- `failed` is never a status; N/A is never automatic.
- An anchor never carries weight or influence on statuses (order/focus only, anchor-blind).
- No "LLM with web search" inside the evaluator; do not restore env-based model reading.
- Do not solve public-site JS challenges by bypassing them: coHound works with **public sites
  only** — honest/hybrid rendering of a page's own JS challenge is executing the page, not
  bypassing it (spec §10). No CAPTCHA solving, no paywalls, no auth barriers.
- `ranking_policy` never overrides sensitivity demotion (requested ranking vs computed
  demotion is an allowed divergence: `requested` vs `computed`).
- Do not mutate immutable index builds; a new round/SERP means a new build.
- Do not widen crawl scope to undeclared hosts, and do not swap `Frontier.host_in_scope` for
  `_host_matches` (profile inheritance ≠ budget rights).
- Do not change a crawl axis "in place": axes are contract fields, and their change yields a
  new `run_id`.
- Do not derive the extraction device from the environment (`auto` deliberately absent).
- Do not gate documents' anchors on arithmetic: re-point by content, then run
  `scripts/check_doc_anchors.py --strict`.
