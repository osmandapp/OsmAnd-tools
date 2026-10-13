# Top organization and word abbreviations: development plan

Basis: [top-abbrs-spec.md](top-abbrs-spec.md) ("spec"). 2026-10-13.

## Principles

- **S1–S4 — one PR, S5 — a separate PR.** All generation and analysis code lives in `tools`
  (`java-tools/OsmAndMapCreatorUtilities`); from `android/OsmAnd-java` only existing search normalization is used
  (`SearchAlgorithms`, `SearchModLocales`, `SearchModRules`). If it needs a change — a separate small PR in `android`.
- **Sequential.** Each stage depends only on previous ones.
- **Plan boundary is the `utilities.sh` command.** Jenkins configuration (`Planet_WorldAbbrs`, env vars, publishing on
  builder, confirmation parameter) is out of scope.
- **Test data.** Unit tests on synthetic `.osm` fixtures in `src/test/resources` (no network or large files).
  Measurements on small real PBFs (NY county, DE state), not committed.
- **H5** is built with the rest of the code, disabled by a schema flag (`<propagate enabled="false">`) until labeled.

## Order

```
S1 extraction ─► S2 reading extractions ─► S3 shared code ─► S4 analyzer ─► S5 generator ─► S6 step 0 and acceptance
```

## PR 1: S1–S4 (PR in `tools`)

Stages 1–4 go in one PR: extraction, reading, shared code and analyzer can only be verified together, on an
abbreviation list from real data. Code: `net.osmand.obf.preparation.PoiNamesExtractor` and package
`net.osmand.util.abbrs` (`PoiExtractionReader`, `RegionTree`, `AbbrText`, `TopAbbrRow`, `TopAbbrPredicate`,
`AbbrMatcher`, `TopAbbrsRules`, `TopAbbrsAnalyzer`, `TopAbbrsReport`); default rules `top-abbrs-rules.xml` next to the
class.

**Done when:** extracted abbreviations analyzed over all PBFs in `pbf\` (Bremen, Wien, Saarland, Upper Franconia,
NYC): extractions via `generate-poi --poi-extraction-dir`, then `top-abbrs-analyzer --period=<YYYY-MM>`; row review (correct, false,
reasons) and rejected candidates go into the PR description. Debug on the smallest PBFs.

**Deferred from S2–S4** (follow-up PRs before enabling the job):
- `RegionTree` is built from `regions.ocbf`, not `.parent`/`.depth`/`.map` from `osm-extract` (not available locally);
  upward summing and service-region collapsing are not needed without H5 and ownership;
- H5 (`<propagate>`) — off until labeled, not implemented;
- diff with last month, labeling sample, auto-publish conditions, `perMap` limit;
- bilingual name splitting (spec 12.15).

## S1. POI extraction from generation (PR 1)

The most important and expensive step: its input defines all counts and is run and evaluated several times.

**Scope** (spec 5.3):
- `PoiNamesExtractor`; extraction dir `IndexCreatorSettings.poiExtractionDir` / `--poi-extraction-dir=` /
  `POI_EXTRACTION_DIR` (like `POI_TOP_INDEXES_URL`); without it generation is unchanged.
- Entity file — in `IndexPoiCreator.iterateEntityInternal` with the entity's first POI that passed the bounds filter:
  raw tags (case kept), once per `(osm_type, osm_id)`; there also the map `obf_id → (osm_type, osm_id, center)`.
- POI file — in `processPOIIntoTree`, when a `poi` row enters the OBF: `osm_type`, `osm_id`, `center`, `obf_id`,
  `type`, `subtype`, `additional`, `lat`, `lon`.
- Only real entities with a name, `operator` or `network`, and their POIs.
- Command `utilities.sh poi-extraction-summary`: entity and POI counts, size, share with `operator`/`network`, types.

**Done when:**
- tests (`PoiNamesExtractorTest`): relation with several centers (all POIs extracted, `obf_id` match OBF); object
  dropped by the bounds filter (not extracted); entity with two subtypes (both POIs extracted); OBF with and without
  the flag match by decoded POIs;
- measured on small PBFs: extraction size, generation time increase — in the PR description.

## S2. Reading extractions and region tree (PR 1)

**Scope** (spec 5.3):
- `PoiExtractionReader`: reads all maps' extractions; duplicates by POI identity `(osm_type, osm_id, center, type,
  subtype)` — freshest extraction, then largest `.depth`, then first by name; different POIs of one entity are kept.
- `maxExtractionAgeMonths`: older extractions are not used, such maps listed in the report; a map outside the region
  tree is not counted.
- Per-entity statistics: types = union of its POIs' `subtype`/`additional`; region and point = center `0`.
- `RegionTree`: `.parent`, `.depth`, `.map` from `osm-extract`; snapshot `regions.csv`; upward summing via `.parent`,
  collapsing service regions without `.map`, as in `TopTagValuesAnalyzer.finishReading`.

**Done when:** tests for duplicates between country and part maps, extraction age, collapsing (`east-asia`), entity
with several POIs.

## S3. Shared code: normalization, CSV row, application predicate (PR 1)

The contract between analyzer and generator: a mismatch here breaks everything, so it is fixed by tests before both
consumers.

**Scope:**
- Normalization (spec 5.2.1): split by `;`, `splitAndNormalize`, `alignChars`, "phrase in name" by word boundaries;
  brand normalization via `TopTagValuesAnalyzer.normalizeTagValue`.
- `top_abbrs.csv` row model (spec 7): read/write, `types` validation against `MapPoiTypes`.
- Application predicate (spec 5.2): name locale (`SearchModLocales.forName`, bilingual names — spec 12.15), `owner`,
  `types` (subtype or additional type), binding `operator`/`network`/`phrase`, `require_absent`, "abbreviation not yet
  in indexed names".

**Done when:** tests on DMV (`bind=operator`), Hbf (`kind=word`, `name:de` on an Italian map), H5 (`bind=phrase` +
`require_absent`), a bilingual name, an unknown `types` key.

## S4. Analyzer `TopAbbrsAnalyzer` (PR 1)

Java class in `net.osmand.util.abbrs`, command via `MainUtilities` / `utilities.sh`. Prototype `alias-proto/mine.py`
is a reference, not a gold standard: the inputs differ in objects and types.

**Scope:**
- Schema `top-abbrs-rules.xml` (spec 6): layers `defaults` → `<lang>` → `<lang>_<CC>`, validation on load, every filter
  can be disabled, schema hash.
- Inputs: extractions (S2), `<locales>` from `rules.xml`, `common_words_groups.tsv`, `brands.lst`.
- H1 (`same`, `near`, sub-span), noise filters, case filter (spec 12.8).
- H2 + H3 on `operator`/`network` groups; H5 (`bind=phrase`, `require_absent`, `maxSpread`) behind the flag.
- Agreement before thresholds (spec 5.2.5), thresholds in unique entities, row kind, row scope (spec 12.2), brands
  (spec 12.10), `abbr` conflict within the list, `include`/`exclude`, limits.
- Output: `top_abbrs.csv`, `top_abbrs.html` (inclusion/exclusion reasons, "brand aliases", "cut by case", diff with
  last month), manifest (spec 8.4).
- Labeling sample (spec 12.13), carrying labels by row key; auto-publish conditions (spec 12.11) — command exit code.
- Determinism: stable sort.

**Done when:**
- unit tests on fixtures: DMV group yields `dmv` by `operator`, Hbf yields `kind=word`, `bp ← blanco petroli` dropped,
  `include`/`exclude` with the same key is a schema error, a repeated run gives the same CSV;
- run on small PBFs; prototype labels (`alias-proto/out*/review_*.tsv`) carried over by `(abbr, full)`, per-stratum
  precision in the PR description.

## S5. Applying in the generator (PR in `tools`)

**Scope** (spec 8):
- `TOP_ABBRS_URL` / `IndexCreatorSettings.topAbbrsUrl`; download CSV and manifest, verify hash; with URL set and an
  error — generation fails.
- Rows for a map by `owner` and `locale`; filter by locale `rules*.xml` (`SAME_AS_RULE`, `CONFLICT_RULE`, ignored
  words, `<class1>`, pair already given by `<index><rule>`, ambiguous `abbr`, unknown `types`).
- Binding via the S3 predicate before `putPoiObjectPrefix`.
- New entry point "list alternative" next to `addAlternativeNames` → `addRuleAlternative`; support for objects without
  `name`.
- Logs `TOP_ABBRS_MANIFEST`, `TOP_ABBRS_RULE` (objects and keys per row).

**Done when:** tests modeled on `AlternativeNameRulesTest`: `dmv` is a key with marker `spellvariantcommon`, its atom
merges with the object; `Hannover Hbf` — `replacedWords`; H5 not applied when `operator` is set; rule conflict — skip;
without `TOP_ABBRS_URL` the OBF is unchanged; a broken manifest is an error.

## S6. Step 0 and acceptance (run and report, not a PR)

- Paired build of control maps (NY, DE states, one map without rows) with and without the list.
- DMV and Hbf sets, simulator (r27/r28 and general rounds), POI name index growth.
- Stage 1 and 2 metrics of spec 11 (stage 2 after labeling `bind=phrase` rows and enabling H5).
- Result — per-query report: which loss is fixed, which remains and why.

## Risks

- **Step 0 after implementation.** The order is sequential, so if an alias weighted `NAME_OTHER` − `wRuleSpelling`
  does not fix Hbf losses, this surfaces after S4–S5. Moving to `NAME_ALIAS` (spec 12.6) is a separate search task.
- **Extraction cost.** If S1 measurements show notable time or space growth, the extraction must be narrowed before
  enabling on all maps.
- **First release** waits for one monthly generation with extraction enabled.
