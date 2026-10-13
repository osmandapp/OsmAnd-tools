# Top organization and word abbreviations: specification

Status: ready for implementation; 2026-10-13. Basis: prototype `alias-proto/` (hypotheses H1–H5, "POI Alias Prototype"
report). Modeled on top brands (`TopTagValuesAnalyzer` → `brands.lst` → `POI_TOP_INDEXES_URL` →
`IndexPoiCreator.collectTopIndexMap`).

## 1. Goal

Search finds an object by a common abbreviation that is in neither its name nor `poi_types.xml`/`phrases.xml`:

- **organization abbreviation**: "dmv college point" → office `name=Queens North Traffic Violations Bureau`, whose
  only "DMV" is in `operator=New York State Department of Motor Vehicles`;
- **name-word abbreviation**: "Hannover Hbf" → `name=Hannover Hauptbahnhof`.

The list is not hand-written: a periodic Jenkins job builds it from OSM data using rules in an XML schema. The OBF
generator reads the list and adds abbreviations to the POI name index; search finds them as ordinary keys.

## 2. Motivation

| Case     | Why it is not found now                                                                                                                                                | Prototype result                                                                                                             |
|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| DMV (NY) | `operator` is not indexed for search (`ObfConstants.isTagIndexedForSearchAsName`: only `name`/`brand` tags); `operator:short=NYSDMV` is one word and not stored in OBF | baseline: 22/22 offices with "dmv" in name at #1, **0/27** without. H2+H3 derives `dmv` for the `operator` group (25 objects, 64%) |
| Hbf (DE) | `Hbf` is not linked to `Hauptbahnhof` in the index; the `<query> hbf→Hauptbahnhof` rule in `rules_de.xml` matches with a form penalty                                  | with the rule at #1: target station 78–79/99, any hit 81–82/99 (71/74 without); at #2 the station loses to literal "Hbf" objects |
| Others   | USPS, NYPD, SUNY, BRK, DLRG, INPS, MVV, Bf/Bhf, Str, Ffw…                                                                                                              | H1: 2640 pairs (top-50 precision 80–88%); H2+H3: 175 acronyms (94–97%)                                                       |

Manual rules (`rules*.xml`, `phrases.xml`) do not scale to thousands of organizations and are bound to a language, not
an organization: a "DMV" synonym for `government=transportation` in `phrases.xml` applies to all English maps, though
in the UK it is DVLA.

## 3. Terms

- **Abbreviation** (`abbr`) — short form: `dmv`, `hbf`.
- **Full form** (`full`) — what is abbreviated: phrase `department of motor vehicles` or word `hauptbahnhof`.
- **Binding** (`bind`) — which objects get the abbreviation:
    - `operator` / `network` — objects with this normalized tag value (DMV);
    - `phrase` — objects whose name contains the full form (Hbf, H5 for DMV).
- **Gain** (`gain_objects`) — objects that do not yet have the abbreviation in any indexed name.
- **H1** — pairs of two spellings of one name:
    - source `same`: `name` vs `short_name`/`alt_name`/`official_name`/`name:<lang>` of one object;
    - source `near`: names of neighboring objects (≤ 500 m, same first word; each relation center is a point, the
      neighbor cells are chosen in meters, so their longitude span grows with latitude);
    - with sub-span search inside the long side.
- **H2** — a word shared by names of objects in one `operator`/`network` group and absent from the tag value.
- **H3** — H2 filter: the word is built from first letters of consecutive words of the value (DMV ⊂ Department of
  Motor Vehicles).
- **H5** — propagating a found organization abbreviation to objects with the full phrase in their name and no
  `operator` tag.
- **Extraction** — by-product of map generation: entities and accepted POIs as the generator wrote them (5.3).
- **OSM entity** — `(osm_type, osm_id)`; unit of threshold counting.
- **POI identity** — `(osm_type, osm_id, center, type, subtype)`; unit of application and coverage. One entity may
  yield several POIs.
- **Application predicate** — condition "CSV row applies to POI"; shared by analyzer and generator (5.2).
- **Ownership** — region holding at least the `ownership` share of the row's supporting entities; the row applies in
  it and its descendants (12.2).

## 4. Data flow

```
 [A] Jenkins: TopAbbrsAnalyzer                       [B] Map generation               [C] Search
 ┌──────────────────────────────┐                   ┌──────────────────────────┐      ┌──────────────────┐
 │ POI extraction of last ──────┼─► H1 candidates   │ TOP_ABBRS_URL            │      │ key "dmv" in     │
 │ month's generation (names,   │   H2/H3 candidates│  → top_abbrs.csv         │      │ POI nameIndex    │
 │  operator, types, point)     │   ↓               │ IndexPoiCreator:         │      │ → object atom    │
 │ top-abbrs-rules.xml ─────────┼─► filters, thresh.│  filter by rules*.xml    │ OBF  │ → name match     │
 │ (thresholds, excl., manual)  │  row scope        │  bind → objects          ├─────►│                  │
 │                              │   ↓               │  addRuleAlternative      │      │  (v2 only)       │
 │                              │ top_abbrs.csv     │                          │      │                  │
 │                              │ top_abbrs.html    │                          │      │                  │
 └──────────────────────────────┘                   └──────────────────────────┘      └──────────────────┘
```

## 5. Component A: `TopAbbrsAnalyzer` (Jenkins job)

New class in `net.osmand.util` next to `TopTagValuesAnalyzer`, invoked via `MainUtilities` as a separate command. Runs
as a separate job (`Planet_WorldAbbrs`) after the month's map generation (12.12); CSV, report and manifest are
published on builder next to `brands.lst`.

### 5.1. Input

1. **POI extraction** of last month's map generation (5.3): one record per POI — OSM type and ID, point, map, OsmAnd
   subtype and additional types, raw `name`, `name:*`, `short_name`, `alt_name`, `official_name`, `operator*`,
   `network*`, `brand*`, wikidata, and per POI the names the generator put into its name index. Same objects, types
   and indexed names as in OBF.
2. **Region tree** — `.parent`, `.depth`, `.map` from `/home/osm-planet/osm-extract/` (5.3).
3. **Rules** `top-abbrs-rules.xml` (section 6).
4. **Map locales** — `<locales>` table from `rules.xml`: region → locale → word-statistics group.
5. **Word classes** — `common_words_groups.tsv`: service and frequent words never become abbreviations.
6. **Brands** — `brands.lst`: an abbreviation equal to a `brand` value of group objects is dropped (`bp ← blanco
   petroli` error); brand pairs — 12.10.

### 5.2. Algorithm

All steps and thresholds are set in XML (section 6); code holds only mechanics:

1. **Normalization** — one for analyzer, generator and search, via existing search functions (SEARCH.md 9.1):
    - tag values (`operator`, `network`, names) are first split by `;`;
    - words — `SearchAlgorithms.splitAndNormalize(value, false)`: `canonicalizePunctuation` (apostrophes → `'`,
      brackets → space); a word is letters and digits, `.` and `'` are part of a word, `-` is part of a word only next
      to a digit (`NA-75`), otherwise a separator; `normalizeToken` — NFC and lower case;
    - `abbr`, `full`, `bind_value` and the H5 phrase are stored as such words joined by single spaces; words are
      compared after `alignChars` (apostrophes, `ß` → `ss`, diacritics), as in search;
    - "phrase in name" — consecutive name words, bounded by this function's word boundaries, not a substring;
    - brands are compared via `TopTagValuesAnalyzer.normalizeTagValue`, as in `brands.lst` (12.10).

   Original case is kept separately for the case filter (12.8).
2. **H1 (by name).** Pairs from `same` and `near`; align common prefix and suffix; check "short is an abbreviation of
   long" (one word: letters in order, same first letter; phrase: first letters of words, chunks ≤ N letters); sub-span
   search (`same` only); noise filters (dot gluing, Roman numerals, compound parts with linking -s-/-es-, service
   words). Aggregated by key `(locale, abbr, full)` with counts per source and POI type.
3. **H2 (by operator/network).** Groups by normalized value; a shared word with share ≥ threshold, not in the value,
   not class 1/2.
4. **H3.** Acronym of a span of the value; check options (up to the last significant word after stripping legal forms
   and places, minimum length) are schema parameters.
5. **Agreement before thresholds.** H1 and H2+H3 candidates are first collected with lowered thresholds (`minSupport` ×
   `supportFactor`, `minShare` × `shareFactor`), then agreement is marked: pair found by both H1 and H2+H3 (DMV, USPS,
   DLRG, INPS, SUNY). Only then thresholds apply: relaxed for agreed, full for the rest. Agreement is also the
   `requireH1` condition for H5 (12.7).
6. **Row kind** is determined by the full form's shape and binding, not by source:
    - `kind=word` — `full` is one word; alias is the object name with that word replaced (`Hannover Hbf`). Source: H1;
    - `kind=org` — `full` is a multi-word phrase; alias is the single word `abbr`. Binding `operator`/`network` (H2+H3)
      or `phrase` (H5).
   An H1 phrase pair without H2+H3 confirmation (ZOB ← zentraler omnibusbahnhof, DMV from names) enters the CSV in v1
   only via `<include>`; the report shows such pairs in a separate section.
7. **Row scope** (12.2). `bind=phrase` — region ownership like brands: region with supporting entities ≥ `ownership`
   share (default 0.7); the row applies to the owner and its descendants. `bind=operator`/`network` — no region
   (`owner=*`). `kind=word` — language or locale by the number of locales with support (`ownership wordLocales`).
8. **Manual edits.** Order: found rows → `<exclude>` (veto) → `<include>` (add unfound, ZOB). `<include>` and
   `<exclude>` with the same key is a schema load error. Mandatory generator filters (8.2: conflict with rules and
   between rows, `types` check) apply to `<include>` too.
9. **Output.** `top_abbrs.csv`, `top_abbrs.html` (like `brands.html`) and manifest (8.4): per row — source, support,
   sample objects and reason for inclusion or exclusion, for manual review.

**Counting units.** All thresholds count **unique OSM entities** `(osm_type, osm_id)`: `minSupport`, `minGroup`,
`minShare`, `minTiles`, `ownership`, `wordLocales`, `maxSpread`. Coverage — `gain_objects` and publishing limits — is
in POI identities (5.3). H1 pairs (`support_pairs`) are diagnostics only: multiple names of one object and many
neighbor pairs inflate them.

**Shared application code.** The predicate "CSV row applies to POI" (name locale, `owner`, `types`, binding,
`require_absent`, "abbreviation not yet in names") is one class shared by analyzer and generator. The analyzer uses it
for `gain_objects`, `maxSpread` and publish coverage; the generator to apply rows. Both work on the same object set
and the same names: the phrase of `bind=phrase` and "abbreviation not yet in names" are checked in the names of the
POI name index (`names` of the extraction, 5.3), not in raw tags.

### 5.3. Extraction: by-product of map generation

The analyzer reads POIs as the OBF generator accepts them, not extracts. Map generation writes the extraction itself;
there is no separate data pass. Cost: one month delay — the M+1 list is built from month M maps (12.1, 12.12).

**Where it is written.** Class `PoiNamesExtractor`, called from `IndexPoiCreator`. Two files in the extraction dir:
`<map>.poi-entities.tsv.gz` and `<map>.poi-records.tsv.gz`, `<map>` = OBF name without `.obf`. Written as `*.tmp`,
closed after the POI section and renamed only after the whole OBF is written and closed; deleted if generation fails
at any step. The reader finds a map by its records file, so publishing removes the old records file first and renames
the new one last; the first line of both files is the same meta line `# poi-extraction data_date=<ms> generated=<ms>`
(`data_date` — date of the map's OSM source file, `IndexCreator.setLastModifiedDate`; `generated` — generation
start), and a pair with different meta lines is not read.

1. **Entity** — in `iterateEntityInternal`, together with its first POI that passed the generator's pre-insert
   filters: map bounds (`bboxFilter.shouldFilterPoiEntity`/`shouldFilterPoiAmenity`), relation centers
   (`collectRelationCenters`), skipping POIs without coordinates. Raw tags (`e.getTags()`, case kept) are written once
   per `(osm_type, osm_id)`. Here `obf_id → (osm_type, osm_id, center)` is also remembered: the entity cannot be
   recovered from `obf_id` because a relation id shifts by `+2` on collision.
2. **POI** — in `processPOIIntoTree`, when a `poi` table row enters the OBF tree. Writing after
   `insertAmenityIntoPoi` is wrong: it only adds to a batch (`addBatch`), and a duplicate `(id, type, subtype)` key
   surfaces later on another insert's `executeBatch`; also `processPOIIntoTree` drops rows itself (wiki duplicates by
   `wikidata`, ids with `PROPAGATE_NODE_BIT`). A POI is written only if its `obf_id` was remembered in step 1.

Enabled by an extraction dir: `IndexCreatorSettings.poiExtractionDir`, option `--poi-extraction-dir=<dir>` or env
`POI_EXTRACTION_DIR`, like `POI_TOP_INDEXES_URL`. Without it generation is unchanged. No extraction for basemap
(`generateBasemapIndex`) or `recreateOnlyBinaryFile` mode (entities are not iterated). Only real OSM entities (id > 0,
no `PROPAGATE_NODE_BIT`) are written whose raw tags have a name (`name`, `short_name`, `alt_name`, `official_name`
with language variants), `operator*` or `network*`, plus their POIs. An object with only `brand` is not extracted,
though in OBF it has a name from the brand.

| File | Field | Source |
|---|---|---|
| entities | `osm_type`, `osm_id` | source entity (`node`/`way`/`relation`, real OSM ID: for nodes and ways `id >> ObfConstants.SHIFT_ID`) |
| entities | `tags` | JSON object of raw tags with keys `name`, `short_name`, `alt_name`, `official_name`, `operator`, `network`, `brand` (each the key itself and `<key>:*`) and `wikidata`; keys sorted |
| POI | `osm_type`, `osm_id`, `center` | entity reference; relation center number (`0` otherwise) |
| POI | `obf_id` | POI ID in OBF (relation centers: `id + 2 * center`) |
| POI | `type`, `subtype`, `additional` | category, subtype key (`government`) and keys of non-text additional types joined by `;` (`government_transportation`), without `top_index_*` |
| POI | `lat`, `lon` | point of the written POI (31-bit coordinates of the `poi` row, 6 digits) |
| POI | `names` | JSON object of the names of the POI name index as `putPoiObjectPrefix` gets them: text additional tags with `ObfConstants.isTagIndexedForSearchAsName` (`name`, `name:*`, `brand`, `name_etymology`…) and `name:en` — the transliteration of `name` when there is no `name:en` |

The meta line is followed by a header. Tabs and newlines in `type`/`subtype` are replaced by spaces. Summary:
`utilities.sh poi-extraction-summary <dir or files> [--top=20]` — entity and POI counts, size, share of entities with
`operator`/`network`, frequent types and additional types.

**Identity.**
- **OSM entity** `(osm_type, osm_id)`: threshold counting (5.2), names and binding tags;
- **POI** `(osm_type, osm_id, center, type, subtype)`: application and coverage. One entity may yield several POIs —
  several categories/subtypes (station and stop) and several relation centers. `subtype` is the OBF subtype as is: a
  tag with several values gives one POI with the keys joined by `;` (`amenity=cafe;restaurant` → `cafe;restaurant`);
  its types are the keys `cafe` and `restaurant` (entity types, row `types`, type counts), never the joined value.

**Duplicates across maps.** Generation is per map, not per leaf: a country map and its parts' maps share objects,
maybe of different OSM data (a map not rebuilt this month). An entity is taken in **one state**: of the maps with the
newest `data_date` that have it. Its POIs from maps with older data are stale and dropped (`stale POI` in the report):
a cafe turned restaurant must not yield both POIs with the new name. Within one `data_date`, deduplicate by **POI**
identity, not entity: different POIs of one entity are kept (relation centers in different maps). Of several copies of
one POI, take the copy of the latest generation, then the deepest map (largest `.depth`), then first by name; the
entity's tags come from its first kept POI. No separate leaf coverage check: an object in a "hole" between child
polygons stays in the parent map.

**Per-entity statistics.** Entity types — union of subtype keys and `additional` of all its POIs; a row with `types` is
supported by an entity if at least one POI matches. Entity region and point for ownership — center `0`; in `near`
each relation center is a separate point, but the entity counts once.

**Month and completeness.** The analyzer runs after month M generation finishes with the explicit period
`--period=M` (`YYYY-MM`, the month of the OSM data of that generation); ages are counted from it, not from the newest
input, so a stale input directory is not taken for a fresh one. If a map was not built this month, its latest
extraction is used if its data month is no older than `<input maxExtractionAgeMonths>` (2) months before M; older
ones and ones with data after M are not, and the map's objects are not counted this month (listed in the report). A
map missing from the region tree is not counted.
The manifest (8.4) records the period, each map's data and generation dates and the hashes of both extraction files. The first list
release waits for one generation with extraction enabled.

**Checks before enabling on all maps.** On one large map measure extraction size and generation time increase. Tests
(`PoiNamesExtractorTest`): relation with several centers (all POIs extracted, `obf_id` match OBF); object dropped by
the bounds filter (not extracted); entity with two subtypes (both POIs extracted); OBF with and without extraction
match by decoded POIs. No byte comparison: two runs without extraction already differ because POI rows are written in
different order.

**Region tree.** Ownership and levels come from `.parent`, `.depth`, `.map` of extracts
`/home/osm-planet/osm-extract/<region>/` — those the generator builds OBF from (`indexes-batch-generate*.xml`:
`siteToDownload="/home/osm-planet/osm-extract/{0}/{0}.pbf"`). The job saves a tree snapshot (`regions.csv`: region,
parent, depth, map flag) and writes its hash to the manifest. Map counts are summed up via `.parent`; service regions
without `.map` (`east-asia`) are collapsed, as in `TopTagValuesAnalyzer.finishReading`.

**How extracts are cut** (Jenkins job `Planet_Generate_OSM_Extract`, no schedule):

1. **Tree.** `utilities.sh generate-obf-extract-script` (`GenerateExtractScript`) builds the region tree from the
   repository's region structure and polygons (`CountryOcbfGeneration`). Each region gets `<region>.poly`, `.depth`,
   `.parent`, `.map` (if `hasMapFiles()`); the parent gets `extract.json`, `extract_2.json`, …: children with polygons,
   at most 20 per file (`MAXIMUM_OSMIUM_SPLIT`). Metadata is rewritten on every run, separately from data.
2. **`.parent` is the cutting parent, not the logical region.** It is the region's `singlePolyExtract` or its parent if
   that has a polygon: for `afghanistan_asia` it is `east-asia`, a service region without a map. A region without a
   polygon or cut parent is skipped with `WARN` and gets no extract.
3. **Cascade.** Top level is cut from `planet-latest.o5m` (`osmium extract -c extract.json --strategy=smart`), then
   depths 1…8 each cut their children from their own PBF (`--strategy=smart -S types=multipolygon`). A child is a
   subset of its parent; the whole tree has one data snapshot.
4. **Border.** `smart` strategy: a line crossing the polygon goes into the extract whole, as does a multipolygon. An
   object on a border between neighbors is in both; a point node is in one.
5. **Month.** With `REGENERATE=true` the previous `<region>.pbf` is renamed to `__<region>.pbf`, and the new file's
   mtime is set to the 1st of the month 00:00 UTC: file time is the month, not the cut moment. `.generated` marks
   "children of this region are cut", unrelated to OBF.

## 6. Rules schema `top-abbrs-rules.xml`

Purpose: tune selection without code changes — narrow (precision) or widen (recall) the output.

```xml
<top-abbrs version="1">
    <defaults>
        <ownership share="0.7" wordLocales="2"/>
        <case org="upper" word="capital" share="0.6" skipAllCapsNames="true"/>
        <limit perMap="500" planet="20000"/>
        <publish firstRelease="manual" maxMapChange="0.02" maxRowGain="2000"/>
        <input maxExtractionAgeMonths="2"/>
        <review perStratum="20" highGain="1000"/>
    </defaults>

    <!-- H1: pairs of spellings of one name -->
    <pairs>
        <source id="same" tags="short_name alt_name official_name name:{lang}" span="true" spanMaxWords="6"/>
        <source id="near" radiusM="500" sameFirstWord="true" maxShortLen="4"/>
        <abbr minLen="2" maxLen="8" maxChunk="3"/>
        <long maxWords="3"/>
        <noise unglue="true" roman="true" compound="true" compoundLinks="s es" serviceWords="true"/>
        <accept minSupport="3" minTiles="2" tileZoom="10"/>
    </pairs>

    <!-- H2 + H3: organization abbreviations -->
    <groups>
        <group tag="operator"/>
        <group tag="network"/>
        <accept minGroup="3" minShare="0.25" acronym="required" glued="separate"
                headWord="true" stripTail="legal place" min2LetterShare="0.6"/>
        <exclude brandValues="true"/>
        <propagate enabled="false" sameLocale="true" sameTypes="true" requireH1="true"
                   minPhraseWords="2" maxSpread="5" excludeBrands="true"/>
    </groups>

    <!-- pair found by both H1 and H2+H3: thresholds are multiplied by factors (5.2.5) -->
    <agree supportFactor="0.5" shareFactor="0.5"/>

    <!-- locale layer: language, then language_country -->
    <locale id="de">
        <pairs>
            <accept minSupport="5"/>
        </pairs>
        <stop words="der die das und für von zu am im an"/>
        <legal words="gmbh mbh ag kg ev"/>
        <places words="stadt kreis landkreis"/>
    </locale>

    <!-- manual edits (5.2.8) -->
    <include kind="org" locale="de" abbr="ZOB" full="zentraler omnibusbahnhof" bind="phrase"
             bind_value="zentraler omnibusbahnhof" require_absent="" types="bus_station"/>
    <exclude locale="en_US" abbr="nys"/>
    <exclude abbr="state" full="south arterial"/>
</top-abbrs>
```

**Defaults and meaning**

| Element, attribute | Default | Meaning |
|---|---|---|
| `ownership share` | `0.7` | owner region's support share for `bind=phrase` (12.2) |
| `ownership wordLocales` | `2` | `kind=word` is language-level if supported in this many locales (12.2) |
| `case` | `upper` / `capital` / `0.6` / `true` | case filter (12.8) |
| `limit perMap`, `planet` | `500`, `20000` | rows per map and total; excess cut by descending `gain_objects` |
| `publish` | `manual`, `0.02`, `2000` | auto-publishing (12.11) |
| `source same span`, `spanMaxWords` | `true`, `6` | sub-span search in the long side, `same` only |
| `source near radiusM`, `sameFirstWord`, `maxShortLen` | `500`, `true`, `4` | neighbors for `near`; short side at most `maxShortLen` |
| `abbr minLen`, `maxLen`, `maxChunk` | `2`, `8`, `3` | abbreviation length; chunk of one word in a phrase abbreviation |
| `long maxWords` | `3` | words in the long side of an H1 pair |
| `pairs accept minSupport` | `3` | unique entities supporting an H1 pair |
| `pairs accept minTiles`, `tileZoom` | `2`, `10` | support from this many distinct zoom-10 tiles (~40 km) |
| `groups accept minGroup`, `minShare` | `3`, `0.25` | `operator`/`network` group size and share of objects with the H2 word |
| `groups accept stripTail` | `legal place` | before H3, strip trailing legal forms (`<legal>`) and places (`<places>`) |
| `agree supportFactor`, `shareFactor` | `0.5`, `0.5` | threshold relaxation on H1 and H2+H3 agreement |
| `propagate` | 12.7 | H5 |
| `input maxExtractionAgeMonths` | `2` | oldest map extraction the analyzer accepts (5.3) |
| `review` | 12.13 | sample of rows for precision labeling |

**Places (`<places>`).** Place words for `stripTail="place"` — names of place POIs from the same map's extraction
(`place` subtypes `state`, `province`, `city`, `town`, `village`, `hamlet`, `borough`) plus the locale layer's
`<places words>`.

**Layers.** `<defaults>` → `<locale id="<lang>">` → `<locale id="<lang>_<CC>">`; locale as in `rules.xml`. A lower
layer replaces an element's **attribute**; word lists (`<stop>`, `<legal>`, `<places>`) are replaced whole.

**Manual edits.** `<include>` defines a full CSV row: `kind`, `locale`, `owner` (default `*`), `abbr`, `full`,
`bind`, `bind_value`, `require_absent` (mandatory for `bind=phrase`, section 7), `types`. `<exclude>` sets any subset
of key columns and removes all matching rows. Order and priority — 5.2.8.

**Schema requirements:**

- **Validation on load** — error on: unknown element or attribute; `span` on `near`; `minLen` > `maxLen`; a `types`
  key absent from `MapPoiTypes` subtypes and additional types; `<include>` and `<exclude>` with the same key.
- **Every filter can be disabled**, to measure its cost.
- **Schema hash** goes into the manifest (8.4).

## 7. Format of `top_abbrs.csv`

UTF-8, header, one row = one application rule.

| Column            | Example (DMV)                                 | Example (Hbf)                              | Description                                                                                                |
|-------------------|-----------------------------------------------|--------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `kind`            | `org`                                         | `word`                                     | `org` — phrase → single word `abbr`; `word` — one name word → `abbr` (5.2.6)                               |
| `locale`          | `en_US`                                       | `de`                                       | data locale; for `kind=word` — language or locale (12.2)                                                   |
| `owner`           | `*`                                           | `*`                                        | owner region for `bind=phrase` with `kind=org` (5.2.7); `*` — no region                                    |
| `abbr`            | `DMV`                                         | `Hbf`                                      | abbreviation in canonical spelling, for the report                                                         |
| `full`            | `department of motor vehicles`                | `hauptbahnhof`                             | full form, normalized                                                                                      |
| `bind`            | `operator`                                    | `phrase`                                   | `operator` / `network` / `phrase`                                                                          |
| `bind_value`      | `new york state department of motor vehicles` | `hauptbahnhof`                             | normalized tag value or phrase                                                                             |
| `require_absent`  |                                               |                                            | `bind=phrase` only: tag the object must lack (`operator` for H5 of an `operator` group); empty — applies to all objects with the phrase |
| `types`           | `government`                                  | `railway_station public_transport_station` | `MapPoiTypes` keys: subtype or additional type (`government_transportation`); empty — any                 |
| `group_size`      | `25`                                          |                                            | objects in the `operator`/`network` group (H2)                                                             |
| `support_objects` | `16`                                          | (from extraction)                          | unique entities supporting the row: in a group — with `abbr` in the name (16/25 = 0.64); for H1 — entities with the pair |
| `support_pairs`   |                                               | `10173`                                    | H1 pairs (`same` + `near`); diagnostics only, not used in thresholds (5.2)                                 |
| `gain_objects`    | `9`                                           |                                            | extraction POI identities (5.3) the row will apply to: predicate holds and the abbreviation is not in their indexed names |
| `sources`         | `h2,h3,h1span`                                | `h1near,h1same`                            | how it was found                                                                                           |
| `status`          | `auto`                                        | `auto`                                     | `auto` / `manual-include`                                                                                  |

Empty cell — metric not applicable. `gain_objects` counts objects, not index size: OBF growth depends on shared keys,
other names and overlap with rules; index growth is measured by a paired build (section 11).

**Types.** An object matches if one of its subtype keys (5.3) or one of its additional types is in `types`. Each key is validated
against `MapPoiTypes` on load: the job does not publish a CSV with an unknown key; the generator skips such a row with
a log entry.

One organization may yield two rows: binding by `operator` (precise) and by `phrase` (H5, broader), with different
`bind`.

## 8. Component B: applying in the generator (`IndexPoiCreator`)

### 8.1. Getting the list

- **Source.** Env `TOP_ABBRS_URL` or `IndexCreatorSettings.topAbbrsUrl`, like `POI_TOP_INDEXES_URL` /
  `poiTopIndexUrl`. Without it generation is unchanged. The URL points to the generation month's file
  (`top_abbrs-<YYYY-MM>.csv`, built from last month's maps, 12.12), not the latest.
- **Rows for a map.** Those whose `owner` is `*`, the map region or its ancestor, and whose `locale` matches:
    - `kind=org` — `locale` equals the map locale per `<locales>`, or is its language (`<include kind="org"
      locale="de">` applies on `de_DE` and `de_CH` maps);
    - `kind=word` — `locale` (language `de` or locale `de_CH`) matches the **name** locale
      (`SearchModLocales.forName`): `name:de` on an Italian map is `de_IT`.

### 8.2. Matching objects

Rows are first filtered by locale rules (`SearchModRules`, same layering `rules.xml` → `rules_<lang>.xml` →
`rules_<lang>_<CC>.xml` as for the name index): for `kind=org` — map locale, for `kind=word` — name locale (8.1).
Rules win (12.4): a **conflict** with a rule is forbidden, the **same pair** is allowed.

If `abbr` has a locale rule `<query><rule from="abbr">`:

| Rule | Example | Row outcome |
|---|---|---|
| one value applicable to POI (`object` empty or contains `poi`), equal to `full` | `hbf → Hauptbahnhof object="poi"` with `full=hauptbahnhof` | **allowed** (`SAME_AS_RULE`): extends new OBF index; the rule keeps working on old OBFs |
| one value applicable to POI, but different | `dr → Doktor` with `full=drive` | skip (`CONFLICT_RULE`) |
| several values (`<to>` ≥ 2) for any `object` | `st` → Street / Saint | skip (`CONFLICT_RULE`): ambiguous; POI names also contain street names |
| values only for other owners (`object="street"`) | `pl → Place object="street"` | rule does not concern POIs; row goes through other checks |

A row is also skipped if:

- **`abbr` or `full` is an ignored word** (`<rule to="">`: `the`, `der`) or a locale `<class1>` word;
- **the pair is already produced by `<index><rule>` for the POI owner**: the index rule applied to `full` yields `abbr`;
- **`abbr` is ambiguous within the list**: the map has rows with the same `abbr` and different `full` whose application
  conditions overlap. Different `types` lists do not separate rows by themselves: one POI may have a subtype from one
  list and an additional type from another. Rows are separate only if no POI satisfies both predicates (5.2): the job
  checks on the extraction, the generator on the map's objects. All rows of the conflicting group are skipped; the job
  flags them in `top_abbrs.html` in advance;
- **a `types` key is unknown** to `MapPoiTypes` (section 7).

Each outcome is logged (`TOP_ABBRS_RULE`: `SAME_AS_RULE`, `CONFLICT_RULE` with the rule's `RuleId`, `SKIPPED_TYPES`).
The job applies the same checks to the report, but the generator decides: rules change more often than the list, and
the rule version used for the map is recorded (8.4). These filters apply to `<include>` rows too.

Then each remaining row's binding is checked (during POI preparation, before `putPoiObjectPrefix`):

- **`operator`/`network`** — the object's normalized tag value (split by `;`) equals `bind_value`. The tag is
  available: `operator` is stored in the POI as a text attribute.
- **`phrase`** — a name of the POI name index contains `bind_value` as consecutive words (5.2.1) **and**
  the object lacks the `require_absent` tag, if set. For H5 rows `require_absent` is the group tag
  (`operator`/`network`): only objects without it get the alias — exactly those the job counted for `maxSpread` and
  `gain_objects`. An object with a different `operator` does not get the phrase alias.
- **`types`**, if set — one of object's subtype keys or additional types is in the list.
- An object that already has the abbreviation among the words of its name-index names is unchanged.

### 8.3. Writing to OBF

The abbreviation is written as an **additional object name in the POI name index**:

```
OsmAndPoiIndex.nameIndex (4)
  IndexedStringTable: key "dmv" (prefix + tail, like any word)
  OsmAndPoiNameIndexData.atoms: object atom (tile zoom/x/y, poiIndInBlock, poiCategories, shiftTo)
```

- **Call path.** Via `<index>` rule alternatives — `AlternativeNameIndexGenerator.addRuleAlternative`. New generator
  entry point "add list alternative" next to `addAlternativeNames`, with per-CSV-row statistics instead of per-rule.
  Effects: marker `spellvariantcommon` with the same id (`atom.ruleSpelling`); "dmv queens north" merges atoms into one
  object (`mergeSame`); a new word is a key, a word replacing a non-key name word one-to-one is attached to shared keys;
  weight `NAME_OTHER` − `wRuleSpelling`.
- **`kind=org`.** One word `abbr`, without the rest of the name. Its word count differs from the name, `replacedWords`
  is empty, so `abbr` is a key. "dmv college point" combines "dmv" (object atom) and "college point" (place)
  spatially. An object without `name` (office with only `operator`) has no main name: the entry point must accept such
  objects — currently the generator is called only when a name exists (`IndexPoiCreator.putPoiObjectPrefix`).
- **`kind=word`.** Alternative name "name with word replaced" (`Hannover Hbf`). `replacedWords` gives
  `hbf → hauptbahnhof`; if the main name did not keep `hauptbahnhof` as a key, `hbf` attaches to shared keys
  (`hannover`), otherwise it becomes a key.
- **OBF format unchanged.** No new proto fields; old clients read the file. Readable does not mean the alias works:
  only search v2 supports it (section 9).
- **POI record unchanged.** The abbreviation is not added to `textValues`; the object card shows original tags.

### 8.4. Reproducibility and statistics

- **Manifest, not an OBF field.** OBF has no place for a list hash (`OBF.proto` has only `version`/`versionConfirm`),
  and a new field contradicts 10.6. The job publishes `top_abbrs-<YYYY-MM>.manifest.json` next to the CSV: hashes of
  `top_abbrs.csv`, `top-abbrs-rules.xml`, `rules*.xml`, `brands.lst`, `common_words_groups.tsv`, `poi_types.xml`,
  region tree snapshot `regions.csv`; `OsmAndMapCreator` version; per map — generation date and extraction file hashes
  (5.3). A regeneration within the month gives a different date and hash, so the input is unambiguous.
  The generator logs (`TOP_ABBRS_MANIFEST`) the manifest URL and hash, plus the hashes of `rules*.xml` and
  `poi_types.xml` it actually used (they may differ from what the job saw).
- **Generator check.** The generator downloads CSV and manifest and verifies the CSV hash matches the manifest and the
  manifest sits next to and describes this CSV (URL — 8.1, fallback to last month — section 11). If `TOP_ABBRS_URL` is
  set but CSV or manifest is unavailable, corrupt or mismatched, map generation **fails**: a silent build without the
  list would mix maps with and without aliases in one wave. Without `TOP_ABBRS_URL` generation is unchanged.
- **List change** means map regeneration.
- **Generation log.** `TOP_ABBRS_RULE` lines: per CSV row — objects that got the abbreviation and keys added.
  Analogous to `ALTERNATIVE_NAMES_RULE`.

## 9. Search

- **Only spatial search (v2) is supported**, with no changes: the alias is a POI name-index key with marker
  `spellvariantcommon` (8.3); the alias atom merges with the same object's atoms.
- **Weight in v2.** `SpatialSearchRanking.nameScore` compares query words with the object's real names, not the atom
  name. The alias is not among them, so the name component is `NAME_OTHER` (0.5), no `exact` bonus. The marker also
  subtracts `wRuleSpelling` (0.02) from the **final** score, not from `nameScore` — the same form penalty as `<index>`
  rule alternatives. The order of different objects also depends on distance, type, rating; "OSM name wins" is
  guaranteed only when choosing an atom of one object (`SpatialSearchToken.addAtom`). See 12.6.
- **Search v1 is not supported.** v1 re-checks the query against the object's names and extra tags after reading a
  candidate (`BinaryMapPoiReaderAdapter`, `matcher.matches` on `getName`, `getEnName`, `getOtherNames`,
  `isTagIndexedForSearchAsName` tags etc.). The alias is not there, so the object is dropped. v1 support is a separate
  task, out of scope.
- **Relation to `rules*.xml`.** The list only adds the abbreviation to objects with the full form; no reverse
  direction (12.9). A pair equal to a `<query>` rule (`hbf → Hauptbahnhof`) is allowed (8.2): in new OBFs it adds key
  `hbf`, the rule keeps working on old OBFs in both query directions. If a list pair needs the reverse direction, move
  it to `rules_<lang>.xml` manually.

## 10. Constraints and principles

1. **Stored name is not rewritten.** Abbreviations only add keys (as rule 2 of `rules-spec.md`).
2. **Explicit beats automatic.** Manual `include`/`exclude` and `rules*.xml` rules win over automatic findings.
3. **Data locale, not UI locale.** Bound to locale and owner region; the same `abbr` in different regions is
   different rows.
4. **Map data only.** `poi_types.xml`/`phrases.xml` untouched; object-type synonyms stay there.
5. **Size budget.** Per-map and planet row limits in the schema; name index growth measured by a paired build of
   control maps (section 11); the generation log (8.4) shows objects and keys per row.
6. **No new OBF fields.**
7. **Determinism.** Same input and schema give the same CSV (stable sort, no randomness).

## 11. Acceptance criteria

Metric kinds are reported separately: list quality (candidates), coverage (data), search quality (OBF).

**Step 0 — trial paired build before implementing the job.** The alias gets the same `NAME_OTHER` name component as a
`<query>` rule match plus the marker penalty (section 9), so a new key alone does not guarantee fixing Hbf losses to
literal "Hbf" objects. Build DE maps with a manual CSV (Hbf, Bhf) and without, and analyze each query: which loss is
fixed, which remains and why (name weight, type, distance). Hbf thresholds below are confirmed by step 0; if there is
no gain, moving to `NAME_ALIAS` (12.6) is a separate task with its own search verification, since it affects all
marker alternatives.

**Stage 1 — first version (H5 off).**

| Metric | Threshold | Prototype baseline |
|---|---|---|
| `kind=word` precision (sample labeling, 12.13) | ≥ 85% in each stratum | 81% top-50 (88% with `near maxShortLen=4`) |
| `kind=org` precision | ≥ 95% in each stratum | 95% top-50 (97% with `headWord`) |
| DMV: NY offices without "dmv" in name, target at #1 | ≥ 9/27 (by `operator`) | 0/27 |
| Hbf: target station at #1 | goal ≥ 95/99, confirmed by step 0 | 78–79/99 |
| Hbf: any hit at #1 (station or nearby "Hbf" object) | not below baseline | 81–82/99 |
| Simulator (r27/r28 abbr and general rounds): Not Found | 0 new | — |
| Simulator: target rank | no worse than baseline in any query; regressions listed in the report | — |
| POI name index growth | ≤ 1% per map, by **paired build** of control maps (with and without list) | not measured |

**Stage 2 — enabling H5.**

| Metric | Threshold |
|---|---|
| `bind=phrase` row precision (20 objects per row, 12.7) | ≥ 95% |
| DMV: target at #1 | ≥ 24/27 |
| All stage 1 metrics | no regression |

**Control maps** — those holding the test sets (NY, DE states), plus one map with no list rows (nothing changes
without rows). `gain_objects` in the job is only a review hint.

**Map release gate.** Each month the control maps are built first with the list (pilot), test sets and simulator run
on them. The full generation wave with this list starts only if the pilot meets the "no worse than baseline"
thresholds (Not Found, target rank, any Hbf hit). Otherwise the wave uses last month's CSV, and regressing rows get
`<exclude>`.

## 12. Decisions

1. **Job data source.** POI extraction as a by-product of last month's map generation (5.3). Objects, types, IDs and
   points are exactly those in OBF, with no extra data pass; raw extracts (`osmconvert`) would miss the generator's tag
   transforms, filters and OsmAnd type keys. Cost: one month delay and one generation with extraction before the first
   release. The analyzer is a separate job (`Planet_WorldAbbrs`) that does not touch `Planet_WorldBrands`.
   `PoiNamesDumper` stays for the prototype and quick checks.
2. **Ownership granularity** depends on row kind:

   | Row kind                              | Scope                                                               | `locale`                         | `owner`       |
   |---------------------------------------|---------------------------------------------------------------------|----------------------------------|---------------|
   | `kind=org`, `bind=operator`/`network` | tag value; no region, per-map row limit applies                     | group locale (`en_US`)           | `*`           |
   | `kind=org`, `bind=phrase` (H5)        | owner region, like brands (5.2.7)                                   | group locale                     | region (`us`) |
   | `kind=word`                           | data language, layered as `rules_<lang>.xml` → `rules_<lang>_<CC>.xml` | language (`de`) or locale (`de_CH`) | `*`        |

   `kind=word` row level: language if support (`support_objects` ≥ `minSupport`, unique entities) exists in at least
   `ownership wordLocales` locales of the language; otherwise the single locale that has it. A language-level row
   applies to all its locales, including data-poor ones (Liechtenstein). Name locale is by tag
   (`SearchModLocales.forName`): `name:de` on an Italian map is `de_IT`. Opting out in a country is done by that
   locale's rule (`rules_de_CH.xml`): the 8.2 filter uses the map locale.
3. **What is written to OBF:** an additional name (8.3), not an `operator` category. It covers all row kinds
   (`kind=word` and H5 included), needs no `poi_types.xml` changes and respects region ownership. An `operator`
   "network category" (USPS, DB Station&Service) may come later as a separate feature; it does not conflict.
4. **Boundary with `rules*.xml`.** Rules win, but only a **value conflict** is forbidden; the **same pair** is allowed
   (8.2), otherwise Hbf — the main motivation — would be excluded. Conflict: a rule with a different POI value or an
   ambiguous rule (`st`); a rule only for other owners (`pl` for streets) does not concern POIs. The generator
   filters by locale rules; the job flags the same cases in `top_abbrs.html`. Reasons to filter at generation: rules
   change more often than the job, the generator already loads locale rules, and the rule version is logged (8.4).
5. **Key decision:** as `<index><rule>` alternatives (`AlternativeNameIndexGenerator.addRuleAlternative`, SEARCH.md
   section 6): a new word is a key; a word that replaced a non-key name word one-to-one attaches to the name's shared
   keys (no shared keys — also a key). For `kind=org` `dmv` is always a key; for `kind=word` `hbf` is a key only where
   the full word was a key. No `keys` column in the CSV and no "always key" in v1; if needed later, it becomes a CSV
   column, not a schema attribute (the generator sees only the CSV).
6. **Ranking:** marker `RULE_SPELLING_COMMON` (`spellvariantcommon`), as for `<index><rule>` alternatives: name
   component `NAME_OTHER` (0.5), `wRuleSpelling` (0.02) subtracted from the final score; between a real-name atom and
   an alias atom of one object the OSM name wins (`SpatialSearchToken.addAtom`). No search change needed (SEARCH.md
   2.2, 8); app versions unaware of the marker see it as an extra name word, as with existing rule alternatives. This
   and decision 5 are one choice: the `addRuleAlternative` path. An object with "DMV" in its own name
   (`NAME_PREFIX`/`NAME_EXACT`) stays higher. Verify on the DMV set (section 11); if offices without "dmv" lose to
   objects with "dmv" inside the name (`NAME_CONTAINS`, 0.65), introduce a `NAME_ALIAS` level (≈ 0.8, between
   `NAME_CONTAINS` and `NAME_PREFIX`) as one search change for aliases and rules.
7. **H5 in v1:** implemented with restrictions, schema switch, off until labeled.

   H5 propagates an `operator`/`network` group alias (H2+H3) to objects without that tag whose name contains the full
   phrase ("Kings County Department of Motor Vehicles" → `dmv`). CSV row `bind=phrase`. DMV gain: +15/27 over 9/27 by
   `operator`; stage 2 of section 11 is unreachable without H5. Risks: a false acronym (`bp`, `ba`) spreads to all
   objects with the phrase; a short phrase ("post office" → `po`) matches unrelated names.

   Conditions for a `bind=phrase` row (`<propagate>` attributes, section 6):

   | Attribute        | Default | Meaning                                                                                                   |
   |------------------|---------|-----------------------------------------------------------------------------------------------------------|
   | `enabled`        | `false` | H5 is enabled after labeling                                                                              |
   | `requireH1`      | `true`  | pair `(abbr, full)` found by both H1 and H2+H3 (5.2.5)                                                    |
   | `minPhraseWords` | `2`     | significant phrase words, excluding service words (class 1), `<legal>` and `stripTail` places             |
   | `maxSpread`      | `5`     | entities with the phrase and without the group tag in the owner region ≤ `maxSpread` × group size; else the phrase is too generic |
   | `sameLocale`     | `true`  | only objects on maps of the group's locale                                                                |
   | `sameTypes`      | `true`  | the row's `types` column = subtypes of group objects                                                      |
   | `excludeBrands`  | `true`  | phrase does not equal a `brands.lst` value                                                                |

   **"Without tag" is carried in the CSV.** An H5 row has `require_absent` = group tag (`operator` or `network`); the
   application predicate (5.2) checks it identically in the job (`maxSpread`, `gain_objects`, publish coverage) and
   the generator (8.2). A manual `bind=phrase` row must set `require_absent` explicitly — a tag or empty for "all
   objects with the phrase".

   *Enabling condition* — stage 2 of section 11: label 20 objects per `bind=phrase` row in `top_abbrs.html`; precision
   ≥ 95%. The generator finds phrases in `name` and indexed names via a locale phrase dictionary keyed by first word;
   written via `addRuleAlternative` (8.3): a multi-word phrase gives no one-to-one replacement, so `abbr` is always a
   key.
8. **Letter case:** requirement by row kind.

   Case affects only pair selection in the job: index and query are lower-cased. Acronyms in OSM are upper case
   (`DMV`, `USPS`), ordinary words capitalized (`State`), word abbreviations mixed (`Hbf`, `Str.`). The filter matters
   mostly for H1, especially `near`, where a short ordinary word accidentally matches a phrase's first letters
   (`state → south arterial`); for H2+H3 H3 nearly replaces it. Raw case is in the extraction (5.3); only the analyzer
   normalizes (5.2.1); canonical `abbr` (section 7) is the most frequent raw spelling.

   Rules (`<case>` element, section 6):
    - `kind=org` (`org="upper"`): share of raw short-side spellings fully upper case ≥ `share`;
    - `kind=word` (`word="capital"`): share of spellings with capital first letter ≥ `share`; all caps not required;
    - `skipAllCapsNames`: a name of **two or more** lettered words, all upper case (`DEPARTMENT OF MOTOR VEHICLES`,
      `NYS DMV OFFICE`), is not counted — every word looks like an acronym there. A one-word name (`DMV`) and a name
      with only some words upper case (`Harlem DMV`) count normally: they are the acronym's support;
    - caseless scripts (Arabic, CJK, Thai, Georgian): filter off; other checks hold precision.

   `share="0.6"` is a starting value. `top_abbrs.html` shows a "cut by case" column so the filter's cost is visible on
   the first run; if recall loss is large, switch to a soft signal (case adjusts thresholds instead of cutting).
9. **Reverse direction:** none. The list only adds `abbr` to objects with the full form; a full-form query
   ("department of motor vehicles") does not find an object with only the abbreviation in its name ("Harlem DMV"). No
   `direction` column. Reasons: for `kind=org` a phrase instead of one word would make every phrase word a key on every
   abbreviated object, and the short form is ambiguous ("DMV" is also Deutscher Mieterverbund); acronyms are typed more
   often than full names. For `kind=word` the reverse direction, where needed, comes from `<query>` rules.
10. **Overlap with brands:** brand pairs are excluded from the list, to first assess how many there are and how useful.

    H1 finds pairs where one side is a `brand` value (`kfc ↔ kentucky fried chicken`, `credem`, `bnl`). They fit the
    list poorly: with `brand=KFC`, `kfc ← kentucky fried chicken` is the reverse direction (12.9); the alias would go
    into every chain object instead of once per map in the brand category; "kfc" already finds the brand category.

    Rule: a pair whose `abbr` or `full` equals (after `normalizeTagValue`) a `brands.lst` value does not enter
    `top_abbrs.csv`. `top_abbrs.html` shows such pairs in a "brand aliases" section: brand, both forms, support, whether
    the form is already in brand objects' `name:*`. This section decides whether to extend the brand mechanism (names
    without wikidata) as a separate task. Rule 5.1.6 (`bp ← blanco petroli`) remains as a special case.
11. **Review process:** review by diff with last month, auto-publish by coverage.
    - `top_abbrs.html` shows changes vs the previous CSV: new, removed and changed rows (by key
      `(kind, locale, owner, abbr, full, bind, bind_value)`; change = `types` or a significant shift of
      `support_objects`/`support_pairs`), and per row — `gain_objects` per map.
    - **Auto-publish by pair change, not by row or object count.** For each map, compare the sets of pairs "POI identity
      (5.3), normalized alias" of the old and new list on the current month's extraction. Change = added pairs **plus**
      removed, with no cancellation: replacing aliases on 1000 objects with aliases on 1000 others is 2000; changing
      `abbr` on the same objects is removal plus addition. The CSV is auto-published if on every map the change is
      ≤ `<publish maxMapChange>` (2%) of the map's named POIs and no row adds more than `<publish maxRowGain>` (2000)
      pairs. The row limit applies to changed rows too: widening `types`, `owner`, `locale` or dropping
      `require_absent` counts by added pairs. Otherwise the job waits for confirmation (Jenkins build parameter).
    - **First release** (`<publish firstRelease="manual">`) and any release after a schema or generator version change —
      only on confirmation.
    - Reviewer veto — `<exclude>` in `top-abbrs-rules.xml` via a repository PR; manual addition — `<include>` likewise.
      The schema is versioned in the repository; its hash goes into the manifest (8.4).
12. **Frequency and stability:** monthly, CSV frozen for the month.
    - Source is the map-generation extraction (5.3), so the cycle is shifted by a month: after month M generation the
      job builds the list from its extraction and publishes it as `top_abbrs-<M+1>.csv`; month M+1 generation applies
      it.
    - The generator takes its month's file, not the latest: `TOP_ABBRS_URL` (8.1) points to the month file. All
      regional builds of one wave get one list; the manifest (8.4) points to the specific file.
    - Re-publishing the same month (after review) replaces the file only before map generation starts; after that,
      edits wait for next month.
13. **Metrics in the job:** list checks in the job; search quality and size after map builds.
    - **Labeling sample** (`<review>`): top-50 does not characterize a list of up to 20,000 rows. Each month these
      strata are labeled: the month's new rows; each source (`h1same`, `h1near`, `h2h3`, `include`) separately; all
      rows with `gain_objects` ≥ `<review highGain>` (1000) entirely; of the rest — random `<review perStratum>` (20)
      rows per stratum. Labels are stored in the repository and carried across months by row key.
    - Auto-publish conditions (in the job): precision in **every** stratum (`kind=org` ≥ 95%, `kind=word` ≥ 85%,
      section 11), row count within `<limit>`, per-map pair change (12.11). Unlabeled stratum rows block
      auto-publishing. Failure means publishing only on confirmation. `gain_objects` is a review hint, not an index
      size estimate.
    - Simulator, DMV/Hbf sets and index growth need built OBFs. They run after generation: sets and simulator on the
      month's maps, index growth by a paired build of control maps with and without the list (section 11). Results go
      into the monthly report, separated by metric kind. Map wave release is gated by the control-map pilot
      (section 11).
14. **Compatibility of old maps and versions:** accepted as is, no measures.
    - Old OBFs and new app: `Hbf` is found via `<query>` (rules ship with the app and work on any OBF); `DMV` is not
      found until regeneration. Maps are rebuilt monthly (12.12); the window lasts until the user updates the map.
    - New OBFs and app versions without the `spellvariantcommon` marker: the marker reads as an extra name word, as with
      existing `<index>` rule alternatives (12.6).
    - Search v1 does not find aliases (section 9); the feature is limited to search v2.
15. **Multilingual regions:** split bilingual names by tags, falling back to excluding them from H1.

    `name:<lang>` tags are covered by 12.2 (name locale by tag). The open case is a main `name` with two languages in
    one string (South Tyrol "Neumarkt - Egna", Belgium, Switzerland): it has one locale (the map's), and H1 `near`
    would compare German and Italian words as spellings of one language.

    Rule (same in job and generator, 8.1):
    - if `name` is the values of two `name:<lang>` tags of the object joined by a separator (`" - "`, `" / "`,
      `" – "`) in any order, each part gets its tag's locale (`de_IT`, `it_IT`). H1 compares parts separately; a
      `kind=word` row with locale `de` applies only to the German part;
    - if `name` has a separator, the object has more than one `name:<lang>`, and the parts do not match them, the name
      does not participate in H1; in the generator it gets the map locale, as now.

## 13. Prototype references

- POI dump: `tools/.../net/osmand/util/PoiNamesDumper.java` (branch `poi-alias-proto`, not committed).
- Analysis: `alias-proto/mine.py` (`--span`, `--span-near`), labels in `alias-proto/out*/review_*.tsv`.
- Sets and runs: `alias-proto/sets/`, `alias-proto/runs/`, `alias-proto/evaluate.py`.
- Report: `alias-proto/report/index.html` (hypothesis toggles, clustering error gallery).
