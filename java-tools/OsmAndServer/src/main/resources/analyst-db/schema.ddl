CREATE TABLE obf (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- datasource-local OBF file id
    name TEXT NOT NULL -- OBF display/file name
);

CREATE TABLE token (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- datasource-local token id
    name TEXT NOT NULL UNIQUE, -- normalized token text
    isCommon INTEGER NOT NULL, -- token is classified as common by search heuristics
    isFrequent INTEGER NOT NULL, -- token is classified as frequently used
    isGenerated INTEGER NOT NULL -- token was generated from object tags instead of read directly from OBF name index
);

CREATE TABLE "object" (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Analyst-local object row id
    osm_id INTEGER PRIMARY KEY, -- canonical OSM id 
    name TEXT, -- display name selected for the object
    lat REAL, -- object latitude
    lon REAL, -- object longitude
    commonTags TEXT, -- JSON object tags used for direct token provenance analysis
    type TEXT, -- Analyst object type, for example POI or address subtype
    osmType TEXT, -- decoded OSM entity type, for example node, way, or relation
    box_id INTEGER, -- owning POI box id when object is decoded from OsmAndPoiBoxDataAtom
    parent_id INTEGER, -- address parent object (CITY for STREET, STREET for BUILDING) id for nested object hierarchy
    UNIQUE (osm_id, parent_id, box_id)
);

CREATE TABLE object_parent ( -- POI objects to higher level objects (CITY, BOUNDARY, SUBURB, POSTCODE) 
    object_id INTEGER NOT NULL, -- source object id
    parent_id INTEGER NOT NULL, -- target related object id, for example CITY, BOUNDARY, SUBURB, POSTCODE, or parent address object
    type TEXT NOT NULL, -- SPATIAL_INSIDE, CONTEXT_CITY, CONTEXT_BOUNDARY, CONTEXT_POSTCODE
    PRIMARY KEY (object_id, parent_id, type)
);

CREATE TABLE box (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- datasource-local POI box id
    obf_id INTEGER NOT NULL, -- OBF file containing this POI box
    parent_id INTEGER, -- parent POI box id for nested box hierarchy
    zoom INTEGER NOT NULL, -- POI box zoom level
    left_tile INTEGER NOT NULL, -- POI box left tile coordinate at box zoom
    top_tile INTEGER NOT NULL -- POI box top tile coordinate at box zoom
);

CREATE TABLE box_tag (
     box_id INTEGER NOT NULL, -- POI box containing this category/subtype/group summary
     tag_id INTEGER NOT NULL, -- classifier tag id
     subtag_id INTEGER NOT NULL, -- classifier subtag/value id
     frequency INTEGER NOT NULL DEFAULT 0, -- aggregate count for this classifier in the box
     PRIMARY KEY (box_id, tag_id, subtag_id)
);

CREATE TABLE object_tag (
     object_id INTEGER NOT NULL, -- POI object carrying this category/subtype/group relation
     tag_id INTEGER NOT NULL, -- classifier tag id
     subtag_id INTEGER NOT NULL, -- classifier subtag/value id
     frequency INTEGER NOT NULL DEFAULT 1, -- relation count, normally 1 for object-level facts
     PRIMARY KEY (object_id, tag_id, subtag_id)
);

CREATE TABLE tag (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- tag id
    name TEXT NOT NULL, -- tag/classifier name
    type TEXT, -- tag kind, 'type' / 'additional' / 'brand'
    UNIQUE(name, type)
);

CREATE TABLE subtag (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- subtag/classifier value id
    tag_id INTEGER NOT NULL, -- parent tag/classifier id
    name TEXT NOT NULL, -- subtag name or classifier child key
    value TEXT, -- optional raw value for subtype/group entries
    type TEXT, -- subtag kind, for example category, subtype, or group
    UNIQUE(tag_id, name, type)
);

CREATE TABLE source_tag ( -- source tag used for token provenance
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- source tag id
    name TEXT NOT NULL, -- object-level tag key used for token provenance
    type TEXT, -- source bucket, currently common for direct object tags
    UNIQUE(name, type)
);

CREATE TABLE source_value ( -- source value used for token provenance
   id INTEGER PRIMARY KEY AUTOINCREMENT, -- value id
   tag_id INTEGER NOT NULL, -- owning source tag id
   value TEXT NOT NULL, -- raw decoded tag value
   UNIQUE(tag_id, value)
);

CREATE TABLE token_source_value (
    token_id INTEGER NOT NULL, -- provenance token generated from this value
    object_id INTEGER NOT NULL, -- object containing this tag/value fact
    tag_id INTEGER NOT NULL, -- source tag key used by this fact
    value_id INTEGER NOT NULL, -- normalized source value dictionary id for this source tag/value
    value TEXT NOT NULL, -- duplicated raw value for faster inspection queries
    PRIMARY KEY (token_id, tag_id, value_id, object_id)
);

CREATE TABLE posting (
    obf_id INTEGER NOT NULL, -- OBF file containing this posting
    token_id INTEGER NOT NULL, -- token read from or generated for the index
    object_id INTEGER NOT NULL, -- object matched by this token
    sequenceId INTEGER NOT NULL, -- per-token object sequence number for stable display
    isAlone INTEGER NOT NULL DEFAULT 0, -- object has this token as the only searchable name token
    PRIMARY KEY (token_id, object_id, obf_id)
);

CREATE TABLE token_stats (
    token_id INTEGER PRIMARY KEY, -- token these aggregate counters belong to
    matched_count INTEGER NOT NULL, -- total objects posted for this token
    alone_count INTEGER NOT NULL, -- total posted objects where this token is the only name token
    poi_matched_count INTEGER NOT NULL, -- POI objects posted for this token
    poi_alone_count INTEGER NOT NULL, -- POI objects where this token is alone
    address_matched_count INTEGER NOT NULL, -- address objects posted for this token
    address_alone_count INTEGER NOT NULL -- address objects where this token is alone
);

CREATE TABLE source_tag_stats (
    source_tag_id INTEGER PRIMARY KEY, -- source tag these aggregate counters belong to
    objects_count INTEGER NOT NULL -- distinct objects containing this source tag
);
