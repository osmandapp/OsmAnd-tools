CREATE INDEX idx_source_value_tag_value ON source_value(tag_id, value);
CREATE INDEX idx_object_source_value_tag_value_object ON object_source_value(tag_id, value_id, object_id);
CREATE INDEX idx_object_source_value_object_tag ON object_source_value(object_id, tag_id);
CREATE INDEX idx_object_source_value_token_object ON object_source_value(token_id, object_id);

CREATE INDEX idx_tag_type_name ON tag(type, name);
CREATE INDEX idx_subtag_tag_type_name ON subtag(tag_id, type, name);
CREATE INDEX idx_box_obf_parent ON box(obf_id, parent_id);
CREATE INDEX idx_box_tag_tag_subtag_box ON box_tag(tag_id, subtag_id, box_id);
CREATE INDEX idx_object_tag_tag_subtag_object ON object_tag(tag_id, subtag_id, object_id);
CREATE INDEX idx_object_tag_object_tag ON object_tag(object_id, tag_id);

CREATE INDEX idx_posting_token_object_obf ON posting(token_id, object_id, obf_id);
CREATE INDEX idx_posting_object_token ON posting(object_id, token_id);
CREATE INDEX idx_token_name_nocase ON token(name COLLATE NOCASE);
CREATE INDEX idx_object_type_id ON "object"(type, id);
CREATE INDEX idx_object_box ON "object"(box_id);

INSERT INTO token_stats(token_id, matched_count, alone_count, poi_matched_count, poi_alone_count, address_matched_count, address_alone_count)
SELECT t.id,
       COALESCE(s.matched, 0),
       COALESCE(s.alone, 0),
       COALESCE(s.poi_matched, 0),
       COALESCE(s.poi_alone, 0),
       COALESCE(s.address_matched, 0),
       COALESCE(s.address_alone, 0)
FROM token t
LEFT JOIN (
    SELECT p.token_id,
           COUNT(p.object_id) matched,
           COALESCE(SUM(CASE WHEN p.isAlone = 1 THEN 1 ELSE 0 END), 0) alone,
           COALESCE(SUM(CASE WHEN o.type = 'POI' THEN 1 ELSE 0 END), 0) poi_matched,
           COALESCE(SUM(CASE WHEN o.type = 'POI' AND p.isAlone = 1 THEN 1 ELSE 0 END), 0) poi_alone,
           COALESCE(SUM(CASE WHEN o.type <> 'POI' THEN 1 ELSE 0 END), 0) address_matched,
           COALESCE(SUM(CASE WHEN o.type <> 'POI' AND p.isAlone = 1 THEN 1 ELSE 0 END), 0) address_alone
    FROM posting p
    JOIN "object" o ON o.id = p.object_id
    GROUP BY p.token_id
) s ON s.token_id = t.id;

INSERT INTO source_tag_stats(tag_id, objects_count)
SELECT st.id, COUNT(DISTINCT osv.object_id)
FROM source_tag st
LEFT JOIN object_source_value osv ON osv.tag_id = st.id
GROUP BY st.id;
