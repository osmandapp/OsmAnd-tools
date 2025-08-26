## What is OSM-Live ?
OSM-Live process creates OBF files that contains changed OSM objects during a short period of time: for 10 (or more) minutes, for 1 day, for 1 month.
These files have specific naming, such as Us_texas_san-angelo_northamerica_**24_09_13**.obf.gz - for sorting after "main" OBF Us_texas_san-angelo_northamerica_**2**.obf.zip
- `*_24_09_13.obf.gz` - daily file (September 13 2024),
- `*_24_09_00.obf.gz` - monthly file (September 2024),
- `/25_08_20/*_16_50.obf.gz` - minutely file (16:40 - 16:50 August 20 2025)

**Purpose**: that file should have all complete updated / added objects, so if they are rendered /searched on top the month maps, maps looks exactly like freshly generated. For deleted objects special tag is generated osmand_change=delete, so they don't appear on the map

## Principle of OSM-id and OsmAnd-id

As we need to update objects we need to have concept of id & version, however objects can move from 1 place to completely another place. In order reading all objects in memory and keep Geo spatial indexes, we use special osmand-id that combines `<osm-id, version, geohash>`. If the object has moved geohash has changed in osmand-id and we generate for old object `osmand_change=delete` in old place and  new object with new osmand-id is present at new place.

## How we generate OSM-Live diffs

**Main principle**: 
- OSM: Generate `before.osm` and `after.osm` as complete as we can (to contain all changes that can influence OsmAnd Map) but as small as possible
- OBF: Then we genenerate from it `before.obf` and `after.obf`
- COMPARE: Compare OsmAnd objects and generate diff from it.
- SPLIT: Split world's `*_diff.obf` into different countries/region files.


### OSM Query 1. - Collect changed node, way, relations

Mainly roads & points are generated from Ways & Nodes and they are mostly generated from way & nodes. <br>
Using **Overpass** queries we got two main files: `*_before.osm` and `*_after.osm`.<br>
For `*_before.osm` (9:30) :
```
...[date:"2025-08-21T09:30:00Z"];
    // 1. get all nodes, ways, relation changed between START - END
    (
      node(changed:"2025-08-21T09:30:00Z","2025-08-21T09:40:00Z");
      way(changed:"2025-08-21T09:30:00Z","2025-08-21T09:40:00Z");
      relation(changed:"2025-08-21T09:30:00Z","2025-08-21T09:40:00Z");
    )->.a;
```
For `*_after.osm` (9:40) :
```
...[date:"2025-08-21T09:40:00Z"];
    // 1. get all nodes, ways, relation changed between START - END
    (
      node(changed:"2025-08-21T09:30:00Z","2025-08-21T09:40:00Z");
      way(changed:"2025-08-21T09:30:00Z","2025-08-21T09:40:00Z");
      relation(changed:"2025-08-21T09:30:00Z","2025-08-21T09:40:00Z");
    )->.a;
```

### OSM Query 1. - Propagating relation tags / multipolygons
In OsmAnd OBF file we do not save relation directly, but propagate tags from them to way and nodes a nd also create multipolygons from relations. So we need the code after to fetch relations that node/way belongs to and make them complete.

For example, we have relation [`restriction=only_right_turn`](https://www.openstreetmap.org/relation/8085812) and need to include to OSM-live OBF any member of this relation that was changed or the relation itself. In our case these are: [From way/1169124687](https://www.openstreetmap.org/way/1169124687), [Via node/206392306](https://www.openstreetmap.org/node/206392306), [To way/1170508451](https://www.openstreetmap.org/way/1170508451), [Relation ownself](https://www.openstreetmap.org/relation/8085812).

**Note:** after step 3 we fetch ways that were not changed and they are not complete (as we don't fetch parent relations). But this hasn't infuence on the result `diff.obf` file. Because these ways same incomplete on both files `before.osm` and `after.osm`.

### OSM Query 1. - Full Query

**Full Overpass query:**
```
    // 1. get all nodes, ways, relation changed between START - END
    (
      node(changed:\"$START_DATE\",\"$END_DATE\");
      way(changed:\"$START_DATE\",\"$END_DATE\");
      relation(changed:\"$START_DATE\",\"$END_DATE\");
    )->.a; 
    // 2.2 retrieve all ways for changed nodes to change ways geometry
    (way(bn.a);.a;) ->.a; // get all ways by nodes
    // 2.3 retrieve all relations for changed nodes / ways, so we can propagate right tags to them
    (relation(bn.a);.a;) ->.a;
    (relation(bw.a);.a;) ->.a;
    // 3. final step make all relations / way / node complete
    (way(r.a);.a;) ->.a; 
    (node(r.a);.a;) ->.a;
    (node(w.a);.a;) ->.a;
    .a out meta;
```
**Purpose**: all changed objects POI, Multipolygons, Roads, Points should be Fully present in this file. Object is changed if any node / way was changed. Note:
so if only parent relation of road has changed it's not necessarily present or complete in this file.

**Incomplete data in after/before file**
- However as we make these relations complete we fetch ways that were not changed and they are not complete (as we don't fetch parent relations again after step 3). So ways that are retrieved on 3rd step shouldn't endup in the final diff! 
- For example, if way is removed from relation, step (3. final step make all relations / way / node complete) won't fetch way and those will create issue like removing road https://github.com/osmandapp/OsmAnd/issues/23030#issuecomment-3092486987.

**Supported cases**:
- If multipolygon has changed (added / deleted ways) - OK 
- If road geometry is changed - OK (geohash might have collision 1/64, then old road will be present in old tile...) 
- If route has changed adding / deleting members - V1 ROUTES PARTIAL (delete is not supported), V2 ROUTES - OK
- If way geometry has changed inside Multipolygon / V2 route - OK
- If way geometry has changed inside V1 route - OK
- If route restriction is added - OK
- If route restriction is modified / deleted - NOT OK (way is missing in _after)


### OSM Query 2. - Support cases of deleted relations
This query mainly use for `<action type="delete">` of relation, when needs to be update all members of this relation.<br>
**Purpose:** get complete objects that are now or used to be **members** of changed/created/deleted relations.<br>
**Full Overpass query:**
```
   // get all relation changed between START - END
    (
      relation(changed:\"$START_DATE\",\"$END_DATE\");
    )->.a;
    // 1. retrieve all members of changed relation (nodes/ways) to set .b    
    (way(r.a);) ->.b; 
    (node(r.a);.b;) ->.b;
    // 2. complete ways
    (node(w.b);.b;) ->.b;
    // 3. find incomplete relations for all members to make road segments complete to set .c
    (relation(bw.b);) ->.c;
    (relation(bn.b);.c;) ->.c;
    // 3. print .b and .c, relations from set .a already in .c set
    .b out meta;
    .c out meta;
```
**Important Note.** - unique set of completed objects different in `before_rel.osm` and `after_rel.osm`! As we can't retrieve ways in `after_rel.osm` for deleted relations this mean that these ways are missing in `after_rel.osm`, but present in `before_rel.osm` and it later creates wrongly deleted ways. We compensate this issue by copying data from `before_rel.osm` (not modified node/ways) and `after.osm` (modified node/ways) to `after_rel.osm`, see utility `generate-relation-osm`.

### OSM 2. - Create `*_after_rel_m.osm`
For avoid getting many "deleted" objects in diff file (`diff_rel.obf`) need to copy node/ways to `after_rel.osm`:
- copy not modified from `before_rel.osm`
- copy modified from `after.osm` (avoid [bug of duplicates geometry](https://github.com/osmandapp/OsmAnd/issues/21561))
- avoid deleted using `diff.osm` (`<action type="delete">`)
After copying we save all data to new file `after_rel_m.osm`.<br>
```
    echo "### 1. Generate relation osm : $(date -u) . All nodes and ways copy from before_rel to after_rel " &
    $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-relation-osm \
        $DATE_DIR/src/${BASENAME}_before_rel.osm.gz $DATE_DIR/src/${BASENAME}_after_rel.osm.gz \
        $DATE_DIR/src/${BASENAME}_diff.osm.gz $DATE_DIR/src/${BASENAME}_after.osm.gz ${BASENAME}_after_rel_m.osm.gz
```

### OBF - Generate obf files:
**Incompleted routes and multipolygons:**<br>
After step 3 of "Full Overpass query" (**OSM Query 2. - Support cases of deleted relations**) we get many incompleted relation. For multipolygons (relation type=multipolygon) and routes (relation type=route) we use Overpass query all members, see [OverpassFetcher.java](https://github.com/osmandapp/OsmAnd-tools/blob/master/java-tools/OsmAndMapCreatorUtilities/src/main/java/net/osmand/obf/preparation/OverpassFetcher.java)
```
    echo "### 2. Generate obf files : $(date -u) . Will store into $DATE_DIR/obf/"
    $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_after.osm.gz  \
        --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" --upload $DATE_DIR/obf/ &
    $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_before.osm.gz  \
        --ram-process --add-region-tags --extra-relations="$LOW_EMMISION_ZONE_FILE" --upload $DATE_DIR/obf/ &
    $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address $DATE_DIR/src/${BASENAME}_before_rel.osm.gz \
        --ram-process --add-region-tags --upload $DATE_DIR/obf/ &
    $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-no-address ${BASENAME}_after_rel_m.osm.gz \
        --ram-process --add-region-tags --upload $DATE_DIR/obf/ &
    wait 
```

### COMPARE - Generate `*_diff.obf` files:
We need to make sure that diff file contains only properly changed and complete objects. So in both files entities are complete & correct.
```
        echo "### 1. Generate diff files : $(date -u)"
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-diff \
            ${BEFORE_OBF_FILE} ${AFTER_OBF_FILE} ${BASENAME}_diff.obf $DIFF_FILE &
        $OSMAND_MAP_CREATOR_PATH/utilities.sh generate-obf-diff-no-transport \
            ${BEFORE_REL_OBF_FILE} ${AFTER_REL_M_OBF_FILE} ${BASENAME}_diff_rel.obf $DIFF_FILE &
        wait
```
Where `$DIFF_FILE` is `*_diff.osm` file with OSM changesets that consist of `<action type="create">`, `<action type="modify">`, `<action type="delete">`

### COMPARE/OBF - Merge `*_diff.obf` and `*_diff_rel.obf`
On this step we copy all objects from `_diff_rel.obf` to `_diff.obf`.<br> 
If object already exist in `_diff.obf` then replace by object from `_diff_rel.obf` (except marked `osmand_change=delete`).<br>
```
$OSMAND_MAP_CREATOR_PATH/utilities.sh merge-obf-diff ${BASENAME}_diff_rel.obf ${BASENAME}_diff.obf ${BASENAME}_diff_merged.obf
```

### SPLIT - split whole world *_diff for countries:
```
$OSMAND_MAP_CREATOR_PATH/utilities.sh split-obf ${BASENAME}_diff.obf $RESULT_DIR  "$DATE_NAME" "_$TIME_NAME" --srtm="$SRTM_DIR"
```

## Issues
#### [Part of the road is missing after edits](https://github.com/osmandapp/OsmAnd/issues/23030#issuecomment-3205108026)
OSM editor deleted [relation](https://www.openstreetmap.org/relation/8060127) and changed tags in the [way](https://www.openstreetmap.org/way/536051747), the way was a member of the relation.<br>
Was manually generated diff files (generate-obf-diff): 
- `diff.obf` <= `25_08_08_10_10_after.obf` ∩ `25_08_08_10_10_before.obf` 
- `diff_rel.obf` <= `25_08_08_10_10_after_rel_m.obf` ∩ `25_08_08_10_10_before_rel.obf` <br>

And using BinaryInspector (inspector.sh) checked diff.obf:
```
./inspector.sh -vmap -vmapobjects ../tmp/live0808/obf/diff.obf | grep 536051747
```
checked diff_rel.obf:
```
./inspector.sh -vmap -vmapobjects ../tmp/live0808/obf/diff_rel.obf | grep 536051747
> Way types [osmand_change-delete (2415)] id 68614623735 osmid 536051747
```
Hmm, why:
1. [way 536051747](https://www.openstreetmap.org/way/536051747/history/14) is not present in `diff.obf` ? - We know that editor changed tags, but no any changes wasn't stored in `diff.obf`.<br>
After analyze changed tags we found that any change had no influence on map section (`-vmap`), so nothing stored to changes (`diff.obf`).
2. [way 536051747](https://www.openstreetmap.org/way/536051747) is marked as deleted (`osmand_change-delete`) in diff_rel.obf ? - We know that was deleted only [relation](https://www.openstreetmap.org/relation/8060127), but no [way](https://www.openstreetmap.org/way/536051747) itself!<br>

So, for fix it we need avoid marked was by `osmand_change-delete` or correctly process it. Check theses two places in the code:
1. Utility `generate-relation-osm` (RelationDiffGenerator.java) - because as we know this tool are copies objects from `*_before_rel.osm` to `*_after_rel.osm` - this is avoiding marking them by `osmand_change-delete`.<br>
Most logic solution is remove [&& !modifiedObjIds.contains(e.getKey())](https://github.com/osmandapp/OsmAnd-tools/blob/f40ef481a16fac296f8d25290707fe6c10427f38/java-tools/OsmAndMapCreatorUtilities/src/main/java/net/osmand/obf/diff/RelationDiffGenerator.java#L105). But is not, it's fix was added in the issue [Geometry duplication in live update](https://github.com/osmandapp/OsmAnd/issues/21561) for avoid add `*_before` objects with old geometry to `diff`.
2. Merging `*_diff.obf` and `*_diff_rel.obf` (`merge-obf-diff` ObfDiffMerger.java). <br>
Found [`commonMapData.put`](https://github.com/osmandapp/OsmAnd-tools/blob/70602a833ae93b64c1bc5d72d870abdd5deac96c/java-tools/OsmAndMapCreatorUtilities/src/main/java/net/osmand/obf/diff/ObfDiffMerger.java#L129) where are adding our `osmand_change-delete` way.<br>
Can we trust `osmand_change-delete` nodes/ways from `*_diff_rel.obf` ? - No, because as we know from this file we must process only modified nodes/ways, and created/deleted nodes/ways we must to process from `*_diff.obf`! See [PR with this fix](https://github.com/osmandapp/OsmAnd-tools/pull/1234).
