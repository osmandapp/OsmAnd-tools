import os
from typing import List, Tuple

from ..lib.database_api import ch_query, ch_insert

# Global Constants (from environment variables)
# PARALLEL = int(os.getenv('PARALLEL', '10'))
TEST_ID = int(os.getenv('TEST_ID', '0'))
"""
#      F1     F2     F3    F4
# F1  [0.0,                   ]   Max - , dup_sim = 0
# F2  [0.6,   0.0,            ]   Max F1, dup_sim = 0.6
# F3  [0.3,   0.9,   0.0      ]   Max F2, dup_sim = 0.9
# F4  [0.1,    - ,    - , 0.0 ]   Max F1, dup_sim = 0.1
"""
images_map = {}
indPhoto = 1


def best_dups(wikidata_id, files: List[Tuple[str, int]]):
    global indPhoto
    bests = []
    if len(files) == 0:
        return bests

    for orig_file, place_id in files:
        if wikidata_id != place_id:
            assert f"{wikidata_id} != {place_id}, {files}"

        variants = []
        score, mediaId, dup_files, dup_sims = images_map[orig_file]
        dup_scores, dup_media_ids = [], []
        for i, dup_file in enumerate(dup_files):
            if dup_file in images_map:
                dup = images_map[dup_file]

                dup_media_ids.append(dup[1])
                dup_scores.append(dup[0])
                if dup[0] > score or dup[0] == score and dup[1] > mediaId:
                    variants.append((wikidata_id, orig_file, mediaId, score, dup_file, float(dup_sims[i]), dup_files, dup_media_ids, dup_sims, dup_scores))
            else:
                dup_media_ids.append(-1)
                dup_scores.append(-1)

        if len(variants) > 0:
            if len(variants) > 1:
                best = sorted(variants, key=lambda x: x[5], reverse=True)[0]
            else:
                best = variants[0]
            bests.append(best)
            if TEST_ID > 0:
                print(f"{indPhoto}. {score}, {best[1]}, {best[2]}, {best[3]}, {best[4]}", flush=True)
                indPhoto += 1
    return bests


if __name__ == "__main__":
    if TEST_ID == 0:
        print(f"Clearing and recreating table prep_top_images_final_dups", flush=True)
        ch_query("DROP TABLE IF EXISTS prep_top_images_final_dups")
        ch_query("""CREATE TABLE prep_top_images_final_dups(
                "wikidata_id"      UInt64,
                "imageTitle"       String,
                "mediaId"          UInt64,
                "score"            Int32,
                "dup_file"         Nullable(String), -- if not empty then there is duplication
                "dup_sim"          Float32, -- 0 means no duplicate
                "dup_files"        Array(String),
                "dup_media_ids"    Array(Int32),
                "dup_sims"         Array(Float32),
                "dup_scores"       Array(Int32)
            ) ENGINE = MergeTree PRIMARY KEY (wikidata_id, imageTitle)""")

    where_clause = "" if TEST_ID == 0 else f"WHERE wikidata_id = {TEST_ID}"
    query = f"""
        SELECT D.wikidata_id, D.imageTitle, max(I.mediaId), max(S.score) score,
            arrayFlatten(groupArray(dup_files)), arrayFlatten(groupArray(similarity))
        FROM wiki.top_images_dups D
        JOIN wiki.wiki_images_downloaded I ON D.imageTitle = I.name
        LEFT JOIN  (SELECT imageTitle, proc_id, argMax(photo_id, run_id),
                        argMax(greatest(value_score, 0), run_id)     s_value,
	                    argMax(greatest(technical_score, 0), run_id) s_technical,
	                    argMax(greatest(overview_score, 0), run_id)  s_overview,
	                    argMax(greatest(safe_score, 0), run_id)      s_safe,
	                    argMax(greatest(reality_score, 0), run_id)   s_reality,
	                    CAST((s_value * 0.50 + s_technical * 0.20 + s_overview * 0.15 + s_reality * 0.15) * 1000, 'UInt32') as score
                FROM wiki.top_images_score WHERE version = 0
                GROUP BY imageTitle, proc_id) S
            ON S.proc_id = D.wikidata_id AND S.imageTitle = D.imageTitle
        {where_clause}
        GROUP BY wikidata_id, D.imageTitle
        ORDER BY wikidata_id, score DESC, D.imageTitle
    """
    print(f"Querying duplicate images ${query}", flush=True)
    result = ch_query(query)
    batch_wikidata = -1
    places = 0
    BATCH_INSERT = 1000
    res = []

    print(f"Processing ...", flush=True)
    prev_score = None
    same_scored = []
    for row in result:
        wikidata_id = row[0]
        imageTitle = row[1]
        mediaId = row[2]
        score = row[3]
        dup_files = row[4]
        dup_sims = row[5]
        if not prev_score:
            prev_score = score
        # New place id processing
        if wikidata_id != batch_wikidata:
            dup_bests = best_dups(batch_wikidata, same_scored)
            res.extend(dup_bests)

            if len(res) > 0:
                places += 1
                ch_insert("prep_top_images_final_dups", res)
                if places % BATCH_INSERT == 0:
                    print(f"Inserted {BATCH_INSERT} places - last place Q{batch_wikidata}...", flush=True)
            res = []
            batch_wikidata = wikidata_id
            images_map = {}
            same_scored = []
        if dup_files is None or len(dup_files) == 0 or imageTitle in images_map:
            continue

        # Calculate best duplicate with max similarity for higher scored images
        images_map[imageTitle] = (score, mediaId, dup_files, dup_sims)
        if prev_score != score:
            dup_bests = best_dups(batch_wikidata, same_scored)
            res.extend(dup_bests)

            prev_score = score
            same_scored = []

        same_scored.append((imageTitle, batch_wikidata))

    dup_bests = best_dups(batch_wikidata, same_scored)
    res.extend(dup_bests)
    if len(res) > 0:
        places += 1
        ch_insert("prep_top_images_final_dups", res)
