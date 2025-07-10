import datetime
import itertools
import logging
import os
from typing import Set, List, Tuple, Dict

logging.basicConfig(level=logging.INFO)

from clickhouse_pool import ChPool

# Maximum 4500 token - looks like 15 photos by 360px or by 720px
PHOTOS_PER_PLACE = int(os.getenv('PHOTOS_PER_PLACE', '40'))
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_PWD = os.getenv('CLICKHOUSE_PWD')
PROCESS_PLACES = int(os.getenv('PROCESS_PLACES', '999999'))  # Default 1000
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '100'))
MAX_PLACES_PER_QUAD = int(os.getenv('MAX_PLACES_PER_QUAD', '999999'))
# Weights for scores
SCORING_WEIGHTS = [float(w) for w in os.getenv('SCORING_WEIGHTS', '0.20, 0.20, 0.30, 0.30').split(",")]
QUAD = os.getenv('QUAD', '**')
MIN_ELO = int(os.getenv('MIN_ELO', '1750'))
MIN_ELO_SUBTYPE = int(os.getenv('MIN_ELO', '1000'))
SAVE_SCORE_ENV = int(os.getenv('SAVE_SCORE_ENV', '0'))  # Environment: 0 - Production, 1 - Test
VALID_EXTENSIONS_LOWERCASE = set(ext.lower() for ext in os.getenv('VALID_EXTENSIONS', 'png|jpg|jpeg').split('|'))
QUAD_ALPHABET = os.getenv('QUAD_ALPHABET', "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~")
MAX_EXECUTION_TIME = int(os.getenv('DB_TIMEOUT', '900'))
POI_SUBTYPE = ",".join([f"'{w.strip()}'" for w in os.getenv('POI_SUBTYPE', '').split(",")])

if not all([CLICKHOUSE_HOST, CLICKHOUSE_PWD]):
    raise ValueError("Missing required environment variables (CLICKHOUSE_HOST, CLICKHOUSE_PWD)")

pool = ChPool(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user='wiki',
    password=CLICKHOUSE_PWD,
    database='wiki',
    connections_min=1,
    connections_max=100,  # Adjust based on concurrency needs
    connect_timeout=3,  # Connection timeout
    settings={"max_execution_time": MAX_EXECUTION_TIME}
)


def ch_client():
    return pool.get_client()


def ch_query(query: str, client=None, with_column=False):
    if client is not None:
        if with_column:
            rows, meta = client.execute(query, with_column_types=True)
            result = []
            for r in rows:
                result.append(dict(zip([c[0] for c in meta], r)))
            return result
        return client.execute(query)

    with ch_client() as client:
        return ch_query(query, client, with_column)


def ch_query_params(query: str, params, client=None):
    if client is not None:
        return client.execute(query, params=params)

    with ch_client() as client:
        return ch_query_params(query, params, client)


def ch_insert(table: str, data, column_names: List[str] = None):
    with ch_client() as client:
        if not column_names:
            return client.execute(f"INSERT INTO {table} VALUES", data)
        return client.execute(f"INSERT INTO {table} ({','.join(column_names)}) VALUES", data)


'''
def ch_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, password=CLICKHOUSE_PWD,
                                         secure=False, username='wiki', database='wiki',
                                         settings={"max_execution_time": 5})

def ch_query(query: str, client=None):
    if client is not None:
        return client.query(query).result_rows

    with ch_client() as client:
        return client.query(query).result_rows

def ch_insert(table: str, data):
    with ch_client() as client:
        return client.insert(table, data)
'''


def process_quad(input_str, alphabet=None):
    if '*' not in input_str:
        return [input_str]
    if alphabet is None:
        alphabet = [s for s in QUAD_ALPHABET]

    options = []
    for char in input_str:
        if char == '*':
            options.append(alphabet)
        else:
            options.append([char])
    product_result = itertools.product(*options)
    return [''.join(combination) for combination in product_result]


def delete_erroneous_from(table: str):
    with ch_client() as client:
        return client.execute(f"DELETE FROM {table} WHERE error <> '' and error <> 'No images.'")


def populate_cache_from_db() -> Set[str]:
    """Queries the database and returns a set of already downloaded image names."""
    query = "SELECT DISTINCT name FROM wiki_images_downloaded"
    downloaded_names = set()
    try:
        with ch_client() as client:
            result = ch_query(query, client)
            downloaded_names = {row[0] for row in result}
        print(f"Populated cache from DB with {len(downloaded_names)} entries.")
    except Exception as e:
        print(f"Error populating cache from DB: {e}")
    return downloaded_names


def scan_and_populate_db(cache_root_folder: str) -> Set[str]:
    """
    Scans the cache directory, populates the database with file info,
    and returns a set of found image names.
    """
    print(f"Starting scan of cache folder: {cache_root_folder}")
    found_files_data = []
    found_names = set()
    batch_size = 10000
    total_inserted = 0

    try:
        for root, _, files in os.walk(cache_root_folder):
            for file in files:
                if is_valid_image_file_name(file):
                    try:
                        full_path = os.path.join(root, file)
                        relative_folder = os.path.dirname(os.path.relpath(full_path, cache_root_folder))
                        file_stat = os.stat(full_path)
                        timestamp = datetime.datetime.fromtimestamp(file_stat.st_mtime)
                        filesize = file_stat.st_size
                        original_name = file  # Placeholder: Adjust if you need to map back to original name

                        found_files_data.append([original_name, relative_folder, timestamp, filesize])
                        found_names.add(original_name)

                        # Insert batch when reaching batch size
                        if len(found_files_data) >= batch_size:
                            with ch_client() as client:
                                print(
                                    f"This code is not correct cause we need to insert mediaId - probably we're not going to use it any more")
                                # client.insert('wiki_images_downloaded', found_files_data,
                                #   column_names=['name', 'folder', 'timestamp', 'filesize'])
                                # total_inserted += len(found_files_data)
                                print(f"Inserted batch of {len(found_files_data)} records (total: {total_inserted})")
                                found_files_data = []  # Reset batch

                    except Exception as e:
                        print(f"Could not process file {full_path}: {e}")

        # Insert any remaining records that didn't make a full batch
        if found_files_data:
            print(
                f"This code is not correct cause we need to insert mediaId - probably we're not going to use it any more")
            # ch_insert('wiki_images_downloaded', found_files_data,
            #           column_names=['name', 'folder', 'timestamp', 'filesize'])
            # total_inserted += len(found_files_data)
            print(f"Inserted final batch of {len(found_files_data)} records (total: {total_inserted})")

        print(f"Scan found {len(found_names)} files. Successfully inserted {total_inserted} records into DB.")
    except Exception as e:
        print(f"Error during cache scan and DB population: {e}")
        # Optionally, you could re-raise the exception here if you want calling code to handle it

    return found_names


def insert_downloaded_image(name: str, folder: str, timestamp: datetime.datetime, filesize: int,
                            mediaId: int, namspace: int):
    """Inserts a record for a newly downloaded image."""
    try:
        ch_insert('wiki_images_downloaded', [[name, folder, mediaId, namspace, timestamp, filesize]],
                  column_names=['name', 'folder', 'mediaId', 'namespace', 'timestamp', 'filesize'])
        # logging.debug(f"Inserted record for {name} in {folder}") # Optional: debug level logging
    except Exception as e:
        print(f"Error inserting record for {name}: {e}")


def get_run_max_id() -> int:
    query = f"""
        SELECT max(run_id) FROM top_images_run LIMIT 1
    """
    result = ch_query(query)
    return result[0][0]


def get_dups_run_max_id() -> int:
    query = f"""
        SELECT max(run_id) FROM top_images_dups LIMIT 1
    """
    result = ch_query(query)
    return result[0][0]


image_columns = [("photo_id", 0), ("value_score", -1), ("technical_score", -1), ("overview_score", -1),
                 ("safe_score", -1), ("reality_score", -1),
                 ("value_reason", ''), ("technical_reason", ''), ("overview_reason", ''),
                 ("safe_reason", ''), ("reality_reason", ''), ("tags", []),
                 ("run_id", 0), ("proc_id", 0),
                 ("timestamp", datetime.datetime.now()), ("imageTitle", ''), ("score", 0), ("version", SAVE_SCORE_ENV)]

image_run_columns = [("run_id", 0), ("wikidata_id", -1), ("batch_id", 0),
                     ("wikititle", ""), ("started", datetime.datetime.now()), ("duration", 0),
                     ("prompt_tokens", 0), ("completion_tokens", 0), ("cached_tokens", 0),
                     ("prompt_photo_ids", []), ("scored_photo_ids", []), ("error", ''), ("version", SAVE_SCORE_ENV)]


def insert_place_batch(place: dict, images: List[dict]):
    data = []
    for img in images:
        values = [img[k] if k in img and img[k] else d for k, d in image_columns]
        data.append(values)

    row = [place[key] if key in place and place[key] else d for key, d in image_run_columns]
    # row = list(place.values())
    ch_insert('top_images_score', data)
    ch_insert('top_images_run', [row])


def insert_dups(run_id: int, place_id: int, size_map: Dict[str, int],
                sim_map: Dict[str, List[Tuple[str, float]]], started,
                duration: float, error: str, version: int):
    data = []
    if not sim_map or error:
        data.append([run_id, place_id, '', 0, [], [], [], started, duration, error, version])
    else:
        for k, v in sim_map.items():
            data.append(
                [run_id, place_id, k, size_map[k],
                 [t[0] for t in v],
                 [size_map[t[0]] for t in v],
                 [t[1] for t in v],
                 started, duration, '', version])

    ch_insert('top_images_dups', data)


def get_place_desc(place_id):
    query = f"""
        SELECT wikiDesc FROM wikidata WHERE id = {place_id} LIMIT 1
    """
    rows = ch_query(query)
    return rows[0][0] if rows and len(rows) > 0 else None


class ImageItem:
    def __init__(self, wikidata_id: int, path: str, size: int, media_id: int, is_processed: bool):
        self.wikidata_id = wikidata_id
        self.path = path
        self.size = size
        self.media_id = media_id
        self.is_processed = is_processed

    def __hash__(self):
        return hash((self.wikidata_id, self.path, self.media_id))

    def __eq__(self, other):
        return self.wikidata_id == other.wikidata_id and self.path == other.path and self.media_id == other.media_id


def _query_batch(query_template: str, all_ids: List[int], batch_size: int = 1000):
    results: Set[int] = set()
    for i in range(0, len(all_ids), batch_size):
        ids = all_ids[i:i + batch_size]
        if not ids:
            continue

        batch_results = ch_query_params(query_template, {'ids': ids})
        for row in batch_results:
            results.add(row[0])
    return results


# Used for Scoring and Duplication
def get_unscored_places_dups(place_ids: List[int]) -> List[int]:
    if not place_ids:
        return []
    query = f"""
        SELECT DISTINCT id FROM wikiimages
        INNER JOIN (SELECT name FROM wiki_images_downloaded 
            WHERE name IN (SELECT imageTitle FROM wikiimages WHERE id IN %(ids)s)) AS d ON d.name = wikiimages.imageTitle
        LEFT JOIN (SELECT 1 as is_processed, imageTitle, wikidata_id FROM top_images_dups
            WHERE wikidata_id IN %(ids)s) AS i ON i.imageTitle = wikiimages.imageTitle AND i.wikidata_id = wikiimages.id
        WHERE id IN %(ids)s AND is_processed = 0 AND wikiimages.imageTitle NOT IN (SELECT imageTitle FROM blocked_images)
    """
    return list(_query_batch(query, place_ids))


def get_unscored_places_images(place_ids: List[int]) -> List[int]:
    if not place_ids:
        return []

    query: str = """
        SELECT DISTINCT id FROM wikiimages
        INNER JOIN (SELECT name FROM wiki_images_downloaded 
            WHERE name IN (SELECT imageTitle FROM wikiimages WHERE id IN %(ids)s)) AS d ON d.name = wikiimages.imageTitle
        LEFT JOIN (SELECT 1 as is_processed, imageTitle, proc_id as wikidata_id FROM top_images_score
            WHERE wikidata_id IN %(ids)s) AS i ON i.imageTitle = wikiimages.imageTitle AND i.wikidata_id = wikiimages.id
        WHERE id IN %(ids)s AND is_processed = 0 AND wikiimages.imageTitle NOT IN (SELECT imageTitle FROM blocked_images)
    """
    return list(_query_batch(query, place_ids))


def get_image_dups(place_id, limit: int = -1) -> List[ImageItem]:
    q = f"""
SELECT -if(type = 'P18', 1000000, views) as score, id, w.imageTitle as imageTitle, w.mediaId as mediaId, d.filesize as filesize, is_processed FROM wikiimages as w
    INNER JOIN (SELECT name, filesize FROM wiki_images_downloaded WHERE 
        name IN (SELECT imageTitle FROM wikiimages WHERE id = {place_id})) AS d ON d.name = w.imageTitle 
    LEFT JOIN (SELECT 1 as is_processed, imageTitle, wikidata_id FROM top_images_dups WHERE imageTitle IN (SELECT imageTitle FROM wikiimages WHERE id = {place_id}) AND version = {SAVE_SCORE_ENV}) 
    as i ON i.imageTitle = w.imageTitle AND i.wikidata_id = w.id
WHERE id = {place_id} AND w.imageTitle NOT IN (SELECT imageTitle FROM blocked_images)
"""
    with ch_client() as client:
        image_paths = ch_query(q, client, with_column=True)

    sorted_result = sorted(image_paths, key=lambda x: (x['score'], x['id']))
    result = list(dict.fromkeys([ImageItem(row["id"], row["imageTitle"], row["filesize"], row["mediaId"], bool(row["is_processed"])) for row in sorted_result]))
    result = result if limit < 0 else result[:limit]

    return result


def get_image_scores(place_id: int, limit: int = -1) -> List[ImageItem]:
    q = f"""
SELECT -if(type = 'P18', 1000000, views) as score, id, w.imageTitle as imageTitle, w.mediaId as mediaId, d.filesize as filesize, is_processed FROM wikiimages as w
    INNER JOIN (SELECT name, filesize FROM wiki_images_downloaded WHERE 
        name IN (SELECT imageTitle FROM wikiimages WHERE id = {place_id})) AS d ON d.name = w.imageTitle 
    LEFT JOIN (SELECT 1 as is_processed, imageTitle, proc_id FROM top_images_score WHERE imageTitle IN (SELECT imageTitle FROM wikiimages WHERE id = {place_id}) AND version = {SAVE_SCORE_ENV}) 
    as i ON i.imageTitle = w.imageTitle AND i.proc_id = w.id
WHERE id = {place_id} AND w.imageTitle NOT IN (SELECT imageTitle FROM blocked_images)
"""
    with ch_client() as client:
        image_paths = ch_query(q, client, with_column=True)

    sorted_result = sorted(image_paths, key=lambda x: (x['score'], x['id']))
    result = list(dict.fromkeys([ImageItem(row["id"], row["imageTitle"], row["filesize"], row["mediaId"], bool(row["is_processed"])) for row in sorted_result]))
    result = result if limit < 0 else result[:limit]

    return result


# Used for Scoring and Duplication
def get_places(numeric_ids_str):
    query = f"""
        SELECT id, wikiTitle, lat, lon, poitype, poisubtype, categories, shortlink, elo FROM elo_rating
        WHERE id IN ({numeric_ids_str}) 
        ORDER BY elo DESC, id 
        LIMIT {PROCESS_PLACES}
    """

    return ch_query(query)


def get_places_per_quad(quad: str, skip_table: str = None) -> List:
    poi_sub_query = f"(poisubtype IN({POI_SUBTYPE}) AND elo >= {MIN_ELO_SUBTYPE} OR elo >= {MIN_ELO})" if POI_SUBTYPE else f"elo >= {MIN_ELO}"
    query = f"""
        SELECT id, wikiTitle, lat, lon, poitype, poisubtype, categories, shortlink, elo FROM elo_rating 
            WHERE startsWith(shortlink, '{quad}') AND {poi_sub_query} 
                {(' AND id NOT IN (SELECT wikidata_id FROM ' + skip_table + ')' if skip_table else '')}
            ORDER BY elo DESC, shortlink, id 
            LIMIT {min(PROCESS_PLACES, MAX_PLACES_PER_QUAD)}
    """
    return ch_query(query)


# Used for Downloading
def get_images_per_page(page_no: int) -> List[Tuple[int, List[Tuple[str, int, int]]]]:
    file_ext_conditions = " OR ".join(
        f"endsWith(lower(imageTitle), '.{ext}')" for ext in VALID_EXTENSIONS_LOWERCASE
    )
    query = f"""
    SELECT id, groupArray({PHOTOS_PER_PLACE})((imageTitle, mediaId, namespace, score))
    FROM (
        SELECT DISTINCT id, imageTitle, mediaId, namespace, if(type = 'P18', 1000000, views) as score
        FROM wikiimages 
        WHERE id in (SELECT w.id FROM wikidata w 
                LEFT JOIN elo_rating e ON e.id = w.id 
                ORDER BY e.elo DESC, w.id
                LIMIT {CHUNK_SIZE} OFFSET {page_no * CHUNK_SIZE})
            AND namespace = 6
            AND ({file_ext_conditions})
            AND imageTitle NOT IN (SELECT imageTitle FROM blocked_images)
        ORDER BY score DESC, imageTitle)
    GROUP BY id
    """
    with ch_client() as client:
        result = ch_query(query, client)
        images = []
        for p in result:
            images.append((p[0], p[1]))
        return images


backslash_quote = "\\" + "'"


def calculate_score(scores: list[float], coeffs: list[float], log_power: float) -> float:
    if len(scores) != len(coeffs):
        raise ValueError("The 'scores' and 'coeffs' lists must be of the same length.")

    scores = [0.0 if s == -1.0 else s for s in scores]
    total_weight = sum(coeffs)
    if total_weight == 0:
        raise ValueError("The sum of coefficients cannot be zero.")

    # Apply non-linear transformation to coefficients
    if log_power > 0:
        scores = [0 if s <= 0.0 else log_power ** (10 * s - 10) for s in scores]

    weighted_sum = sum(score * coeff for score, coeff in zip(scores, coeffs))
    final_score = weighted_sum / total_weight

    # Clamp the result to the range [0, 1] for safety
    return round(max(0.0, min(final_score, 1.0)), 2)


def _check(n, v):
    try:
        v = float(v)
    except ValueError as e:
        print(f"Warning: Value '{v}' for '{n}' is not a number. Default -1 is substituted.")
        return -1.0
    return abs(v) if abs(v) <= 1.0 else abs(v) / 10.0


def get_score(item, log_power: float = -1):
    scores = []
    for k, v in image_columns:
        if k.endswith('_reason'):
            item[k] = str(item[k]) if k in item else ''
            continue
        if not k.endswith('_score'):
            continue
        item[k] = _check(k, item[k] if k in item else v) if k in item else -1.0
        if k != 'safe_score':
            scores.append(item[k])

    s0 = item["safe_score"]
    if s0 < 1:
        final_score = s0 - 1
    else:
        final_score = calculate_score(scores, SCORING_WEIGHTS, log_power)
    return int(final_score * 100)


def is_valid_image_file_name(file_name):
    file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
    return file_ext in VALID_EXTENSIONS_LOWERCASE
