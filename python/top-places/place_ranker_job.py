import concurrent.futures
import math
import os
import random
import time
from collections import defaultdict
from datetime import datetime

import cython
import requests
from clickhouse_driver import Client

# 64 chars to encode 6 bits
_array = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~'
_array_map = {c: i for i, c in enumerate(_array)}
_array_map['@'] = _array_map['~']  # backwards compatibility

# Retrieve environment variables
batch_size = 10000
ROUNDS_ARRAY = [int(x) for x in os.getenv('ROUNDS', '3').split(',')]
SLEEP = int(os.getenv('SLEEP', '1'))
FILTER_QUAD_TREE = os.getenv('FILTER_QUAD_TREE', '')
PROCESS_PLACES = int(os.getenv('PROCESS_PLACES', '1000'))
INROUND_PLACES = int(os.getenv('INROUND_PLACES', '100'))
INROUND_TOP = int(os.getenv('INROUND_TOP', '20'))
PARALLEL = int(os.getenv('PARALLEL', '10'))
MIN_PROCESS_TOP = 10

print(f"Process {PROCESS_PLACES} places, each place {ROUNDS_ARRAY} rounds.")

MODEL = os.getenv('MODEL')
API_URL = os.getenv('API_URL')
K_FACTOR = 16  # 32

CLICKHOUSE_PWD = os.getenv('CLICKHOUSE_PWD')
API_KEY = os.getenv('API_KEY')
if not CLICKHOUSE_PWD:
    raise ValueError("Please set the CLICKHOUSE_PWD environment variable.")


#### SHORTLINK
def shortlink_encode(lon: float, lat: float, zoom: int) -> str:
    x: cython.uint = int(((lon + 180) % 360) * 11930464.711111112)
    y: cython.uint = int((lat + 90) * 23860929.422222223)
    c: cython.ulonglong = 0

    for i in range(31, -1, -1):
        c = (c << 2) | (((x >> i) & 1) << 1) | ((y >> i) & 1)

    d: cython.int = (zoom + 8) // 3
    r: cython.int = (zoom + 8) % 3

    if r > 0:
        d += 1

    str_list = ['-'] * (d + r)
    for i in range(d):
        digit: cython.int = (c >> (58 - 6 * i)) & 0x3F
        str_list[i] = _array[digit]

    return ''.join(str_list)


def calculate_bbox_shortlink(places):
    if not places:
        return ""
    shortlinks = [shortlink_encode(place['lon'], place['lat'], 16) for place in places]
    prefix = shortlinks[0] if shortlinks else ""
    for s in shortlinks[1:]:
        while len(prefix) > 0 and not s.startswith(prefix):
            prefix = prefix[:-1]
    return prefix


### ELO
def initial_elo(qrank):
    UP = 1000000
    LOW = 1000
    if qrank > UP:  # 30 000 places
        return 2000
    if qrank < LOW:
        return 1000
    return 1000 + 1000 * math.log10(qrank / 1000) / math.log10(UP / LOW)


# Function to calculate the expected score
def expected_score(rating_a, rating_b):
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))


# Function to update Elo ratings
def update_elo(rating_a, rating_b, score_a):
    expected_a = expected_score(rating_a, rating_b)
    expected_b = expected_score(rating_b, rating_a)
    new_rating_a = rating_a + K_FACTOR * (score_a - expected_a)
    new_rating_b = rating_b + K_FACTOR * ((1 - score_a) - expected_b)
    return new_rating_a, new_rating_b


def process_elo_updates(selected_places, winning_places):
    for place in selected_places:
        place['rounds'] += 1
        if place['id'] in winning_places:
            place['rounds_win'] += 1

    # for i in range(len(winning_places)):
    #     for j in range(i + 1, len(winning_places)):
    #         place_i = next((p for p in selected_places if p['id'] == winning_places[i]), None)
    #         place_j = next((p for p in selected_places if p['id'] == winning_places[j]), None)
    #         if place_i and place_j:
    #             place_i['elo'], place_j['elo'] = update_elo(place_i['elo'], place_j['elo'], 0.5)

    for winner_id in winning_places:
        winner_place = next((p for p in selected_places if p['id'] == winner_id), None)
        if winner_place:
            for place in selected_places:
                if place['id'] not in winning_places:
                    winner_place['elo'], place['elo'] = update_elo(winner_place['elo'], place['elo'], 1)


### ROUND

def format_prompt(selected_places, inround_top):
    formatted_data = "\n".join([f"id:{row['id']}, lat:{row['lat']}, lon:{row['lon']}, poi type:{row['poisubtype']}, wikiTitle:{row['wikiTitle']},"
                                for row in selected_places])
    return f"""Select the top {inround_top} places from the specified list
            based on your knowledge that are the most attractive for tourists.
            Please print only id and name in this format:
                id,lat,lon,wikiTitle,
                id,lat,lon,wikiTitle,
                ...
            List:
                {formatted_data}"""


def call_llm_api(prompt):
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    payload = {"model": MODEL, "messages": [{"role": "user", "content": prompt}]}
    response = requests.post(API_URL, headers=headers, json=payload, timeout=300)
    response.raise_for_status()
    return response.json()


def parse_llm_response(decoded_content):
    winning_places = []
    for line in decoded_content.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')
        if len(parts) >= 4:
            id_part = parts[0].strip()
            if id_part.startswith('id:'):
                id_str = id_part[3:].strip()
            else:
                id_str = id_part
            try:
                winning_places.append(int(id_str))
            except ValueError:
                print(f"Skipping invalid line: {line}")
    return winning_places


def process_round_results(selected_places, winning_places, bbox_shortlink, client):
    # bbox_shortlink = calculate_bbox_shortlink(selected_places)
    client.execute(
        """INSERT INTO wiki.top_places_rounds (modelid, runtime, pcount, wcount, participants, winners, bbox_shortlink)
           VALUES (%(modelid)s, %(runtime)s, %(pcount)s, %(wcount)s, %(participants)s, %(winners)s, %(bbox_shortlink)s)""",
        {
            'modelid': MODEL,
            'runtime': datetime.now(),
            'pcount': len(selected_places),
            'wcount': len(winning_places),
            'participants': [p['id'] for p in selected_places],
            'winners': winning_places,
            'bbox_shortlink': bbox_shortlink
        }
    )


def run_one_round(selected_places, selectTop, shortlink, round_num, rounds):
    # Create a new client for this thread
    client = Client('localhost', user='default', password=CLICKHOUSE_PWD, database='wiki')
    response = {}
    try:
        time.sleep(SLEEP)
        start_time = time.perf_counter()
        prompt = format_prompt(selected_places, selectTop)
        response = call_llm_api(prompt)
        decoded_content = response['choices'][0]['message']['content'].encode('utf-8').decode('unicode_escape')
        winning_places = parse_llm_response(decoded_content)
        process_elo_updates(selected_places, winning_places)
        process_round_results(selected_places, winning_places, shortlink, client)
        winner_places = [next((p for p in selected_places if p['id'] == winner_id), None) for winner_id in winning_places]
        print(
            f"Round {round_num} series {rounds + 1} {shortlink} took {time.perf_counter() - start_time:.3f} seconds, selected {len(winning_places)} of {len(selected_places)}: " +
            f"\n\t {', '.join([place['wikiTitle'][:25] for place in winner_places if place])} " +
            f"\n\t of {', '.join([place['wikiTitle'][:25] for place in selected_places[:25]])} " +
            f"{' ...' if len(selected_places) > 25 else ''} ")
    except Exception as e:
        print(f"Error processing round: {e} {response}")
    finally:
        client.disconnect()


def run_rounds(data, shortlink, series, llm_executor):
    client = Client('localhost', user='default', password=CLICKHOUSE_PWD, database='wiki')
    data = [item for item in data if item['shortlink'].startswith(shortlink)]
    # Load existing rounds and process
    existing_rounds = client.execute(
        """SELECT participants, winners FROM wiki.top_places_rounds
            WHERE modelid = %(modelid)s and bbox_shortlink = %(shortlink)s and wcount > 1""",
        {'modelid': MODEL, 'shortlink': shortlink}
    )
    participants_cnt = 0
    for participants, winners in existing_rounds:
        selected_places = [p for p in data if p['id'] in participants]
        participants_cnt += len(selected_places)
        process_elo_updates(selected_places, winners)
    rounds = int(participants_cnt / len(data))
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"\n------------\n[{current_time}]\nStart processing {shortlink} {len(data)} places - loaded and applied {len(existing_rounds)} rounds - series to go {series - rounds}.")
    round_num = 1
    place_ind = 0

    while rounds < series:
        ELO_RANDOM_DIFF = 300
        for place in data:
            place['elo_sort'] = place['elo'] + (random.random() * ELO_RANDOM_DIFF) - ELO_RANDOM_DIFF / 2
        data = sorted(data, key=lambda x: x['elo_sort'], reverse=True)
        batches = int(len(data) / INROUND_PLACES) + 1
        batchSize = int(len(data) / batches) + 1

        futures = []
        while place_ind < len(data):
            selectTop = max(2, int(batchSize * INROUND_TOP / INROUND_PLACES))
            selected_places = data[place_ind:min(len(data), place_ind + batchSize)]
            futures.append(llm_executor.submit(run_one_round, selected_places, selectTop, shortlink, round_num, rounds))
            place_ind += batchSize
            round_num += 1

        # Wait for all futures to complete
        concurrent.futures.wait(futures)

        rounds += 1
        place_ind = 0

    client.disconnect()


def run_series(data, roundsArray, filter_):
    nextlevelgroups = defaultdict(list)
    for item in data:
        prefix = item['shortlink'][:3]
        if prefix.startswith(filter_):
            nextlevelgroups[prefix].append(item)

    for prefix_length in [3, 2, 1, 0]:
        if prefix_length < len(filter_):
            break
        groups = nextlevelgroups
        nextlevelgroups = defaultdict(list)
        print(f"=================\nProcess prefixes length={prefix_length}\n")

        seriesToRun = roundsArray[min(len(roundsArray) - 1, prefix_length - len(filter_))]
        llmSeriesAll = 0
        for prefix, group_data in groups.items():
            if len(group_data) > MIN_PROCESS_TOP:
                llmSeriesAll += int(seriesToRun * max(1, len(group_data) / INROUND_PLACES))
        print(f"About to run {llmSeriesAll} llm series \n")

        # Use ThreadPoolExecutor to run run_rounds in parallel
        round_executor = concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL)  # For run_rounds
        llm_executor = concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL)  # For run_one_round (LLM calls)
        futures = []
        for prefix, group_data in groups.items():
            if len(group_data) < MIN_PROCESS_TOP:
                print(f"Skip processing {prefix} as it has {len(group_data)} places")
                if prefix_length > 0:
                    nextlevelgroups[prefix[:-1]].extend(group_data)
                continue
            # Submit the task to the executor
            futures.append(round_executor.submit(run_rounds, group_data, prefix, seriesToRun, llm_executor))
        # Wait for all futures to complete
        concurrent.futures.wait(futures)

        # Shutdown executors
        round_executor.shutdown()
        llm_executor.shutdown()

        if prefix_length > 0:
            for prefix, group_data in groups.items():
                if len(group_data) >= MIN_PROCESS_TOP:
                    group_data = sorted(group_data, key=lambda x: x['elo'], reverse=True)
                    # proceed with 20%
                    next_round_subgroup = group_data[0:int(len(group_data) * (INROUND_TOP * 1.0 / INROUND_PLACES))]
                    print(f"\nFilter {prefix} {len(group_data)} --> {len(next_round_subgroup)}: elo " +
                          f"{next_round_subgroup[0]['elo']} -> {next_round_subgroup[len(next_round_subgroup) - 1]['elo']}")
                    nextlevelgroups[prefix[:-1]].extend(next_round_subgroup)


# Connect to ClickHouse
client = Client('localhost', user='default', password=CLICKHOUSE_PWD, database='wiki')

# Fetch initial data
query = f"""SELECT wikiTitle, lat, lon, poitype, poisubtype, qrank, osmid, osmtype, osmcnt, id
            FROM wiki.wikidata
            WHERE (wlat != 0 OR wlon != 0 OR osmcnt = 1)
            ORDER BY qrank DESC
            LIMIT {PROCESS_PLACES}
              """
result = client.execute(query)
data = [
    {
        'id': row[9], 'wikiTitle': row[0], 'lat': row[1], 'lon': row[2],
        'poitype': row[3], 'poisubtype': row[4], 'qrank': row[5],
        'osmid': row[6], 'osmtype': row[7], 'osmcnt': row[8],
        'shortlink': shortlink_encode(row[2], row[1], 16),  # lon, lat
        'elo': initial_elo(row[5]),
        'rounds': 0, 'rounds_win': 0,
    }
    for row in result
]

# Run rounds
allfilter = 0
for place in data:
    if place['shortlink'].startswith(FILTER_QUAD_TREE):
        allfilter += 1

print(f"Process {allfilter} filtering by '{FILTER_QUAD_TREE}' - ~ {allfilter / INROUND_PLACES * ROUNDS_ARRAY[0]} llm queries")

run_series(data, ROUNDS_ARRAY, FILTER_QUAD_TREE)

categories_remap_result = client.execute(
    "SELECT fromCategory, toCategory FROM wiki.top_category_mapping WHERE uiCategory > 0", {})
categories_remap_dict = {row[0]: row[1] for row in categories_remap_result}

categories_query = """SELECT tpc.placeid, tpc.categories, tpc.topic FROM
    wiki.top_places_categories AS tpc
    INNER JOIN
    (   SELECT modelid, placeid, MAX(runtime) AS max_runtime
        FROM wiki.top_places_categories
        WHERE modelid = %(modelid)s GROUP BY modelid, placeid
    ) AS latest_tpc
    ON
        tpc.modelid = latest_tpc.modelid
        AND tpc.placeid = latest_tpc.placeid
        AND tpc.runtime = latest_tpc.max_runtime
    WHERE tpc.modelid = %(modelid)s"""
# use processed categories for fine tuned control
categories_query = "SELECT placeid, categories, topic FROM top_places_categories_processed"
categories_result = client.execute(categories_query, {'modelid': MODEL})

# Create a dictionary for easy lookup of categories by placeid
categories_dict = {}
OTHER_CATEGORY = 'Other'
OTHER_TOPIC = 7
for row in categories_result:
    remapped_cats = [categories_remap_dict[cat] for cat in row[1] if cat in categories_remap_dict]
    categories_dict[row[0]] = remapped_cats or [OTHER_CATEGORY]
topics_dict = {row[0]: min(row[2], OTHER_TOPIC) if row[2] > 0 else OTHER_TOPIC for row in categories_result}

# 3. Update elo_rating table (Dropping and recreating is often faster than updating for large tables)
client.execute("DROP TABLE IF EXISTS wiki.elo_rating")
client.execute("""
    CREATE TABLE wiki.elo_rating (
        `wikiTitle` String, `id` UInt32, `lat` Float64, `lon` Float64, `shortlink` String,
        `poitype` String, `poisubtype` String, `osmid` UInt64, `osmtype` UInt8, `osmcnt` UInt32,
        `qrank` UInt64, `elo` Float64, `rounds` UInt32, `rounds_win` UInt32,
        `topic` UInt32, `categories` Array(String)
    ) ENGINE = MergeTree() ORDER BY id;
""")

data = sorted(data, key=lambda x: x['elo'], reverse=True)
batch_data = []
UNDEFINED_TOPIC = 7
for place in data:
    if not place['shortlink'].startswith(FILTER_QUAD_TREE):
        continue
    topic = min(UNDEFINED_TOPIC, topics_dict.get(place['id'], UNDEFINED_TOPIC))
    batch_data.append((
        place['wikiTitle'] if place['wikiTitle'] is not None else '',
        place['id'], place['lat'], place['lon'], place['shortlink'],

        place['poitype'] if place['poitype'] is not None else '', place['poisubtype'] if place['poisubtype'] is not None else '',
        place['osmid'], place['osmtype'], place['osmcnt'],

        place['qrank'], place['elo'], place['rounds'], place['rounds_win'],
        topic if topic > 0 else UNDEFINED_TOPIC,
        categories_dict.get(place['id'], [OTHER_CATEGORY])  # Use the retrieved categories
    ))

# Split the data into batches and insert
for i in range(0, len(batch_data), batch_size):
    batch = batch_data[i:i + batch_size]
    client.execute("""
        INSERT INTO wiki.elo_rating
        (   wikiTitle, id, lat, lon, shortlink,
            poitype, poisubtype, osmid, osmtype, osmcnt,
            qrank, elo, rounds, rounds_win,
            topic, categories)
        VALUES """, batch)

print("Data successfully written to wiki.elo_rating, including categories")
client.disconnect()
