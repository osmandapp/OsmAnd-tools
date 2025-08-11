import concurrent.futures
import os
import re
import time
from datetime import datetime, timezone  # Import timezone

import clickhouse_connect
import requests

# Global Constants (from environment variables)
MODEL = os.getenv('MODEL')
API_URL = os.getenv('API_URL')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'data.osmand.net')
CLICKHOUSE_PWD = os.getenv('CLICKHOUSE_PWD')
API_KEY = os.getenv('API_KEY')
PLACES_BATCH = int(os.getenv('PLACES_BATCH', '100'))  # Default to 100
PROCESS_PLACES = int(os.getenv('PROCESS_PLACES', '1000'))  # Default 1000
PARALLEL = int(os.getenv('PARALLEL', '10'))  # Default 10
ITERATIONS = int(os.getenv('ITERATIONS', '3'))  # Default to 3
SLEEP = int(os.getenv('SLEEP', '3'))
WEB_SERVER_CONFIG_PATH = os.getenv('WEB_SERVER_CONFIG_PATH')

# Check for required environment variables
if not all([MODEL, API_URL, CLICKHOUSE_PWD, API_KEY]):
    raise ValueError("Missing required environment variables (MODEL, API_URL, CLICKHOUSE_PWD, API_KEY)")


# Load prompt from file
def load_prompt_template():
    """Loads the prompt template from a file, handling relative paths."""
    filepath = os.path.join(WEB_SERVER_CONFIG_PATH, "llm/prompt_categories.txt")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"Prompt file not found at '{filepath}'.")


PROMPT_TEMPLATE = load_prompt_template()


def categorize_places():
    """
    Fetches places from ClickHouse, categorizes them using an LLM API,
    and inserts the results into another ClickHouse table.
    """
    EXCLUDE_SUBQUERY = f"""SELECT placeid FROM (
                        SELECT placeid, count() AS runs
                        FROM top_places_categories WHERE modelid = '{MODEL}'
                        GROUP BY placeid HAVING runs >= {ITERATIONS})"""
    if ITERATIONS == 0:
        EXCLUDE_SUBQUERY = f"""SELECT placeid FROM wiki.top_places_categories_processed"""
    try:
        # Build the base query, handling ITERATIONS
        base_query = f"""
            SELECT wikiTitle, id, lat, lon, poitype, poisubtype FROM elo_rating
            WHERE id not IN (
                {EXCLUDE_SUBQUERY}
            )
            ORDER BY elo DESC LIMIT {PROCESS_PLACES}
        """

        # Use a single connection to fetch all the places at once.
        with clickhouse_connect.get_client(host=CLICKHOUSE_HOST, username='wiki', password=CLICKHOUSE_PWD, database='wiki') as client:
            result = client.query(base_query)
            all_places = result.result_rows

            # Create batches
            place_batches = [all_places[i:i + PLACES_BATCH] for i in range(0, len(all_places), PLACES_BATCH)]

            # Process batches in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL) as executor:
                futures = [executor.submit(process_batch, batch) for batch in place_batches]
                concurrent.futures.wait(futures)

    except Exception as e:
        print(f"An error occurred: {e}")


def process_batch(batch):
    """Processes a single batch of places.  Now takes only the batch data."""
    # Create a *new* ClickHouse client *within* the thread.
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, username='wiki', password=CLICKHOUSE_PWD, database='wiki')
    try:
        time.sleep(SLEEP)
        start_time = time.time()
        prompt = build_prompt(batch)
        categories_by_place, topics_by_place = call_llm_api(prompt)
        llm_end_time = time.time()

        insert_categories(client, categories_by_place, topics_by_place, batch)
        end_time = time.time()

        llm_time = llm_end_time - start_time
        total_time = end_time - start_time
        num_places = len(batch)
        num_distinct_categories = len(set(cat for cats in categories_by_place.values() for cat in cats))

        print(f"Processed batch of {num_places} places. LLM time: {llm_time:.2f}s, "
              f"Total time: {total_time:.2f}s, Distinct categories: {num_distinct_categories}")
    except Exception as e:
        print(f"Error processing batch: {e}")
    finally:
        client.close()  # Close the client in the thread.


def build_prompt(places_batch):
    """Constructs the prompt for the LLM API call."""
    places_list_str = ""
    for index, place in enumerate(places_batch):
        wikititle, place_id, lat, lon, poitype, poisubtype = place
        places_list_str += f"{index + 1}. {wikititle}, {poitype} {poisubtype}, Q{place_id}, {lat:.5f}, {lon:.5f}\n"

    # print(f"DEBUG: {places_list_str}");
    return PROMPT_TEMPLATE.format(places_list=places_list_str)


def call_llm_api(prompt):
    """Calls the LLM API."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "model": MODEL,
        "prompt": prompt,
        "temperature": 0.1,
    }
    try:
        response = requests.post(API_URL, headers=headers, json=data)
        response.raise_for_status()
        response_json = response.json()
        llm_output = response_json['choices'][0]['text']
        return parse_llm_output(llm_output)
    except requests.exceptions.RequestException as e:
        print(f"Error calling LLM API: {e}")
        return {}
    except (KeyError, IndexError) as e:
        print(f"Error parsing LLM response: {e}, Response: {response.text}")
        return {}
    except Exception as e:
        print("Unexpected error:", e)
        return {}


def parse_llm_output(llm_output):
    """Parses the LLM output to extract categories and topics by place index."""
    categories_by_place = {}
    topics_by_place = {}
    # print(f"DEBUG: {llm_output}")
    lines = llm_output.strip().split('\n')
    for line in lines:
        line = line.strip()
        if not line:  # Skip empty lines
            continue

        match = re.match(r'(\d+)\.\s*(\d+)\s*-\s*(.*)', line)
        if match:
            index_str, topic_str, categories_str = match.groups()
            try:
                index = int(index_str) - 1  # 0-based index
                topic = int(topic_str)  # Topic is now an integer
                categories = [cat.strip() for cat in categories_str.split(',') if cat.strip()]

                categories_by_place[index] = categories
                topics_by_place[index] = topic

            except ValueError:
                print(f"Warning: Could not parse (ValueError) line: {line}")
                continue
        else:
            print(f"Warning: Could not parse (No Match) line: {line}")
            continue

    return categories_by_place, topics_by_place


def insert_categories(client, categories_by_place, topics_by_place, places_batch):
    """Inserts the categorized places into the ClickHouse table."""

    if not categories_by_place or not topics_by_place:
        print("No categories to insert.")
        return

    runtime = datetime.now(timezone.utc)  # Use UTC time
    data_to_insert = []

    for index, categories in categories_by_place.items():
        # Retrieve place id by index from places_batch
        placeid = places_batch[index][1]
        if index in topics_by_place and topics_by_place[index] > 0:
            data_to_insert.append((MODEL, runtime, placeid, topics_by_place[index], categories))

    client.insert(table='top_places_categories',
                  column_names=['modelid', 'runtime', 'placeid', 'topic', 'categories'], data=data_to_insert)
    print(f"Inserted {len(data_to_insert)} rows into top_places_categories.")


if __name__ == "__main__":
    categorize_places()
