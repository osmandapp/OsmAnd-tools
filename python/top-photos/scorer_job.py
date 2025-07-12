import os
import threading
import time
import traceback
from datetime import datetime
from threading import current_thread
from typing import List

import clickhouse_connect
import openai
import requests

from QueueThreadPoolExecutor import BoundedThreadPoolExecutor
from python.lib.database_api import insert_place_batch, get_run_max_id, get_places_per_quad, get_image_scores, \
    get_score, QUAD, PHOTOS_PER_PLACE, PROCESS_PLACES, MIN_ELO, SAVE_SCORE_ENV, MAX_PLACES_PER_QUAD, process_quad, get_places, ImageItem, get_unscored_places_images
from python.lib.download_utils import download_image_as_base64
from llm_scoring import prompts, MODEL, MAX_PHOTOS_PER_REQUEST, call_llm

# Global Constants (from environment variables)
PARALLEL = int(os.getenv('PARALLEL', '10'))
SELECTED_PLACE_IDS = os.getenv('SELECTED_PLACE_IDS', '')
SELECTED_MEDIA_IDS = os.getenv('SELECTED_MEDIA_IDS', '')
STATUS_TIME_OUT = int(os.getenv('STATUS_TIME_OUT', '60'))

if not any([QUAD, SELECTED_PLACE_IDS]):
    raise ValueError("Missing required environment variables: QUAD or SELECTED_PLACE_IDS is required.")

print(
    f"LLM: {MODEL}, SELECTED: {SELECTED_PLACE_IDS}/{SELECTED_MEDIA_IDS}, ENV: {SAVE_SCORE_ENV}, PHOTOS_PER_REQUEST: {MAX_PHOTOS_PER_REQUEST}, QUAD={QUAD}, PHOTOS_PER_PLACE={PHOTOS_PER_PLACE}, PLACES_PER_QUAD={MAX_PLACES_PER_QUAD}, MIN_ELO={MIN_ELO}",
    flush=True)

stop_immediately = False
total_place_count = 0


def done_callback(future):
    global stop_immediately, total_place_count

    stop, place = future.result()
    if stop:
        stop_immediately = True
    total_place_count += 1


def score_places():
    media_ids = [int(p.strip()) for p in SELECTED_MEDIA_IDS.split(',') if p.strip() != '']
    place_ids = [str(p.strip()) for p in SELECTED_PLACE_IDS.split(',') if p.strip() != '']
    is_selected = len(place_ids) > 0

    run_id = get_run_max_id() + 1
    try:
        quads = process_quad(QUAD) if QUAD and not is_selected else ['']
        for quad in quads:
            places = get_places(place_ids) if is_selected else get_places_per_quad(quad)
            place_ids = [p[0] for p in places]
            if not is_selected and len(place_ids) > 0:
                place_ids = get_unscored_places_images(place_ids)
            if stop_immediately or total_place_count >= PROCESS_PLACES:
                break

            print(f"Run #{run_id} is processing {len(places)} places from quad '{quad}': {place_ids} ...", flush=True)
            if len(places) == 0:
                continue

            sub_start_time = time.time()
            for place in places:
                if place[0] in place_ids:
                    executor.submit(run_id, place, is_selected, media_ids)

            print(f"Run #{run_id} processed {len(places)} places from quad {quad}. Time:{(time.time() - sub_start_time):.2f}s (Total: {total_place_count} / {(time.time() - start_time):.2f}s)",
                  flush=True)
    except Exception as e:
        print(f"Error processing batch: {e}")

    print(f"Run #{run_id} finished. Places: {total_place_count}. Stop: {stop_immediately}, Total time: {(time.time() - start_time):.0f}s, Timestamp: {datetime.now()}", flush=True)


PROMPT = prompts['TAG_PROMPT']


def split_array(arr: List, n) -> List[List]:
    if n <= 0:
        return [arr]

    length = len(arr)
    base = length // n
    remainder = length % n
    start = 0

    result = []
    for i in range(n):
        end = start + base + (1 if i < remainder else 0)

        end = min(end, length)
        result.append(arr[start:end])
        start = end

    return result


def _mix_images(place_id, is_selected: bool, non_scored_items: List[ImageItem], scored_items: List[ImageItem], selected_items: List[ImageItem]):
    images = []

    len_non_scored, len_scored, len_selected = len(non_scored_items), len(scored_items), len(selected_items)
    print(f"Place {place_id} with {len_non_scored} non-scored, {len_scored} scored and {len_selected} selected photos is processing ...", flush=True)
    if not is_selected and len_non_scored == 0 and len_selected == 0:
        return images

    i, scored_ix, non_scored_ix = 0, 0, 0
    for item in selected_items:
        image_base64 = download_image_as_base64(item.path)
        if image_base64:
            images.append((item.path, image_base64, item.media_id))
            i += 1

    if len(selected_items) > 0:
        scored_items = [p for p in scored_items if p not in selected_items]
        non_scored_items = [p for p in non_scored_items if p not in selected_items]
        len_non_scored, len_scored = len(non_scored_items), len(scored_items)

    # mix 2:1 unscored and scored photos
    while i < PHOTOS_PER_PLACE:
        if i % 3 == 2 and scored_ix < len_scored:
            path = scored_items[scored_ix].path
            media_id = scored_items[scored_ix].media_id
            scored_ix += 1
        elif non_scored_ix < len_non_scored:
            path = non_scored_items[non_scored_ix].path
            media_id = non_scored_items[non_scored_ix].media_id
            non_scored_ix += 1
        elif scored_ix < len_scored and (is_selected or 0 < i < MAX_PHOTOS_PER_REQUEST):
            path = scored_items[scored_ix].path
            media_id = scored_items[scored_ix].media_id
            scored_ix += 1
        else:
            break

        image_base64 = download_image_as_base64(path)
        if image_base64:
            images.append((path, image_base64, media_id))
            i += 1
    return images


def process_place(run_id, place_info, is_selected, media_ids):
    start_time = time.time()
    photos = {}
    place_id, wiki_title, lat, lon, poi_type, poi_subtype, categories, short_link, elo = place_info
    place_run = {
        'run_id': run_id,
        'wikidata_id': place_id,
        'wikititle': wiki_title,
        'started': datetime.now(),
        'duration': 0.0,
        'batch_id': 0,
        'prompt_photo_ids': [],
        'scored_photo_ids': [],
        'prompt_tokens': 0,
        'completion_tokens': 0,
        'cached_tokens': 0,
        'error': '',
        'version': SAVE_SCORE_ENV
    }
    try:
        paths = get_image_scores(place_id, PHOTOS_PER_PLACE)
        images = _mix_images(place_id, is_selected,
                             [p for p in paths if not p.is_processed],
                             [p for p in paths if p.is_processed],
                             [p for p in paths if p.media_id in media_ids])
        if len(images) == 0:
            print(f"#{current_thread().name}. Warning: SKIPPED place {place_id} because of image absence.")
            return False, place_run

        print(f"#{current_thread().name}. Place {place_id} with {len(images)} images is processing ...", flush=True)
        scored_photos_count = 0
        split_images = split_array(images, 0 if len(images) <= MAX_PHOTOS_PER_REQUEST else int(len(images) // MAX_PHOTOS_PER_REQUEST) + 1)
        batch_id = 0
        for batch_images in split_images:
            place_run['prompt_photo_ids'] = []
            for i, (path, image_base64, media_id) in enumerate(batch_images):
                place_run['prompt_photo_ids'].append(media_id)
            place_run['batch_id'] = batch_id
            batch_id += MAX_PHOTOS_PER_REQUEST
            prompt = PROMPT.format(photo_count="%PHOTO_COUNT%",
                                   long_title=f"for the place: '{wiki_title}'",
                                   place_id=place_id, poi=f"POI: {poi_type} {poi_subtype},",
                                   categories=f"place's categories: {categories},",
                                   photoids="%PHOTO_IDS%",
                                   lat=lat, lon=lon)
            place_run['scored_photo_ids'] = []

            try:
                res = call_llm(prompt, batch_images)
                if res:
                    place_run['duration'] = res['duration']
                    place_run['prompt_tokens'] = res['prompt_tokens']
                    place_run['completion_tokens'] = res['completion_tokens']
                    place_run['cached_tokens'] = res['cached_tokens']
            except (
                    openai.PermissionDeniedError,
                    openai.AuthenticationError,
                    openai.RateLimitError,
                    openai.NotFoundError,
            ) as e:
                print(f"#{current_thread().name}. STOPPED place {place_id}! Error: {e}")
                return True, place_run
            except RuntimeError as e:
                place_run['error'] = f"{e}"
                insert_place_batch(place_run, [])
                return False, place_run

            if not res or 'results' not in res or not res['results'] or res['results'] is None:
                print(f"#{current_thread().name}. Warning: SKIPPED place {place_id} because of incorrect LLM response.")

                return False, place_run

            results = res['results']
            photos = {}
            for i, photo_res in enumerate(results):
                if photo_res is None or photo_res['photo_id'] is None:
                    continue
                if not isinstance(photo_res['photo_id'], int) and photo_res['photo_id'].isdigit():
                    photo_res['photo_id'] = int(photo_res['photo_id'])
                batched_image = None
                for image in batch_images:
                    if image[2] == photo_res['photo_id']:
                        batched_image = image
                        break
                if batched_image is None:
                    continue

                # photo_res['photo_id'] = batch_images[i][2]
                place_run['scored_photo_ids'].append(photo_res['photo_id'])
                photo_res['run_id'] = run_id
                photo_res['proc_id'] = place_id
                photo_res['imageTitle'] = batch_images[i][0]
                photo_res['score'] = get_score(photo_res, -1)  # check and calculate score
                photo_res['version'] = SAVE_SCORE_ENV
                photo_res['timestamp'] = datetime.now()
                photos[i] = photo_res

            if len(place_run['scored_photo_ids']) != len(place_run['prompt_photo_ids']):
                place_run['error'] = f"Missing some scoring results"
                print(
                    f"#{current_thread().name}. Warning: LLM response for place {place_id} contains missing results: {len(place_run['scored_photo_ids'])} scored of {len(place_run['prompt_photo_ids'])} .",
                    flush=True)
            scored_photos_count += len(photos)
            photos = sorted(photos.values(), key=lambda x: x['score'], reverse=True)
            # Insert into ClickHouse
            insert_place_batch(place_run, photos)
        print(
            f"#{current_thread().name}. Place {place_id} ({int(elo)}, {short_link}) with {scored_photos_count} images are scored and saved. Time:{(time.time() - start_time):.2f}s Timestamp: {datetime.now()}",
            flush=True)
        return False, place_run
    except clickhouse_connect.driver.exceptions.ProgrammingError as e:
        print(f"#{current_thread().name}. Could not process place {place_id}. Error: {e}.")
        return True, place_run
    except (
            requests.exceptions.RequestException,
            clickhouse_connect.driver.exceptions.DatabaseError,
    ) as e:
        print(f"#{current_thread().name}. Warning: Could not process place {place_id}: {e}. Need to wait 30 sec and retry again ...")
        time.sleep(30)
        return False, place_run
    except Exception as e:
        traceback.print_exc()
        print(f"#{current_thread().name}. Error!\n Place: {place_id}\n Images: {photos}\n Error: {e}")

        place_run['error'] = f"{e}"
        insert_place_batch(place_run, [])
        return False, place_run


executor = BoundedThreadPoolExecutor(process_place, done_callback, PARALLEL, "Thread")


def check_status():
    time.sleep(STATUS_TIME_OUT)

    while not is_done:
        tasks_args = executor.undone_futures_args()

        timings = [((time.time() - t), args[1]) for t, args in tasks_args if (time.time() - t) > STATUS_TIME_OUT]
        timings = [f"Q{place[0]}:{t:.0f}s" for t, place in timings]
        print(f"Timeout status: {timings}. Total time: {(time.time() - start_time):.0f}s", flush=True)
        time.sleep(STATUS_TIME_OUT)


if __name__ == "__main__":
    is_done = False
    start_time = time.time()

    status_thread = threading.Thread(target=check_status)
    status_thread.start()
    try:
        with executor:
            score_places()
    finally:
        is_done = True
