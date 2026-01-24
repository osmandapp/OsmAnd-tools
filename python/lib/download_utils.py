import base64
import datetime
import hashlib
import os
import sys
import tempfile
import threading
import time
import random
import signal
from concurrent.futures import ThreadPoolExecutor
from urllib.request import Request, urlopen
from io import BytesIO
from pathlib import Path
from typing import Optional

import requests
from PIL import Image
from openai.types.beta.threads import ImageFile

downloaded_files_cache: set = set()

from .database_api import VALID_EXTENSIONS_LOWERCASE, is_valid_image_file_name, ch_client, ch_query
from .database_api import populate_cache_from_db, scan_and_populate_db, insert_downloaded_image

USER_AGENT = "OsmAnd-Bot/1.0 (+https://osmand.net; support@osmand.net) OsmAndPython/1.0"
WIKI_MEDIA_URL = os.getenv('WIKI_MEDIA_URL', "https://data.osmand.net/wikimedia/images-1280/")
PROXY_FILE_URL = os.getenv('PROXY_FILE_URL', None)
MAX_TRIES = int(os.getenv('MAX_TRIES', '10'))
MAX_SLEEP = int(os.getenv('MAX_SLEEP', '60'))
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 60

MAX_IMG_DIMENSION = int(os.getenv('MAX_IMG_DIMENSION', 720))
IMAGE_SIZE = 1280

CACHE_DIR = os.getenv('CACHE_DIR', './wiki')
cache_folder = f"{CACHE_DIR}/images-{IMAGE_SIZE}/"

OFFSET_BATCH = int(os.getenv('OFFSET_BATCH', '0'))
DOWNLOAD_IF_EXISTS = os.getenv('DOWNLOAD_IF_EXISTS', 'false').lower() == 'true'
PROCESS_PLACES = int(os.getenv('PROCESS_PLACES', '1000'))
PLACES_PER_THREAD = int(os.getenv('PLACES_PER_THREAD', '10000'))
ERROR_LIMIT_PERCENT = int(os.getenv('ERROR_LIMIT_PERCENT', '5'))
MONITORING_INTERVAL = int(os.getenv('MONITORING_INTERVAL', '600'))
PARALLEL = int(os.getenv('PARALLEL', '2'))

PHOTOS_PER_PLACE = int(os.getenv('PHOTOS_PER_PLACE', '40'))
ASTRO_IMAGES_ONLY = os.getenv("ASTRO_IMAGES_ONLY", "false") == "true"

monitoring_error_count = 0
monitoring_success_count = 0
monitoring_counter_lock = threading.Lock()
clickhouse_slow_query_lock = threading.Lock()

class ProxyManager:
    def __init__(self, proxy_file_path):
        self.proxies = []
        self.lock = threading.Lock()
        self.load_proxies(proxy_file_path)

    def load_proxies(self, proxy_file_url: str):
        proxies: list[str] = []

        try:
            if proxy_file_url.startswith(("http://", "https://")):
                # TO-THINK proxy_file_url should be refreshed regularly
                req = Request(proxy_file_url, headers={"User-Agent": "ProxyManager/1.0"})
                with urlopen(req, timeout=30) as resp:
                    text = resp.read().decode("utf-8", errors="replace")
                proxies = [l.strip() for l in text.splitlines() if l.strip()]
            else:
                with open(proxy_file_url, "r", encoding="utf-8", errors="replace") as f:
                    proxies = [l.strip() for l in f if l.strip()]

            with self.lock:
                self.proxies = proxies

            print(f"Loaded {len(proxies)} proxies from {proxy_file_url}")

        except FileNotFoundError:
            print(f"Proxy file {proxy_file_url} not found.")
            with self.lock:
                self.proxies = []
        except Exception as e:
            print(f"Failed to load proxies from {proxy_file_url}: {e}.")
            with self.lock:
                self.proxies = []

    def get_rotated_proxy(self):
        with self.lock:
            if not self.proxies:
                return None
            # Rotate proxies to distribute load
            proxy = self.proxies.pop(0)
            self.proxies.append(proxy)
            return proxy

    def get_random_proxy(self) -> Optional[str]:
        with self.lock:
            return random.choice(self.proxies) if self.proxies else None


def _cached_path(file_name: str, create_directory: bool = False):
    filename_encoded = file_name.replace(' ', '_')
    hash_md5 = hashlib.md5(filename_encoded.encode()).hexdigest()
    hash_prefix = f"{hash_md5[0]}/{hash_md5[0:2]}"
    file_path = os.path.join(cache_folder, hash_prefix, file_name)

    if create_directory:
        Path(cache_folder, hash_prefix).mkdir(parents=True, exist_ok=True)

    return file_path


def _generate_image_url(file_name, base="https://upload.wikimedia.org/wikipedia/commons/", width=IMAGE_SIZE):
    filename_encoded = file_name.replace(' ', '_')
    base = base.rstrip("/")

    hash_md5 = hashlib.md5(filename_encoded.encode()).hexdigest()
    hash_prefix = f"{hash_md5[0]}/{hash_md5[0:2]}"
    filename_encoded = filename_encoded.replace('?', '%3F')
    if width == 0:
        return f"{base}/{hash_prefix}/{filename_encoded}"
    return f"{base}/thumb/{hash_prefix}/{filename_encoded}/{width}px-{filename_encoded}"


def get_image_url(file_name):
    return _generate_image_url(file_name, WIKI_MEDIA_URL, 0)


def download_image_as_base64(file_name):
    if not is_valid_image_file_name(file_name):
        print(f"Unsupported extension: {file_name}", flush=True)
        return ""

    url = _generate_image_url(file_name, WIKI_MEDIA_URL, 0)
    response = requests.get(url, timeout=(30, 30))
    if response.status_code == 200:
        tmp_file_name = "tmp." + os.path.splitext(file_name)[1].lower().lstrip('.')
        base_path = os.path.join(tempfile.mkdtemp(), tmp_file_name)
        os.makedirs(os.path.dirname(base_path), exist_ok=True)
        with open(base_path, "wb") as image_file:
            image_file.write(response.content)
    else:
        print(f"Failed to download image from {url}. Status code: {response.status_code}", flush=True)
        return ""

    try:
        with Image.open(base_path) as img:
            img, replaced = resize_image(img)
            if replaced:
                img.save(base_path, format=file_name_image_format_lowercase(file_name).upper())
        with open(base_path, "rb") as f:
            base64_encoded = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        print(f"Error processing image {base_path}: {e}", flush=True)
        return ""
    finally:
        os.remove(base_path)
        os.rmdir(os.path.dirname(base_path))

    return base64_encoded


def resize_image(image: ImageFile):
    width, height = image.size
    if width > MAX_IMG_DIMENSION or height > MAX_IMG_DIMENSION:
        if width > height:
            new_width = MAX_IMG_DIMENSION
            new_height = int((MAX_IMG_DIMENSION / width) * height)
        else:
            new_height = MAX_IMG_DIMENSION
            new_width = int((MAX_IMG_DIMENSION / height) * width)
        image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
        if image.mode != 'RGB' and image.format != 'PNG':
            image = image.convert('RGB')
        return image, True
    return image, False


def download_pil_image(file_name):
    url = get_image_url(file_name)
    response = requests.get(url, timeout=(30, 30), verify=False)
    if response.status_code != 200:
        print(f"Failed to download image from {url}. Status code: {response.status_code}", flush=True)
        return None

    try:
        image_bytes = BytesIO(response.content)

        img = Image.open(image_bytes)
        img.load()

        img, _ = resize_image(img)
        return img
    except Exception as e:
        print(f"SKIPPED {file_name}. Error processing image: {e}", flush=True)
        return None


def download_image(file_name, override: bool = False, proxy_manager=None):
    start_time = time.time()

    file_path = _cached_path(file_name, True)
    if not override and os.path.exists(file_path):
        return True

    status_code = 0
    reuse_same_proxy = False
    url = _generate_image_url(file_name)
    headers = {"User-Agent": USER_AGENT}

    proxy = None
    proxies = None

    for attempt in range(1, MAX_TRIES + 1):
        if proxy_manager and not reuse_same_proxy:
            proxy = proxy_manager.get_random_proxy()
            if proxy:
                proxies = {
                    'http': f'http://{proxy}',
                    'https': f'http://{proxy}'
                }
            else:
                proxies = None

        reuse_same_proxy = False

        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        except Exception as e:
            print(f"Retry exception {url} proxy {proxy}: {e} [{attempt}]")
            time.sleep(attempt)
            status_code = 666
            continue

        status_code = response.status_code

        if status_code == 200:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as image_file:
                image_file.write(response.content)
            if attempt > 1:
                print(f"{file_path} is downloaded. Time:{(time.time() - start_time):.2f}s [{attempt}]")
            # else:
            #     print("+") # debug
            return True
        elif status_code == 429:
            reuse_same_proxy = True
            seconds = int(attempt * (MAX_SLEEP / MAX_TRIES))
            print(f"Sleep {seconds}s HTTP-{status_code} {url} proxy {proxy} [{attempt}]")
            time.sleep(seconds)
            continue
        elif status_code == 404 or (500 <= status_code <= 599):
            break

        print(f"Retry HTTP-{status_code} {url} proxy {proxy} [{attempt}]")
        time.sleep(attempt)
        continue

    print(f"Failed to get {url}. Last status code: {status_code}")
    return False


def download_images_per_page(page_no: int, override: bool = False, proxy_manager=None):
    global monitoring_error_count, monitoring_success_count
    start_time = time.time()

    t0 = time.perf_counter()
    with clickhouse_slow_query_lock:
        image_records = get_images_per_page(page_no, PLACES_PER_THREAD)
    print(f"get_images_per_page({page_no}, {PLACES_PER_THREAD}) ~ {(time.perf_counter() - t0) * 1000:.2f} ms")

    error_count, place_count, image_count, image_all_count = 0, 0, 0, 0
    for place_id, place_paths in image_records:
        place_img_loaded = 0
        place_img_error = 0
        sub_start_time = time.time()
        # print(f"Places: {place_paths}", flush=True)
        for img_path, mediaId, namespace, _ in place_paths:
            image_all_count += 1
            cached_file_path = _cached_path(img_path, False) # TODO might be removed
            # Directly check the set (no os.path.exists here)
            if not override and img_path in downloaded_files_cache:
                continue  # Skip if found in the pre-scanned set

            success = download_image(img_path, override, proxy_manager)
            if not success:
                error_count += 1
                place_img_error += 1
                with monitoring_counter_lock:
                    monitoring_error_count += 1
            else:
                # downloaded_files_cache.append(img_path) # check multithread
                file_stat = os.stat(cached_file_path)
                timestamp = datetime.datetime.fromtimestamp(file_stat.st_mtime)
                filesize = file_stat.st_size
                relative_folder = os.path.dirname(os.path.relpath(cached_file_path, cache_folder))
                insert_downloaded_image(img_path, relative_folder, timestamp, filesize, mediaId, namespace)
                place_img_loaded += 1
                image_count += 1
                with monitoring_counter_lock:
                    monitoring_success_count += 1

        place_count += 1
        duration = time.time() - sub_start_time
        if place_img_loaded > 0 or place_img_error > 0:
            print(
                f"Processed place Q{place_id} (#{place_count}) downloaded {place_img_loaded} images, {place_img_error} errors. Time: {duration:.2f}s",
                flush=True)

    if place_count > 0:
        duration = time.time() - start_time
        print(
            f"Processed batch {page_no} - {duration:.2f}s:" +
            f" {place_count} places, {image_all_count} images (loaded {image_count}), {error_count} errors." +
            f" Total: {total_place_count} places, {total_image_count} images, {total_error_count} errs.",
            flush=True)
    return error_count, place_count, image_count


def process_chunk(proxy_manager=None):
    global chunk_i, total_place_count, total_image_count, total_error_count, total_time  # Declare globals
    error_count, place_count, image_count = 0, 0, 0
    stopped = False
    # while not stop_event.is_set():
    while not stopped:
        with chunk_lock:
            current_chunk = chunk_i
            chunk_i += 1  # Now chunk_i is recognized as global

        error_count, place_count, image_count = download_images_per_page(current_chunk, DOWNLOAD_IF_EXISTS, proxy_manager)

        with total_lock:
            total_place_count += place_count # PLACES_PER_THREAD is a wrong way
            total_image_count += image_count
            total_error_count += error_count

            # if (place_count <= 0 and image_count <= 0) or total_place_count >= PROCESS_PLACES: # wrong way
            if total_place_count >= PROCESS_PLACES:
                print(f"Thread stopped! Last chunk: {error_count} errors, {place_count} places, {image_count} images. "
                      f"Total: {total_place_count} places, {total_image_count} images.")
                stopped = True
                # stop_event.set()
    return error_count, place_count, image_count


def initialize_download_cache():
    """Populates the in-memory cache from DB or by scanning disk."""
    global downloaded_files_cache
    populate_db_flag = os.getenv('POPULATE_DOWNLOADED_DB', 'false').lower() == 'true'

    if populate_db_flag:
        print("POPULATE_DOWNLOADED_DB is set. Scanning disk and populating DB...")
        # Ensure cache_folder exists before scanning
        Path(cache_folder).mkdir(parents=True, exist_ok=True)
        downloaded_files_cache = scan_and_populate_db(cache_folder)
    else:
        print("Reading downloaded files cache from database...")
        downloaded_files_cache = populate_cache_from_db()
    print(f"Initialized cache with {len(downloaded_files_cache)} items.")


def file_name_image_format_lowercase(file_name):
    file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
    if file_ext == 'jpg':
        return 'jpeg'
    return file_ext


def get_images_per_page(page_no: int, places_per_thread: int) -> list[tuple[int, list[tuple[str, int, int]]]]:
    file_ext_conditions = " OR ".join(
        f"endsWith(lower(imageTitle), '.{ext}')" for ext in VALID_EXTENSIONS_LOWERCASE
    )

    if ASTRO_IMAGES_ONLY:
        id_select = f"""
            SELECT id FROM wiki_coords WHERE poitype = 'starmap'
                LIMIT {places_per_thread} OFFSET {page_no * places_per_thread}
        """
    else:
        id_select = f"""
            SELECT w.id FROM wikidata w
                LEFT JOIN elo_rating e ON e.id = w.id
                ORDER BY e.elo DESC, w.id
                LIMIT {places_per_thread} OFFSET {page_no * places_per_thread}
        """

    query = f"""
        SELECT id, groupArray({PHOTOS_PER_PLACE})((imageTitle, mediaId, namespace, score))
        FROM (
            SELECT DISTINCT id, imageTitle, mediaId, namespace, if(type = 'P18', 1000000, views) as score
            FROM wikiimages
            WHERE
                id IN ({id_select})
                AND namespace = 6
                AND ({file_ext_conditions})
                AND imageTitle NOT IN (SELECT imageTitle FROM blocked_images)
            ORDER BY score DESC, imageTitle
        )
        GROUP BY id
        ORDER BY rand()
    """
    with ch_client() as client:
        result = ch_query(query, client)
        images = []
        for p in result:
            images.append((p[0], p[1]))
        return images

def count_images_to_download() -> tuple[int, int]:
    all_places = get_images_per_page(0, sys.maxsize)
    total_images = sum(len(paths) for _, paths in all_places)
    return len(all_places), total_images


def monitoring_thread() -> None:
    while True:
        errors_before = monitoring_error_count
        success_before = monitoring_success_count

        for i in range(MONITORING_INTERVAL):
            if monitoring_error_count + monitoring_success_count >= images_to_download:
                print("Monitoring finished")
                return
            time.sleep(1)

        errors = monitoring_error_count - errors_before
        success = monitoring_success_count - success_before

        if errors > 0 and (monitoring_success_count > 0 or monitoring_error_count > 100):
            current_errors_percent = int(errors / (errors + success) * 100)
            if current_errors_percent > ERROR_LIMIT_PERCENT:
                print(f"Monitoring thread terminates downloading")
                print(f"Total success {monitoring_success_count}, errors {monitoring_error_count}")
                print(f"Interval success {success}, errors {errors} ({current_errors_percent}%)")
                os.kill(os.getpid(), signal.SIGTERM)

        now = datetime.datetime.now().replace(microsecond=0)
        if success > 0 or errors > 0:
            images_left = images_to_download - (monitoring_error_count + monitoring_success_count)
            eta_seconds = int(images_left / ((success + errors) / MONITORING_INTERVAL))
            success_per_day = int(success * 86400 / MONITORING_INTERVAL)
            print("\n"
                  f"{now} "
                  f"todo {images_left:,} "
                  f"performance {success_per_day // 1000}k/day "
                  f"ETA {datetime.timedelta(seconds=eta_seconds)}"
                  "\n")
        else:
            print(f"{now} nothing happened")


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)

    proxy_manager = None
    if PROXY_FILE_URL:
        proxy_manager = ProxyManager(PROXY_FILE_URL)

    if not proxy_manager or not proxy_manager.get_random_proxy():
        proxy_manager = None
        PARALLEL = min(2, PARALLEL)
        print(f"Warning. No proxies loaded. PARALLEL is limited to {PARALLEL}.")

    places_to_download, images_to_download = count_images_to_download()
    print(f"Total {places_to_download} places with {images_to_download} images to download")

    if images_to_download == 0:
        sys.exit(0)

    if places_to_download < PLACES_PER_THREAD / PARALLEL:
        PLACES_PER_THREAD = max(1, places_to_download // PARALLEL)
        print(f"Warning. PLACES_PER_THREAD was cut down to {PLACES_PER_THREAD}")

    if places_to_download < PROCESS_PLACES:
        PROCESS_PLACES = places_to_download
        print(f"Warning. PROCESS_PLACES was cut down to {PROCESS_PLACES}")

    initialize_download_cache()

    chunk_i = OFFSET_BATCH
    total_time = time.time()
    chunk_lock = threading.Lock()
    total_place_count = 0
    total_image_count = 0
    total_error_count = 0
    total_lock = threading.Lock()
    stop_event = threading.Event()

    print(f"Number of workers: {PARALLEL}", flush=True)
    threading.Thread(target=monitoring_thread, daemon=True).start()

    with ThreadPoolExecutor(max_workers=PARALLEL) as executor:
        futures = [executor.submit(process_chunk, proxy_manager) for _ in range(PARALLEL)]
        for future in futures:
            future.result()

    print(f"Processed {total_place_count} places with {total_image_count} images, {total_error_count} errors.")
    sys.exit(0)
