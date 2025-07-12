# Compute hashes for images
import os
import threading
import time
import traceback
from datetime import datetime
from threading import current_thread
from typing import List, Tuple, Dict

import clickhouse_connect
import faiss
import numpy as np
import requests
import torch
from transformers import CLIPModel, CLIPImageProcessor

from .QueueThreadPoolExecutor import BoundedThreadPoolExecutor
from python.lib.database_api import get_dups_run_max_id, PROCESS_PLACES, insert_dups, MIN_ELO, MIN_ELO_SUBTYPE, \
    get_image_dups, QUAD, process_quad, get_places_per_quad, SAVE_SCORE_ENV, get_places, ImageItem, get_unscored_places_dups
from python.lib.download_utils import download_pil_image

PARALLEL = int(os.getenv('PARALLEL', '10'))
SELECTED_PLACE_IDS = os.getenv('SELECTED_PLACE_IDS', '')
SELECTED_MEDIA_IDS = os.getenv('SELECTED_MEDIA_IDS', '')
STATUS_TIME_OUT = int(os.getenv('STATUS_TIME_OUT', '60'))

# Load CLIP model
# openai/clip-vit-base-patch32, openai/clip-vit-large-patch14, openai/clip-vit-base-patch16
# google/vit-base-patch16-224, google/vit-large-patch16-224, microsoft/beit-base-patch16-224
# sentence-transformers/clip-ViT-B-32
print(faiss.get_compile_options())
model = CLIPModel.from_pretrained("openai/clip-vit-large-patch14")
processor = CLIPImageProcessor.from_pretrained("openai/clip-vit-large-patch14")

# Check for available devices
if torch.backends.mps.is_available():
    device = torch.device("mps")  # Use MPS for Apple Silicon
elif torch.cuda.is_available():
    device = torch.device("cuda")  # Use CUDA for NVIDIA GPUs
else:
    device = torch.device("cpu")  # Fallback to CPU
if not any([QUAD, SELECTED_PLACE_IDS]):
    raise ValueError("Missing required environment variables: QUAD or SELECTED_PLACE_IDS is required.")

print(f"Using device: {device}, PARALLEL: {PARALLEL}, QUAD: {QUAD}, SELECTED: {SELECTED_PLACE_IDS}/{SELECTED_MEDIA_IDS}, MIN_ELO={MIN_ELO}, MIN_ELO_SUBTYPE={MIN_ELO_SUBTYPE}")
model.to(device)


# Generate embeddings for images
def _get_embeddings(images: List):
    embeddings = []
    for image in images:
        inputs = processor(images=image, return_tensors="pt").to(device)
        with torch.no_grad():
            outputs = model.get_image_features(**inputs)
        embeddings.append(outputs.squeeze().cpu().numpy())
        image.close()

    #    torch.mps.empty_cache()
    return np.vstack(embeddings)


def duplicates_similarity(images: List, image_paths: List[str], rng) -> Dict[
    Tuple[str, str], float]:
    # Generate embeddings (e.g., using CLIP)
    embeddings = _get_embeddings(images)

    faiss.normalize_L2(embeddings)
    index = faiss.IndexFlatIP(embeddings.shape[1])
    # index = faiss.IndexHNSWFlat(embeddings.shape[1], 32)  # HNSW often faster than IVF on ARM
    # index.hnsw.efSearch = 128  # Tune for speed/recall
    index.add(embeddings)

    # Iterate over each image embedding to find duplicates.
    similarity = {}
    for i in rng:
        query_embedding = embeddings[i:i + 1]
        distances, indices = index.search(query_embedding, k=2)

        for j, dist in zip(indices[0], distances[0]):
            # Skip comparing the image with itself.
            if j == i:
                continue

            sim = float(dist)
            sim = 0.0 if sim < 0.0 else sim
            sim = 1.0 if sim > 1.0 else sim
            similarity[(image_paths[i], image_paths[j])] = sim

    return similarity


def _get_images(image_paths: List[ImageItem]) -> Tuple[List, List[str], List[int]]:
    images, paths, sizes = [], [], []
    for item in image_paths:
        img = download_pil_image(item.path)
        if img is None or item.size == 0:
            continue

        images.append(img)
        paths.append(item.path)
        sizes.append(item.size)
    return images, paths, sizes


def process_place(run_id: int, place_id, is_selected: bool, media_ids: List[int]):
    started = datetime.now()
    start_time = time.time()
    try:
        paths = get_image_dups(place_id)
        if len(media_ids) > 0:
            new_paths = [row.path for row in paths if row.media_id in media_ids]
        elif not is_selected:
            new_paths = [row.path for row in paths if not row.is_processed]
        else:
            new_paths = []

        if not is_selected and len(new_paths) == 0:
            print(f"#{current_thread().name}. Place {place_id} is up to date. Time: {(time.time() - start_time):.0f}s", flush=True)
            return False, place_id

        print(f"#{current_thread().name}. Place {place_id} with {len(paths)}/{len(new_paths)} images are going to be loaded. Expected time: {(len(paths) / 60 * 0.55):.1f}min", flush=True)
        images, image_paths, sizes = _get_images(paths)
        if len(images) == 0:
            print(f"#{current_thread().name}. Warning: Place Q{place_id} with {len(images)} images is skipped.")
            insert_dups(run_id, place_id, {}, {}, started, time.time() - start_time, "No images.", SAVE_SCORE_ENV)
            return False, place_id

        rng = range(len(images)) if len(new_paths) == 0 else [image_paths.index(n) for n in new_paths if n in image_paths]
        sims = duplicates_similarity(images, image_paths, rng)

        sims_sym = [(k, v) for k, v in sims.items() if (k[1], k[0]) not in sims]
        for k, v in sims_sym:
            sims[k[1], k[0]] = v

        sim_maps = {n: [] for n in image_paths}
        for k, v in sims.items():
            sim_maps[k[0]].append((k[1], v))
        sim_maps = {k: v for k, v in sim_maps.items() if len(v) > 0}

        insert_dups(run_id, place_id, {image_paths[i]: s for i, s in enumerate(sizes)}, sim_maps, started, time.time() - start_time, '', SAVE_SCORE_ENV)
        print(f"#{current_thread().name}. Place {place_id} with {len(images)}/{len(sim_maps)} image's similarity are saved. Time: {(time.time() - start_time):.0f}s, Timestamp: {datetime.now()}",
              flush=True)
    except (
            requests.exceptions.ConnectionError,
            clickhouse_connect.driver.exceptions.DatabaseError,
    ) as e:
        print(f"#{current_thread().name}. Warning: Could not process place Q{place_id}: {e}. Need to wait 30 sec and retry again ...")
        time.sleep(30)
    except Exception as e:
        traceback.print_exc()
        print(f"#{current_thread().name}. Error for place Q{place_id}: {e}")
        insert_dups(run_id, place_id, {}, {}, started, time.time() - start_time, f"{e}", SAVE_SCORE_ENV)
    return False, place_id


stop_immediately = False
total_place_count = 0


def done_callback(future):
    global stop_immediately, total_place_count

    stop, place = future.result()
    if stop:
        stop_immediately = True
    total_place_count += 1


def find_duplicates():
    run_id = get_dups_run_max_id() + 1
    media_ids = [int(p.strip()) for p in SELECTED_MEDIA_IDS.split(',') if p.strip() != '']
    place_ids = [str(p.strip()) for p in SELECTED_PLACE_IDS.split(',') if p.strip() != '']
    is_selected = len(place_ids) > 0
    try:
        quads = process_quad(QUAD) if QUAD and not is_selected else ['']
        for quad in quads:
            places = get_places(place_ids) if is_selected else get_places_per_quad(quad)
            place_ids = [p[0] for p in places]
            if not is_selected and len(place_ids) > 0:
                place_ids = get_unscored_places_dups(place_ids)

            if stop_immediately or total_place_count >= PROCESS_PLACES:
                break

            print(f"Run #{run_id} is processing {len(place_ids)} places ({quad}) from chunk:{place_ids}...", flush=True)

            sub_start_time = time.time()
            for place_id in place_ids:
                executor.submit(run_id, place_id, is_selected, media_ids)
                if stop_immediately:
                    break

            print(f"Run #{run_id} processed {total_place_count} places totally. Time:{(time.time() - sub_start_time):.0f}s (Total: {(time.time() - start_time):.0f}s, Timestamp: {datetime.now()})",
                  flush=True)
    except Exception as e:
        traceback.print_exc()
        print(f"Error processing batch: {e}")

    print(f"Run #{run_id} finished. Places: {total_place_count}. Stop: {stop_immediately}, Total time: {(time.time() - start_time):.0f}s, Timestamp: {datetime.now()}", flush=True)


executor = BoundedThreadPoolExecutor(process_place, done_callback, PARALLEL, "Thread")


def check_status():
    time.sleep(STATUS_TIME_OUT)

    while not is_done:
        tasks_args = executor.undone_futures_args()

        timings = [((time.time() - t), args[1]) for t, args in tasks_args if (time.time() - t) > STATUS_TIME_OUT]
        timings = [f"Q{place_id}:{t:.0f}s" for t, place_id in timings]
        print(f"Timeout status: {timings}. Total time: {(time.time() - start_time):.0f}s", flush=True)
        time.sleep(STATUS_TIME_OUT)


if __name__ == "__main__":
    is_done = False
    start_time = time.time()

    status_thread = threading.Thread(target=check_status)
    status_thread.start()
    try:
        with executor:
            find_duplicates()
    finally:
        is_done = True
