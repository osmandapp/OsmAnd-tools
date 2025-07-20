#!/usr/bin/env python3

import gc
import os
import sys
import time
from collections import defaultdict
from multiprocessing import Pool

from PIL import Image

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'lib')))

from python.lib.block_images_utils import IMAGE_WIDTH_MAIN, IMAGE_WIDTH_ARRAY, IMAGE_STORAGE_PATH
from python.lib.block_images_utils import BLOCK_INVALID, block_images, remove_image_files
from python.lib.download_utils import is_valid_image_file_name

# Configuration
SIZES = IMAGE_WIDTH_ARRAY
SOURCE_DIR = IMAGE_STORAGE_PATH
PARALLEL = int(os.getenv('PARALLEL', '15'))
BUCKET_SIZE = int(os.getenv('BUCKET_SIZE', '1000'))  # Files per task


def scan_directory(base_path: str) -> set[str]:
    """Scan a directory and return set of relative paths"""
    files = set()
    if os.path.exists(base_path):
        for root, _, filenames in os.walk(base_path):
            for f in filenames:
                if is_valid_image_file_name(f):
                    rel_path = os.path.relpath(str(os.path.join(root, f)), base_path)
                    files.add(rel_path)
    return files


def build_incomplete_source_files() -> list[str]:
    """Build a map of all existing files across all sizes"""
    resized_map = defaultdict(set)
    start_time = time.time()
    print("Scanning directories...", flush=True)

    source_files = scan_directory(SOURCE_DIR)
    duration = time.time() - start_time
    print(f"Found {len(source_files)} source images - {duration:.2f}s", flush=True)

    for size in SIZES:
        start_time = time.time()
        target_dir = SOURCE_DIR.replace(IMAGE_WIDTH_MAIN, str(size))
        resized_map[size] = scan_directory(target_dir)
        duration = time.time() - start_time
        print(f"Found {len(resized_map[size])} resized images in {target_dir} - {duration:.2f}s", flush=True)

    incomplete_source_files: list[str] = list()

    for src in source_files:
        completed = True
        for size in SIZES:
            if src not in resized_map[size]:
                completed = False
                break
        if not completed:
            incomplete_source_files.append(src)

    source_files.clear()
    resized_map.clear()
    gc.collect()

    return incomplete_source_files


def process_bucket(args):
    """Process a bucket of images"""
    bucket_files, bucket_num = args
    start_time = time.time()
    processed = errors = 0
    invalid_files = set()

    for rel_path in bucket_files:
        src_path = os.path.join(SOURCE_DIR, rel_path)
        try:
            for size in SIZES:
                target_rel = os.path.join(os.path.dirname(rel_path), os.path.basename(rel_path))
                target_path = os.path.join(SOURCE_DIR.replace(IMAGE_WIDTH_MAIN, str(size)), target_rel)
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                # print(f"Resize: src({src_path}) dst({target_path})")
                with Image.open(src_path) as img:
                    img.resize(
                        (size, int(size * img.height / img.width)),
                        Image.Resampling.LANCZOS
                    ).save(target_path)
                    processed += 1
        except Exception as e:
            print(f"Error processing {src_path}: {e}", file=sys.stderr, flush=True)
            invalid_files.add(src_path)
            errors += 1

    duration = time.time() - start_time
    return len(bucket_files), processed, errors, invalid_files, duration, bucket_num


def main():
    source_files = build_incomplete_source_files()

    # Split files into buckets
    buckets = [source_files[i:i + BUCKET_SIZE]
               for i in range(0, len(source_files), BUCKET_SIZE)]

    print(f"\nStarting processing with {PARALLEL} workers across {len(buckets)} tasks...", flush=True)
    print(f"Each task processes up to {BUCKET_SIZE} files\n", flush=True)

    # task_results = []
    total_processed = total_resized = total_errors = 0
    start_time = time.time()
    invalid_files = set()

    with Pool(PARALLEL) as pool:
        args = [(bucket, i) for i, bucket in enumerate(buckets)]
        ### 1. Requires large batches and tasks > 5-10s
        for result in pool.imap_unordered(process_bucket, args):
            files_in_task, resized_in_task, errors_in_task, invalid_files_in_task, duration, bucket_num = result
            ### 2. Faster but no progress
            # results = pool.map_async(process_bucket, args).get()
            # for i, (files_in_task, resized_in_task, duration, bucket_num) in enumerate(results):
            # task_results.append((bucket_num, files_in_task, resized_in_task, duration))
            total_processed += files_in_task
            total_resized += resized_in_task
            total_errors += errors_in_task
            total_duration = time.time() - start_time
            invalid_files.update(invalid_files_in_task)
            print(
                f"Task {bucket_num:03d} completed: "
                f"{files_in_task} files, {resized_in_task} resized, {errors_in_task} errors "
                f"in {duration:.2f}s - Total {total_duration:.2f}s ", flush=True)

    total_duration = time.time() - start_time

    print(f"\nTotal: {total_processed} files processed, {total_resized} resized, {total_errors} errors", flush=True)
    print(f"Total time: {total_duration:.2f}s ({total_processed / max(total_duration, 1):.2f} files/s)", flush=True)

    block_images(invalid_files, BLOCK_INVALID)
    remove_image_files(invalid_files)


if __name__ == '__main__':
    main()
