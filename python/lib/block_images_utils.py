import os
import time
import hashlib
from typing import Any

from database_api import ch_insert, ch_query, ch_query_params

PRODUCTION_MODE = True

BLOCK_BANNED = "banned"
BLOCK_INVALID = "invalid"
BLOCK_PROHIBITED = "prohibited"

IMAGE_WIDTH_MAIN = "1280"
IMAGE_WIDTH_ARRAY = [int(size) for size in os.getenv("SIZES", "160,320,480").split(",")]

IMAGE_DIR = "images"  # test|images
IMAGE_BASE = os.getenv("IMAGE_BASE", "/mnt/hdd1/wikimedia/")
IMAGE_STORAGE_PATH = os.path.join(IMAGE_BASE, f"{IMAGE_DIR}-{IMAGE_WIDTH_MAIN}")

DB = "wiki"
CLEANUP_TABLES = [
    {"table": "top_images_dups", "field": "imageTitle", "sync": True},
    {"table": "wiki_images_downloaded", "field": "name", "sync": True},  # exchange 10s - sync 1s
    {"table": "top_images_final", "field": "imageTitle", "sync": False},  # exchange 30s - sync 1s
    {"table": "categoryimages", "field": "imgName", "sync": False},  # exchange 350s - sync 16s
]

SELECT_BLOCKED = "SELECT imageTitle FROM blocked_images"


def cleanup_tables() -> None:
    for t in CLEANUP_TABLES:
        sync = t["sync"]
        field = t["field"]
        table = f"{DB}.{t['table']}"

        start_time = time.time()

        if PRODUCTION_MODE:
            if sync:
                ch_query(f"DELETE FROM {table} WHERE {field} IN ({SELECT_BLOCKED})")
            else:
                ch_query(f"ALTER TABLE {table} DELETE WHERE {field} IN ({SELECT_BLOCKED})")

        print(f"Cleanup table {table} field {field} ({time.time() - start_time:.2f}s)")


def cleanup_files() -> None:
    blocked_titles = set(row[0] for row in ch_query(SELECT_BLOCKED))
    remove_image_files(blocked_titles)


def _get_where_and_params_for_reason(reason: str | bool | None = True) -> tuple[str, dict[str, Any]]:
    all_reasons = reason is True
    params = {} if all_reasons else {"reason": reason}
    where = "WHERE true" if all_reasons else "WHERE blockReason = %(reason)s"
    return where, params


def unblock_reason(reason: str | bool | None = True) -> None:
    where, params = _get_where_and_params_for_reason(reason)
    select = f"SELECT COUNT(*) FROM blocked_images {where}"
    delete = f"DELETE FROM blocked_images {where}"
    count = ch_query_params(select, params)[0][0]
    if count > 0:
        ch_query_params(delete, params)
    print(f"Unblock by reason {reason} ({count})")


def unblock_title(title: str) -> None:
    params = {"title": title}
    where = "WHERE imageTitle = %(title)s"
    select = f"SELECT COUNT(*) FROM blocked_images {where}"
    delete = f"DELETE FROM blocked_images {where}"
    count = ch_query_params(select, params)[0][0]
    if count > 0:
        ch_query_params(delete, params)
    print(f"Unblock by file {title} ({count})")


def list_blocked(reason: str | bool | None = True) -> None:
    where, params = _get_where_and_params_for_reason(reason)
    query = f"SELECT imageTitle, blockReason, blockedAt FROM blocked_images {where} ORDER BY blockedAt"

    total = 0
    blocked = ch_query_params(query, params)
    for title, reason, blocked_at in blocked:
        total += 1
        print(f"{blocked_at} {title} ({reason})")

    print(f"Total listed: {total}")


def block_images(files: set[str], reason: str) -> None:
    all_titles = set(os.path.basename(path) for path in files)
    already_blocked = set(row[0] for row in ch_query_params(
        f"{SELECT_BLOCKED} WHERE imageTitle IN %(titles)s",
        {'titles': list(all_titles)}
    ))
    for title in all_titles - already_blocked:
        _block_title(title, reason)


def _block_title(title: str, reason: str) -> None:
    print(f"Block title ({reason}): {title}")
    ch_insert("blocked_images", [[title, reason]], column_names=["imageTitle", "blockReason"])


def remove_image_files(files: set[str]) -> None:
    width_list = [IMAGE_WIDTH_MAIN] + [str(width) for width in IMAGE_WIDTH_ARRAY]
    for path in files:
        basename = os.path.basename(path)
        for width in width_list:
            width_dir = f"{IMAGE_DIR}-{width}"
            main_dir = f"{IMAGE_DIR}-{IMAGE_WIDTH_MAIN}"
            width_storage = IMAGE_STORAGE_PATH.replace(main_dir, width_dir)
            _silent_remove(os.path.join(width_storage, _folder_md5(basename), basename))
            if (path.startswith(IMAGE_BASE)):
                _silent_remove(path.replace(main_dir, width_dir))  # fallback for broken md5-folders


def _silent_remove(path: str) -> None:
    try:
        if os.path.isfile(path):
            print(f"Remove file {path}")
            if PRODUCTION_MODE:
                os.remove(path)
    except FileNotFoundError:
        pass


def _folder_md5(filename: str) -> str:
    hash_md5 = hashlib.md5(filename.encode()).hexdigest()
    hash_prefix = f"{hash_md5[0]}/{hash_md5[0:2]}"
    return hash_prefix
