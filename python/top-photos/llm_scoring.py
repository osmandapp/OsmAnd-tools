import os
import re
import sys
from pathlib import Path
from typing import List, Callable, Dict, Tuple

import json5
import yaml

from ..lib.OpenAIClient import OpenAIClient
from ..lib.block_images_utils import block_images, BLOCK_PROHIBITED

MODEL = os.getenv('MODEL')
API_URL = os.getenv('API_URL', 'http://localhost:11434/v1')
API_KEY = os.getenv('API_KEY')
MAX_PHOTOS_PER_REQUEST = int(os.getenv('MAX_PHOTOS_PER_REQUEST', '15'))
WEB_SERVER_CONFIG_PATH = os.getenv('WEB_SERVER_CONFIG_PATH')

# Check for required environment variables
if not all([MODEL, API_KEY, WEB_SERVER_CONFIG_PATH]):
    raise ValueError("Missing required environment variables (MODEL, API_KEY, WEB_SERVER_CONFIG_PATH)")

with open(Path(WEB_SERVER_CONFIG_PATH) / 'llm/scoring_prompts.yaml', 'r', encoding='utf-8') as f:
    prompts = yaml.safe_load(f)


def calculate_aggr(data: List, aggr_fun: Callable) -> dict[str, float]:
    aggr = {}
    if len(data) <= 1:
        return aggr

    keys = list(filter(lambda x: x.endswith('_score'), data[0].keys()))
    for k in keys:
        # Extract all values for the current score
        values = [entry[k] for entry in data]
        # Calculate standard deviation
        aggr[k] = aggr_fun(values)
    return aggr


llm_client = OpenAIClient(MODEL, API_KEY, API_URL)


def _get_json(body: str) -> List[dict] | None:
    if not body:
        print(f"Incorrect LLM response: {body}")
        return None

    try:
        json_resp = re.search(r'\[.*]', body, re.DOTALL).group()
        items = json5.loads(json_resp)
        if not hasattr(items, "__len__"):
            items = [items]
        return items
    except Exception as e:
        print(f"Incorrect response JSON. {e}")
        print(f"LLM response: '{body}'")
    return None


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


def split_list_by_n(l: List, n) -> List[List]:
    return [l[i:i + n] for i in range(0, len(l), n)]


def _filter_prohibited_content(prompt, images, pos) -> Tuple[List, List]:
    res_items, prohibited_items, prohibited_files = [], [], set()
    for i, im in enumerate(images):
        reason, is_error = llm_client.ask_with_image(prompt, [im], pos + i, "Check image.")
        if is_error:
            prohibited_items.append({"photo_id": im[2], "safe_reason": reason})
            prohibited_files.add(im[0])
            continue
        res_items.append(im)

    block_images(prohibited_files, BLOCK_PROHIBITED)
    return res_items, prohibited_items


def call_llm(prompt, images) -> Dict:
    res, pos = [], 0
    duration, prompt_tokens, completion_tokens = [0, 0, 0]
    split_images = split_array(images, 0 if len(images) <= MAX_PHOTOS_PER_REQUEST else int(len(images) // MAX_PHOTOS_PER_REQUEST) + 1)
    for chunk_images in split_images:
        new_prompt = prompt.replace("%PHOTO_COUNT%", f"{len(chunk_images)}").replace("%PHOTO_IDS%", f"{[im[2] for im in chunk_images]}")
        llm_resp, is_error = llm_client.ask_with_image(new_prompt, chunk_images, pos)
        prohibited_ims = []
        if is_error and llm_resp == "PROHIBITED_CONTENT":
            duration += llm_client.duration
            prompt_tokens += llm_client.prompt_tokens
            completion_tokens += llm_client.completion_tokens

            allowed_ims, prohibited_ims = _filter_prohibited_content("Does image include prohibited content?", chunk_images, pos)
            new_prompt = prompt.replace("%PHOTO_COUNT%", f"{len(allowed_ims)}").replace("%PHOTO_IDS%", f"{[im[2] for im in allowed_ims]}")
            llm_resp, is_error = llm_client.ask_with_image(new_prompt, allowed_ims, pos)
        if is_error:
            raise RuntimeError(llm_resp)

        pos += len(chunk_images)

        duration += llm_client.duration
        prompt_tokens += llm_client.prompt_tokens
        completion_tokens += llm_client.completion_tokens

        res_item = _get_json(llm_resp)
        if not res_item:
            res_item = [None] * len(chunk_images)
        else:
            for im in prohibited_ims:
                res_item.append(im)

        if len(chunk_images) != len(res_item):
            print(f"Warning: LLM response contains {len(res_item)} but expected {len(chunk_images)} results.")
            if len(chunk_images) < len(res_item):
                res_item = res_item[:len(chunk_images)]
            else:
                res_item.extend([None] * (len(chunk_images) - len(res_item)))

        res.extend(res_item)
    if len(images) != len(res):
        print(f"Assert: {len(images)} but expected {len(res)} results.")

    return {'cached_tokens': 0, 'results': res, 'duration': duration, 'prompt_tokens': prompt_tokens, 'completion_tokens': completion_tokens}
