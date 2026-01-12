import os
import base64
import io
import copy
import json
import re
import time
from pathlib import Path

import yaml
from PIL import Image

from python.lib.OpenAIClient import OpenAIClient

MODEL = os.getenv('MODEL', 'or@google/gemini-3-flash-preview')
API_KEY = os.getenv('API_KEY')
TEST_JSON_DIR = os.getenv('TEST_JSON_DIR')
TEST_IMAGES_DIR = os.getenv('TEST_IMAGES_DIR')
WEB_SERVER_CONFIG_PATH = os.getenv('WEB_SERVER_CONFIG_PATH')
OUTPUT_JSON_PATH = os.getenv('OUTPUT_JSON_PATH', 'turn_lanes_results_llm.json')
IS_ROLE_CHECKER = os.getenv('ROLE', 'checker').lower() == 'checker'

if not TEST_IMAGES_DIR:
    raise ValueError("Missing required environment variable: TEST_IMAGES_DIR is required.")
if not TEST_JSON_DIR:
    raise ValueError("Missing required environment variable: TEST_JSON_DIR is required.")
if not WEB_SERVER_CONFIG_PATH:
    raise ValueError("Missing required environment variable: WEB_SERVER_CONFIG_PATH is required.")

with open(Path(WEB_SERVER_CONFIG_PATH) / 'llm/turn_lanes_prompts.yaml', 'r', encoding='utf-8') as f:
    prompts = yaml.safe_load(f)

test_json_path = Path(TEST_JSON_DIR)
llm_client = OpenAIClient(MODEL, API_KEY)


def _pil_image_to_base64(image_path: Path) -> str:
    with Image.open(image_path) as img:
        image_format = (img.format or image_path.suffix.lstrip('.')).upper()
        if image_format == 'JPG':
            image_format = 'JPEG'
        if image_format != 'PNG' and img.mode != 'RGB':
            img = img.convert('RGB')
        buffer = io.BytesIO()
        img.save(buffer, format=image_format)
    return base64.b64encode(buffer.getvalue()).decode('utf-8')


def _iter_test_images(images_dir: Path):
    if not images_dir.exists() or not images_dir.is_dir():
        raise FileNotFoundError(f"TEST_IMAGES_DIR not found or not a directory: {images_dir}")

    allowed_ext = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.gif', '.tif', '.tiff'}
    for file_path in sorted(images_dir.iterdir()):
        if file_path.is_file() and file_path.suffix.lower() in allowed_ext:
            yield file_path


def _load_test_cases(test_cases_dir: Path) -> dict:
    if not test_cases_dir.exists() or not test_cases_dir.is_dir():
        raise FileNotFoundError(f"TEST_JSON_DIR not found or not a directory: {test_cases_dir}")

    test_cases_by_id = {}
    for file_path in sorted(test_cases_dir.iterdir()):
        if not file_path.is_file() or file_path.suffix.lower() != '.json':
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            loaded = json.load(f)

        item_id = loaded.get('id')
        if item_id is None or str(item_id).strip() == '':
            raise ValueError(f"Unexpected JSON type in test case file: {file_path} type={type(loaded)}")
        test_cases_by_id[str(item_id)] = loaded

    if not test_cases_by_id:
        raise ValueError(f"No test cases loaded from TEST_JSON_DIR: {test_cases_dir}")
    return test_cases_by_id


def _lane_indexes_from_lanes(lanes) -> list[int] | None:
    if lanes is None:
        return None
    if not isinstance(lanes, list):
        raise ValueError(f"Expected 'lanes' to be a list, got: {type(lanes)}")

    lane_indexes: list[int] = []
    for idx, lane in enumerate(lanes):
        if not isinstance(lane, dict):
            continue
        if lane.get('active'):
            lane_indexes.append(idx)
    return lane_indexes

def _extract_response(input_test_case: dict, response_text: str):
    test_case_id = input_test_case.get('id')
    expected_simplified_segments = []
    expected_segments = input_test_case['segments']
    for expected_segment in expected_segments:
        if not isinstance(expected_segment, dict):
            continue
        expected_lanes = expected_segment.get('lanes')
        expected_simplified_segments.append({
            'segment_id': expected_segment.get('segment_id'),
            'lanes' : expected_lanes,
            'lane_indexes': _lane_indexes_from_lanes(expected_lanes) if isinstance(expected_lanes, list) else None,
            'turn_type': expected_segment.get('turn_type'),
        })

    if not isinstance(response_text, str) or not response_text.strip():
        error = f"Empty LLM response: {response_text}"
        return {"id": test_case_id, "expected": expected_simplified_segments, "error": error}

    fenced_match = re.search(r"```json\s*(.*?)\s*```", response_text, flags=re.DOTALL | re.IGNORECASE)
    json_text = fenced_match.group(1).strip() if fenced_match else response_text.strip()

    try:
        parsed_json = json.loads(json_text)
    except Exception as ex:
        error = f"Cannot parse JSON: {ex}"
        return {"id": test_case_id, "expected": expected_simplified_segments, "error": error, "response": json_text}

    if isinstance(parsed_json, dict) and isinstance(parsed_json.get('segment_id'), int):
        resp_test_case_segments = [parsed_json]
    elif isinstance(parsed_json, list):
        resp_test_case_segments = parsed_json
    else:
        error = f"Unexpected response JSON type: {type(parsed_json)}"
        return {"id": test_case_id, "expected": expected_simplified_segments, "error": error, "response": json_text}

    expected_by_id = {}
    for expected_segment in expected_segments:
        if not isinstance(expected_segment, dict):
            continue
        segment_id = expected_segment.get('segment_id')
        if segment_id is None:
            continue
        expected_by_id[str(segment_id)] = expected_segment

    predicted_by_id = {}
    predicted_ids_order = []
    for predicted_segment in resp_test_case_segments:
        if not isinstance(predicted_segment, dict):
            continue
        segment_id = predicted_segment.get('segment_id')
        if segment_id is None:
            continue
        segment_id_str = str(segment_id)
        predicted_by_id[segment_id_str] = predicted_segment
        predicted_ids_order.append(segment_id_str)

    return {"id": test_case_id, "expected": expected_simplified_segments, "response": resp_test_case_segments}

test_cases = _load_test_cases(test_json_path)

def _create_test_case(case_id: str, is_few_shot: bool = False):
    test_case = copy.deepcopy(test_cases[case_id])
    for s in test_case["segments"]:
        lanes = s.get('lanes')
        for lane in lanes:
            del lane['active']
        #del s["turn_type"]

    if not is_few_shot:
        prompt = prompts['INPUT_PROMPT'].format(json=json.dumps(test_case, ensure_ascii=False))
    else:
        prompt = prompts['SHOT_PROMPT_' + case_id].format(json=json.dumps(test_case, ensure_ascii=False))

    image_path = images_dir / (case_id + '.png')
    base64 = _pil_image_to_base64(image_path)
    return test_case, prompt, image_path, base64


def _load_schema_text() -> str:
    schema_path = Path(__file__).resolve().parent / 'RouteShot.py'
    if not schema_path.exists() or not schema_path.is_file():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    with open(schema_path, 'r', encoding='utf-8') as f:
        return f.read()


def _create_few_shots(example_ids: list[str], is_few_shot: bool = False):
    examples = []
    for example_id in example_ids:
        _, example_prompt, example_image_path, example_image_base64 = _create_test_case(str(example_id), is_few_shot)
        examples.append((example_prompt, example_image_path, example_image_base64))
    return examples

if __name__ == '__main__':
    total_start_time = time.time()
    images_dir = Path(TEST_IMAGES_DIR)
    results = []

    lanes_percentage_sum = 0.0
    turn_type_percentage_sum = 0.0
    processed_test_cases_count = 0
    per_test_case_lanes = {}
    llm_duration_total = 0.0
    schema = _load_schema_text()
    prompt = prompts['SYSTEM_PROMPT_CHECKER' if IS_ROLE_CHECKER else 'SYSTEM_PROMPT_PREDICTOR'].format(schema=schema)

    if IS_ROLE_CHECKER:
        OUTPUT_JSON_PATH = 'checker_' + OUTPUT_JSON_PATH
        for image_path in _iter_test_images(images_dir):
            image_prefix = image_path.stem
            if image_prefix not in test_cases:
                print(f"{image_path.name}: No matching test case {image_prefix}", flush=True)
                continue

            test_case = test_cases[image_prefix]
            image_path = images_dir / (image_prefix + '.png')
            test_case_prompt = prompts['INPUT_PROMPT'].format(json=json.dumps(test_case, ensure_ascii=False))
            image_base64 = _pil_image_to_base64(image_path)

            response, is_error = llm_client.request_with_image(prompt, [(image_path.name, image_base64)], [test_case_prompt])
            if is_error:
                extracted = {"image": image_prefix, "error": response}
            else:
                extracted = _extract_response(test_case, response)
            results.append(extracted)
    else:
        few_shot_list =[] # ["38", "27", "23"]
        few_shots = [] #_create_few_shots(few_shot_list, True)

        for image_path in _iter_test_images(images_dir):
            image_prefix = image_path.stem
            if image_prefix not in test_cases or image_prefix in few_shot_list:
                print(f"{image_path.name}: No matching test case {image_prefix}", flush=True)
                continue

            _, test_case_prompt, image_path, image_base64 = _create_test_case(image_prefix)
            images_payload = [(shot_image_path.name, shot_image_base64) for _, shot_image_path, shot_image_base64 in few_shots]
            images_payload.append((image_path.name, image_base64))

            prompts_payload = [shot_prompt for shot_prompt, _, _ in few_shots]
            prompts_payload.append(test_case_prompt)
            assert len(images_payload) == len(prompts_payload)

            response, is_error = llm_client.request_with_image(prompt, images_payload, prompts_payload)
            llm_duration_total += float(getattr(llm_client, 'duration', 0.0) or 0.0)
            print(f"{image_path.name}: {response}", flush=True)

            try:
                if is_error:
                    results.append({"image": image_path.name, "error": response})
                else:
                    extracted = _extract_response(test_cases[image_prefix], response)
                    results.append(extracted)
            except Exception as e:
                results.append({"image": image_path.name, "error": str(e), "raw": response})

    output_path = Path(OUTPUT_JSON_PATH)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(results)} items to {output_path}", flush=True)

    total_duration = time.time() - total_start_time
    print(f"DURATION: total={total_duration:.2f}s llm={llm_duration_total:.2f}s", flush=True)
