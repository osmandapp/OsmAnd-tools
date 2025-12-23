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
ROLE = os.getenv('ROLE', 'predictor')

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
        if not file_path.stem.isdigit():
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            loaded = json.load(f)

        if isinstance(loaded, list):
            for item in loaded:
                if not isinstance(item, dict):
                    continue
                item_id = item.get('id')
                if item_id is None or str(item_id).strip() == '':
                    continue
                test_cases_by_id[str(item_id)] = item
        elif isinstance(loaded, dict):
            item_id = loaded.get('id')
            if item_id is None or str(item_id).strip() == '':
                loaded = dict(loaded)
                loaded['id'] = file_path.stem
                item_id = loaded['id']
            test_cases_by_id[str(item_id)] = loaded
        else:
            raise ValueError(f"Unexpected JSON type in test case file: {file_path} type={type(loaded)}")

    if not test_cases_by_id:
        raise ValueError(f"No test cases loaded from TEST_JSON_DIR: {test_cases_dir}")
    return test_cases_by_id


def _normalize_lanes_string(lanes_string: str | None) -> str | None:
    if lanes_string is None:
        return None
    lanes_string = str(lanes_string).strip()
    if lanes_string == '':
        return ''

    parts = [p.strip() for p in lanes_string.split('|')]

    if '+' not in lanes_string:
        normalized_parts = []
        for part in parts:
            if part == '':
                normalized_parts.append('')
            elif part.startswith('+'):
                normalized_parts.append(part)
            else:
                normalized_parts.append('+' + part)
        return '|'.join(normalized_parts)

    return '|'.join(parts)


def _extract_response(input_test_case: dict, response_text: str):
    if not isinstance(response_text, str) or not response_text.strip():
        raise ValueError("Empty LLM response")

    fenced_match = re.search(r"```json\s*(.*?)\s*```", response_text, flags=re.DOTALL | re.IGNORECASE)
    json_text = fenced_match.group(1).strip() if fenced_match else response_text.strip()

    try:
        parsed_json = json.loads(json_text)
    except json.JSONDecodeError:
        first_brace = json_text.find('{')
        last_brace = json_text.rfind('}')
        if first_brace >= 0 and last_brace > first_brace:
            parsed_json = json.loads(json_text[first_brace:last_brace + 1])
        else:
            raise

    if isinstance(parsed_json, dict) and isinstance(parsed_json.get('segments'), list):
        resp_test_case_segments = parsed_json.get('segments')
    elif isinstance(parsed_json, list):
        resp_test_case_segments = parsed_json
    else:
        raise ValueError(f"Unexpected response JSON type: {type(parsed_json)}")

    expected_segments = input_test_case.get('segments') if isinstance(input_test_case, dict) else None
    if not isinstance(expected_segments, list):
        raise ValueError("Expected test case must contain a 'segments' list")

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

    expected_ids_order = [str(s.get('segment_id')) for s in expected_segments if isinstance(s, dict) and s.get('segment_id') is not None]
    expected_id_set = set(expected_ids_order)
    predicted_id_set = set(predicted_ids_order)

    missing_ids = [segment_id for segment_id in expected_ids_order if segment_id not in predicted_id_set]
    extra_ids = [segment_id for segment_id in predicted_ids_order if segment_id not in expected_id_set]
    common_ids_in_expected_order = [segment_id for segment_id in expected_ids_order if segment_id in predicted_id_set]
    common_ids_in_predicted_order = [segment_id for segment_id in predicted_ids_order if segment_id in expected_id_set]
    is_order_exact = expected_ids_order == predicted_ids_order
    is_order_preserved_for_common = common_ids_in_expected_order == common_ids_in_predicted_order

    lanes_total = 0
    lanes_correct = 0
    next_total = 0
    next_correct = 0

    rows = []
    for segment_id in expected_ids_order:
        expected_segment = expected_by_id.get(segment_id)
        predicted_segment = predicted_by_id.get(segment_id)

        expected_lanes = expected_segment.get('lanes_string') if isinstance(expected_segment, dict) else None
        predicted_lanes = predicted_segment.get('lanes_string') if isinstance(predicted_segment, dict) else None
        expected_next = expected_segment.get('turn_type') if isinstance(expected_segment, dict) else None
        predicted_next = predicted_segment.get('turn_type') if isinstance(predicted_segment, dict) else None

        lanes_ok = False
        if expected_lanes is not None:
            lanes_total += 1
            lanes_ok = _normalize_lanes_string(expected_lanes) == _normalize_lanes_string(predicted_lanes)
            if lanes_ok:
                lanes_correct += 1

        next_ok = False
        if expected_next is not None:
            next_total += 1
            next_ok = expected_next == predicted_next
            if next_ok:
                next_correct += 1

        rows.append({
            'segment_id': segment_id,
            'lanes_ok': lanes_ok,
            'next_ok': next_ok,
            'expected_lanes': expected_lanes,
            'predicted_lanes': predicted_lanes,
            'expected_turn_type': expected_next,
            'predicted_turn_type': predicted_next,
            'missing_in_response': predicted_segment is None,
        })

    test_case_id = input_test_case.get('id') if isinstance(input_test_case, dict) else None
    header = f"TestCase {test_case_id}: segments={len(expected_ids_order)} missing={len(missing_ids)} extra={len(extra_ids)} lanes={lanes_correct}/{lanes_total} next={next_correct}/{next_total} order_exact={is_order_exact} order_preserved_common={is_order_preserved_for_common}"
    print(header, flush=True)

    col_id = 'segment_id'
    col_lanes = 'lanes_string'
    col_lanes_ok = 'lanes_ok'
    col_next = 'turn_type'
    col_next_ok = 'next_ok'
    print(f"{col_id:>6}  {col_lanes_ok:>8}  {col_next_ok:>7}  {col_lanes:<20}  {col_next}", flush=True)
    for r in rows:
        expected_lanes_str = '' if r['expected_lanes'] is None else str(r['expected_lanes'])
        predicted_lanes_str = '' if r['predicted_lanes'] is None else str(r['predicted_lanes'])
        lanes_str = f"{expected_lanes_str} -> {predicted_lanes_str}".strip()

        expected_next_str = '' if r['expected_turn_type'] is None else str(r['expected_turn_type'])
        predicted_next_str = '' if r['predicted_turn_type'] is None else str(r['predicted_turn_type'])
        next_str = f"{expected_next_str} -> {predicted_next_str}".strip()

        lanes_ok_str = 'OK' if r['lanes_ok'] else ('MISS' if r['missing_in_response'] else 'NO')
        next_ok_str = 'OK' if r['next_ok'] else ('MISS' if r['missing_in_response'] else 'NO')
        print(f"{r['segment_id']:>6}  {lanes_ok_str:>8}  {next_ok_str:>7}  {lanes_str:<20}  {next_str}", flush=True)

    lanes_percentage = round((lanes_correct / lanes_total) * 100.0, 2) if lanes_total else 0.0
    turn_type_percentage = round((next_correct / next_total) * 100.0, 2) if next_total else 0.0

    quality = {
        'lanes_percentage': lanes_percentage,
        'turn_type_percentage': turn_type_percentage,
    }

    expected_simplified_segments = []
    for segment_id in expected_ids_order:
        expected_segment = expected_by_id.get(segment_id)
        if not isinstance(expected_segment, dict):
            continue
        expected_simplified_segments.append({
            'lanes_string': expected_segment.get('lanes_string'),
            'turn_type': expected_segment.get('turn_type'),
        })

    return {"id": test_case_id, "expected": expected_simplified_segments, "response": resp_test_case_segments, "quality": quality}

test_cases = _load_test_cases(test_json_path)

def _create_test_case(id: str):
    if 'SHOT_PROMPT_' + id in prompts:
        test_case = test_cases[id]
        for s in test_case["segments"]:
            s["lanes_string"] = s["lanes_string"].replace("+", "")
            del s["turn_type"]
        prompt = prompts['SHOT_PROMPT_' + id].format(json=json.dumps(test_case, ensure_ascii=False))
        del test_cases[id]
    else:
        test_case = copy.deepcopy(test_cases[id])
        for s in test_case["segments"]:
            s["lanes_string"] = s["lanes_string"].replace("+", "")
            del s["turn_type"]
        prompt = prompts['INPUT_PROMPT'].format(json=json.dumps(test_case, ensure_ascii=False))

    image_path = images_dir / (id + '.png')
    base64 = _pil_image_to_base64(image_path)
    return test_case,prompt, image_path, base64


def _load_schema_text() -> str:
    schema_path = Path(__file__).resolve().parent / 'RouteShot.py'
    if not schema_path.exists() or not schema_path.is_file():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    with open(schema_path, 'r', encoding='utf-8') as f:
        return f.read()

if __name__ == '__main__':
    total_start_time = time.time()
    images_dir = Path(TEST_IMAGES_DIR)
    results = []

    lanes_percentage_sum = 0.0
    turn_type_percentage_sum = 0.0
    processed_test_cases_count = 0
    per_test_case_lanes = {}
    llm_duration_total = 0.0

    _, example1_prompt, example1_image_path, example1_image_base64 = _create_test_case("16")
    _, example2_prompt, example2_image_path, example2_image_base64 = _create_test_case("31")
    _, example3_prompt, example3_image_path, example3_image_base64 = _create_test_case("23")
    _, example4_prompt, example4_image_path, example4_image_base64 = _create_test_case("27")

    schema = _load_schema_text()
    prompt = prompts['SYSTEM_PROMPT'].format(schema=schema)

    for image_path in _iter_test_images(images_dir):
        image_prefix = image_path.stem
        if image_prefix not in test_cases:
            print(f"{image_path.name}: No matching test case in {test_json_path}", flush=True)
            continue

        test_case, test_case_prompt, image_path, image_base64 = _create_test_case(image_prefix)
        images_payload = [
            (example1_image_path.name, example1_image_base64),
            (example2_image_path.name, example2_image_base64),
            (example3_image_path.name, example3_image_base64),
            (example4_image_path.name, example4_image_base64),
            (image_path.name, image_base64),
        ]
        prompts_payload = [
            example1_prompt,
            example2_prompt,
            example3_prompt,
            example4_prompt,
            test_case_prompt,
        ]
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
                quality = extracted.get('quality') if isinstance(extracted, dict) else None
                if isinstance(quality, dict):
                    lanes_percentage = float(quality.get('lanes_percentage', 0.0) or 0.0)
                    turn_type_percentage = float(quality.get('turn_type_percentage', 0.0) or 0.0)
                    lanes_percentage_sum += lanes_percentage
                    turn_type_percentage_sum += turn_type_percentage
                    processed_test_cases_count += 1

                    original_test_case = test_cases.get(image_prefix)
                    test_case_id = original_test_case.get('id') if isinstance(original_test_case, dict) else None
                    test_case_key = str(test_case_id) if test_case_id is not None else image_prefix

                    existing = per_test_case_lanes.get(test_case_key)
                    if not existing:
                        per_test_case_lanes[test_case_key] = {
                            'id': test_case_id,
                            'lanes_percentage': lanes_percentage,
                            'turn_type_percentage': turn_type_percentage,
                            'count': 1,
                        }
                    else:
                        existing['lanes_percentage'] += lanes_percentage
                        existing['turn_type_percentage'] += turn_type_percentage
                        existing['count'] += 1
        except Exception as e:
            results.append({"image": image_path.name, "error": str(e), "raw": response})

    lanes_avg = round(lanes_percentage_sum / processed_test_cases_count, 2) if processed_test_cases_count else 0.0
    next_avg = round(turn_type_percentage_sum / processed_test_cases_count, 2) if processed_test_cases_count else 0.0

    print("\n=== FINAL SUMMARY ===", flush=True)
    print(f"TOTAL AVG: lanes={lanes_avg:.2f}% turn_type={next_avg:.2f}% (cases={processed_test_cases_count})", flush=True)

    total_duration = time.time() - total_start_time
    print(f"DURATION: total={total_duration:.2f}s llm={llm_duration_total:.2f}s", flush=True)

    per_case_items = []
    for item in per_test_case_lanes.values():
        count = int(item.get('count', 1) or 1)
        per_case_items.append({
            'id': item.get('id'),
            'lanes_percentage': round(float(item.get('lanes_percentage', 0.0)) / count, 2),
            'turn_type_percentage': round(float(item.get('turn_type_percentage', 0.0)) / count, 2),
            'count': count,
        })

    output_path = Path(OUTPUT_JSON_PATH)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(results)} items to {output_path}", flush=True)
