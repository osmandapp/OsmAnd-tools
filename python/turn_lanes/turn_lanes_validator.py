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
TEST_IMAGES_DIR = os.getenv('TEST_IMAGES_DIR')
WEB_SERVER_CONFIG_PATH = os.getenv('WEB_SERVER_CONFIG_PATH')
OUTPUT_JSON_PATH = os.getenv('OUTPUT_JSON_PATH', 'turn_lanes_results_llm.json')
IS_ROLE_CHECKER = os.getenv('ROLE', 'checker').lower() == 'checker'

if not TEST_IMAGES_DIR:
    raise ValueError("Missing required environment variable: TEST_IMAGES_DIR is required.")
if not WEB_SERVER_CONFIG_PATH:
    raise ValueError("Missing required environment variable: WEB_SERVER_CONFIG_PATH is required.")

with open(Path(WEB_SERVER_CONFIG_PATH) / 'llm/turn_lanes_prompts.yaml', 'r', encoding='utf-8') as f:
    prompts = yaml.safe_load(f)

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

    allowed_ext = {'.png'}
    for file_path in sorted(images_dir.iterdir()):
        if file_path.is_file() and file_path.suffix.lower() in allowed_ext:
            yield file_path


def _extract_response(case_id: int, input_test_case: dict, response_text: str):
    actual_segments = input_test_case['segments']

    if not isinstance(response_text, str) or not response_text.strip():
        error = f"Empty LLM response: {response_text}"
        return {"case_id": case_id, "actual": actual_segments, "error": error}

    fenced_match = re.search(r"```json\s*(.*?)\s*```", response_text, flags=re.DOTALL | re.IGNORECASE)
    json_text = fenced_match.group(1).strip() if fenced_match else response_text.strip()

    try:
        parsed_json = json.loads(json_text)
    except Exception as ex:
        error = f"Cannot parse JSON: {ex}"
        return {"case_id": case_id, "actual": actual_segments, "error": error, "response": json_text}

    return {"case_id": case_id, "actual": actual_segments, "expected": parsed_json}

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

    schema = _load_schema_text()
    prompt = prompts['SYSTEM_PROMPT_VALIDATOR'].format(schema=schema)

    OUTPUT_JSON_PATH = 'validator_' + OUTPUT_JSON_PATH
    for image_path in _iter_test_images(images_dir):
        case_id = int(image_path.stem)
        test_case_json_path = images_dir / f'{case_id}.json'
        with open(test_case_json_path, 'r', encoding='utf-8') as f:
            test_case = json.load(f)

        image_path = images_dir / f'{case_id}.png'
        test_case_prompt = prompts['INPUT_PROMPT'].format(json=json.dumps(test_case, ensure_ascii=False))
        image_base64 = _pil_image_to_base64(image_path)

        response, is_error = llm_client.request_with_image(prompt, [(image_path.name, image_base64)], [test_case_prompt])
        if is_error:
            extracted = {"case_id": case_id, "error": response}
        else:
            extracted = _extract_response(case_id, test_case, response)
        results.append(extracted)

    output_path = Path(OUTPUT_JSON_PATH)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(results)} items to {output_path}", flush=True)

    total_duration = time.time() - total_start_time
    print(f"DURATION: total={total_duration:.2f}s", flush=True)
