import csv
import io
import json
import os
import sys
from pathlib import Path

OUTPUT_JSON_PATH = os.getenv('OUTPUT_JSON_PATH', 'validator_turn_lanes_results_llm.json')
INCORRECT_SEGMENTS_JSON_PATH = os.getenv('INCORRECT_SEGMENTS_JSON_PATH', 'incorrect_segments.json')


def _load_results(path: Path) -> list:
    with open(path, 'r', encoding='utf-8') as f:
        loaded = json.load(f)
    if not isinstance(loaded, list):
        raise ValueError(f"Unexpected JSON type in results file: {type(loaded)}")
    return loaded


def analyze(results: list) -> None:
    def _to_int(value, field_name: str, context: str) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            stripped_value = value.strip()
            if stripped_value.isdigit():
                return int(stripped_value)
        raise ValueError(f"{field_name} must be int (or numeric string) in {context}, got {value!r} ({type(value)})")

    def _lane_code_from_lane_dict(lane: dict) -> str:
        primary = lane.get('primary')
        secondary = lane.get('secondary')
        tertiary = lane.get('tertiary')

        codes: list[str] = []
        for value in (primary, secondary, tertiary):
            if isinstance(value, str) and value:
                codes.append(value)
        return ','.join(codes)

    def _encode_lane_codes_and_actives(codes: list[str], actives: list[bool]) -> str:
        encoded_parts: list[str] = []
        for lane_code, lane_active in zip(codes, actives):
            normalized_code = lane_code if isinstance(lane_code, str) else ''
            if lane_active:
                encoded_parts.append(f"+{normalized_code}")
            else:
                encoded_parts.append(normalized_code)
        return '|'.join(encoded_parts)

    def _encode_lanes_from_lane_dicts(lanes: list[dict]) -> str:
        lane_codes: list[str] = []
        lane_actives: list[bool] = []
        for lane in lanes:
            if not isinstance(lane, dict):
                continue
            lane_codes.append(_lane_code_from_lane_dict(lane))
            lane_actives.append(bool(lane.get('active')))
        return _encode_lane_codes_and_actives(lane_codes, lane_actives)

    def _extract_lane_actives(lanes: object) -> list[bool]:
        if not isinstance(lanes, list):
            return []

        extracted_actives: list[bool] = []
        for lane in lanes:
            if isinstance(lane, dict):
                extracted_actives.append(bool(lane.get('active')))
            elif isinstance(lane, bool):
                extracted_actives.append(lane)
            elif isinstance(lane, int) and lane in (0, 1):
                extracted_actives.append(bool(lane))
        return extracted_actives

    def _encode_actives_only(actives: list[bool]) -> str:
        return '|'.join('1' if active else '0' for active in actives)

    incorrect_segments_path = Path(INCORRECT_SEGMENTS_JSON_PATH)
    ground_truth_incorrect_segments = _load_results(incorrect_segments_path)

    ground_truth_incorrect_points_by_case: dict[int, dict[int, dict]] = {}
    for ground_truth_item in ground_truth_incorrect_segments:
        if not isinstance(ground_truth_item, dict):
            continue
        case_id = _to_int(ground_truth_item.get('case_id'), 'case_id', 'incorrect_segments.json')
        point = _to_int(ground_truth_item.get('point'), 'point', f"incorrect_segments.json case_id={case_id}")
        ground_truth_incorrect_points_by_case.setdefault(case_id, {})[point] = ground_truth_item

    writer = csv.writer(sys.stdout, lineterminator='\n')
    writer.writerow(['case_id', 'point_id', 'gt_is_correct', 'llm_is_correct', 'gt_lanes', 'actual_lanes_encoded', 'llm_active_lanes', 'reason', ])

    confusion_true_positive = 0
    confusion_false_positive = 0
    confusion_true_negative = 0
    confusion_false_negative = 0
    missing_expected_points = 0
    skipped_no_lanes_points = 0
    lane_count_mismatch_points = 0

    output_rows: list[tuple[int, int, list]] = []

    for case in results:
        if not isinstance(case, dict):
            raise ValueError(f"Unexpected case type in results: {type(case)}")

        case_id = _to_int(case.get('case_id'), 'case_id', 'results')
        actual = case.get('actual', [])
        expected = case.get('expected', [])

        if not isinstance(actual, list):
            raise ValueError(f"Unexpected 'actual' type for case_id={case_id}: {type(actual)}")
        if not isinstance(expected, list):
            raise ValueError(f"Unexpected 'expected' type for case_id={case_id}: {type(expected)}")

        expected_by_point: dict[int, dict] = {}
        for expected_item in expected:
            if not isinstance(expected_item, dict):
                continue
            point_value = expected_item.get('point')
            if point_value is None:
                continue
            point = _to_int(point_value, 'point', f"results expected case_id={case_id}")
            expected_by_point[point] = expected_item

        ground_truth_incorrect_points = ground_truth_incorrect_points_by_case.get(case_id, {})

        for actual_item in actual:
            if not isinstance(actual_item, dict):
                continue

            point = _to_int(actual_item.get('point'), 'point', f"results actual case_id={case_id}")

            actual_lanes = actual_item.get('lanes')
            has_lanes_data = isinstance(actual_lanes, list) and len(actual_lanes) > 0

            actual_lanes_encoded = ''
            if has_lanes_data:
                actual_lanes_encoded = _encode_lanes_from_lane_dicts(actual_lanes)

            ground_truth_item = ground_truth_incorrect_points.get(point)
            ground_truth_is_correct = ground_truth_item is None

            ground_truth_lanes_encoded = ''
            ground_truth_lane_count: int | None = None
            if has_lanes_data:
                if ground_truth_is_correct:
                    ground_truth_lanes_encoded = actual_lanes_encoded
                    ground_truth_lane_count = len(actual_lanes)
                else:
                    expected_lane_codes = ground_truth_item.get('expected_lane_codes', [])
                    expected_lane_actives = ground_truth_item.get('expected_lane_actives', [])
                    if not isinstance(expected_lane_codes, list):
                        expected_lane_codes = []
                    if not isinstance(expected_lane_actives, list):
                        expected_lane_actives = []
                    ground_truth_lanes_encoded = _encode_lane_codes_and_actives(
                        [str(code) for code in expected_lane_codes],
                        [bool(active) for active in expected_lane_actives],
                    )
                    ground_truth_lane_count = min(len(expected_lane_codes), len(expected_lane_actives))

            expected_item = expected_by_point.get(point)
            if expected_item is None:
                llm_is_correct = None
                llm_lanes_encoded = ''
                llm_active_lanes: list[bool] = []
                llm_lane_count = None
                reason = ''
            else:
                llm_is_correct = expected_item.get('is_correct')
                if not isinstance(llm_is_correct, bool):
                    llm_is_correct = None

                expected_lanes = expected_item.get('lanes', [])
                llm_active_lanes = _extract_lane_actives(expected_lanes)
                if expected_lanes and not llm_active_lanes:
                    llm_lanes_encoded = _encode_lanes_from_lane_dicts(expected_lanes)
                else:
                    llm_lanes_encoded = _encode_actives_only(llm_active_lanes)
                llm_lane_count = len(llm_active_lanes)

                reason = expected_item.get('reason', '')
                if reason is None:
                    reason = ''

            if not has_lanes_data:
                skipped_no_lanes_points += 1
            elif llm_is_correct is None:
                missing_expected_points += 1
            else:
                if ground_truth_is_correct and llm_is_correct:
                    confusion_true_positive += 1
                elif (not ground_truth_is_correct) and llm_is_correct:
                    confusion_false_positive += 1
                elif (not ground_truth_is_correct) and (not llm_is_correct):
                    confusion_true_negative += 1
                else:
                    confusion_false_negative += 1

            has_lane_count_mismatch = False
            if has_lanes_data and llm_lane_count is not None and llm_lane_count != len(actual_lanes):
                has_lane_count_mismatch = True
            if has_lanes_data and (not ground_truth_is_correct) and ground_truth_lane_count is not None and ground_truth_lane_count != len(actual_lanes):
                has_lane_count_mismatch = True
            if has_lane_count_mismatch:
                lane_count_mismatch_points += 1

            output_rows.append((
                case_id,
                point,
                [
                    case_id,
                    point,
                    ground_truth_is_correct,
                    '' if llm_is_correct is None else llm_is_correct,
                    '' if not has_lanes_data else ground_truth_lanes_encoded,
                    actual_lanes_encoded,
                    '' if llm_is_correct is None else json.dumps(llm_active_lanes),
                    reason,
                ],
            ))

    confusion_total = confusion_true_positive + confusion_false_positive + confusion_true_negative + confusion_false_negative
    accuracy = (confusion_true_positive + confusion_true_negative) / confusion_total if confusion_total else 0.0

    output_rows.sort(key=lambda row: (row[0], row[1]))
    for _, __, csv_row in output_rows:
        writer.writerow(csv_row)

        gt_is_correct = csv_row[2]
        llm_is_correct = csv_row[3]
        gt_lanes_encoded = csv_row[4]
        actual_lanes_encoded = csv_row[5]
        llm_active_lanes_json = csv_row[6]

        is_correctness_mismatch = (
            isinstance(gt_is_correct, bool)
            and isinstance(llm_is_correct, bool)
            and gt_is_correct != llm_is_correct
        )
        is_lane_count_mismatch = False
        if gt_lanes_encoded and actual_lanes_encoded and len(gt_lanes_encoded.split('|')) != len(actual_lanes_encoded.split('|')):
            is_lane_count_mismatch = True

        parsed_llm_active_lanes: list[bool] = []
        if isinstance(llm_active_lanes_json, str) and llm_active_lanes_json:
            try:
                loaded_llm_active_lanes = json.loads(llm_active_lanes_json)
            except Exception:
                loaded_llm_active_lanes = None
            parsed_llm_active_lanes = _extract_lane_actives(loaded_llm_active_lanes)

        if actual_lanes_encoded and isinstance(llm_is_correct, bool):
            if len(parsed_llm_active_lanes) != len(actual_lanes_encoded.split('|')):
                is_lane_count_mismatch = True

        is_expected_actives_mismatch = False
        if isinstance(gt_is_correct, bool) and gt_is_correct is False:
            expected_actives: list[bool] = []
            ground_truth_for_point = ground_truth_incorrect_points_by_case.get(csv_row[0], {}).get(csv_row[1])
            if isinstance(ground_truth_for_point, dict):
                expected_actives = _extract_lane_actives(ground_truth_for_point.get('expected_lane_actives'))
            if expected_actives or parsed_llm_active_lanes:
                is_expected_actives_mismatch = expected_actives != parsed_llm_active_lanes

        if is_correctness_mismatch or is_lane_count_mismatch or is_expected_actives_mismatch:
            buffer = io.StringIO()
            stderr_csv_writer = csv.writer(buffer, lineterminator='')
            stderr_csv_writer.writerow(csv_row)
            mismatched_line = buffer.getvalue()
            print(f"\x1b[31m{mismatched_line}\x1b[0m", file=sys.stderr)

    print((f"Confusion matrix (positive=correct; gt_is_correct,llm_is_correct): "
           f"TP(T,T)={confusion_true_positive}, FP(F,T)={confusion_false_positive}, "
           f"TN(F,F)={confusion_true_negative}, FN(T,F)={confusion_false_negative}, "
           f"total={confusion_total}, accuracy={accuracy:.4f}"), file=sys.stderr, )
    print(f"Missing expected points (have lanes, missing/invalid llm_is_correct): {missing_expected_points}", file=sys.stderr, )
    print(f"Skipped points (no lanes data in actual): {skipped_no_lanes_points}", file=sys.stderr, )
    print(f"Lane-count mismatch points: {lane_count_mismatch_points}", file=sys.stderr, )


if __name__ == '__main__':
    output_path = Path(OUTPUT_JSON_PATH)
    results = _load_results(output_path)
    analyze(results)
