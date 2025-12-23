import json
import os
from pathlib import Path

OUTPUT_JSON_PATH = os.getenv('OUTPUT_JSON_PATH', 'turn_lanes_results_gemini-3.0-flash-new.json')


MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT = [
    'TU',
    'TSHL',
    'TL',
    'TSLL',
    'C',
    'TSLR',
    'TR',
    'TSHR',
    'TRU',
]

_MANEUVER_TYPE_SCALE = {m: idx - MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT.index('C') for idx, m in enumerate(MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT)}


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


def _calculate_activation_penalty(expected_lanes: str | None, predicted_lanes: str | None) -> tuple[int, int]:
    if expected_lanes is None:
        return 0, 0
    if predicted_lanes is None:
        return 1, 0

    expected_lanes = str(expected_lanes).strip()
    predicted_lanes = str(predicted_lanes).strip()

    expected_lanes_parts = [p.strip() for p in expected_lanes.split('|')]
    predicted_lanes_parts = [p.strip() for p in predicted_lanes.split('|')]
    if len(expected_lanes_parts) != len(predicted_lanes_parts):
        return 1, 0

    default_all_active = '+' not in expected_lanes and '+' not in predicted_lanes

    structural_errors = 0
    expected_active_flags: list[bool] = []
    predicted_active_flags: list[bool] = []

    for expected_lane, predicted_lane in zip(expected_lanes_parts, predicted_lanes_parts, strict=True):
        expected_plus_count = expected_lane.count('+')
        predicted_plus_count = predicted_lane.count('+')
        if expected_plus_count > 1 or predicted_plus_count > 1:
            structural_errors += 1

        expected_base = expected_lane.replace('+', '').strip()
        predicted_base = predicted_lane.replace('+', '').strip()
        if expected_base != predicted_base:
            structural_errors += 1

        if default_all_active:
            expected_active_flags.append(True)
            predicted_active_flags.append(True)
        else:
            expected_active_flags.append('+' in expected_lane)
            predicted_active_flags.append('+' in predicted_lane)

    if structural_errors:
        return structural_errors, 0

    lanes_count = len(expected_active_flags)
    if lanes_count == 0:
        return 0, 0

    distance = 0
    if (lanes_count % 2) == 1:
        center_index = (lanes_count - 1) // 2
        for idx, (exp_active, pred_active) in enumerate(zip(expected_active_flags, predicted_active_flags, strict=True)):
            if exp_active ^ pred_active:
                distance += abs(idx - center_index)
    else:
        # Uses doubled distances to avoid fractional center at (n-1)/2.
        # For n=4, weights are [3,1,1,3] relative to center=1.5 => abs(i-1.5)*2.
        center_times2 = lanes_count - 1
        for idx, (exp_active, pred_active) in enumerate(zip(expected_active_flags, predicted_active_flags, strict=True)):
            if exp_active ^ pred_active:
                distance += abs((2 * idx) - center_times2)

    return 0, int(distance)


def _calculate_test_case_activation_penalty(item: dict) -> tuple[int, int]:
    expected_segments = item.get('expected') if isinstance(item, dict) else None
    response_segments = item.get('response') if isinstance(item, dict) else None
    if not isinstance(expected_segments, list) or not isinstance(response_segments, list):
        return 1, 0
    if len(expected_segments) != len(response_segments):
        return 1, 0

    structural_errors = 0
    activation_penalty_sum = 0
    activation_penalty_total = 0
    for expected_segment, response_segment in zip(expected_segments, response_segments, strict=True):
        if not isinstance(expected_segment, dict) or not isinstance(response_segment, dict):
            return 1, 0
        expected_lanes = expected_segment.get('lanes_string')
        predicted_lanes = response_segment.get('lanes_string')
        if expected_lanes is None:
            continue

        segment_structural_errors, segment_penalty = _calculate_activation_penalty(expected_lanes, predicted_lanes)
        structural_errors += int(segment_structural_errors)
        if segment_structural_errors == 0:
            activation_penalty_sum += int(segment_penalty)
        activation_penalty_total += 1

    if activation_penalty_total == 0:
        return 0, 0
    return structural_errors, int(activation_penalty_sum)


def _calculate_test_case_quality(item: dict) -> dict:
    expected_segments = item.get('expected') if isinstance(item, dict) else None
    response_segments = item.get('response') if isinstance(item, dict) else None

    if not isinstance(expected_segments, list) or not isinstance(response_segments, list):
        return {
            'lanes_percentage': 0.0,
            'turn_type_percentage': 0.0,
            'activation_penalty': (10, -1),
        }

    lanes_total = 0
    lanes_correct = 0
    turn_type_total = 0
    turn_type_correct = 0
    turn_type_penalty_sum = 0.0
    turn_type_penalty_total = 0

    for idx, expected_segment in enumerate(expected_segments):
        predicted_segment = response_segments[idx] if idx < len(response_segments) else None

        if not isinstance(expected_segment, dict):
            continue
        expected_lanes = expected_segment.get('lanes_string')
        expected_turn_type = expected_segment.get('turn_type')

        predicted_lanes = predicted_segment.get('lanes_string') if isinstance(predicted_segment, dict) else None
        predicted_turn_type = predicted_segment.get('turn_type') if isinstance(predicted_segment, dict) else None

        if expected_lanes is not None:
            lanes_total += 1
            if _normalize_lanes_string(expected_lanes) == _normalize_lanes_string(predicted_lanes):
                lanes_correct += 1

        if expected_turn_type is not None:
            turn_type_total += 1
            if expected_turn_type == predicted_turn_type:
                turn_type_correct += 1
            expected_scale = _MANEUVER_TYPE_SCALE.get(str(expected_turn_type))
            predicted_scale = _MANEUVER_TYPE_SCALE.get(str(predicted_turn_type))
            if expected_scale is None or predicted_scale is None:
                turn_type_penalty_sum += len(MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT) + 1
            else:
                turn_type_penalty_sum += abs(float(expected_scale) - float(predicted_scale))
            turn_type_penalty_total += 1

    lanes_percentage = round((lanes_correct / lanes_total) * 100.0, 2) if lanes_total else 0.0
    turn_type_percentage = round((turn_type_correct / turn_type_total) * 100.0, 2) if turn_type_total else 0.0
    activation_penalty = _calculate_test_case_activation_penalty(item)
    turn_type_penalty = turn_type_penalty_sum

    return {
        'lanes_percentage': lanes_percentage,
        'turn_type_percentage': turn_type_percentage,
        'activation_penalty': activation_penalty,
        'turn_type_penalty': turn_type_penalty,
    }


def _load_results(path: Path) -> list:
    with open(path, 'r', encoding='utf-8') as f:
        loaded = json.load(f)
    if not isinstance(loaded, list):
        raise ValueError(f"Unexpected JSON type in results file: {type(loaded)}")
    return loaded


def _get_case_key(item: dict) -> str:
    case_id = item.get('id') if isinstance(item, dict) else None
    if case_id is None:
        return ''
    return str(case_id)


def analyze(results: list) -> None:
    lanes_percentage_sum = 0.0
    turn_type_percentage_sum = 0.0
    activation_structural_errors_sum = 0
    activation_distance_sum = 0.0
    turn_type_penalty_sum = 0.0
    processed_test_cases_count = 0

    per_test_case = {}

    for item in results:
        if not isinstance(item, dict):
            continue
        if item.get('error') is not None:
            continue

        computed_quality = _calculate_test_case_quality(item)
        lanes_percentage = float(computed_quality.get('lanes_percentage', 0.0) or 0.0)
        turn_type_percentage = float(computed_quality.get('turn_type_percentage', 0.0) or 0.0)
        activation_penalty = computed_quality.get('activation_penalty')
        if not isinstance(activation_penalty, tuple) or len(activation_penalty) != 2:
            activation_penalty = (1, 0.0)
        activation_structural_errors, activation_distance = activation_penalty
        activation_structural_errors = int(activation_structural_errors)
        activation_distance = float(activation_distance)
        turn_type_penalty = float(computed_quality.get('turn_type_penalty', 0.0) or 0.0)

        lanes_percentage_sum += lanes_percentage
        turn_type_percentage_sum += turn_type_percentage
        activation_structural_errors_sum += activation_structural_errors
        activation_distance_sum += activation_distance
        turn_type_penalty_sum += turn_type_penalty
        processed_test_cases_count += 1

        test_case_key = _get_case_key(item)
        existing = per_test_case.get(test_case_key)
        if not existing:
            per_test_case[test_case_key] = {
                'id': item.get('id'),
                'lanes_percentage': lanes_percentage,
                'turn_type_percentage': turn_type_percentage,
                'activation_structural_errors': activation_structural_errors,
                'activation_distance': activation_distance,
                'turn_type_penalty': turn_type_penalty,
                'count': 1,
            }
        else:
            existing['lanes_percentage'] += lanes_percentage
            existing['turn_type_percentage'] += turn_type_percentage
            existing['activation_structural_errors'] += activation_structural_errors
            existing['activation_distance'] += activation_distance
            existing['turn_type_penalty'] += turn_type_penalty
            existing['count'] += 1

    lanes_avg = round(lanes_percentage_sum / processed_test_cases_count, 2) if processed_test_cases_count else 0.0
    next_avg = round(turn_type_percentage_sum / processed_test_cases_count, 2) if processed_test_cases_count else 0.0
    activation_structural_errors_avg = round(activation_structural_errors_sum / processed_test_cases_count, 2) if processed_test_cases_count else 0.0
    activation_distance_avg = round(activation_distance_sum / processed_test_cases_count, 1) if processed_test_cases_count else 0.0
    turn_type_penalty_avg = round(turn_type_penalty_sum / processed_test_cases_count, 1) if processed_test_cases_count else 0.0

    print("\n=== FINAL SUMMARY ===", flush=True)
    print(
        f"TOTAL AVG: lanes={lanes_avg:.2f}% turn_type={next_avg:.2f}% turn_type_penalty={turn_type_penalty_avg:.2f} activation_errors={activation_structural_errors_avg:.2f} activation_distance={activation_distance_avg:.1f} (cases={processed_test_cases_count})",
        flush=True,
    )

    per_case_items = []
    for case_item in per_test_case.values():
        count = int(case_item.get('count', 1) or 1)
        per_case_items.append({
            'id': case_item.get('id'),
            'lanes_percentage': round(float(case_item.get('lanes_percentage', 0.0)) / count, 2),
            'turn_type_percentage': round(float(case_item.get('turn_type_percentage', 0.0)) / count, 2),
            'activation_structural_errors': int(round(float(case_item.get('activation_structural_errors', 0.0)) / count, 0)),
            'activation_distance': round(float(case_item.get('activation_distance', 0.0)) / count, 1),
            'turn_type_penalty': round(float(case_item.get('turn_type_penalty', 0.0)) / count, 2),
            'count': count,
        })

    per_case_items.sort(key=lambda x: (-x.get('activation_structural_errors', 0), -x.get('activation_distance', 0.0), -x.get('turn_type_penalty', 0.0), -x.get('count', 1)))
    worst_items = per_case_items[:10]

    print("WORST 10 TEST CASES (by activation penalty, then turn_type penalty)", flush=True)
    print(f"{'id':>6}  {'act_error':>7}  {'act_dist':>7}  {'tt_dist':>7}  {'lanes%':>7}  {'turn_type%':>10}  {'n':>3} ", flush=True)
    for case_item in worst_items:
        test_case_id = case_item.get('id')
        test_case_id_str = '' if test_case_id is None else str(test_case_id)
        activation_structural_errors = int(case_item.get('activation_structural_errors', 0) or 0)
        activation_distance = float(case_item.get('activation_distance', 0.0) or 0.0)
        lanes_pct = float(case_item.get('lanes_percentage', 0.0) or 0.0)
        turn_type_pct = float(case_item.get('turn_type_percentage', 0.0) or 0.0)
        turn_type_penalty = float(case_item.get('turn_type_penalty', 0.0) or 0.0)
        count = int(case_item.get('count', 1) or 1)
        print(f"{test_case_id_str:>6}  {activation_structural_errors:>7d}  {int(activation_distance):>7d}  {int(turn_type_penalty):>7d}  {lanes_pct:>7.2f}  {turn_type_pct:>10.2f}  {count:>3} ", flush=True)


if __name__ == '__main__':
    output_path = Path(OUTPUT_JSON_PATH)
    results = _load_results(output_path)
    analyze(results)
