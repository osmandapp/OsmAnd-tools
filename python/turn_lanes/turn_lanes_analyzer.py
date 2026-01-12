import json
import os
from pathlib import Path

OUTPUT_JSON_PATH = os.getenv('OUTPUT_JSON_PATH', 'checker_turn_lanes_results_new6-high.json')


try:
    from python.turn_lanes.RouteShot import MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT as _SCHEMA_MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT
except ImportError:
    from RouteShot import MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT as _SCHEMA_MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT


MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT = [
    (m.value if hasattr(m, 'value') else str(m)) for m in _SCHEMA_MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT
]

_MANEUVER_TYPE_SCALE = {m: idx - MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT.index('C') for idx, m in enumerate(MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT)}


_MANEUVER_TYPE_SET = set(MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT)


def _normalize_turn_type_code(turn_type) -> str | None:
    if turn_type is None:
        return None
    value = str(turn_type)
    start_idx = 0
    while start_idx < len(value) and value[start_idx].isdigit():
        start_idx += 1

    end_idx = len(value)
    while end_idx > start_idx and value[end_idx - 1].isdigit():
        end_idx -= 1

    normalized = value[start_idx:end_idx]
    return normalized if normalized != '' else None


def _lanes_list_to_lanes_string(lanes, active_indexes: list[int]) -> str | None:
    if lanes is None:
        return None
    if not isinstance(lanes, list):
        return None

    normalized_active_indexes: set[int] = set()
    if isinstance(active_indexes, list):
        for idx in active_indexes:
            if isinstance(idx, bool):
                continue
            if isinstance(idx, (int, float)) and int(idx) == idx:
                normalized_active_indexes.add(int(idx))

    lane_strings: list[str] = []
    for lane_index, lane in enumerate(lanes):
        if not isinstance(lane, dict):
            lane_strings.append('')
            continue

        parts: list[str] = []
        for key in ('primary', 'secondary', 'tertiary'):
            value = lane.get(key)
            if value is None:
                continue
            value_str = str(value).strip()
            if value_str == '':
                continue
            parts.append(value_str)

        lane_value = ','.join(parts)
        if lane_index in normalized_active_indexes and lane_value != '':
            lane_value = f"+{lane_value}"
        lane_strings.append(lane_value)

    return '|'.join(lane_strings)

def _lane_indexes_from_lanes(lanes) -> list[int] | None:
    if lanes is None:
        return None
    if not isinstance(lanes, list):
        return None

    lane_indexes: list[int] = []
    for idx, lane in enumerate(lanes):
        if not isinstance(lane, dict):
            continue
        if lane.get('active') is True:
            lane_indexes.append(idx)
    return lane_indexes


def _normalize_lane_indexes(lane_indexes, lanes_count: int | None) -> list[int] | None:
    if lane_indexes is None:
        return None

    if isinstance(lane_indexes, str):
        lane_indexes = lane_indexes.strip()
        if lane_indexes == '':
            return []
        try:
            lane_indexes = json.loads(lane_indexes)
        except json.JSONDecodeError:
            return None

    if not isinstance(lane_indexes, list):
        return None

    normalized: list[int] = []
    for item in lane_indexes:
        if isinstance(item, bool):
            continue
        if isinstance(item, (int, float)) and int(item) == item:
            normalized.append(int(item))
            continue
        if isinstance(item, str) and item.strip().isdigit():
            normalized.append(int(item.strip()))
            continue

    normalized = sorted(set(normalized))
    if lanes_count is not None:
        normalized = [idx for idx in normalized if 0 <= idx < lanes_count]
    return normalized


def _activation_penalty_from_indexes(expected_lane_indexes: list[int] | None, predicted_lane_indexes: list[int] | None, lanes_count: int) -> tuple[int, int]:
    if expected_lane_indexes is None:
        return 0, 0
    if predicted_lane_indexes is None:
        return 1, 0
    if lanes_count <= 0:
        return 0, 0

    expected_active = [False] * lanes_count
    for idx in expected_lane_indexes:
        if 0 <= idx < lanes_count:
            expected_active[idx] = True

    predicted_active = [False] * lanes_count
    for idx in predicted_lane_indexes:
        if 0 <= idx < lanes_count:
            predicted_active[idx] = True

    distance = 0
    if (lanes_count % 2) == 1:
        center_index = (lanes_count - 1) // 2
        for idx, (exp_active, pred_active) in enumerate(zip(expected_active, predicted_active, strict=True)):
            if exp_active ^ pred_active:
                distance += abs(idx - center_index)
    else:
        center_times2 = lanes_count - 1
        for idx, (exp_active, pred_active) in enumerate(zip(expected_active, predicted_active, strict=True)):
            if exp_active ^ pred_active:
                distance += abs((2 * idx) - center_times2)
    return 0, int(distance)


def _calculate_test_case_activation_penalty(item: dict) -> tuple[int, int]:
    expected_segments = item.get('expected') if isinstance(item, dict) else None
    response_segments = item.get('response') if isinstance(item, dict) else None
    if isinstance(item, dict) and ('error' in item):
        return 1, 0
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

        if 'error' in response_segment:
            structural_errors += 1
            continue

        predicted_turn_type = _normalize_turn_type_code(response_segment.get('turn_type'))
        if predicted_turn_type is not None and predicted_turn_type not in _MANEUVER_TYPE_SET:
            structural_errors += 1
            continue

        expected_lanes = expected_segment.get('lanes')
        if not isinstance(expected_lanes, list) or len(expected_lanes) == 0:
            continue

        lanes_count = len(expected_lanes)
        expected_lane_indexes = _normalize_lane_indexes(_lane_indexes_from_lanes(expected_lanes), lanes_count)
        if len(expected_lane_indexes) == 0:
            continue

        predicted_lane_indexes = response_segment.get('lane_indexes')
        if 0 < lanes_count < len(predicted_lane_indexes):
            structural_errors += 1
            continue
        predicted_lane_indexes = _normalize_lane_indexes(predicted_lane_indexes, lanes_count)

        segment_structural_errors, segment_penalty = _activation_penalty_from_indexes(
            expected_lane_indexes,
            predicted_lane_indexes,
            lanes_count,
        )
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

    if not isinstance(expected_segments, list):
        item['id'] = expected_segments['id']
        expected_segments = expected_segments["segments"]
        item["expected"] = expected_segments

    if not isinstance(response_segments, list):
        response_segments = response_segments["segments"]
        item["response"] = response_segments
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
        expected_lanes = expected_segment.get('lanes')
        expected_turn_type = _normalize_turn_type_code(expected_segment.get('turn_type'))

        predicted_lane_indexes = predicted_segment.get('lane_indexes') if isinstance(predicted_segment, dict) else None
        predicted_turn_type = _normalize_turn_type_code(predicted_segment.get('turn_type') if isinstance(predicted_segment, dict) else None)

        if isinstance(expected_lanes, list):
            lanes_total += 1

            lanes_count = len(expected_lanes)
            expected_lane_indexes = _normalize_lane_indexes(_lane_indexes_from_lanes(expected_lanes), lanes_count)
            predicted_lane_indexes = _normalize_lane_indexes(predicted_lane_indexes, lanes_count)
            if (len(expected_lane_indexes) == 0 or predicted_lane_indexes == expected_lane_indexes):
                #predicted_lane_indexes and set(expected_lane_indexes).intersection(predicted_lane_indexes) and abs(len(predicted_lane_indexes) - len(expected_lane_indexes)) <= 1):
                lanes_correct += 1

        if expected_turn_type is not None:
            turn_type_total += 1
            if expected_turn_type == predicted_turn_type:
                turn_type_correct += 1
            else:
                expected_scale = _MANEUVER_TYPE_SCALE.get(expected_turn_type)
                predicted_scale = _MANEUVER_TYPE_SCALE.get(predicted_turn_type) if predicted_turn_type is not None else None
                if expected_scale is None or predicted_scale is None:
                    turn_type_penalty_sum += len(MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT) + 1
                else:
                    turn_type_penalty_sum += abs(expected_scale - predicted_scale)
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
    activation_distance_cases: dict[tuple[str, str], dict] = {}
    turn_type_penalty_cases: dict[tuple[str, str], dict] = {}
    segments_count_total = 0
    for item in results:
        if not isinstance(item, dict):
            continue

        if 'error' in item:
            activation_structural_errors_sum += 1
            processed_test_cases_count += 1
            test_case_key = _get_case_key(item)
            per_test_case[test_case_key] = {
                'id': item.get('id'),
                'lanes_percentage': 0.0,
                'turn_type_percentage': 0.0,
                'activation_structural_errors': 1,
                'activation_distance': 0.0,
                'turn_type_penalty': 0.0,
                'run_count': 1,
                'segments_count': 0,
            }
            continue

        computed_quality = _calculate_test_case_quality(item)
        expected_segments = item.get('expected') if isinstance(item, dict) else None
        segments_count = len(expected_segments) if isinstance(expected_segments, list) else 0
        segments_count_total += segments_count
        lanes_percentage = float(computed_quality.get('lanes_percentage', 0.0) or 0.0)
        turn_type_percentage = float(computed_quality.get('turn_type_percentage', 0.0) or 0.0)
        activation_penalty = computed_quality.get('activation_penalty')
        activation_structural_errors, activation_distance = activation_penalty

        turn_type_penalty = float(computed_quality.get('turn_type_penalty', 0.0) or 0.0)

        if activation_distance > 0:
            expected_segments_for_debug = item.get('expected')
            response_segments_for_debug = item.get('response')
            if isinstance(expected_segments_for_debug, list) and isinstance(response_segments_for_debug, list):
                segments_to_check = min(len(expected_segments_for_debug), len(response_segments_for_debug))
                for segment_index in range(segments_to_check):
                    expected_segment = expected_segments_for_debug[segment_index]
                    response_segment = response_segments_for_debug[segment_index]
                    if not isinstance(expected_segment, dict) or not isinstance(response_segment, dict):
                        continue

                    expected_lanes = expected_segment.get('lanes')
                    if not isinstance(expected_lanes, list) or len(expected_lanes) == 0:
                        continue

                    lanes_count = len(expected_lanes)
                    expected_lane_indexes = _normalize_lane_indexes(_lane_indexes_from_lanes(expected_lanes), lanes_count)
                    if not isinstance(expected_lane_indexes, list) or len(expected_lane_indexes) == 0:
                        continue

                    predicted_lane_indexes = _normalize_lane_indexes(response_segment.get('lane_indexes'), lanes_count)
                    if predicted_lane_indexes is None:
                        predicted_lane_indexes = []

                    _, segment_distance = _activation_penalty_from_indexes(
                        expected_lane_indexes,
                        predicted_lane_indexes,
                        lanes_count,
                    )
                    if segment_distance <= 0:
                        continue

                    segment_id = expected_segment.get('segment_id')
                    segment_id_str = '' if segment_id is None else str(segment_id)
                    test_case_id = item.get('id')
                    test_case_id_str = '' if test_case_id is None else str(test_case_id)
                    entry_key = (test_case_id_str, segment_id_str)

                    expected_lanes_string = _lanes_list_to_lanes_string(expected_lanes, expected_lane_indexes)
                    actual_lanes_string = _lanes_list_to_lanes_string(expected_lanes, predicted_lane_indexes)
                    if expected_lanes_string is None or actual_lanes_string is None:
                        continue

                    existing_debug = activation_distance_cases.get(entry_key)
                    if (not existing_debug) or int(existing_debug.get('distance', 0) or 0) < int(segment_distance):
                        activation_distance_cases[entry_key] = {
                            'id': test_case_id_str,
                            'segment_id': segment_id_str,
                            'distance': int(segment_distance),
                            'expected': expected_lanes_string,
                            'actual': actual_lanes_string,
                        }

        if turn_type_penalty > 0:
            expected_segments_for_debug = item.get('expected')
            response_segments_for_debug = item.get('response')

            if isinstance(expected_segments_for_debug, list) and isinstance(response_segments_for_debug, list):
                segments_to_check = min(len(expected_segments_for_debug), len(response_segments_for_debug))
                for segment_index in range(segments_to_check):
                    expected_segment = expected_segments_for_debug[segment_index]
                    response_segment = response_segments_for_debug[segment_index]
                    if not isinstance(expected_segment, dict) or not isinstance(response_segment, dict):
                        continue

                    expected_turn_type = _normalize_turn_type_code(expected_segment.get('turn_type'))
                    predicted_turn_type = _normalize_turn_type_code(response_segment.get('turn_type'))
                    if expected_turn_type is None:
                        continue
                    if expected_turn_type == predicted_turn_type:
                        continue

                    expected_scale = _MANEUVER_TYPE_SCALE.get(expected_turn_type)
                    predicted_scale = _MANEUVER_TYPE_SCALE.get(predicted_turn_type) if predicted_turn_type is not None else None
                    if expected_scale is None or predicted_scale is None:
                        segment_distance = len(MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT) + 1
                    else:
                        segment_distance = abs(expected_scale - predicted_scale)
                    if segment_distance <= 0:
                        continue

                    segment_id = expected_segment.get('segment_id')
                    segment_id_str = '' if segment_id is None else str(segment_id)
                    test_case_id = item.get('id')
                    test_case_id_str = '' if test_case_id is None else str(test_case_id)
                    entry_key = (test_case_id_str, segment_id_str)

                    existing_debug = turn_type_penalty_cases.get(entry_key)
                    if (not existing_debug) or int(existing_debug.get('distance', 0) or 0) < int(segment_distance):
                        turn_type_penalty_cases[entry_key] = {
                            'id': test_case_id_str,
                            'segment_id': segment_id_str,
                            'distance': int(segment_distance),
                            'expected': expected_turn_type,
                            'actual': predicted_turn_type,
                        }

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
                'run_count': 1,
                'segments_count': segments_count,
            }
        else:
            existing['lanes_percentage'] += lanes_percentage
            existing['turn_type_percentage'] += turn_type_percentage
            existing['activation_structural_errors'] += activation_structural_errors
            existing['activation_distance'] += activation_distance
            existing['turn_type_penalty'] += turn_type_penalty
            existing['run_count'] += 1
            existing['segments_count'] += segments_count

    lanes_avg = round(lanes_percentage_sum / processed_test_cases_count, 2) if processed_test_cases_count else 0.0
    next_avg = round(turn_type_percentage_sum / processed_test_cases_count, 2) if processed_test_cases_count else 0.0

    print("\n=== FINAL SUMMARY ===", flush=True)
    print(
        f"TOTAL AVG: lanes={lanes_avg:.2f}% turn_type={next_avg:.2f}% turn_type_penalty={turn_type_penalty_sum:.2f} activation_errors={activation_structural_errors_sum:.2f} activation_distance={activation_distance_sum:.1f} (cases={processed_test_cases_count}, segments={segments_count_total})",
        flush=True,
    )

    per_case_items = []
    for case_item in per_test_case.values():
        run_count = int(case_item.get('run_count', 1) or 1)
        segments_count = int(case_item.get('segments_count', 0) or 0)
        per_case_items.append({
            'id': case_item.get('id'),
            'lanes_percentage': round(float(case_item.get('lanes_percentage', 0.0)) / run_count, 2),
            'turn_type_percentage': round(float(case_item.get('turn_type_percentage', 0.0)) / run_count, 2),
            'activation_structural_errors': int(round(float(case_item.get('activation_structural_errors', 0.0)) / run_count, 0)),
            'activation_distance': round(float(case_item.get('activation_distance', 0.0)) / run_count, 1),
            'turn_type_penalty': round(float(case_item.get('turn_type_penalty', 0.0)) / run_count, 2),
            'count': segments_count,
        })

    per_case_items.sort(key=lambda x: (-x.get('activation_structural_errors', 0), -x.get('turn_type_penalty', 0.0), -x.get('activation_distance', 0.0), -x.get('count', 0)))

    print("WORST TEST CASES", flush=True)
    print(f"{'id':>6}  {'error':>6}  {'act_dist':>5}  {'tt_dist':>5}  {'lanes%':>5}  {'turn_type%':>7}  {'seg_count':>5} ", flush=True)
    for case_item in per_case_items:
        test_case_id = case_item.get('id')
        test_case_id_str = '' if test_case_id is None else str(test_case_id)
        activation_structural_errors = int(case_item.get('activation_structural_errors', 0) or 0)
        activation_distance = float(case_item.get('activation_distance', 0.0) or 0.0)
        lanes_pct = float(case_item.get('lanes_percentage', 0.0) or 0.0)
        turn_type_pct = float(case_item.get('turn_type_percentage', 0.0) or 0.0)
        turn_type_penalty = float(case_item.get('turn_type_penalty', 0.0) or 0.0)
        segments_count = int(case_item.get('count', 0) or 0)
        print(f"{test_case_id_str:>6}  {activation_structural_errors:>7d}  {int(activation_distance):>7d}  {int(turn_type_penalty):>7d}  {lanes_pct:>7.2f}  {turn_type_pct:>10.2f}  {segments_count:>3} ", flush=True)

    print("\nCASES WITH activation_distance > 0", flush=True)
    print(f"{'case_id':<8}  {'segment_id':<10}  {'distance':<8}  {'expected':<25}  {'actual':<10}", flush=True)
    rows = list(activation_distance_cases.values())
    rows.sort(key=lambda x: (-int(x.get('distance', 0) or 0), str(x.get('id') or ''), str(x.get('segment_id') or '')))
    for row in rows:
        test_case_id = row.get('id')
        test_case_id_str = '' if test_case_id is None else str(test_case_id)
        segment_id = row.get('segment_id')
        segment_id_str = '' if segment_id is None else str(segment_id)
        distance = int(row.get('distance', 0) or 0)
        expected = str(row.get('expected') or '')
        actual = str(row.get('actual') or '')
        print(f"{test_case_id_str:<8}  {segment_id_str:<10}  {distance:<8}  {expected:<25}  {actual:<20}", flush=True)

    print("\nCASES WITH turn_type_penalty > 0", flush=True)
    print(f"{'case_id':<8}  {'segment_id':<10}  {'distance':<8}  {'expected':<8}  {'actual':<8}", flush=True)
    rows = list(turn_type_penalty_cases.values())
    rows.sort(key=lambda x: (-int(x.get('distance', 0) or 0), str(x.get('id') or ''), str(x.get('segment_id') or '')))
    for row in rows:
        test_case_id = row.get('id')
        test_case_id_str = '' if test_case_id is None else str(test_case_id)
        segment_id = row.get('segment_id')
        segment_id_str = '' if segment_id is None else str(segment_id)
        distance = int(row.get('distance', 0) or 0)
        expected = str(row.get('expected') or '')
        actual = str(row.get('actual') or '')
        print(f"{test_case_id_str:<8}  {segment_id_str:<10}  {distance:<8}  {expected:<8}  {actual:<8}", flush=True)


if __name__ == '__main__':
    output_path = Path(OUTPUT_JSON_PATH)
    results = _load_results(output_path)
    analyze(results)
