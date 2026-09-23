#!/usr/bin/env python3
"""Compare the turn lanes recorded in a generate-turn-lanes-test json with what Valhalla says on the same drives.

    python3 compare.py --json /path/to/test.json --valhalla http://localhost:8002 [--out test.csv]

Every case is routed by Valhalla (OSRM format, which is the only one with lanes), and the route shape is fed
back to /trace_attributes to learn the OSM way of every edge. A Valhalla maneuver is then keyed like an OsmAnd
expectation: by the way the maneuver turns onto (the segment that carries the TurnType in OsmAnd). The lanes
are written in the OsmAnd form - "TURN:lane|lane", "+" for an active lane, the used arrow of a lane first.

CSV columns: num,start,end,segment,osmand,valhalla,status,obf,link
    status  ok        same lanes, and turn types at most --turn-tolerance apart in TurnType.orderFromLeftToRight
                      (TSLR vs KR is 1); the "[MUTE] " prefix is ignored, Valhalla has no such notion
            lanes-ok  same lanes, turn types further apart
            no-lanes  OsmAnd has lanes, Valhalla only the turn (OsmAnd made them up for an untagged road, mostly)
            bug       as many lanes, but no active lane in common: TL|TL|+C|C,TR vs TL|TL|C|+TR,C
            diff      different lanes otherwise (+C|+C|+C|TR vs +C|C|C|TR shares an active lane: diff)
            missing   Valhalla gives nothing on that way (other route, or no route at all)
Only expectations with lanes on either side are written, --all writes every expectation.
"""
import argparse
import csv
import json
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor

LANE = {'straight': 'C', 'slight left': 'TSLL', 'left': 'TL', 'sharp left': 'TSHL',
        'slight right': 'TSLR', 'right': 'TR', 'sharp right': 'TSHR', 'uturn': 'TU', 'none': 'C'}
KEEP_TYPES = ('fork', 'off ramp', 'on ramp', 'merge')
SKIP_TYPES = ('depart', 'arrive', 'exit roundabout', 'exit rotary')
MUTE = '[MUTE] '
# TurnType.orderFromLeftToRight
ORDER = {'TU': -5, 'TSHL': -4, 'TL': -3, 'TSLL': -2, 'KL': -1, 'C': 0,
         'KR': 1, 'TSLR': 2, 'TR': 3, 'TSHR': 4, 'TRU': 5}


def post(url, body):
    req = urllib.request.Request(url, json.dumps(body).encode(), {'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        return json.loads(e.read() or b'{}')


def decode(polyline, precision=6):
    points, idx, lat, lon, f = [], 0, 0, 0, 10.0 ** precision
    while idx < len(polyline):
        for coord in (0, 1):
            shift = result = 0
            while True:
                b = ord(polyline[idx]) - 63
                idx += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            d = ~(result >> 1) if result & 1 else result >> 1
            if coord == 0:
                lat += d
            else:
                lon += d
        points.append((lat / f, lon / f))
    return points


def lane_code(indication, left_side):
    if indication == 'uturn' and left_side:
        return 'TRU'
    return LANE.get(indication, 'C')


def lanes_string(lanes, left_side):
    out = []
    for lane in lanes:
        ind = list(lane.get('indications') or ['none'])
        used = lane.get('valid_indication')
        if used in ind:
            ind.remove(used)
            ind.insert(0, used)
        codes = []
        for i in ind:
            c = lane_code(i, left_side)
            if c not in codes:
                codes.append(c)
        out.append(('+' if lane.get('active', lane.get('valid')) else '') + ','.join(codes))
    return '|'.join(out)


def turn_type(maneuver, left_side):
    kind, mod = maneuver.get('type'), maneuver.get('modifier')
    if kind in ('roundabout', 'rotary'):
        return ('RNLB' if left_side else 'RNDB') + str(maneuver.get('exit', 1))
    if kind in KEEP_TYPES and mod:
        if 'left' in mod:
            return 'KL'
        if 'right' in mod:
            return 'KR'
    return lane_code(mod, left_side) if mod else 'C'


def way_at(point, shape, edges):
    """way id of the edge leaving the shape point closest to point ([lon, lat] as OSRM gives it)"""
    lon, lat = point
    best = min(range(len(shape)), key=lambda i: (shape[i][0] - lat) ** 2 + (shape[i][1] - lon) ** 2)
    for e in edges:
        if e.get('begin_shape_index') == best:
            return e.get('way_id')
    for e in edges:
        if e.get('begin_shape_index', 0) <= best < e.get('end_shape_index', 0):
            return e.get('way_id')
    return None


def valhalla_records(url, case, costing):
    """[(way_id, is_maneuver, 'TURN:lanes', [lon, lat])] along the Valhalla route, in route order"""
    left_side = str(case.get('params', {}).get('leftSide', '')).lower() == 'true'
    locs = [{'lat': p['latitude'], 'lon': p['longitude']} for p in (case['startPoint'], case['endPoint'])]
    route = post(url + '/route', {'locations': locs, 'costing': costing, 'format': 'osrm',
                                  'banner_instructions': True})
    if not route.get('routes'):
        return None
    geometry = route['routes'][0]['geometry']
    trace = post(url + '/trace_attributes', {
        'encoded_polyline': geometry, 'shape_match': 'edge_walk', 'costing': costing,
        'filters': {'attributes': ['edge.way_id', 'edge.begin_shape_index', 'edge.end_shape_index', 'shape'],
                    'action': 'include'}})
    edges, shape = trace.get('edges'), decode(trace.get('shape', ''))
    if not edges or not shape:
        return None
    records = []
    steps = [s for leg in route['routes'][0]['legs'] for s in leg['steps']]
    for step in steps:
        m = step['maneuver']
        ints = step.get('intersections') or []
        if m.get('type') not in SKIP_TYPES:
            # the lanes to take the maneuver from are on its own intersection
            lanes = ints[0].get('lanes') if ints else None
            turn = turn_type(m, left_side)
            text = turn + ':' + lanes_string(lanes, left_side) if lanes else turn
            records.append((way_at(m['location'], shape, edges), True, text, m['location']))
        for x in ints[1:]:
            if x.get('lanes'):
                records.append((way_at(x['location'], shape, edges), False, 'C:' + lanes_string(x['lanes'], left_side),
                                x['location']))
    return records


def road_key(key):
    way, _, start = key.partition(':')
    return int(way), int(start) if start else -1


def match(expected, records):
    """{expectation key: (valhalla string, [lon, lat]) or None}"""
    by_way = {}
    for way, maneuver, text, location in records or []:
        by_way.setdefault(way, []).append((maneuver, (text, location)))
    out = {}
    groups = {}
    for key in expected:
        way, start = road_key(key)
        groups.setdefault(way, []).append((start, key))
    for way, keys in groups.items():
        found = by_way.get(way, [])
        # a turn onto the way first, the lanes passed along it only when there is no turn
        found = [t for m, t in found if m] + [t for m, t in found if not m]
        for i, (_, key) in enumerate(sorted(keys)):
            out[key] = found[i] if i < len(found) else None
    return out


def strip_mute(s):
    return s[len(MUTE):] if s.startswith(MUTE) else s


def close_turns(a, b, tolerance):
    if a == b:
        return True
    return a in ORDER and b in ORDER and abs(ORDER[a] - ORDER[b]) <= tolerance


def active_lanes(lanes):
    return [lane.startswith('+') for lane in lanes.split('|')] if lanes else []


def status(osmand, valhalla, tolerance):
    if valhalla is None:
        return 'missing'
    a_turn, _, a_lanes = strip_mute(osmand).partition(':')
    b_turn, _, b_lanes = valhalla.partition(':')
    if a_lanes == b_lanes:
        return 'ok' if close_turns(a_turn, b_turn, tolerance) else 'lanes-ok'
    if not b_lanes:
        return 'no-lanes'
    a_active, b_active = active_lanes(a_lanes), active_lanes(b_lanes)
    # the lanes may be lit differently, as long as at least one lane both light is the same
    if len(a_active) == len(b_active) and not any(a and b for a, b in zip(a_active, b_active)):
        return 'bug'
    return 'diff'


def link(case, location):
    """osmand.net route between the case's points, zoomed on the Valhalla maneuver (the middle when there is none)"""
    a, b = case['startPoint'], case['endPoint']
    if location:
        lon, lat = location
    else:
        lat, lon = (a['latitude'] + b['latitude']) / 2, (a['longitude'] + b['longitude']) / 2
    return 'https://osmand.net/map/navigate/?start=%.6f,%.6f&end=%.6f,%.6f&profile=car#18/%.5f/%.5f' % (
        a['latitude'], a['longitude'], b['latitude'], b['longitude'], lat, lon)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--json', required=True, help='cases written by generate-turn-lanes-test')
    parser.add_argument('--valhalla', default='http://localhost:8002', help='Valhalla url (default: %(default)s)')
    parser.add_argument('--out', help='csv to write, <json name>.csv beside the json by default')
    parser.add_argument('--costing', default='auto')
    parser.add_argument('--turn-tolerance', type=int, default=2,
                        help='turn types this far apart in orderFromLeftToRight still match (default: %(default)s)')
    parser.add_argument('--threads', type=int, default=8)
    parser.add_argument('--all', action='store_true', help='also expectations without lanes on either side')
    args = parser.parse_args()

    with open(args.json) as f:
        cases = json.load(f)
    url = args.valhalla.rstrip('/')
    if not url.startswith('http'):
        url = 'http://' + url
    out = args.out or os.path.splitext(args.json)[0] + '.csv'
    try:
        urllib.request.urlopen(url + '/status', timeout=10).close()
    except OSError as e:
        sys.exit('Valhalla is not answering on %s: %s' % (url, e))

    def one(case):
        try:
            return valhalla_records(url, case, args.costing)
        except Exception as e:
            print('%s: %s' % (case.get('testName'), e), file=sys.stderr)
            return None

    with ThreadPoolExecutor(args.threads) as pool:
        all_records = list(pool.map(one, cases))

    counts, num = {}, 0
    with open(out, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['num', 'start', 'end', 'segment', 'osmand', 'valhalla', 'status', 'obf', 'link'])
        for case, records in zip(cases, all_records):
            expected = case.get('expectedResults', {})
            got = match(expected, records)
            start = '%.5f,%.5f' % (case['startPoint']['latitude'], case['startPoint']['longitude'])
            end = '%.5f,%.5f' % (case['endPoint']['latitude'], case['endPoint']['longitude'])
            for key, osmand in expected.items():
                valhalla, location = got.get(key) or (None, None)
                if not args.all and ':' not in strip_mute(osmand) and ':' not in (valhalla or ''):
                    continue
                st = status(osmand, valhalla, args.turn_tolerance)
                counts[st] = counts.get(st, 0) + 1
                num += 1
                w.writerow([num, start, end, key, osmand, valhalla or '', st, case.get('params', {}).get('map', ''),
                            link(case, location)])
    print('%s: %d rows, %s' % (out, num, ', '.join('%s %d' % kv for kv in sorted(counts.items()))))
    print('no Valhalla route for %d of %d cases' % (sum(r is None for r in all_records), len(cases)))


if __name__ == '__main__':
    main()
