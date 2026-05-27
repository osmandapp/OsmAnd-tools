#!/usr/bin/env python3
import xml.etree.ElementTree as ET
from math import radians, cos, log10
import math
from collections import defaultdict
from statistics import median
import concurrent.futures
import os

# This script processes data downloaded with generate_prominent_islands.py script
# and generates points_prominent_islands.osm.
# Islands ranking is done using area and quantity of important tags (mostly name tags)

try:
    from scipy.spatial import Voronoi

    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("Warning: scipy not installed. Fallback method will be used.")

# ====================== SETTINGS ======================
MIN_AREA_KM2 = 5.0
LARGE_ISLAND_THRESHOLD = 3000.0
MAX_WORKERS = min(12, os.cpu_count() or 4)

# === SETTINGS FOR island_rank ===
MIN_ISLAND_RANK = 35.0  # Main threshold

# thresholds for area_rounded tag
AREA_THRESHOLDS = [100, 200, 500, 1000, 2000, 4000, 8000, 16000, 32000,
                   64000, 128000, 256000, 512000, 1024000, 2048000]
# =======================================================

def clean_tags_for_output(tags: dict) -> dict:
    """
    Removes all 'natural' tags
    """
    cleaned = {}
    for k, v in tags.items():
        if not k.startswith('natural'):
            cleaned[k] = v
    return cleaned

def get_area_rounded(area_km2: float) -> int:
    for threshold in AREA_THRESHOLDS:
        if area_km2 <= threshold:
            return threshold
    return AREA_THRESHOLDS[-1]


def count_important_tags(tags: dict) -> int:
    """
    Calculated important tags for ranking:
    - All tags containing 'name' itself
    - wikimedia_commons, wikipedia, website, population
    """
    count = 0
    important_keys = {'wikimedia_commons', 'wikipedia', 'website', 'population', 'contact:website', 'ssr:stedsnr'}

    for k in tags.keys():
        k_lower = k.lower()
        if 'name' in k_lower or k_lower in important_keys:
            count += 1
    return count


def calculate_island_rank(area_km2: float, important_tag_count: int) -> float:
    """
    Calculates island_rank:
    - 30% — weight of area (log)
    - 70% — weight of important tags (name* + wikimedia_commons, wikipedia, website, population)
    """
    if area_km2 <= 0:
        area_score = 0.0
    else:
        area_score = min(100.0, 10 * log10(area_km2))

    tag_score = min(100.0, important_tag_count * 9.0)

    rank = 0.3 * area_score + 0.7 * tag_score
    return round(rank, 1)


def point_in_polygon(point, polygon):
    """point = (lon, lat), polygon = [(lon, lat), ...]"""
    x, y = point
    n = len(polygon)
    inside = False
    p1x, p1y = polygon[0]
    for i in range(n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside


def compute_geometric_centroid(points):
    if len(points) < 3:
        if not points:
            return 0.0, 0.0
        lats = [p[0] for p in points]
        lons = [p[1] for p in points]
        return sum(lats) / len(lats), sum(lons) / len(lons)

    poly = points[:-1] if points[0] == points[-1] else points[:]
    n = len(poly)
    area = 0.0
    cx = cy = 0.0
    for i in range(n):
        j = (i + 1) % n
        lat1, lon1 = poly[i]
        lat2, lon2 = poly[j]
        temp = lon1 * lat2 - lon2 * lat1
        area += temp
        cx += (lon1 + lon2) * temp
        cy += (lat1 + lat2) * temp
    area *= 0.5
    if abs(area) < 1e-10:
        lats = [p[0] for p in poly]
        lons = [p[1] for p in poly]
        return sum(lats) / len(lats), sum(lons) / len(lons)
    return cy / (6.0 * area), cx / (6.0 * area)


def simplify_for_test(points, max_pts=250):
    if len(points) <= max_pts:
        return points
    step = len(points) // max_pts + 1
    return points[::step]


def distance_to_polygon(point, polygon_points):
    px, py = point
    min_dist = float('inf')
    n = len(polygon_points)
    for i in range(n):
        x1, y1 = polygon_points[i]
        x2, y2 = polygon_points[(i + 1) % n]
        dx = x2 - x1
        dy = y2 - y1
        if dx == 0 and dy == 0:
            dist = math.hypot(px - x1, py - y1)
        else:
            t = ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)
            t = max(0, min(1, t))
            proj_x = x1 + t * dx
            proj_y = y1 + t * dy
            dist = math.hypot(px - proj_x, py - proj_y)
        if dist < min_dist:
            min_dist = dist
    return min_dist


def compute_interior_centroid(points):
    if len(points) < 3:
        return compute_geometric_centroid(points)

    simple_pts = simplify_for_test(points, max_pts=300)
    polygon_lonlat = [(lon, lat) for lat, lon in simple_pts]

    if not SCIPY_AVAILABLE:
        return _fallback_centroid(points)

    try:
        vor = Voronoi(polygon_lonlat)
    except Exception:
        return _fallback_centroid(points)

    best_point = None
    best_radius = -1.0
    for vertex in vor.vertices:
        if not (-180 <= vertex[0] <= 180 and -90 <= vertex[1] <= 90):
            continue
        if point_in_polygon(vertex, polygon_lonlat):
            radius = distance_to_polygon(vertex, polygon_lonlat)
            if radius > best_radius:
                best_radius = radius
                best_point = vertex

    if best_point is not None:
        return best_point[1], best_point[0]

    return _fallback_centroid(points)


def _fallback_centroid(points):
    geo_lat, geo_lon = compute_geometric_centroid(points)
    xy_poly = [(p[1], p[0]) for p in points]
    if point_in_polygon((geo_lon, geo_lat), xy_poly):
        return geo_lat, geo_lon

    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    med_lat = median(lats)
    med_lon = median(lons)
    if point_in_polygon((med_lon, med_lat), xy_poly):
        return med_lat, med_lon

    simple_pts = simplify_for_test(points, max_pts=300)
    xy_simple = [(lon, lat) for lat, lon in simple_pts]

    best_lat, best_lon = med_lat, med_lon
    best_dist2 = 0.0
    search_levels = [(0.003, 150), (0.001, 200), (0.0003, 250), (0.0001, 300)]

    for step_deg, max_steps in search_levels:
        for deg in range(0, 360, 5):
            angle = math.radians(deg)
            dx = math.cos(angle) * step_deg
            dy = math.sin(angle) * step_deg
            for step in range(1, max_steps + 1):
                test_lon = geo_lon + step * dx
                test_lat = geo_lat + step * dy
                if not (-90 <= test_lat <= 90 and -180 <= test_lon <= 180):
                    break
                if point_in_polygon((test_lon, test_lat), xy_simple):
                    dist2 = (test_lon - geo_lon) ** 2 + (test_lat - geo_lat) ** 2
                    if dist2 > best_dist2:
                        best_dist2 = dist2
                        best_lat, best_lon = test_lat, test_lon
                    break

    if best_dist2 > 0.0:
        cur_lat, cur_lon = best_lat, best_lon
        for i in range(20):
            alpha = 0.5 ** (i / 5)
            cand_lat = cur_lat * alpha + geo_lat * (1 - alpha)
            cand_lon = cur_lon * alpha + geo_lon * (1 - alpha)
            if point_in_polygon((cand_lon, cand_lat), xy_simple):
                cur_lat, cur_lon = cand_lat, cand_lon
            else:
                cand_lat = cur_lat * 0.9 + geo_lat * 0.1
                cand_lon = cur_lon * 0.9 + geo_lon * 0.1
                if point_in_polygon((cand_lon, cand_lat), xy_simple):
                    cur_lat, cur_lon = cand_lat, cand_lon
        return cur_lat, cur_lon

    first_lat, first_lon = points[0]
    step_lat = (geo_lat - first_lat) / 1000.0
    step_lon = (geo_lon - first_lon) / 1000.0
    for t in range(1, 1000):
        test_lat = first_lat + step_lat * t
        test_lon = first_lon + step_lon * t
        if point_in_polygon((test_lon, test_lat), xy_simple):
            return test_lat, test_lon
    return med_lat, med_lon


def approximate_polygon_area(points):
    if len(points) < 3:
        return 0.0
    poly = points[:-1] if points[0] == points[-1] else points[:]
    if len(poly) > 3000:
        step = len(poly) // 50 + 1
        poly = poly[::step]
    n = len(poly)
    if n < 3:
        return 0.0
    avg_lat = sum(p[0] for p in poly) / n
    km_per_lon = 111.32 * cos(radians(avg_lat))
    km_per_lat = 111.32
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        x1 = poly[i][1] * km_per_lon
        y1 = poly[i][0] * km_per_lat
        x2 = poly[j][1] * km_per_lon
        y2 = poly[j][0] * km_per_lat
        area += x1 * y2 - x2 * y1
    return abs(area) / 2.0


def assemble_outer_rings(outer_chains, name=None):
    if not outer_chains:
        return []
    chains = [chain[:] for chain in outer_chains if len(chain) >= 2]
    endpoint_to_chains = defaultdict(list)
    for idx, chain in enumerate(chains):
        if not chain:
            continue
        start = tuple(chain[0])
        end = tuple(chain[-1])
        endpoint_to_chains[start].append((idx, False))
        endpoint_to_chains[end].append((idx, True))
    used = [False] * len(chains)
    rings = []
    for start_idx in range(len(chains)):
        if used[start_idx]:
            continue
        current_chain = chains[start_idx][:]
        used[start_idx] = True
        current_end = tuple(current_chain[-1])
        extended = True
        while extended:
            extended = False
            for c_idx, reverse in endpoint_to_chains.get(current_end, []):
                if used[c_idx]:
                    continue
                next_chain = chains[c_idx][::-1] if reverse else chains[c_idx]
                if (abs(next_chain[0][0] - current_end[0]) < 1e-6 and
                        abs(next_chain[0][1] - current_end[1]) < 1e-6):
                    current_chain.extend(next_chain[1:])
                    used[c_idx] = True
                    current_end = tuple(current_chain[-1])
                    extended = True
                    break
        if len(current_chain) >= 3:
            first = tuple(current_chain[0])
            last = tuple(current_chain[-1])
            if abs(first[0] - last[0]) < 1e-6 and abs(first[1] - last[1]) < 1e-6:
                rings.append(current_chain)
            else:
                closed = False
                for c_idx, reverse in endpoint_to_chains.get(first, []):
                    if used[c_idx]:
                        continue
                    closing_chain = chains[c_idx][::-1] if reverse else chains[c_idx]
                    if (abs(closing_chain[-1][0] - last[0]) < 1e-6 and
                            abs(closing_chain[-1][1] - last[1]) < 1e-6):
                        current_chain.extend(closing_chain[:-1])
                        used[c_idx] = True
                        closed = True
                        break
                if closed or (abs(current_chain[0][0] - current_chain[-1][0]) < 1e-6 and
                              abs(current_chain[0][1] - current_chain[-1][1]) < 1e-6):
                    rings.append(current_chain)
    if not rings and chains:
        all_contours = sorted([chain[:] for chain in chains], key=len, reverse=True)
        best = all_contours[0]
        if abs(best[0][0] - best[-1][0]) > 1e-6 or abs(best[0][1] - best[-1][1]) > 1e-6:
            best = best + [best[0]]
        rings = [best]
    return rings


def process_rings_to_nodes(rings, tags, elem_id, source, counter):
    nodes = []
    count = 0
    for ring_points in rings:
        if len(ring_points) < 3:
            continue

        area_km2 = approximate_polygon_area(ring_points)
        important_tag_count = count_important_tags(tags)

        has_wikidata = 'wikidata' in tags
        is_large = area_km2 > LARGE_ISLAND_THRESHOLD

        island_rank = calculate_island_rank(area_km2, important_tag_count)

        if not ((area_km2 >= MIN_AREA_KM2 and has_wikidata) or is_large):
            continue
        if island_rank < MIN_ISLAND_RANK:
            continue

        center_lat, center_lon = compute_interior_centroid(ring_points)
        area_rounded = get_area_rounded(area_km2)

        cleaned_tags = clean_tags_for_output(tags)

        c_id = str(next(counter))
        node = ET.Element('node', id=c_id, lat=str(center_lat), lon=str(center_lon))

        for k, v in cleaned_tags.items():
            ET.SubElement(node, 'tag', k=k, v=v)

        ET.SubElement(node, 'tag', k='original_id', v=str(elem_id))
        ET.SubElement(node, 'tag', k='osmand_prominent_island', v='yes')
        ET.SubElement(node, 'tag', k='area_km2', v=f"{area_km2:.1f}")
        ET.SubElement(node, 'tag', k='area_rounded', v=str(area_rounded))
        ET.SubElement(node, 'tag', k='island_rank', v=str(island_rank))
        ET.SubElement(node, 'tag', k='important_tags_count', v=str(important_tag_count))
        ET.SubElement(node, 'tag', k='source_centroid', v='voronoi_skeleton' if SCIPY_AVAILABLE else 'fallback')
        ET.SubElement(node, 'tag', k='source', v=source)

        nodes.append(node)
        count += 1

    return nodes, count

def process_relation(relation_elem, counter):
    outer_chains = []
    for member in relation_elem.findall('member'):
        if member.get('type') == 'way' and member.get('role', '') == 'outer':
            way_points = [(float(nd.get('lat')), float(nd.get('lon')))
                          for nd in member.findall('nd') if nd.get('lat') and nd.get('lon')]
            if way_points:
                outer_chains.append(way_points)
    if not outer_chains:
        return [], 0
    rings = assemble_outer_rings(outer_chains)
    tags = {tag.get('k'): tag.get('v') for tag in relation_elem.findall('tag')}
    return process_rings_to_nodes(rings, tags, relation_elem.get('id'), 'relation', counter)


def process_way(way_elem, counter):
    points = []
    for nd in way_elem.findall('nd'):
        lat = nd.get('lat')
        lon = nd.get('lon')
        if lat and lon:
            points.append((float(lat), float(lon)))
    if len(points) < 3:
        return [], 0
    rings = [points] if (abs(points[0][0] - points[-1][0]) < 1e-6 and
                         abs(points[0][1] - points[-1][1]) < 1e-6) else []
    if not rings:
        rings = assemble_outer_rings([points])
    tags = {tag.get('k'): tag.get('v') for tag in way_elem.findall('tag')}
    return process_rings_to_nodes(rings, tags, way_elem.get('id'), 'way', counter)


def main():
    counter = iter(range(-1, -10000000, -1))

    relations = []
    try:
        tree = ET.parse('islands_relations.osm')
        relations = list(tree.getroot().findall('relation'))
        print(f"relations loaded: {len(relations)}")
    except Exception as e:
        print(f"Error loading islands_relations.osm: {e}")

    ways = []
    try:
        tree_ways = ET.parse('islands_ways.osm')
        ways = list(tree_ways.getroot().findall('way'))
        print(f"Ways loaded: {len(ways)}")
    except Exception as e:
        print(f"Error loading islands_ways.osm: {e}")

    total_items = len(relations) + len(ways)
    print(f"Object count for processing: {total_items}")
    print(f"MIN_AREA_KM2 = {MIN_AREA_KM2} км²")
    print(f"MIN_ISLAND_RANK = {MIN_ISLAND_RANK}")
    print(f"Threads: {MAX_WORKERS}")

    rel_nodes = []
    rel_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_relation, rel, counter): rel for rel in relations}
        for future in concurrent.futures.as_completed(futures):
            nodes, cnt = future.result()
            rel_nodes.extend(nodes)
            rel_count += cnt

    way_nodes = []
    way_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_way, way, counter): way for way in ways}
        for future in concurrent.futures.as_completed(futures):
            nodes, cnt = future.result()
            way_nodes.extend(nodes)
            way_count += cnt

    osm_centroids_rel = ET.Element('osm', version='0.6', generator='centroids_from_relations')
    for node in rel_nodes:
        osm_centroids_rel.append(node)

    osm_centroids_ways = ET.Element('osm', version='0.6', generator='centroids_from_ways')
    for node in way_nodes:
        osm_centroids_ways.append(node)

    ET.ElementTree(osm_centroids_rel).write('centroids_from_relations.osm', encoding='utf-8', xml_declaration=True)
    ET.ElementTree(osm_centroids_ways).write('centroids_from_ways.osm', encoding='utf-8', xml_declaration=True)

    print(f"Done!")
    print(f"   Centroids generated from relations: {rel_count}")
    print(f"   Centroids generated from ways:      {way_count}")

    def merge_osm_files(file1, file2, output):
        root = ET.Element('osm', version='0.6', generator='merged_centroids')
        for f in (file1, file2):
            tree = ET.parse(f)
            for node in tree.getroot().findall('node'):
                root.append(node)
        ET.ElementTree(root).write(output, encoding='utf-8', xml_declaration=True)

    merge_osm_files('centroids_from_relations.osm', 'centroids_from_ways.osm', 'points_prominent_islands.osm')

if __name__ == '__main__':
    main()