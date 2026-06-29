#!/usr/bin/env python3

import gzip
import math
import sys
import urllib.error
import urllib.parse
import urllib.request

import mapbox_vector_tile


EXTENT = 4096
EPS = 4
DOMAINS = sys.argv[1:] or [None]


def base_url(url, domain):
    if domain is None:
        p = urllib.parse.urlparse(url)
        return p.scheme, p.netloc
    scheme = "http" if domain.startswith("127.0.0.1") or domain.startswith("localhost") else "https"
    return scheme, domain


def map_base_url(url, domain):
    if domain == "maptile.osmand.net":
        return "https", "osmand.net"
    return base_url(url, domain)


def read_urls(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                yield line.split()[0]


def tile_from_url(url):
    p = urllib.parse.urlparse(url)
    parts = p.path.strip("/").split("/")
    if len(parts) >= 4 and parts[-1].endswith(".mvt"):
        return int(parts[-3]), int(parts[-2]), int(parts[-1][:-4])
    if p.fragment:
        parts = p.fragment.split("/")
        if len(parts) >= 3:
            z = int(parts[0])
            lat = float(parts[1])
            lon = float(parts[2])
            n = 1 << z
            x = int((lon + 180.0) / 360.0 * n)
            y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
            return z, x, y
    raise ValueError("unsupported tile url")


def vector_url(url, domain):
    z, x, y = tile_from_url(url)
    scheme, netloc = base_url(url, domain)
    return urllib.parse.urlunparse((scheme, netloc, f"/vector/{z}/{x}/{y}.mvt", "", "", ""))


def map_url(url, domain):
    z, x, y = tile_from_url(url)
    n = 1 << z
    lon = (x + 0.5) / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * (y + 0.5) / n))))
    scheme, netloc = map_base_url(url, domain)
    return urllib.parse.urlunparse((scheme, netloc, "/map/", "", "", f"{z + 1}/{lat:.6f}/{lon:.6f}"))


def download(url):
    req = urllib.request.Request(url, headers={"Accept": "application/x-protobuf"})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = r.read()
    return gzip.decompress(data) if data.startswith(b"\x1f\x8b") else data


def clip_edge(points, inside, intersect):
    if not points:
        return []
    result = []
    prev = points[-1]
    prev_inside = inside(prev)
    for cur in points:
        cur_inside = inside(cur)
        if cur_inside:
            if not prev_inside:
                result.append(intersect(prev, cur))
            result.append(cur)
        elif prev_inside:
            result.append(intersect(prev, cur))
        prev, prev_inside = cur, cur_inside
    return result


def clip_ring(points):
    def ix(a, b, x):
        dx = b[0] - a[0]
        if dx == 0:
            return (x, a[1])
        return (x, a[1] + (b[1] - a[1]) * (x - a[0]) / dx)

    def iy(a, b, y):
        dy = b[1] - a[1]
        if dy == 0:
            return (a[0], y)
        return (a[0] + (b[0] - a[0]) * (y - a[1]) / dy, y)

    points = clip_edge(points, lambda p: p[0] >= 0, lambda a, b: ix(a, b, 0))
    points = clip_edge(points, lambda p: p[0] <= EXTENT, lambda a, b: ix(a, b, EXTENT))
    points = clip_edge(points, lambda p: p[1] >= 0, lambda a, b: iy(a, b, 0))
    points = clip_edge(points, lambda p: p[1] <= EXTENT, lambda a, b: iy(a, b, EXTENT))
    return points


def ring_area(points):
    if len(points) < 3:
        return 0
    return abs(sum(points[i][0] * points[(i + 1) % len(points)][1] -
                   points[(i + 1) % len(points)][0] * points[i][1]
                   for i in range(len(points))) / 2.0)


def polygon_area(rings):
    if not rings:
        return 0
    return max(0, ring_area(clip_ring(rings[0])) -
               sum(ring_area(clip_ring(r)) for r in rings[1:]))


def coastline_fills_tile(data):
    tile = mapbox_vector_tile.decode(data)
    for layer in tile.values():
        for feature in layer.get("features", []):
            if feature.get("properties", {}).get("natural") != "coastline":
                continue
            geom = feature.get("geometry", {})
            coords = geom.get("coordinates", [])
            if geom.get("type") == "Polygon":
                polys = [coords]
            elif geom.get("type") == "MultiPolygon":
                polys = coords
            else:
                continue
            if sum(polygon_area(p) for p in polys) >= EXTENT * EXTENT - EPS:
                return True
    return False


def check(kind, url, domain):
    actual_url = vector_url(url, domain)
    try:
        is_ocean = coastline_fills_tile(download(actual_url))
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        print(kind, actual_url, "http-error")
        return False
    expected_ocean = kind == "ocean"
    status = "ok" if is_ocean == expected_ocean else "failed"
    suffix = f" {map_url(url, domain)}" if status == "failed" else ""
    print(kind, actual_url, status + suffix)
    return status == "ok"


def main():
    ok = True
    for domain in DOMAINS:
        for kind, path in (("ocean", "ocean.txt"), ("land", "land.txt")):
            for url in read_urls(path):
                ok = check(kind, url, domain) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
