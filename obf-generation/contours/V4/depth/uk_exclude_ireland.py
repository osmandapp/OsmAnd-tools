#!/usr/bin/env python3
"""Remove Ireland coverage from the UK survey grid without deleting source archives.

Each UK cell is masked when its centre is inside the OsmAnd Ireland .poly boundary or when the
INFOMAR VRT has a value at that location. A survey that straddles the boundary keeps its UK cells.
Empty tifs are moved to grid_unused; their zips are moved from incoming_full to
incoming_full_unused and their downloaded markers follow the tifs. Partial surveys keep their zips.

Usage:
  uk_exclude_ireland.py SRC_UK_DIR IRELAND_VRT IRELAND_POLY [-j JOBS] [--apply]

Without --apply, only read the rasters and print the proposed changes.
"""

import argparse, os, re, sys, zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import rasterio
from rasterio.coords import disjoint_bounds
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterio.windows import from_bounds

UUID_SUFFIX = re.compile(r"-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                         r"\.(bag|csv\.zip)$", re.I)


def poly_shapes(path):
    polys, ring = [], []
    hole_of = None
    with open(path) as f:
        lines = f.readlines()
    for line in lines[1:]:
        s = line.strip()
        if not s:
            continue
        if s == "END":
            if ring:
                if hole_of is not None:
                    polys[hole_of].append(ring)
                else:
                    polys.append([ring])
                ring = []
            continue
        if line[0] != " " and s.lstrip("!").isdigit():
            hole_of = len(polys) - 1 if s.startswith("!") else None
            continue
        p = s.split()
        if len(p) == 2:
            ring.append((float(p[0]), float(p[1])))
    return [{"type": "Polygon", "coordinates": p} for p in polys]


def mask_one(path, ireland, shapes, poly_bbox, apply):
    with rasterio.open(ireland) as src:
        ireland_bbox = src.bounds
    with rasterio.open(path, "r+" if apply else "r") as d:
        if disjoint_bounds(d.bounds, poly_bbox) and disjoint_bounds(d.bounds, ireland_bbox):
            return path, 0, 0, 0, None
        a = d.read(1)
        valid = np.isfinite(a)
        before = int(valid.sum())
        if not before:
            return path, 0, 0, 0, 0
        poly = np.zeros(a.shape, dtype=bool)
        if not disjoint_bounds(d.bounds, poly_bbox):
            poly = rasterize(shapes, out_shape=a.shape, transform=d.transform,
                             fill=0, all_touched=False, dtype="uint8").astype(bool)
        infomar = np.zeros(a.shape, dtype=bool)
        with rasterio.open(ireland) as src:
            if not disjoint_bounds(d.bounds, src.bounds):
                window = from_bounds(*d.bounds, transform=src.transform)
                other = src.read(1, window=window, out_shape=a.shape, boundless=True,
                                 fill_value=np.nan, resampling=Resampling.nearest)
                infomar = np.isfinite(other)
        remove = valid & (poly | infomar)
        in_poly = int((valid & poly).sum())
        in_infomar = int((valid & infomar & ~poly).sum())
        remaining = before - int(remove.sum())
        if apply and remove.any():
            a[remove] = np.nan
            d.write(a, 1)
    return path, in_poly, in_infomar, remaining, before


def archive_title(path):
    try:
        with zipfile.ZipFile(path) as z:
            members = [n for n in z.namelist() if n.lower().endswith((".bag", ".csv.zip"))]
    except (OSError, zipfile.BadZipFile):
        return None
    return UUID_SUFFIX.sub("", os.path.basename(members[0])) if members else None


def marker_tifs(uk, incoming):
    refs = {}
    markers = os.path.join(uk, "downloaded")
    for survey_id in os.listdir(markers):
        marker = os.path.join(markers, survey_id)
        try:
            with open(marker) as f:
                tif = f.read().strip()
        except OSError:
            continue
        if not tif:
            title = archive_title(os.path.join(incoming, survey_id + ".zip"))
            if title:
                tif = "grid/" + title + ".tif"
        if tif:
            refs.setdefault(tif, []).append(survey_id)
    return refs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("uk", help="src/uk directory")
    ap.add_argument("ireland", help="src/ireland/ireland.vrt")
    ap.add_argument("poly", help="OsmAnd-misc ireland.poly")
    ap.add_argument("-j", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    ap.add_argument("--apply", action="store_true", help="write masks and move unused files")
    args = ap.parse_args()
    uk = os.path.abspath(args.uk)
    incoming = os.path.join(uk, "incoming_full")
    unused = os.path.join(uk, "incoming_full_unused")
    shapes = poly_shapes(args.poly)
    poly_bbox = (min(x for p in shapes for r in p["coordinates"] for x, y in r),
                 min(y for p in shapes for r in p["coordinates"] for x, y in r),
                 max(x for p in shapes for r in p["coordinates"] for x, y in r),
                 max(y for p in shapes for r in p["coordinates"] for x, y in r))
    paths = [os.path.join(uk, "grid", n) for n in os.listdir(os.path.join(uk, "grid"))
             if n.endswith(".tif")]
    refs = marker_tifs(uk, incoming)
    results = []
    with ProcessPoolExecutor(max_workers=max(1, args.j)) as pool:
        futures = [pool.submit(mask_one, p, os.path.abspath(args.ireland), shapes,
                               poly_bbox, args.apply) for p in paths]
        for f in as_completed(futures):
            results.append(f.result())
    changed = [r for r in results if r[1] or r[2]]
    empty = [r for r in results if r[4] is not None and r[3] == 0]
    print(f"UK tifs: {len(paths)}; masked: {len(changed)}; empty after masking: {len(empty)}")
    print(f"Cells removed: Ireland boundary {sum(r[1] for r in results)}, "
          f"INFOMAR outside that boundary {sum(r[2] for r in results)}")
    if not args.apply:
        for path, _, _, _, _ in sorted(empty):
            print("unused:", os.path.basename(path))
        print("Dry run: no files changed; use --apply to mask and move", flush=True)
        return

    grid_unused = os.path.join(uk, "grid_unused")
    warps_unused = os.path.join(uk, "warps_unused")
    os.makedirs(grid_unused, exist_ok=True)
    os.makedirs(unused, exist_ok=True)
    moved_zips = 0
    for path, _, _, _, _ in sorted(empty):
        rel = "grid/" + os.path.basename(path)
        new_rel = "grid_unused/" + os.path.basename(path)
        dest = os.path.join(uk, new_rel)
        if os.path.exists(dest):
            raise RuntimeError(f"Refusing to overwrite {dest}")
        os.rename(path, dest)
        warp = os.path.join(uk, "warps", os.path.basename(path)[:-4] + ".vrt")
        if os.path.exists(warp):
            os.makedirs(warps_unused, exist_ok=True)
            os.rename(warp, os.path.join(warps_unused, os.path.basename(warp)))
        for survey_id in refs.get(rel, []):
            marker = os.path.join(uk, "downloaded", survey_id)
            with open(marker, "w") as f:
                f.write(new_rel + "\n")
            src = os.path.join(incoming, survey_id + ".zip")
            if os.path.exists(src):
                zip_dest = os.path.join(unused, survey_id + ".zip")
                if os.path.exists(zip_dest):
                    raise RuntimeError(f"Refusing to overwrite {zip_dest}")
                os.rename(src, zip_dest)
                moved_zips += 1
    print(f"Moved {len(empty)} empty tifs to grid_unused and {moved_zips} zips to {unused}", flush=True)


if __name__ == "__main__":
    main()
