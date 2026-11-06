#!/usr/bin/env python3
"""Build src/uk/uk.vrt from grid/*.tif with newer and finer surveys taking priority.

Surveys coarser than 120 m are excluded. Masked EMODnet is the lowest-priority source of the
unblended uk_sources.vrt. Fully hidden older sources are omitted and the remaining overlaps are
feathered by uk_blend.py; --feather 0 keeps the hard source boundaries.

Usage: uk_build_vrt.py SRC_UK_DIR [--feather 10] [-j JOBS]
"""

import argparse, json, math, os, re, subprocess, zipfile

import rasterio
from uk_blend import atomic_vrt, build_full

CELL = 0.0002  # the grid of the converted tifs, the resolution of the mosaic

UUID_SUFFIX = re.compile(r"-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                         r"\.(bag|csv\.zip)$", re.I)


def warped_source(path, uk):
    """A 0.0002 VRT aligned to the common grid.

    gdalwarp with -tap and explicit NaN nodata prevents the nearest-neighbour edge
    stretch that gdalbuildvrt does when a source ends between grid cells. Coarse
    surveys are interpolated bilinearly; fine surveys keep nearest so their values
    are not changed.
    """
    with rasterio.open(path) as d:
        coarse = d.res[0] > CELL * 1.2
    warps = os.path.join(uk, "warps_grid")
    os.makedirs(warps, exist_ok=True)
    out = os.path.join(warps, os.path.basename(path)[:-4] + ".vrt")
    if not os.path.exists(out) or os.path.getmtime(out) < os.path.getmtime(path):
        if os.path.exists(out):
            os.remove(out)  # gdalwarp refuses to replace an existing VRT
        subprocess.run(["gdalwarp", "-q", "-of", "VRT", "-t_srs", "EPSG:4326",
                        "-tr", str(CELL), str(CELL), "-tap",
                        "-r", "bilinear" if coarse else "near",
                        "-srcnodata", "nan", "-dstnodata", "nan", path, out], check=True)
    return out


def zip_titles(uk):
    """Survey title (the tif name) -> survey id. The pipeline deletes a zip once its tif is
    built, so the title comes from the downloaded/<id> marker: its content is the tif path.
    Zips still in incoming (a survey that failed to convert) are read as well."""
    titles = {}
    done = os.path.join(uk, "downloaded")
    if os.path.isdir(done):
        for name in os.listdir(done):
            try:
                with open(os.path.join(done, name)) as f:
                    tif = f.read().strip()
            except OSError:
                continue
            if tif.lower().endswith(".tif"):
                titles[os.path.splitext(os.path.basename(tif))[0]] = name
    incoming = os.path.join(uk, "incoming")
    for name in os.listdir(incoming):
        if not name.endswith(".zip"):
            continue
        try:
            with zipfile.ZipFile(os.path.join(incoming, name)) as z:
                members = [n for n in z.namelist()
                           if n.lower().endswith((".bag", ".csv.zip"))]
        except (OSError, zipfile.BadZipFile):
            continue
        if members:
            titles[UUID_SUFFIX.sub("", os.path.basename(members[0]))] = name[:-4]
    return titles


def survey_meta(uk):
    """Survey id -> (data date, resolution in meters), from the cached index."""
    meta = {}
    path = os.path.join(uk, "featuremetadata.json")
    if not os.path.exists(path):
        return meta
    with open(path) as f:
        ds = json.load(f)["data"]["metadataDataset"]
    for s in ds:
        date = (s.get("endDate") or s.get("startDate") or "")[:10]
        res = None
        for r in s.get("datasetSpatialResolutions") or []:
            if r.get("spatialResolutionType", {}).get("spatialResolutionType") == "distance" \
                    and r.get("spatialResolution"):
                res = float(r["spatialResolution"])
                break
        meta[s["id"]] = (date, res)
    return meta


def title_date(title):
    """Data date from a title that the index does not know: 20250927 or 2025, else empty."""
    m = re.match(r"(\d{4})(\d{2})(\d{2})", title)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"(\d{4})", title)
    return m.group(1) if m else ""


def survey_sources(uk, max_cell_m=120):
    titles = zip_titles(uk)
    meta = survey_meta(uk)
    files = []
    for name in os.listdir(os.path.join(uk, "grid")):
        if not name.endswith(".tif"):
            continue
        title = name[:-4]
        date, res = meta.get(titles.get(title), ("", None))
        if not date:
            date = title_date(title)
        files.append((date, res, title))
    files.sort(key=lambda v: (v[0], -(v[1] if v[1] is not None else float("inf"))))
    accepted, excluded = [], []
    for date, res, title in files:
        path = os.path.join(uk, "grid", title + ".tif")
        with rasterio.open(path) as d:
            lat = (d.bounds.top + d.bounds.bottom) / 2
            meters = max(abs(d.res[0]) * 111320 * math.cos(math.radians(lat)),
                         abs(d.res[1]) * 111320)
        if meters > max_cell_m:
            excluded.append((path, meters, title))
        else:
            accepted.append(warped_source(path, uk))
    with open(os.path.join(uk, "uk_lowres_excluded.txt"), "w") as f:
        for path, meters, title in excluded:
            f.write(f"{meters:.1f}\t{title}\t{path}\n")
    print(f"Native resolution <= {max_cell_m} m: {len(accepted)} sources; "
          f"excluded: {len(excluded)}", flush=True)
    return accepted, excluded


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("uk")
    ap.add_argument("--feather", type=int, default=10)
    ap.add_argument("-j", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    args = ap.parse_args()
    if args.feather < 0:
        ap.error("--feather must not be negative")
    uk = os.path.abspath(args.uk)
    paths, _ = survey_sources(uk)
    underlay = os.path.join(uk, "emodnet_fill.vrt")
    if not os.path.exists(underlay):
        raise SystemExit(f"No {underlay}; run uk_emodnet_fill.py --build-vrt first")
    paths.insert(0, underlay)
    raw = os.path.join(uk, "uk_sources.vrt")
    atomic_vrt(paths, os.path.join(uk, "uk_vrt_sources.txt"), raw, CELL)
    print(f"uk_sources.vrt: {len(paths)} sources", flush=True)
    if args.feather:
        build_full(uk, raw, args.feather, args.j)
    else:
        atomic_vrt([raw], os.path.join(uk, "uk_unblended_source.txt"),
                   os.path.join(uk, "uk.vrt"), CELL)
        print("uk.vrt: unblended", flush=True)


if __name__ == "__main__":
    main()
