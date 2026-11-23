#!/usr/bin/env python3
"""Convert one UKHO survey zip (BAG or CSV points, whichever is finer) into an EPSG:4326
GeoTIFF in grid/. The source zip is never modified.

Usage: uk_zip_to_tif.py ZIP SRC_UK_DIR [--force]

SRC_UK_DIR is the src/uk folder: the zip lives in SRC_UK_DIR/incoming, the tif is written to
SRC_UK_DIR/grid/<TITLE>.tif (TITLE = the survey name without the -<uuid> suffix). A zip whose
output tif already exists is skipped (--force converts again), so a rerun resumes. BAG data is
warped to 0.0002 degree cells; CSV points are binned to 0.0002 or to their own spacing if the
survey is coarser.

Nothing is unpacked to disk unless a BAG is the source: the .csv.zip member is itself stored in
the survey zip, so the CSV is read straight from the nested archive. The BAG is extracted only
when the CSV is absent, unreadable or coarser.

On success the final line is `tif=<path>`, so a caller can delete the source zip.
"""

import argparse, contextlib, math, os, re, subprocess, sys, tempfile, zipfile

import numpy as np

CELL = 0.0002  # degrees, the output grid
MAX_CELLS = 60_000_000  # accumulators of one survey: about 1.4 GB, keeps a huge extent in bounds


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pixel_size_m(info):
    """Ground resolution in meters from a gdalinfo report."""
    m = re.search(r"Pixel Size = \(([-0-9.eE]+),[-0-9.eE]+\)", info)
    if not m:
        return None
    px = abs(float(m.group(1)))
    if "UTM" in info or "PROJCRS" in info:
        return px
    # geographic degrees: convert to meters at the raster's latitude
    mlat = re.search(r"Center.*\(([-0-9.]+)\s*,\s*([-0-9.]+)\)", info)
    lat = abs(float(mlat.group(2))) if mlat else 54.0
    return px * 111320.0 * math.cos(math.radians(lat))


def bag_resolution_zip(zip_path, member, td):
    """Ground resolution of the BAG in meters, read in place: the members are stored, so /vsizip/
    seeks inside the zip and no multi-GB file is unpacked. Falls back to extraction when GDAL
    cannot open the member (a name in a legacy codepage)."""
    p = sh(["gdalinfo", f"/vsizip/{zip_path}/{member}"])
    if p.returncode != 0:
        with zipfile.ZipFile(zip_path) as z:
            p = sh(["gdalinfo", z.extract(member, td)])
    return pixel_size_m(p.stdout)


class CsvSource:
    """The CSV of a survey: <survey>.csv.zip inside the survey zip, holding one .csv. Both members
    are stored, so the inner zip is opened on a seekable view of the outer member and the CSV
    never reaches the disk; a deflated csv.zip (never seen) falls back to the temp directory."""

    def __init__(self, zip_path, cz_member, td):
        self.zip_path, self.cz_member, self.td = zip_path, cz_member, td
        with zipfile.ZipFile(zip_path) as z:
            self.stored = z.getinfo(cz_member).compress_type == zipfile.ZIP_STORED
        self.name = None
        self.path = None

    def _extract(self):
        if self.path is None:
            with zipfile.ZipFile(self.zip_path) as z:
                cz_path = z.extract(self.cz_member, self.td)
            with zipfile.ZipFile(cz_path) as cz:
                self.name = [n for n in cz.namelist() if n.lower().endswith(".csv")][0]
                self.path = cz.extract(self.name, self.td)
        return self.path

    @contextlib.contextmanager
    def open(self):
        if not self.stored:
            with open(self._extract(), "rb") as f:
                yield f
            return
        with zipfile.ZipFile(self.zip_path) as outer:
            stream = outer.open(self.cz_member)
            try:
                with zipfile.ZipFile(stream) as inner:
                    if self.name is None:
                        self.name = [n for n in inner.namelist()
                                     if n.lower().endswith(".csv")][0]
                    with inner.open(self.name) as f:
                        yield f
            finally:
                stream.close()


def _csv_chunks(src):
    """Rows of a UKHO point CSV (lat, lon, depth) in bounded chunks: a survey can unpack to
    several GB, which np.genfromtxt cannot hold."""
    import pandas as pd
    with src.open() as f:
        for ch in pd.read_csv(f, usecols=(0, 1, 2), header=0, names=("lat", "lon", "dep"),
                              dtype=np.float64, on_bad_lines="skip", chunksize=2_000_000):
            yield ch


def _csv_scan(src, stride):
    """One pass: row count, bounds, mean latitude and every stride-th row as a spacing sample."""
    st = {"n": 0, "lat_sum": 0.0, "lat_min": math.inf, "lat_max": -math.inf,
          "lon_min": math.inf, "lon_max": -math.inf}
    slat, slon = [], []
    for ch in _csv_chunks(src):
        ch = ch.dropna()
        if ch.empty:
            continue
        la = ch["lat"].to_numpy(); lo = ch["lon"].to_numpy()
        st["n"] += len(ch)
        st["lat_sum"] += float(la.sum())
        st["lat_min"] = min(st["lat_min"], float(la.min()))
        st["lat_max"] = max(st["lat_max"], float(la.max()))
        st["lon_min"] = min(st["lon_min"], float(lo.min()))
        st["lon_max"] = max(st["lon_max"], float(lo.max()))
        if sum(len(x) for x in slat) < 1_000_000:  # a cap keeps a huge survey in bounded memory
            slat.append(la[::stride]); slon.append(lo[::stride])
    return st, (np.concatenate(slat) if slat else np.empty(0)), \
               (np.concatenate(slon) if slon else np.empty(0))


def csv_scan(src):
    """Bounds and point spacing of a survey CSV. Spacing is the median distance to the 5th
    nearest neighbour of every 20th point (points converted to meters); a handful of critical
    soundings does not survive that sampling, so such a file is read row by row. Returns
    {n, lat_min, lat_max, lon_min, lon_max, lat_mean, space}, space None when unknown, and None
    when the file holds no points."""
    stride = 20
    for stride in (20, 1):
        st, lat, lon = _csv_scan(src, stride)
        if st["n"] == 0:
            return None
        if len(lat) >= 10:
            break
    st["lat_mean"] = st["lat_sum"] / st["n"]
    st["space"] = None
    if len(lat) >= 3:
        import scipy.spatial
        xy = np.column_stack((lon * 111320.0 * math.cos(math.radians(st["lat_mean"])),
                              lat * 111320.0))
        # points can coincide (duplicate rows): the 5th neighbour keeps the median away from the
        # duplicates at ~0 m; every-stride-th rows are 1/stride of the density, so the sampled
        # spacing is sqrt(stride) times the real one
        k = min(5, len(lat))
        d, _ = scipy.spatial.cKDTree(xy).query(xy, k=k)
        step = float(np.median(d[:, -1])) * math.sqrt(math.pi / (k * stride))
        st["space"] = step if step > 0 else None
    return st


def convert_bag(bag_path, srs, tif_tmp):
    subprocess.run(["gdalwarp", "-q", "-overwrite", "-s_srs", srs, "-t_srs", "EPSG:4326",
                    "-tr", str(CELL), str(CELL), "-r", "average", "-b", "1", "-ot", "Float32",
                    "-srcnodata", "1000000", "-dstnodata", "nan",
                    "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES",
                    bag_path, tif_tmp], check=True,
                   stderr=subprocess.DEVNULL)


def convert_csv(src, tif_tmp, st):
    """Bin the points into cells of CELL degrees (the mean depth per cell, like gdalwarp
    average). A survey coarser than CELL is binned at its own point spacing: at CELL most
    cells would be empty, which reads as holes instead of the surveyed area. The CSV is read
    again in chunks, so the memory does not follow the survey size."""
    import rasterio
    from rasterio.transform import from_origin
    cell = CELL
    if st["space"]:
        cell = max(CELL, st["space"] / (111320.0 * math.cos(math.radians(st["lat_mean"]))))
    i0 = int(math.floor(st["lat_min"] / cell)); i1 = int(math.floor(st["lat_max"] / cell))
    j0 = int(math.floor(st["lon_min"] / cell)); j1 = int(math.floor(st["lon_max"] / cell))
    # a survey whose extent and spacing together blow up the cell count (a coarse sounding set
    # spread over degrees) is binned coarser instead of exhausting the memory of the worker
    while (i1 - i0 + 1) * (j1 - j0 + 1) > MAX_CELLS:
        cell *= 2.0
        i0 = int(math.floor(st["lat_min"] / cell)); i1 = int(math.floor(st["lat_max"] / cell))
        j0 = int(math.floor(st["lon_min"] / cell)); j1 = int(math.floor(st["lon_max"] / cell))
    h, w = i1 - i0 + 1, j1 - j0 + 1
    sums = np.zeros(h * w, dtype=np.float64)
    cnts = np.zeros(h * w, dtype=np.int64)
    for ch in _csv_chunks(src):
        ch = ch.dropna()
        if ch.empty:
            continue
        ii = np.floor(ch["lat"].to_numpy() / cell).astype(np.int64) - i0
        jj = np.floor(ch["lon"].to_numpy() / cell).astype(np.int64) - j0
        np.clip(ii, 0, h - 1, out=ii); np.clip(jj, 0, w - 1, out=jj)
        flat = ii * w + jj
        sums += np.bincount(flat, weights=ch["dep"].to_numpy(), minlength=h * w)
        cnts += np.bincount(flat, minlength=h * w)
    if cnts.max() == 0:
        raise ValueError("no valid points")
    grid = np.full(h * w, np.nan, dtype=np.float32)
    np.divide(sums, cnts, out=grid, where=cnts > 0)
    grid = grid.reshape(h, w)[::-1]  # north-up
    transform = from_origin(west=j0 * cell, north=(i1 + 1) * cell, xsize=cell, ysize=cell)
    with rasterio.open(tif_tmp, "w", driver="GTiff", height=h, width=w, count=1,
                       dtype="float32", crs="EPSG:4326", transform=transform,
                       nodata=np.nan, compress="deflate", tiled=True) as dst:
        dst.write(grid, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("zip")
    ap.add_argument("uk_dir")
    ap.add_argument("--force", action="store_true", help="convert again even if the tif exists")
    args = ap.parse_args()
    zip_path, uk = args.zip, args.uk_dir
    uuid = os.path.splitext(os.path.basename(zip_path))[0]
    grid = os.path.join(uk, "grid")
    os.makedirs(grid, exist_ok=True)
    tmpdir = os.environ.get("UK_TMPDIR") or tempfile.gettempdir()

    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()
    bags = [n for n in names if n.lower().endswith(".bag")]
    csvz = [n for n in names if n.lower().endswith(".csv.zip")]
    if not bags and not csvz:
        print(f"skip {uuid}: neither BAG nor CSV", flush=True)
        return
    # survey title: the inner directory/name without the -<uuid> suffix
    base = os.path.basename((bags or csvz)[0])
    title = re.sub(r"-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                   r"\.(bag|csv\.zip)$", "", base, flags=re.I)
    out = os.path.join(grid, title + ".tif")
    if os.path.exists(out) and not args.force:
        print(f"have {uuid} {title}", flush=True)
        print(f"tif={out}", flush=True)
        return

    with tempfile.TemporaryDirectory(dir=tmpdir) as td:
        csv_st = None
        csv_src = None
        csv_done = False
        if csvz:
            try:
                csv_src = CsvSource(zip_path, csvz[0], td)
                csv_st = csv_scan(csv_src)  # read in place, nothing is unpacked
            except (zipfile.BadZipFile, IndexError, ValueError, OSError):
                csv_src, csv_st = None, None  # treat a broken csv.zip as absent
        bag_m = bag_resolution_zip(zip_path, bags[0], td) if bags else None
        # without a BAG the CSV is the only source, even when its spacing is unknown
        if csv_st is not None and (bag_m is None or (
                csv_st["space"] is not None and csv_st["space"] <= bag_m * 1.2)):
            tif_tmp = out + ".tmp.tif"
            try:
                convert_csv(csv_src, tif_tmp, csv_st)
                csv_done = True
            except Exception as e:
                if bag_m is None:
                    print(f"FAILED {uuid}: csv: {type(e).__name__} {e}", flush=True)
                    return
                os.path.exists(tif_tmp) and os.remove(tif_tmp)
                csv_st = None  # fall back to the BAG
        if csv_done:
            os.replace(out + ".tmp.tif", out)
            print(f"ok {uuid} csv (bag {bag_m and round(bag_m, 1)} m, csv "
                  f"{csv_st['space'] and round(csv_st['space'], 1)} m) {title[:60]}", flush=True)
            print(f"tif={out}", flush=True)
            return
        if bags:
            # the BAG is unpacked only when it is the source; a stored member could be read in
            # place as well, but /vsizip/ breaks on names in a legacy codepage
            with zipfile.ZipFile(zip_path) as z:
                bag_path = z.extract(bags[0], td)
            # the horizontal EPSG code of the BAG CRS: the compound CRS (UTM or Mercator +
            # ALAT heights) is not parsed by PROJ, so it is given explicitly — the first
            # projected id of the WKT, else plain WGS 84 when there is no PROJCRS
            p = sh(["gdalinfo", "-json", bag_path]).stdout
            try:
                import json as _json
                wkt = _json.loads(p).get("coordinateSystem", {}).get("wkt", "")
            except Exception:
                wkt = ""
            ids = [int(x) for x in re.findall(r'ID\["EPSG",(\d+)\]', wkt)]
            proj = [x for x in ids if 32601 <= x <= 32760 or x == 3395]
            zone = re.search(r"UTM zone (\d+)([NS])", wkt)
            if proj:
                srs = f"EPSG:{proj[0]}"
            elif "Mercator (variant A)" in wkt:
                srs = "EPSG:3395"  # World Mercator: this WKT carries no EPSG id
            elif zone:
                srs = f"EPSG:{(32600 if zone.group(2) == 'N' else 32700) + int(zone.group(1))}"
            elif 4326 in ids and "PROJCRS" not in wkt:
                srs = "EPSG:4326"
            else:
                print(f"FAILED {uuid}: unknown CRS in BAG", flush=True)
                return
            tif_tmp = out + ".tmp.tif"
            try:
                convert_bag(bag_path, srs, tif_tmp)
            except subprocess.CalledProcessError:
                os.path.exists(tif_tmp) and os.remove(tif_tmp)
                print(f"FAILED {uuid}: gdalwarp {title[:50]}", flush=True)
                return
            os.replace(tif_tmp, out)
            print(f"ok {uuid} bag ({bag_m and round(bag_m, 1)} m) {title[:60]}", flush=True)
            print(f"tif={out}", flush=True)
            return
    print(f"FAILED {uuid}: no usable source", flush=True)


if __name__ == "__main__":
    main()
