#!/usr/bin/env python3
"""Download the UKHO ADMIRALTY surveys that cover the Great_Britain_contours region.

Selects from the Seabed Mapping Service index (GET /api/featuremetadata, cached) every survey
whose extent overlaps the Great_Britain_contours bbox: no year, resolution or per-cell filter.
Surveys in the waters of Ireland (Dublin Bay, Lambay, Tuskar, Waterford, Cork, Galway, ...) are
skipped: they belong to Ireland_contours. Where surveys overlap, uk.vrt is ordered by
uk_build_vrt.py so that the newer and finer survey wins.

Each survey is processed end to end by its worker: the zip is downloaded into DIR/incoming (or
taken from there if a run was interrupted), converted to DIR/grid/<TITLE>.tif by uk_zip_to_tif.py,
and then deleted, so the disk holds at most as many zips as there are workers plus the tifs. A zip
left in incoming is converted on start, not downloaded again. DIR/downloaded/<id> records the tif.
Zips taken from --zip-dir are a source archive and are never deleted; without UKHO_TOKEN such a
run converts what is there and reports the missing surveys.

Usage:
  UKHO_TOKEN=... [UKHO_REFRESH_TOKEN=...] download_ukho.py -o DIR/src/uk [-j 4] [--retries 5]
                                                          [--limit N] [--keep-zip] [--refresh-index]

  -o DIR          src/uk directory (created); zips land in DIR/incoming, tifs in DIR/grid
  -j JOBS         conversion workers (default: half the cores); slow BAG warps and huge CSVs
                  saturate the cores long after the download has finished
  --download-jobs N  parallel downloads (default 4): the server is slow, more streams do not pay
  --retries N     attempts per survey; a stream cut mid-way restarts it (default 5)
  --limit N       only the first N surveys of the selection (smallest first), for a test run
  --zip-dir DIR   take the zips from DIR instead of DIR/incoming and never delete them: a set
                  downloaded earlier can be converted without a token and without touching it
  --keep-zip      do not delete a zip after its tif is built
  --refresh-index refetch the feature metadata instead of using the cache
  --list          only write/print the selection, no download

Auth: Bearer token of a signed-in seabed.admiralty.co.uk session (lasts about an hour). With
UKHO_REFRESH_TOKEN set (from the B2C login, scope offline_access) a 401 is recovered by refreshing.
"""

import argparse, json, os, re, subprocess, sys, threading, time, zipfile
import urllib.parse, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

API = "https://seabedmappingservice-live.azurewebsites.net/api/"
TOKEN_URL = ("https://mgiaidprdb2c.b2clogin.com/mgiaidprdb2c.onmicrosoft.com/"
             "b2c_1a_signin_spa/oauth2/v2.0/token")
CLIENT_ID = "5aeea2b8-3a8f-44bc-9b1c-c6b39d4a9f87"  # the public client of seabed.admiralty.co.uk
GB_BBOX = (-8.7, 49.8, 2.0, 60.9)  # Great_Britain_contours
# survey titles in the waters of Ireland (Ireland_contours covers them); the shared Irish Sea and
# Celtic Sea stay; Northern Ireland (Lough Foyle, Carlingford, Ardglass, Castlerock, Ballagan) stays
IRISH = re.compile(r"Dublin|Lambay|Tuskar|Dunany|Kish Bank|Arklow|Waterford|Cork(?!\s+Sand)|Galway|"
                   r"Donegal|Malin Head|Malin More|Glengad|Inishowen|Rosslare|Wexford|Howth|Wicklow|"
                   r"Dingle|Bantry|Ballycotton|Ballyvoyle|Galley Head|Hook Head|Boyne|Cahore|"
                   r"Ballyteige|Courtmacshery|Saltee|Termonfeckin|Dundalk Bay|Irelands Eye|"
                   # the west and northwest coasts: surveys of these waters belong to Ireland_contours
                   r"Clew|Killala|Loop Head|Brandon|Branson|Kenmare|Dursey|Sybil|Bloody Foreland|"
                   r"Mulroy|Lough Swilly|Sligo|Bunmahon|Mine Head|Mizen|Courtown|Westport Harbour|"
                   r"Roonah|Dillisk|Newport Bay|Kerry Head|\bAchill|Clare Island|Kinsale|Tralee|"
                   r"Aran Island|\bInish|Erris|Broadhaven|Broad Haven|Blacksod|Belmullet|Tory Island|"
                   r"Slyne Head|Hags Head|Skerd Rocks|River Shannon|Aughinish|Limerick|Reanies|"
                   r"Old Head", re.I)

_token = os.environ.get("UKHO_TOKEN", "")
_refresh = os.environ.get("UKHO_REFRESH_TOKEN", "")
_token_lock = threading.Lock()


def refresh_token(prev_token=None):
    """Renew the bearer token. With prev_token another thread may already have done it, in
    which case this is a no-op: the caller can retry with the current token."""
    global _token, _refresh
    with _token_lock:
        if prev_token is not None and _token != prev_token:
            return True
        if not _refresh:
            return False
        data = urllib.parse.urlencode({
            "grant_type": "refresh_token", "client_id": CLIENT_ID,
            "refresh_token": _refresh}).encode()
        with urllib.request.urlopen(urllib.request.Request(TOKEN_URL, data=data), timeout=60) as r:
            d = json.load(r)
        _token, _refresh = d["access_token"], d.get("refresh_token", _refresh)
        print(f"token refreshed, expires in {d.get('expires_in')} s", flush=True)
        return True


def api_get(path):
    for attempt in range(2):
        req = urllib.request.Request(API + path, headers={"Authorization": "Bearer " + _token})
        try:
            with urllib.request.urlopen(req, timeout=600) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 401 and attempt == 0 and refresh_token():
                continue
            raise


def api_post_to_file(payload, path):
    """POST the selection and stream the zip to path. The server sends no Content-Length and
    ignores Range, so a broken stream means starting the survey over."""
    token = _token
    req = urllib.request.Request(API + "features", data=json.dumps(payload).encode(),
                                 headers={"Authorization": "Bearer " + token,
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r, open(path, "wb") as f:
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    break
                f.write(chunk)
            return f.tell()
    except urllib.error.HTTPError as e:
        if e.code == 401 and refresh_token(token):
            return api_post_to_file(payload, path)
        raise


def zip_ok(path):
    try:
        with zipfile.ZipFile(path) as z:
            return bool(z.namelist())
    except Exception:
        return False


def end_year(s):
    try:
        return int(s["endDate"][:4])
    except Exception:
        return 0


def size_bytes(s):
    return sum(int(f.get("size") or 0) for f in (s.get("datafiles") or []))


def rings_of(s):
    """EPSG:4326 shells of the survey extent polygons (a list of [x, y] rings).
    The index mixes geometry layouts: coordinates[0] is either a shell ring
    [[x,y],...], a polygon [shell, hole,...] or a list of those polygons."""
    try:
        polys = s["derivedSpatialExtent"]["coordinates"][0]
        if isinstance(polys[0][0], (int, float)):  # a shell ring itself
            return [polys]
        return [p if isinstance(p[0][0], (int, float)) else p[0] for p in polys]
    except Exception:
        return []


def select_surveys(meta_path, bbox):
    """Every survey whose extent overlaps bbox, Irish titles skipped: no year or resolution
    filter. Which survey wins where they overlap is decided by the uk.vrt source order
    (uk_build_vrt.py: newer data first, then the finer resolution)."""
    with open(meta_path) as f:
        ds = json.load(f)["data"]["metadataDataset"]
    surveys = {}
    for s in ds:
        if IRISH.search(s.get("resourceTitle", "")):
            continue
        rings = rings_of(s)
        if not rings:
            continue
        xs = [p[0] for r in rings for p in r]
        ys = [p[1] for r in rings for p in r]
        if min(xs) > bbox[2] or max(xs) < bbox[0] or min(ys) > bbox[3] or max(ys) < bbox[1]:
            continue
        surveys[s["id"]] = {"id": s["id"], "title": s.get("resourceTitle", ""),
                            "size": size_bytes(s), "year": end_year(s), "cells": 0}
    return sorted(surveys.values(), key=lambda v: (v["size"], v["id"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", required=True, help="src/uk directory")
    ap.add_argument("-j", type=int, default=max(1, (os.cpu_count() or 2) // 2),
                    help="conversion workers (default: half the cores)")
    ap.add_argument("--download-jobs", type=int, default=4,
                    help="parallel downloads (default 4)")
    ap.add_argument("--retries", type=int, default=5, help="attempts per survey (default 5)")
    ap.add_argument("--limit", type=int, help="only the first N surveys (smallest first)")
    ap.add_argument("--zip-dir", help="take the zips from here and never delete them "
                    "(default: DIR/src/uk/incoming, where the downloads land)")
    ap.add_argument("--keep-zip", action="store_true", help="keep a zip after its tif is built")
    ap.add_argument("--refresh-index", action="store_true")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()
    uk = args.o
    jobs = max(1, args.j)
    dl_jobs = max(1, args.download_jobs)
    incoming = os.path.abspath(f"{uk}/incoming")
    zip_dir = os.path.abspath(args.zip_dir) if args.zip_dir else incoming
    if not os.path.isdir(zip_dir):
        sys.exit(f"--zip-dir {zip_dir} does not exist")
    os.makedirs(incoming, exist_ok=True)
    os.makedirs(f"{uk}/downloaded", exist_ok=True)
    os.makedirs(f"{uk}/tmp", exist_ok=True)
    meta = f"{uk}/featuremetadata.json"

    if args.refresh_index or not os.path.exists(meta):
        print("fetching the survey index (about 50 MB)...", flush=True)
        with open(meta, "wb") as f:
            f.write(api_get("featuremetadata"))
    surveys = select_surveys(meta, GB_BBOX)
    total = sum(s["size"] for s in surveys)
    print(f"{len(surveys)} surveys, {total / 1e9:.1f} GB of zips", flush=True)
    with open(f"{uk}/ukho_surveys_gb.txt", "w") as f:
        f.write("# every UKHO ADMIRALTY survey whose extent overlaps Great_Britain_contours "
                f"{GB_BBOX}, picked from the Seabed Mapping Service index; Irish waters skipped. "
                "uk.vrt orders the overlapping ones by date and resolution (uk_build_vrt.py)\n"
                "# id<TAB>size bytes<TAB>title\n")
        for s in surveys:
            f.write(f"{s['id']}\t{s['size']}\t{s['title']}\n")
    if args.list:
        for s in surveys[:args.limit]:
            print(f"{s['size'] / 1e6:9.1f} MB  {s['year']}  {s['title'][:90]}")
        return
    if not _token:
        print("UKHO_TOKEN is not set: only the zips that are already there can be converted",
              flush=True)
    if args.limit:
        surveys = surveys[:args.limit]
        print(f"limited to the first {len(surveys)} surveys", flush=True)
    print(f"{dl_jobs} download workers, {jobs} conversion workers", flush=True)

    def convert(z):
        """Run uk_zip_to_tif.py on z, always re-converting: the zip is about to be deleted, so the
        tif must come from it and not from an earlier run. Returns the tif path or None."""
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uk_zip_to_tif.py")
        env = dict(os.environ)
        env.setdefault("UK_TMPDIR", f"{uk}/tmp")
        p = subprocess.run([sys.executable, script, z, uk, "--force"],
                           capture_output=True, text=True, env=env)
        tif = None
        for line in p.stdout.splitlines():
            if line.startswith("tif="):
                tif = line[4:].strip()
            elif line.strip():
                print(f"   {line}", flush=True)
        if p.returncode != 0 or not tif or not os.path.exists(tif):
            for line in p.stderr.splitlines()[-3:]:
                print(f"   ! {line}", flush=True)
            return None
        return tif

    def obtain(s):
        """Marker check and the zip of one survey: taken from incoming (our download, may be
        deleted after conversion) or from --zip-dir (never deleted), else downloaded. Returns
        (zip, owned, message) or a final message string when there is nothing to convert."""
        marker = f"{uk}/downloaded/{s['id']}"
        if os.path.exists(marker):
            with open(marker) as f:
                tif = f.read().strip()
            if tif and os.path.exists(os.path.join(uk, tif)):
                return f"done {s['id']}"
        # the index size is the sum of the datafiles, a complete zip is never smaller
        z, owned = None, False
        bases = [(incoming, True)] if zip_dir == incoming else [(incoming, True), (zip_dir, False)]
        for base, own in bases:
            cand = os.path.join(base, s["id"] + ".zip")
            if os.path.exists(cand) and os.path.getsize(cand) >= s["size"] and zip_ok(cand):
                z, owned = cand, own
                break
        if z is not None:
            return z, owned, f"have {s['id']} ({os.path.getsize(z) / 1e6:.1f} MB)"
        if not _token:
            return f"FAILED {s['id']}: no zip and UKHO_TOKEN is not set"
        z = os.path.join(incoming, s["id"] + ".zip")
        for f in (z, z + ".part"):
            if os.path.exists(f):
                os.remove(f)
        attempt = 0
        while True:
            attempt += 1
            try:
                n = api_post_to_file({"featureIds": [s["id"]]}, z + ".part")
            except Exception as e:
                print(f"retry {s['id']} ({attempt}/{args.retries}): "
                      f"{type(e).__name__} {e}", flush=True)
                if attempt >= args.retries:
                    break
                time.sleep(min(300, 20 * 2 ** (attempt - 1)))
                continue
            if n >= s["size"] and zip_ok(z + ".part"):
                os.rename(z + ".part", z)
                return z, True, f"ok {s['id']} ({n / 1e6:.1f} MB) {s['title'][:60]}"
            print(f"retry {s['id']} ({attempt}/{args.retries}): "
                  f"short {n} < {s['size']} bytes", flush=True)
            if attempt >= args.retries:
                break
            time.sleep(min(300, 20 * 2 ** (attempt - 1)))
        if os.path.exists(z + ".part"):
            os.remove(z + ".part")
        return f"FAILED {s['id']}: {args.retries} attempts"

    def finish(s, z, owned, msg):
        """Conversion pool task: build the tif, drop the zip we downloaded, write the marker."""
        try:
            tif = convert(z)
            if tif is None:
                r = f"FAILED {s['id']}: conversion, zip kept in {os.path.dirname(z)}"
            else:
                if owned and not args.keep_zip:
                    os.remove(z)
                with open(f"{uk}/downloaded/{s['id']}", "w") as f:
                    f.write(os.path.relpath(tif, uk) + "\n")
                r = f"{msg} -> {os.path.basename(tif)}"
        except Exception as e:
            r = f"FAILED {s['id']}: {type(e).__name__} {e}"
        finally:
            slots.release()
        report(r)

    def stage(s):
        """Download pool task: hand the survey to the conversion pool. The semaphore bounds how
        many zips wait for conversion, so a fast download cannot fill the disk."""
        slots.acquire()
        try:
            got = obtain(s)
            if isinstance(got, str):
                slots.release()
                return got
            conv.submit(finish, s, *got)
        except Exception:
            slots.release()
            raise
        return None

    lock = threading.Lock()
    stats = {"done": 0, "failed": 0}

    def report(r):
        with lock:
            stats["failed" if r.startswith("FAILED") else "done"] += 1
            i = stats["done"] + stats["failed"]
        print(f"[{i}/{len(surveys)}] {r}", flush=True)

    slots = threading.Semaphore(dl_jobs + jobs)
    conv = ThreadPoolExecutor(max_workers=jobs)
    try:
        with ThreadPoolExecutor(max_workers=dl_jobs) as dl:
            futs = {dl.submit(stage, s): s for s in surveys}
            for fut in as_completed(futs):
                try:
                    r = fut.result()
                except Exception as e:
                    r = f"FAILED {futs[fut]['id']}: {type(e).__name__} {e}"
                if r is not None:
                    report(r)
    finally:
        conv.shutdown(wait=True)

    failed, done = stats["failed"], stats["done"]
    print(f"done: {done} surveys, {failed} failed", flush=True)
    if failed:
        print("failed zips stay in incoming and are converted on the next run", flush=True)
    else:
        print("run uk_build_vrt.py to rebuild the vrt (download_all.sh --only uk does it)",
              flush=True)


if __name__ == "__main__":
    main()
