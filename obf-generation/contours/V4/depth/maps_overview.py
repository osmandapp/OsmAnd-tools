#!/usr/bin/env python3
"""Overview of the detailed depth maps: the area every OBF covers, on an OSM background.

The area of a map is the place its grid has data: the same sources that were compiled
into the OBF (the EMODnet base, the surveys, the coastal fill), cropped to the bbox of
the region and cut by the coverage of the neighbouring maps, exactly as the build does.
The frame of the region (the bbox of the map) is dashed, the data area is filled.

The background is the OSM basemap of the core (World_basemap_2.obf or another *.obf with
the roads and the coastline), rendered with eyepiece_standalone; the areas are drawn on
top of it. A place where two maps have data is drawn in red.

    maps_overview.py VIEW_JSON OUT_PNG [--work DIR] [--basemap FILE]

VIEW_JSON holds the view and the title:

    {"lat": 53.6, "lon": -2.25, "zoom": 9, "width": 7000, "height": 5200,
     "title": "the area of the data of the depth maps"}
"""
import argparse
import json
import math
import os
import subprocess
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.patheffects
import matplotlib.pyplot as plt
import numpy as np
import shapely
from osgeo import ogr
from PIL import Image
from shapely import wkb
from shapely.geometry import box
from shapely.ops import unary_union

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_WORK = "/mnt/wd_2tb/depth_work"
EYE = os.path.expanduser("~/utilites/OsmAndMapCreator-main/amd64-linux-clang/eyepiece_standalone")
STYLES = os.path.expanduser("~/git/OsmAnd-resources/rendering_styles")
LAND = os.path.join(DEFAULT_WORK, "mask/land_polygons.gpkg")
LAND_CACHE = os.path.join(DEFAULT_WORK, "preview/coverage/land")

# the coverage of every map (the files the build makes in src/coverage), the bbox of its region and the
# coverage of the neighbours it gives way to.  `wide` maps are drawn only when they are asked for
# explicitly: on a view of a detailed map they would fill the whole picture.  size, sections and source
# are the 2026-11 build and are shown only in the --auto titles.
REGIONS = {
    "Great_Britain_contours": dict(
        color="#e6194b", bbox=(-8.7, 49.8, 2.0, 60.9),
        coverage=["src/coverage/uk.gpkg"], exclude=[], land=True,
        label=(-2.4, 54.3), hatch="///",
        size="208 MB", sections=5, source="UKHO + EMODnet"),
    "Ireland_contours": dict(
        color="#3cb44b", bbox=(-11.8, 51.2, -5.3, 55.6),
        coverage=["src/coverage/ireland.gpkg"], exclude=["uk_ireland.gpkg"], land=True,
        label=(-8.6, 53.2), hatch="...",
        size="51 MB", sections=5, source="INFOMAR + EMODnet"),
    "Netherlands_contours": dict(
        color="#4363d8", bbox=(1.7, 51.1, 7.3, 55.7),
        coverage=["src/coverage/netherlands.gpkg"], exclude=["uk_netherlands.gpkg"], land=True,
        label=(4.6, 52.6), hatch="xxx",
        size="17 MB", sections=5, source="Rijkswaterstaat + EMODnet"),
    "France_contours": dict(
        color="#911eb4", bbox=(-5.5, 43.3, 2.6, 51.2),
        coverage=["src/coverage/france.gpkg"], exclude=["uk_filled.gpkg"], land=True,
        label=(-2.4, 47.5), hatch="\\\\\\",
        size="14 MB", sections=5, source="SHOM + EMODnet"),
    "Norway_contours": dict(
        color="#f58231", bbox=(-0.49, 56.09, 38.0, 81.0),
        coverage=["src/coverage/norway.gpkg"], exclude=[], land=False,
        label=(12.0, 68.0), hatch="...", extra=True,
        size="685 MB", sections=6, source="Kartverket charts"),
    "New-zealand_contours": dict(
        color="#42d4f4", bbox=(145.0, -78.5, -143.8, -7.0),
        coverage=["src/coverage/new-zealand.gpkg"],
        exclude=[], land=True,
        label=(172.0, -42.0), hatch="///", extra=True,
        size="78 MB", sections=18, source="LINZ charts + GEBCO_2026"),
    "Gulf_of_Mexico_north-west_contours": dict(
        color="#f032e6", bbox=(-98.0, 24.0, -80.5, 31.5),
        coverage=["src/coverage/gulf.gpkg"], exclude=[], land=False,
        label=(-90.0, 27.0), hatch="xx", extra=True,
        size="60 MB", sections=14, source="CUDEM + GEBCO_2026 + NOAA ENC"),
    "Europe_contours": dict(
        color="#1f77b4", bbox=(-31.3, 25.4, 36.0, 71.2),
        coverage=["src/coverage/emodnet_europe.gpkg"], land=False,
        exclude=["ireland.gpkg", "france.gpkg", "uk.gpkg", "netherlands.gpkg", "norway.gpkg"],
        label=(2.0, 50.0), hatch="\\\\\\", wide=True, show_excluded=True, overlap=False,
        sea_mask="src/coverage/emodnet_europe.gpkg",
        exclude_label="covered by the regional maps",
        size="726 MB", sections=2, source="EMODnet DTM 2024"),
    "Europe_points": dict(
        color="#17becf", bbox=(-31.3, 25.4, 36.0, 71.2),
        coverage=["src/coverage/emodnet_europe.gpkg"], land=False,
        exclude=["ireland.gpkg", "france.gpkg", "uk.gpkg", "netherlands.gpkg", "norway.gpkg"],
        label=(2.0, 55.0), hatch="...", wide=True, show_excluded=True, overlap=False,
        sea_mask="src/coverage/emodnet_europe.gpkg",
        exclude_label="covered by the regional maps",
        size="124 MB", sections=5, source="EMODnet DTM 2024"),
    "World_contours": dict(
        color="#7f7f7f", bbox=(-180.0, -79.0, 180.0, 85.0),
        coverage=["src/coverage/world_ocean.gpkg"], land=False,
        exclude=["emodnet_europe.gpkg", "ireland.gpkg", "france.gpkg", "uk.gpkg",
                 "netherlands.gpkg", "norway.gpkg", "new-zealand.gpkg", "gulf.gpkg"],
        label=(-30.0, 0.0), hatch="///", wide=True, show_excluded=True, overlap=False,
        sea_mask="src/coverage/world_ocean.gpkg",
        exclude_label="covered by Europe_contours and the regional maps",
        size="1.8 GB", sections=5, source="GEBCO_2026 15 arc-second"),
    "World_Northern_hemisphere_points": dict(
        color="#bcbd22", bbox=(-180.0, 0.0, 180.0, 85.0),
        coverage=["src/coverage/world_ocean.gpkg"], land=False,
        exclude=["emodnet_europe.gpkg", "ireland.gpkg", "france.gpkg", "uk.gpkg",
                 "netherlands.gpkg", "norway.gpkg", "new-zealand.gpkg", "gulf.gpkg"],
        label=(-30.0, 40.0), hatch="...", wide=True, overlap=False,
        size="336 MB", sections=3, source="GEBCO_2026 15 arc-second"),
    "World_Southern_hemisphere_points": dict(
        color="#8c564b", bbox=(-180.0, -79.0, 180.0, 0.0),
        coverage=["src/coverage/world_ocean.gpkg"], land=False,
        exclude=["emodnet_europe.gpkg", "ireland.gpkg", "france.gpkg", "uk.gpkg",
                 "netherlands.gpkg", "norway.gpkg", "new-zealand.gpkg", "gulf.gpkg"],
        label=(-30.0, -40.0), hatch="...", wide=True, overlap=False,
        size="313 MB", sections=3, source="GEBCO_2026 15 arc-second"),
}

_world_land = None


def load_gpkg(path):
    ds = ogr.Open(path)
    geoms = []
    for f in ds.GetLayer(0):
        g = f.GetGeometryRef()
        if g is not None and not g.IsEmpty():
            geoms.append(wkb.loads(bytes(g.ExportToWkb())))
    ds = None
    if not geoms:
        return None
    try:
        return unary_union(geoms)
    except Exception:
        # a hole of the polygon touches its shell (the lakes cut out of the ocean), GEOS cannot unite it
        return unary_union([shapely.make_valid(g) for g in geoms])


def load_land_source(path):
    ds = ogr.Open(path)
    geoms = []
    for f in ds.GetLayer(0):
        g = f.GetGeometryRef()
        if g is not None and not g.IsEmpty():
            geoms.append(wkb.loads(bytes(g.ExportToWkb())))
    ds = None
    return unary_union(geoms) if geoms else None


def world_land():
    """The OSM land of the whole world, simplified and united once into a local cache."""
    global _world_land
    if _world_land is not None:
        return _world_land
    os.makedirs(LAND_CACHE, exist_ok=True)
    simple = os.path.join(LAND_CACHE, "world_land.gpkg")
    union = os.path.join(LAND_CACHE, "world_land_union.gpkg")
    if not os.path.exists(simple):
        subprocess.run(["ogr2ogr", "-f", "GPKG", simple, LAND, "-simplify", "0.02",
                        "-nln", "land"], check=True, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    if not os.path.exists(union):
        subprocess.run(["ogr2ogr", "-f", "GPKG", union, simple, "-dialect", "SQLITE",
                        "-sql", "SELECT ST_Union(geom) AS geom FROM land", "-nlt", "MULTIPOLYGON"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _world_land = load_land_source(union)
    return _world_land


def land_for(bbox, name):
    """OSM land clipped to a bbox; the whole-world cache is used for wide views."""
    w, s, e, n = bbox
    span = max(e - w, n - s)
    if span > 40:
        return world_land().intersection(bbox_geom(bbox))
    os.makedirs(LAND_CACHE, exist_ok=True)
    # the bbox is part of the name: a cached clip of another box (the map had the box of New Zealand only,
    # now its whole Pacific area) must not be used
    tag = "_".join(f"{v:g}" for v in bbox)
    cache = os.path.join(LAND_CACHE, f"clip_{name}_{tag}.geojson")
    if not os.path.exists(cache):
        simplify = max(0.001, round(span / 6000, 4))
        # a box that crosses the antimeridian is two boxes
        rects = [(w, s, e, n)] if w <= e else [(w, s, 180, n), (-180, s, e, n)]
        for i, r in enumerate(rects):
            subprocess.run(["ogr2ogr", "-f", "GeoJSON", cache, LAND, "-update" if i else "-overwrite",
                            "-spat", *[f"{v:g}" for v in r], "-simplify", f"{simplify:g}"],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return load_land_source(cache).intersection(bbox_geom(bbox))


def bbox_geom(bbox):
    """The box of a region; a box that crosses the antimeridian is two boxes."""
    w, s, e, n = bbox
    if w <= e:
        return box(w, s, e, n)
    return box(w, s, 180, n).union(box(-180, s, e, n))


def coverage(name, bbox, work, cfg):
    """The polygon of the data of a map: the coverage files the build makes in src/coverage."""
    cfg = cfg or {}
    geoms = []
    for f in cfg.get("coverage", []):
        path = os.path.join(work, f)
        if not os.path.exists(path):
            raise SystemExit(f"{f} is missing: build the maps first (build_depth_all.sh)")
        g = load_gpkg(path)
        if g is not None:
            geoms.append(g)
    if not geoms:
        return None
    geom = unary_union(geoms)
    return geom.intersection(bbox_geom(bbox)) if bbox else geom


def render_basemap(view, work, basemap, name):
    """The OSM background of the view, rendered by the core."""
    d = os.path.join(work, "preview/overview/bg_" + name)
    os.makedirs(d, exist_ok=True)
    for f in os.listdir(d):
        os.remove(os.path.join(d, f))
    os.symlink(basemap, os.path.join(d, os.path.basename(basemap)))
    out = os.path.join(d, name)
    cmd = [EYE, "-obfsPath=" + d, "-stylesPath=" + STYLES, "-styleName=default",
           f"-latLon={view['lat']}:{view['lon']}", f"-zoom={view['zoom']}",
           f"-outputRasterWidth={view['width']}", f"-outputRasterHeight={view['height']}",
           "-referenceTileSize=256", "-displayDensityFactor=1.0", "-outputImageFilename=" + out]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return out + ".png"


def merc(lat):
    return math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))


def continuous(lon, lon0):
    """The longitude in the space of the view: the view is cut at lon0-180, so the longitudes are moved into it."""
    while lon - lon0 < -180:
        lon += 360
    while lon - lon0 > 180:
        lon -= 360
    return lon


def to_px(lon, lat, lon0, lat0, zoom, width, height, wrap=True):
    """Web Mercator pixels of the rendered raster. A ring that was already unwrapped and placed next to the view
    must not be wrapped again: its part beyond the cut of the view is clipped, not drawn across the whole picture."""
    scale = 2 ** zoom * 256
    dx = (continuous(lon, lon0) if wrap else lon) - lon0
    return (dx / 360 * scale + width / 2,
            -(merc(lat) - merc(lat0)) / (2 * math.pi) * scale + height / 2)


def unwrap_ring(coords):
    """The points of a ring with a crossing of the antimeridian continued over it. A ring that goes around the
    globe (the window of the view, Antarctica) is not unwrapped: the jump from -180 to 180 is its own edge, not a
    crossing, and unwrapping it turns the ring into a degenerate triangle."""
    pts = []
    prev = None
    for x, y in coords:
        if prev is not None:
            if x - prev > 180:
                x -= 360
            elif x - prev < -180:
                x += 360
        pts.append((x, y))
        prev = x
    xs = [p[0] for p in pts]
    if max(xs) - min(xs) >= 359.9:
        return [(x, y) for x, y in coords]
    return pts


def ring_shift(pts, lon0):
    """The shift that brings a ring next to the view. A ring that goes around the globe is the window of the view:
    its left edge is the cut of the view and its right edge the same meridian 360 degrees further."""
    xs = [p[0] for p in pts]
    if max(xs) - min(xs) >= 359.9:
        return (lon0 - 180) - min(xs)
    mean = sum(xs) / len(xs)
    shift = 0
    while mean + shift - lon0 > 180:
        shift -= 360
    while mean + shift - lon0 < -180:
        shift += 360
    return shift


def place_ring(coords, lon0, shift=None):
    """The points of a ring in the space of the view; a hole must take the shift of its exterior ring."""
    pts = unwrap_ring(coords)
    if shift is None:
        shift = ring_shift(pts, lon0)
    return [(x + shift, y) for x, y in pts]


def draw(geom, color, alpha, lw, z, hatch=None, prj=None, lon0=0.0):
    """The geometry is in lon/lat, the axes are in the pixels of the rendered raster."""
    from matplotlib.path import Path
    from matplotlib.patches import PathPatch
    if geom.is_empty:
        return []
    if geom.geom_type == "GeometryCollection":
        geom = unary_union([g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")])
    out = []
    for poly in geom.geoms if geom.geom_type == "MultiPolygon" else [geom]:
        # the holes take the shift of the exterior ring: shifted on their own, the land holes of the world
        # ocean land over the continents and their edges run across the whole view
        ext = unwrap_ring(poly.exterior.coords)
        shift = ring_shift(ext, lon0)
        rings = [ext] + [unwrap_ring(r.coords) for r in poly.interiors]
        verts, codes = [], []
        for ring in rings:
            pts = [prj(x + shift, y) for x, y in ring]
            verts.extend(pts)
            codes.append(Path.MOVETO)
            codes.extend([Path.LINETO] * (len(pts) - 2))
            codes.append(Path.CLOSEPOLY)
        rgba = (*[int(color[i:i + 2], 16) / 255 for i in (1, 3, 5)], alpha)
        out.append(PathPatch(Path(verts, codes), facecolor=rgba, edgecolor=color,
                             linewidth=lw, zorder=z, hatch=hatch))
    return out


def view_for_bbox(bbox, max_width=4096, max_height=4200, pad=0.08):
    """A view (center, integer zoom, raster size) that frames bbox with a small margin.
    A box that crosses the antimeridian is measured eastward through it: the center is then east of 180."""
    w, s, e, n = bbox
    whole = w <= -179.999 and e >= 179.999
    if w > e:
        e += 360.0
    dx, dy = (e - w) * pad, (n - s) * pad
    w0 = w if whole else w - dx
    e0 = e if whole else e + dx
    s0, n0 = max(-85.0, s - dy), min(85.0, n + dy)
    lon0 = (w0 + e0) / 2
    m_s, m_n = merc(s0), merc(n0)
    lat0 = math.degrees(2 * math.atan(math.exp((m_s + m_n) / 2)) - math.pi / 2)
    z = 1
    for z in range(15, 0, -1):
        world = 256 * 2 ** z
        pw = (e0 - w0) / 360 * world
        ph = (m_n - m_s) / (2 * math.pi) * world
        if pw <= max_width and ph <= max_height:
            break
    return dict(lon=(lon0 + 180) % 360 - 180, lon0=lon0, lat=lat0, zoom=z,
                width=max(600, int(round(pw))), height=max(400, int(round(ph))))


def select_maps(names):
    if names:
        return {n: REGIONS[n] for n in names.split(",") if n in REGIONS}
    return {n: c for n, c in REGIONS.items() if not c.get("wide") and not c.get("extra")}


def render_view(view, out, maps, work, basemap):
    name = os.path.splitext(os.path.basename(out))[0]
    bg = np.asarray(Image.open(render_basemap(view, work, basemap, name)).convert("RGB"))
    # the background is muted, so that the areas of the maps are the first thing to see
    bg = (bg * 0.45 + 255 * 0.55).astype(np.uint8)
    h, w = bg.shape[:2]
    fig, ax = plt.subplots(figsize=(w / 100, h / 100), dpi=100)
    ax.imshow(bg, extent=(0, w, h, 0), interpolation="bilinear", zorder=0)

    lon0, lat0 = view.get("lon0", view["lon"]), view["lat"]

    def px(lon, lat):
        return to_px(lon, lat, lon0, lat0, view["zoom"], w, h)

    def px_raw(lon, lat):
        return to_px(lon, lat, lon0, lat0, view["zoom"], w, h, wrap=False)

    areas, handles = {}, []
    for map_name, cfg in maps.items():
        geom = coverage(map_name, cfg["bbox"], work, cfg)
        if geom is None:
            continue
        if cfg.get("land", False):
            geom = geom.difference(land_for(cfg["bbox"], "map_" + map_name))
        cuts = []
        for ex in cfg["exclude"]:
            cut = load_gpkg(os.path.join(work, "src/coverage", ex))
            cuts.append(cut)
            geom = geom.difference(cut)
        areas[map_name] = geom
        print(f"{map_name}: {geom.area:.3f} square degrees")
        z = 2 if cfg.get("wide") else 3
        hatch = "" if view.get("solid") else cfg["hatch"]
        for p in draw(geom, cfg["color"], 0.42, 2.5, z, hatch=hatch, prj=px_raw, lon0=lon0):
            ax.add_patch(p)
        if cfg.get("show_excluded") and cuts and view.get("show_excluded", True):
            cut = unary_union(cuts).intersection(bbox_geom(cfg["bbox"]))
            if cfg.get("sea_mask"):
                cut = cut.intersection(load_gpkg(os.path.join(work, cfg["sea_mask"])))
            for p in draw(cut, "#7d8a91", 0.16, 1.0, z + 0.2, hatch="///", prj=px_raw, lon0=lon0):
                ax.add_patch(p)
            handles.append(plt.Rectangle((0, 0), 1, 1, facecolor="none", edgecolor="#7d8a91",
                                         hatch="///", label=cfg.get("exclude_label",
                                                                    "covered by a regional map")))
        rgba = (*[int(cfg["color"][i:i + 2], 16) / 255 for i in (1, 3, 5)], 0.42)
        handles.append(plt.Rectangle((0, 0), 1, 1, facecolor=rgba, edgecolor=cfg["color"],
                                     hatch=hatch,
                                     label=f"{map_name} ({geom.area:.1f} sq deg)"))
        b = cfg["bbox"]
        x0, y0 = px(b[0], b[3])
        x1, y1 = px(b[2], b[1])
        ax.add_patch(plt.Rectangle((x0, y0), x1 - x0, y1 - y0, fill=False, edgecolor=cfg["color"],
                                   linestyle=(0, (7, 5)), linewidth=1.6, zorder=4))
        x, y = px(*cfg["label"])
        if 0 <= x <= w and 0 <= y <= h:
            ax.text(x, y, map_name, color=cfg["color"], fontsize=16, fontweight="bold", zorder=6,
                    ha="center", va="center", clip_on=True,
                    path_effects=[matplotlib.patheffects.withStroke(linewidth=4, foreground="white")])

    names = list(areas)
    for i in range(len(names)):
        if not maps[names[i]].get("overlap", True):
            continue
        for j in range(i + 1, len(names)):
            if not maps[names[j]].get("overlap", True):
                continue
            inter = areas[names[i]].intersection(areas[names[j]])
            if inter.area > 0.0005:
                print(f"OVERLAP {names[i]} & {names[j]}: {inter.area:.3f} sq deg")
                for p in draw(inter, "#dc143c", 0.6, 1.5, 5, hatch="xx", prj=px_raw, lon0=lon0):
                    ax.add_patch(p)
                if inter.area > 0.01:
                    point = inter.representative_point()
                    x, y = px(point.x, point.y)
                    ax.text(x, y, "overlap", color="#dc143c", fontsize=14, fontweight="bold", zorder=7,
                            ha="center", va="center", clip_on=True,
                            path_effects=[matplotlib.patheffects.withStroke(linewidth=3, foreground="white")])

    ax.set_xlim(0, w)
    ax.set_ylim(h, 0)
    ax.set_xticks([])
    ax.set_yticks([])
    title = view.get("title")
    subtitle = view.get("subtitle")
    if title:
        ax.set_title(title + (f"\n{subtitle}" if subtitle else ""),
                     fontsize=view.get("title_fontsize", 18), fontweight="bold")
    handles.append(plt.Rectangle((0, 0), 1, 1, fill=False, edgecolor="#555555",
                                 linestyle=(0, (7, 5)), linewidth=1.4,
                                 label="region box (bbox)"))
    leg = ax.legend(handles=handles, loc="lower left", fontsize=14, framealpha=0.9,
                    title="the area of the data of the map", title_fontsize=14)
    leg.get_frame().set_edgecolor("#666666")
    fig.tight_layout()
    fig.savefig(out)
    print(out)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("view", nargs="?", help="JSON file with the view and the title")
    ap.add_argument("out", nargs="?", help="the PNG to write")
    ap.add_argument("--work", default=DEFAULT_WORK, help="the working folder of the depth maps")
    ap.add_argument("--basemap", default=os.path.expanduser("~/osmand/World_basemap_2.obf"),
                    help="the OSM basemap of the background")
    ap.add_argument("--maps", help="comma-separated map names (default: the detailed maps)")
    ap.add_argument("--auto", metavar="OUTDIR", help="one PNG per map, framed from its bbox")
    ap.add_argument("--frame", nargs=4, type=float, metavar=("W", "S", "E", "N"),
                    help="view box for --auto or a JSON view")
    args = ap.parse_args()

    if args.auto:
        os.makedirs(args.auto, exist_ok=True)
        names = args.maps.split(",") if args.maps else list(REGIONS)
        for name in names:
            cfg = REGIONS[name]
            bbox = tuple(args.frame) if args.frame else cfg["bbox"]
            view = view_for_bbox(bbox)
            view["title"] = name
            if cfg.get("size"):
                view["subtitle"] = (f"{cfg['size']} · {cfg['sections']} map sections · "
                                    f"{cfg['source']}")
            view["title_fontsize"] = 22
            # one map on the picture: a solid fill reads better than the hatch used to tell
            # the maps apart on a combined view
            cfg = dict(cfg, hatch="")
            render_view(view, os.path.join(args.auto, name + ".png"), {name: cfg},
                        args.work, args.basemap)
        return

    if not args.view or not args.out:
        ap.error("VIEW_JSON and OUT_PNG are needed without --auto")
    view = json.load(open(args.view))
    if args.frame:
        view.update(view_for_bbox(args.frame))
    render_view(view, args.out, select_maps(args.maps), args.work, args.basemap)


if __name__ == "__main__":
    sys.exit(main())
