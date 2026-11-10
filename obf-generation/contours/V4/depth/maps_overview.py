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
from osgeo import ogr
from PIL import Image
from shapely import wkb
from shapely.geometry import box
from shapely.ops import unary_union

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_WORK = "/mnt/wd_2tb/depth_work"
EYE = os.path.expanduser("~/utilites/OsmAndMapCreator-main/amd64-linux-clang/eyepiece_standalone")
STYLES = os.path.expanduser("~/git/OsmAnd-resources/rendering_styles")

# the sources of the grid of every map, the bbox of its region and the coverage of the
# neighbours it gives way to
REGIONS = {
    "Great_Britain_contours": dict(
        color="#e6194b", bbox=(-8.7, 49.8, 2.0, 60.9),
        sources=["src/uk/emodnet_home.vrt", "src/uk/uk_filled.vrt"],
        exclude=[], label=(-2.4, 54.3), hatch="///"),
    "Ireland_contours": dict(
        color="#3cb44b", bbox=(-11.8, 51.2, -5.3, 55.6),
        sources=["src/emodnet/emodnet_2024.vrt", "src/ireland/fill/ireland_filled.vrt"],
        exclude=["uk_ireland.gpkg"], label=(-8.6, 53.2), hatch="..."),
    "Netherlands_contours": dict(
        color="#4363d8", bbox=(1.7, 51.1, 7.3, 55.7),
        sources=["src/emodnet/emodnet_2024.vrt", "src/netherlands/fill/netherlands_filled.vrt"],
        exclude=["uk_netherlands.gpkg"], label=(4.6, 52.6), hatch="xxx"),
    "France_contours": dict(
        color="#911eb4", bbox=(-5.5, 43.3, 2.6, 51.2),
        sources=["src/france/fill/france_filled.vrt"],
        exclude=["uk_filled.gpkg"], label=(-2.4, 47.5), hatch="\\\\\\"),
}


def load_gpkg(path):
    ds = ogr.Open(path)
    geoms = []
    for f in ds.GetLayer(0):
        g = f.GetGeometryRef()
        if g is not None and not g.IsEmpty():
            geoms.append(wkb.loads(bytes(g.ExportToWkb())))
    ds = None
    return unary_union(geoms) if geoms else None


def coverage(name, bbox, sources, work, cell=0.001):
    """The polygon of the data of a map, as depth_coverage.py builds it."""
    path = os.path.join(work, "src/coverage", name + ".gpkg")
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        cmd = ["python3", os.path.join(HERE, "depth_coverage.py"),
               "--bbox", *[str(v) for v in bbox], "--cell", str(cell), "--shrink", "0"] + \
              [os.path.join(work, s) for s in sources] + [path]
        subprocess.run(cmd, check=True)
    return load_gpkg(path)


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


def to_px(lon, lat, lon0, lat0, zoom, width, height):
    """Web Mercator pixels of the rendered raster."""
    scale = 2 ** zoom * 256
    return ((lon - lon0) / 360 * scale + width / 2,
            -(merc(lat) - merc(lat0)) / (2 * math.pi) * scale + height / 2)


def draw(geom, color, alpha, lw, z, hatch=None, prj=None):
    """The geometry is in lon/lat, the axes are in the pixels of the rendered raster."""
    from matplotlib.path import Path
    from matplotlib.patches import PathPatch
    if geom.is_empty:
        return []
    if geom.geom_type == "GeometryCollection":
        geom = unary_union([g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")])
    out = []
    for poly in geom.geoms if geom.geom_type == "MultiPolygon" else [geom]:
        verts, codes = [], []
        for ring in [poly.exterior] + list(poly.interiors):
            pts = [prj(x, y) for x, y in ring.coords]
            verts.extend(pts)
            codes.append(Path.MOVETO)
            codes.extend([Path.LINETO] * (len(pts) - 2))
            codes.append(Path.CLOSEPOLY)
        rgba = (*[int(color[i:i + 2], 16) / 255 for i in (1, 3, 5)], alpha)
        out.append(PathPatch(Path(verts, codes), facecolor=rgba, edgecolor=color,
                             linewidth=lw, zorder=z, hatch=hatch))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("view", help="JSON file with the view and the title")
    ap.add_argument("out", help="the PNG to write")
    ap.add_argument("--work", default=DEFAULT_WORK, help="the working folder of the depth maps")
    ap.add_argument("--basemap", default=os.path.expanduser("~/osmand/World_basemap_2.obf"),
                    help="the OSM basemap of the background")
    ap.add_argument("--cell", type=float, default=0.001, help="the grid of the coverage in degrees")
    args = ap.parse_args()

    view = json.load(open(args.view))
    work = args.work
    name = os.path.splitext(os.path.basename(args.out))[0]

    bg = np.asarray(Image.open(render_basemap(view, work, args.basemap, name)).convert("RGB"))
    # the background is muted, so that the areas of the maps are the first thing to see
    bg = (bg * 0.45 + 255 * 0.55).astype(np.uint8)
    h, w = bg.shape[:2]
    fig, ax = plt.subplots(figsize=(w / 100, h / 100), dpi=100)
    ax.imshow(bg, extent=(0, w, h, 0), interpolation="bilinear", zorder=0)

    lon0, lat0 = view["lon"], view["lat"]

    def px(lon, lat):
        return to_px(lon, lat, lon0, lat0, view["zoom"], w, h)

    areas, handles = {}, []
    for map_name, cfg in REGIONS.items():
        geom = coverage("overview_" + map_name, cfg["bbox"], cfg["sources"], work, args.cell)
        for ex in cfg["exclude"]:
            geom = geom.difference(load_gpkg(os.path.join(work, "src/coverage", ex)))
        areas[map_name] = geom
        print(f"{map_name}: {geom.area:.3f} square degrees")
        for p in draw(geom, cfg["color"], 0.42, 2.5, 3, hatch=cfg["hatch"], prj=px):
            ax.add_patch(p)
        rgba = (*[int(cfg["color"][i:i + 2], 16) / 255 for i in (1, 3, 5)], 0.42)
        handles.append(plt.Rectangle((0, 0), 1, 1, facecolor=rgba, edgecolor=cfg["color"],
                                     hatch=cfg["hatch"],
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
        for j in range(i + 1, len(names)):
            inter = areas[names[i]].intersection(areas[names[j]])
            if inter.area > 0.0005:
                print(f"OVERLAP {names[i]} & {names[j]}: {inter.area:.3f} sq deg")
                for p in draw(inter, "#dc143c", 0.6, 1.5, 5, hatch="xx", prj=px):
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
    if view.get("title"):
        ax.set_title(view["title"], fontsize=16, fontweight="bold")
    leg = ax.legend(handles=handles, loc="lower left", fontsize=13, framealpha=0.9,
                    title="the area of the data of the map", title_fontsize=13)
    leg.get_frame().set_edgecolor("#666666")
    fig.tight_layout()
    fig.savefig(args.out)
    print(args.out)


if __name__ == "__main__":
    sys.exit(main())
