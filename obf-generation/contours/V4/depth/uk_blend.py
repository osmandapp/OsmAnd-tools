#!/usr/bin/env python3
"""Blend UK surveys near their boundaries while keeping the newest survey authoritative.

Older sources with no visible cell in the unblended mosaic are omitted. Adjacent detailed
surveys are cross-faded symmetrically: the boundary value is the mean of the two sources,
ramping to each source's own value over the feather width. Against the coarse EMODnet underlay
the detailed survey keeps its edge value and the underlay is pulled towards it outside. The hard
mosaic is built by priority from the sources warped to the common 0.0002 grid; it is not read
from uk_sources.vrt, where gdalbuildvrt can stretch a source's edge pixel into the neighbouring
cell. Single-source areas and the valid-data footprint are preserved. Inputs are never modified.

Full build: uk_blend.py SRC_UK_DIR --full [--feather 10] [-j JOBS]
Preview:    uk_blend.py SRC_UK_DIR --bbox W S E N --output PREVIEW_DIR
"""

import argparse, math, os, subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache

import numpy as np
import rasterio
from affine import Affine
from rasterio.coords import disjoint_bounds
from rasterio.enums import Resampling
from rasterio.windows import Window, bounds as window_bounds, from_bounds
from scipy import ndimage


@lru_cache(maxsize=64)
def open_source(path):
    return rasterio.open(path)


def read_source(path, bounds, shape):
    """Read a source on the requested grid without resampling.

    The sources are already on the 0.0002 grid (see warped_source). Reading the
    native window and placing it at its exact offset avoids the nearest-neighbour
    edge stretch that out_shape produces at a source boundary.
    """
    src = open_source(path)
    window = from_bounds(*bounds, transform=src.transform)
    col0 = max(0, int(math.floor(window.col_off)))
    row0 = max(0, int(math.floor(window.row_off)))
    col1 = min(src.width, int(math.ceil(window.col_off + window.width)))
    row1 = min(src.height, int(math.ceil(window.row_off + window.height)))
    out = np.full(shape, np.nan, dtype=np.float32)
    if col0 >= col1 or row0 >= row1:
        return out
    data = src.read(1, window=Window(col0, row0, col1 - col0, row1 - row0))
    x0 = max(0, int(round(col0 - window.col_off)))
    y0 = max(0, int(round(row0 - window.row_off)))
    h = min(data.shape[0], shape[0] - y0)
    w = min(data.shape[1], shape[1] - x0)
    if h > 0 and w > 0:
        out[y0:y0 + h, x0:x0 + w] = data[:h, :w]
    return out


def outer_window(source_bounds, base):
    w = from_bounds(*source_bounds, transform=base.transform)
    col0 = max(0, math.floor(w.col_off))
    row0 = max(0, math.floor(w.row_off))
    col1 = min(base.width, math.ceil(w.col_off + w.width))
    row1 = min(base.height, math.ceil(w.row_off + w.height))
    if col0 >= col1 or row0 >= row1:
        return None
    return Window(col0, row0, col1 - col0, row1 - row0)


def _visibility_chunk(args):
    """One worker of visible_sources: the same per-source check over a slice of the list."""
    raw_path, chunk = args
    result = []
    with rasterio.open(raw_path) as base:
        for path in chunk:
            if os.path.basename(path) == "emodnet_fill.vrt":
                result.append((path, True))
                continue
            src = open_source(path)
            window = outer_window(src.bounds, base)
            visible = False
            if window is not None:
                shape = (int(window.height), int(window.width))
                a = read_source(path, window_bounds(window, base.transform), shape)
                raw = base.read(1, window=window)
                visible = bool((np.isfinite(a) & (a == raw)).any())
            result.append((path, visible))
    return result


def visible_sources(raw_path, paths, jobs=1):
    """Sources that own at least one cell of the unblended mosaic.

    The check reads one window per source, so it runs in parallel chunks; hidden
    sources are omitted later, which makes the blend pass cheaper.
    """
    jobs = max(1, min(jobs, len(paths)))
    chunks = [paths[i::jobs] for i in range(jobs)]
    with ProcessPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(_visibility_chunk, (raw_path, c)) for c in chunks]
        seen, checked = {}, 0
        for future in as_completed(futures):
            chunk = future.result()
            seen.update(chunk)
            checked += len(chunk)
            print(f"Visibility: {checked}/{len(paths)} sources", flush=True)
    used = [p for p in paths if seen[p]]
    superseded = [p for p in paths if not seen[p]]
    print(f"Visibility: {len(paths)} sources, {len(superseded)} fully hidden", flush=True)
    return used, superseded


def blend_boundaries(before, ids, width, underlay_width=None, underlay_id=None,
                     priority=0.5, underlay_priority=1.0):
    """Cross-fade across source boundaries.

    Between two detailed surveys the boundary value is the mean of the two
    (priority 0.5), ramping to each source's own value over width/2 pixels on its
    side. Against the coarse EMODnet underlay the detailed survey keeps its edge
    value (priority 1.0) and the underlay is pulled towards it outside; a
    symmetric 50/50 there eats shallow features at the survey edge and leaves a
    step where three sources meet. The boundary mask marks both cells of every
    pair, so the transition is continuous on both sides.
    """
    shape = before.shape
    boundary = np.zeros(shape, dtype=bool)
    b = (ids[:, 1:] != ids[:, :-1]) & (ids[:, 1:] >= 0) & (ids[:, :-1] >= 0)
    boundary[:, 1:] |= b
    boundary[:, :-1] |= b
    b = (ids[1:, :] != ids[:-1, :]) & (ids[1:, :] >= 0) & (ids[:-1, :] >= 0)
    boundary[1:, :] |= b
    boundary[:-1, :] |= b
    after = before.copy()
    if not boundary.any():
        return after, np.zeros(shape, dtype=bool)
    other_value = np.full(shape, np.nan, dtype=np.float32)
    other_id = np.full(shape, -1, dtype=np.int32)
    for dy, dx in ((0, 1), (0, -1), (1, 0), (-1, 0)):
        shifted_ids = np.full(shape, -1, dtype=np.int32)
        shifted_values = np.full(shape, np.nan, dtype=np.float32)
        if dy == 0 and dx == 1:
            shifted_ids[:, :-1] = ids[:, 1:]
            shifted_values[:, :-1] = before[:, 1:]
        elif dy == 0 and dx == -1:
            shifted_ids[:, 1:] = ids[:, :-1]
            shifted_values[:, 1:] = before[:, :-1]
        elif dy == 1:
            shifted_ids[:-1, :] = ids[1:, :]
            shifted_values[:-1, :] = before[1:, :]
        else:
            shifted_ids[1:, :] = ids[:-1, :]
            shifted_values[1:, :] = before[:-1, :]
        take = boundary & (shifted_ids != ids) & (shifted_ids >= 0) & ~np.isfinite(other_value)
        other_value[take] = shifted_values[take]
        other_id[take] = shifted_ids[take]
    distance, indices = ndimage.distance_transform_edt(~boundary, return_indices=True)
    nearest_other = other_value[indices[0], indices[1]]
    nearest_other_id = other_id[indices[0], indices[1]]
    if underlay_width is not None and underlay_id is not None and underlay_id >= 0:
        widths = np.where((ids == underlay_id) | (nearest_other_id == underlay_id),
                          underlay_width, width).astype(np.float32)
    else:
        widths = np.full(shape, float(width), dtype=np.float32)
    if underlay_id is not None and underlay_id >= 0:
        boundary_priority = np.where((ids == underlay_id) | (nearest_other_id == underlay_id),
                                     underlay_priority, priority).astype(np.float32)
    else:
        boundary_priority = np.full(shape, priority, dtype=np.float32)
    own_boundary = np.where(ids > nearest_other_id,
                            boundary_priority, 1.0 - boundary_priority).astype(np.float32)
    t = np.clip(distance / (widths / 2.0), 0.0, 1.0)
    own_weight = (own_boundary + (1.0 - own_boundary) * t * t * (3.0 - 2.0 * t)).astype(np.float32)
    blend = (ids >= 0) & np.isfinite(before) & np.isfinite(nearest_other) & (distance < widths / 2.0)
    after[blend] = before[blend] * own_weight[blend] + nearest_other[blend] * (1.0 - own_weight[blend])
    transitions = blend & (np.abs(after - before) > 1e-5)
    return after, transitions


def mosaic_window(base, window, sources, feather, emodnet_feather=20):
    shape = (int(window.height), int(window.width))
    bounds = window_bounds(window, base.transform)
    if len(sources) <= 1:
        if not sources:
            before = base.read(1, window=window).astype(np.float32)
            if np.isfinite(before).any():
                raise RuntimeError(f"No source covers valid cells at {window}")
            return before, before, 0, 0.0
        a = read_source(sources[0][0], bounds, shape)
        return a, a, 0, 0.0
    before = np.full(shape, np.nan, dtype=np.float32)
    ids = np.full(shape, -1, dtype=np.int32)
    for index, (path, _) in enumerate(sources):
        a = read_source(path, bounds, shape)
        valid = np.isfinite(a)
        if not valid.any():
            continue
        before[valid] = a[valid]
        ids[valid] = index
    underlay_id = next((i for i, (path, _) in enumerate(sources)
                        if os.path.basename(path) == "emodnet_fill.vrt"), None)
    after, transitions = blend_boundaries(before, ids, feather, emodnet_feather, underlay_id,
                                          underlay_priority=1.0)
    if not np.array_equal(np.isfinite(after), np.isfinite(before)):
        raise RuntimeError(f"Blending changed coverage at {window}")
    if not np.allclose(after[~transitions], before[~transitions], equal_nan=True, rtol=0, atol=1e-5):
        raise RuntimeError(f"Blending changed cells outside transitions at {window}")
    count = int(transitions.sum())
    change = float(np.abs(after[transitions] - before[transitions]).sum()) if count else 0.0
    return after, before, count, change


def blend_strip(raw_path, strip, source_infos, feather, out_path):
    open_source.cache_clear()
    index, row0, row1 = strip
    with rasterio.open(raw_path) as base:
        # Blend needs a halo: a source boundary just outside a block must still feather into it,
        # otherwise the block/strip edge becomes a hard step. Sources are therefore filtered with
        # the same margin, and only the central part of each expanded block is written.
        margin = max(feather, 20) // 2 + 2
        strip_window = Window(0, row0, base.width, row1 - row0)
        strip_bounds = window_bounds(strip_window, base.transform)
        pad = margin * base.res[0]
        wide_bounds = (strip_bounds[0], strip_bounds[1] - pad, strip_bounds[2], strip_bounds[3] + pad)
        sources = [s for s in source_infos if not disjoint_bounds(s[1], wide_bounds)]
        profile = {
            "driver": "GTiff", "width": base.width, "height": row1 - row0,
            "count": 1, "dtype": "float32", "crs": base.crs, "nodata": np.nan,
            "transform": base.transform * Affine.translation(0, row0),
            "tiled": True, "blockxsize": 512, "blockysize": 512,
            "compress": "DEFLATE", "predictor": 3, "num_threads": 2,
            "bigtiff": "IF_SAFER", "sparse_ok": True,
        }
        transition_cells, total_change = 0, 0.0
        with rasterio.open(out_path, "w", **profile) as dst:
            for row in range(row0, row1, 512):
                height = min(512, row1 - row)
                for col in range(0, base.width, 512):
                    width = min(512, base.width - col)
                    erow0 = max(0, row - margin)
                    erow1 = min(base.height, row + height + margin)
                    ecol0 = max(0, col - margin)
                    ecol1 = min(base.width, col + width + margin)
                    expanded = Window(ecol0, erow0, ecol1 - ecol0, erow1 - erow0)
                    bounds = window_bounds(expanded, base.transform)
                    candidates = [s for s in sources if not disjoint_bounds(s[1], bounds)]
                    after, before, _, _ = mosaic_window(base, expanded, candidates, feather)
                    ys = slice(row - erow0, row - erow0 + height)
                    xs = slice(col - ecol0, col - ecol0 + width)
                    out = after[ys, xs]
                    dst.write(out, 1, window=Window(col, row - row0, width, height))
                    diff = np.abs(out - before[ys, xs])
                    changed = diff > 1e-5
                    transition_cells += int(changed.sum())
                    total_change += float(diff[changed].sum())
    print(f"Strip {index + 1}: {transition_cells} transition cells", flush=True)
    return index, transition_cells, total_change


def atomic_vrt(inputs, list_path, out_path, cell):
    with open(list_path, "w") as f:
        f.write("\n".join(inputs) + "\n")
    tmp = out_path[:-4] + ".tmp.vrt"
    if os.path.exists(tmp):
        os.remove(tmp)
    try:
        subprocess.run(["gdalbuildvrt", "-q", "-tr", str(cell), str(cell),
                        "-input_file_list", list_path, tmp], check=True)
        os.chmod(tmp, 0o444)
        os.replace(tmp, out_path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def build_full(uk, raw_path, feather=10, jobs=16):
    uk = os.path.abspath(uk)
    with open(os.path.join(uk, "uk_vrt_sources.txt")) as f:
        paths = [p.strip() for p in f if p.strip()]
    used, superseded = visible_sources(raw_path, paths, jobs)
    with open(os.path.join(uk, "uk_vrt_sources_used.txt"), "w") as f:
        f.write("\n".join(used) + "\n")
    with open(os.path.join(uk, "uk_superseded.txt"), "w") as f:
        f.write("\n".join(superseded) + "\n")
    print(f"Fully hidden older sources omitted: {len(superseded)}; remaining: {len(used)}", flush=True)
    source_infos = []
    for path in used:
        src = open_source(path)
        source_infos.append((path, src.bounds))
    blend_dir = os.path.join(uk, "blend")
    os.makedirs(blend_dir, exist_ok=True)
    jobs = max(1, min(jobs, len(used)))
    with rasterio.open(raw_path) as base:
        height, cell = base.height, base.res[0]
    strips = [(i, height * i // jobs, height * (i + 1) // jobs) for i in range(jobs)]
    futures = []
    with ProcessPoolExecutor(max_workers=jobs) as pool:
        for strip in strips:
            tmp = os.path.join(blend_dir, f"strip_{strip[0]:02d}.tmp.tif")
            futures.append(pool.submit(blend_strip, raw_path, strip, source_infos, feather, tmp))
        results = [f.result() for f in as_completed(futures)]
    finals = []
    for i in range(jobs):
        tmp = os.path.join(blend_dir, f"strip_{i:02d}.tmp.tif")
        final = os.path.join(blend_dir, f"strip_{i:02d}.tif")
        os.replace(tmp, final)
        finals.append(final)
    atomic_vrt(finals, os.path.join(uk, "uk_blend_sources.txt"),
               os.path.join(uk, "uk.vrt"), cell)
    cells = sum(r[1] for r in results)
    change = sum(r[2] for r in results)
    print(f"uk.vrt: {len(used)} sources, {jobs} strips, feather {feather} px (EMODnet 20 px), "
          f"{cells} transition cells, mean absolute change {change / cells:.3f} m", flush=True)


def blend(sources, before, bounds, widths):
    shape = before.shape
    covered = np.zeros(shape, dtype=bool)
    kept, redundant = [], []
    for path in reversed(sources):
        valid = np.isfinite(read_source(path, bounds, shape))
        if valid.any() and not (valid & ~covered).any():
            redundant.append(path)
        else:
            kept.append((path, valid))
        covered |= valid
    kept.reverse()
    hard = np.full(shape, np.nan, dtype=np.float32)
    ids = np.full(shape, -1, dtype=np.int32)
    for index, (path, _) in enumerate(kept):
        a = read_source(path, bounds, shape)
        valid = np.isfinite(a)
        hard[valid] = a[valid]
        ids[valid] = index
    results, transitions = {}, {}
    underlay_id = next((i for i, (path, _) in enumerate(kept)
                        if os.path.basename(path) == "emodnet_fill.vrt"), None)
    for width in widths:
        after, changed = blend_boundaries(hard, ids, width, 20, underlay_id,
                                          underlay_priority=1.0)
        results[width] = after
        transitions[width] = changed
    return hard, results, transitions, redundant


def save_image(path, array, title):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
    rgb = plt.get_cmap("gray")(Normalize(vmin=-45, vmax=5)(array), bytes=True)
    rgb[~np.isfinite(array), :3] = (173, 216, 230)
    rgb[~np.isfinite(array), 3] = 255
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    ax.imshow(rgb, interpolation="nearest")
    ax.set_title(title)
    ax.set_axis_off()
    fig.tight_layout(pad=0.3)
    fig.savefig(path, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def save_comparison(path, arrays, titles):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
    fig, axes = plt.subplots(3, 1, figsize=(12, 18), dpi=150)
    norm = Normalize(vmin=-45, vmax=5)
    for ax, a, title in zip(axes, arrays, titles):
        rgb = plt.get_cmap("gray")(norm(a), bytes=True)
        rgb[~np.isfinite(a), :3] = (173, 216, 230)
        rgb[~np.isfinite(a), 3] = 255
        ax.imshow(rgb, interpolation="nearest")
        ax.set_title(title)
        ax.set_axis_off()
    fig.tight_layout(pad=0.5)
    fig.savefig(path, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def preview(uk, bbox, output):
    import matplotlib
    matplotlib.use("Agg")
    raw_path = os.path.join(uk, "uk_sources.vrt")
    if not os.path.exists(raw_path):
        raw_path = os.path.join(uk, "uk.vrt")
    os.makedirs(output, exist_ok=True)
    with rasterio.open(raw_path) as base:
        requested = from_bounds(*bbox, transform=base.transform).round_offsets().round_lengths()
        col0 = max(0, int(requested.col_off) - 20)
        row0 = max(0, int(requested.row_off) - 20)
        col1 = min(base.width, int(requested.col_off + requested.width) + 20)
        row1 = min(base.height, int(requested.row_off + requested.height) + 20)
        window = Window(col0, row0, col1 - col0, row1 - row0)
        before = base.read(1, window=window, boundless=True, fill_value=np.nan).astype(np.float32)
        bounds = window_bounds(window, base.transform)
    with open(os.path.join(uk, "uk_vrt_sources.txt")) as f:
        paths = [p.strip() for p in f if p.strip()]
    sources = []
    for path in paths:
        src = open_source(path)
        if not disjoint_bounds(src.bounds, bounds):
            sources.append(path)
    print(f"Preview {bounds}, {before.shape[1]}x{before.shape[0]} cells, "
          f"{len(sources)} overlapping sources", flush=True)
    hard, results, transitions, redundant = blend(sources, before, bounds, [10, 20])
    print(f"Completely covered older sources in the preview: {len(redundant)}")
    for width, after in results.items():
        if not np.array_equal(np.isfinite(after), np.isfinite(hard)):
            raise RuntimeError(f"Blending {width} changed the coverage")
        changed = transitions[width]
        if not np.allclose(after[~changed], hard[~changed], equal_nan=True, rtol=0, atol=1e-5):
            raise RuntimeError(f"Blending {width} changed pixels outside transitions")
        print(f"Feather {width}: {changed.sum()} transition cells, "
              f"mean absolute change {np.mean(np.abs(after[changed] - hard[changed])):.3f} m")
    crop = (slice(20, hard.shape[0] - 20), slice(20, hard.shape[1] - 20))
    arrays = [hard[crop], results[10][crop], results[20][crop]]
    titles = ["Before", "After - 10 px transition", "After - 20 px transition"]
    names = ["before", "after_10px", "after_20px"]
    for name, a, title in zip(names, arrays, titles):
        save_image(os.path.join(output, f"uk_skegness_{name}.png"), a, title)
    save_comparison(os.path.join(output, "uk_skegness_comparison.png"), arrays, titles)
    print("Preview images written to", output, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("uk")
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--feather", type=int, default=10)
    ap.add_argument("-j", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    ap.add_argument("--bbox", nargs=4, type=float, metavar=("W", "S", "E", "N"))
    ap.add_argument("--output")
    args = ap.parse_args()
    uk = os.path.abspath(args.uk)
    if args.full:
        raw_path = os.path.join(uk, "uk_sources.vrt")
        if not os.path.exists(raw_path):
            raw_path = os.path.join(uk, "uk.vrt")
        build_full(uk, raw_path, args.feather, args.j)
    else:
        if not args.bbox or not args.output:
            ap.error("--bbox and --output are required unless --full is used")
        preview(uk, args.bbox, args.output)


if __name__ == "__main__":
    main()
