#!/usr/bin/env python3
"""Fill the small holes and thin edge notches of the survey tifs in SRC_UK_DIR/grid.

A binned point survey leaves one-cell gaps where the sounding lines are wider apart than the
output cell, and the footprint of a survey rarely ends on a sharp line. In uk.vrt those show up
as a lattice of holes a few pixels wide and as thin seams between neighbouring sheets, and a
coarser survey shows through instead of the newer one. This fills such holes from their own
surroundings and leaves alone both the real gaps and the outside of the survey.

Usage: uk_fill_holes.py SRC_UK_DIR [-j N] [--half-width 3] [--sigma 2] [--passes 3]
                              [--report]

Every NaN cell within --half-width pixels of valid data is filled when valid data lies within the
same distance on both sides of one axis: a lattice of one-cell gaps chained to the outer
background is one huge component, yet each such gap is 1 px wide and surrounded by data, while a
wide gap keeps its edge. A diagonal chain of such gaps needs its neighbours filled first, so a
few passes run per tif (the count falls fast: 3.2M, 1.3M, 0.23M on the UK set). The values come
from a normalized Gaussian average of the valid neighbours; valid pixels are never changed. Tifs
are rewritten atomically (a <tif>.tmp.tif then a replace); a rerun only closes a few cells left
at concave corners.
"""

import argparse, itertools, math, os, sys
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import rasterio
from scipy import ndimage


def shifted_any(valid, r, axis, sign):
    """OR of the valid mask shifted by 1..r cells along axis: data within r cells on that side."""
    out = np.zeros_like(valid)
    n = valid.shape[axis]
    for k in range(1, min(r, n - 1) + 1):
        dst = [slice(None)] * valid.ndim
        src = [slice(None)] * valid.ndim
        if sign < 0:  # data k cells before the cell
            dst[axis], src[axis] = slice(k, n), slice(0, n - k)
        else:         # data k cells after the cell
            dst[axis], src[axis] = slice(0, n - k), slice(k, n)
        out[tuple(dst)] |= valid[tuple(src)]
    return out


def both_sides(valid, axis, radius):
    """True where valid data lies within radius cells on both sides along axis."""
    n = valid.shape[axis]
    shape = [1] * valid.ndim
    shape[axis] = n
    pos = np.arange(n).reshape(shape)
    before = np.maximum.accumulate(np.where(valid, pos, -1), axis=axis)
    after = np.flip(np.minimum.accumulate(np.flip(np.where(valid, pos, n), axis=axis), axis=axis), axis=axis)
    return (before >= 0) & (after < n) & ((pos - before) <= radius) & ((after - pos) <= radius)


def grid_survey(a, sigma, valid, fill):
    """Resample a survey over its own area with a Gaussian of sigma cells.

    A systematic survey is run in parallel lines, and the strips between the lines were never
    measured: the lines carry the crests of the sand waves and the strips the mean level, so
    at the grid of the mosaic the survey shows its line pattern as a regular structure. A low
    pass over the survey merges the lines into one surface. Only the cells of the survey (its
    data and the strips between its lines) are written, so the neighbouring surveys are not
    touched.
    """
    w = ndimage.gaussian_filter(valid.astype(np.float32), sigma)
    v = ndimage.gaussian_filter(np.where(valid, a, 0).astype(np.float32), sigma)
    est = np.divide(v, w, out=np.full(a.shape, np.nan, np.float32), where=w > 0)
    out = a.copy()
    cells = valid | fill
    out[cells] = est[cells]
    # the intertidal is measured: the drying heights and the shallow banks keep their value,
    # the resampling would flatten them
    keep = cells & (a > -1.0)
    out[keep] = a[keep]
    return out


def fill_one(path, half_width, closed_width, sigma, passes, write, wide_width=40.0, grid_sigma=4.0):
    """Returns (path, filled pixels, NaN pixels before); filled is -1 when the tif has failed."""
    try:
        with rasterio.open(path) as d:
            a = d.read(1)
            prof = d.profile
            # the cells that were interpolated in an earlier run keep their mark
            previous = d.read(2) > 0.5 if d.count > 1 else None
        nan = ~np.isfinite(a)
        if not nan.any():
            return path, 0, 0
        nan0 = int(nan.sum())
        valid0 = ~nan.copy()
        r = int(math.ceil(half_width))
        filled = 0
        a = a.copy()
        mask = previous.copy() if previous is not None else np.zeros(a.shape, dtype=bool)
        # each pass closes the narrow holes of the current surface; a second pass is needed where
        # a diagonal chain of one-cell gaps leaves no data on both sides until its neighbours are
        # filled, and the count falls fast (3.2M, 1.3M, 0.23M on the UK set), so a few passes
        # settle it without eating into the wide gaps
        for _ in range(passes):
            dist = ndimage.distance_transform_edt(nan)  # distance to the nearest valid cell
            # narrow: a NaN cell within half_width of valid data that also has data within the
            # same distance on both sides of one axis. A one-cell lattice or a stripe between
            # sounding lines qualifies, the fringe along a wide gap or the outside of the survey
            # footprint does not. The distance is per cell, not per component: such a lattice is
            # one huge NaN component, yet every gap in it is one cell wide
            valid = ~nan
            thin_x = shifted_any(valid, r, 1, -1) & shifted_any(valid, r, 1, 1)
            thin_y = shifted_any(valid, r, 0, -1) & shifted_any(valid, r, 0, 1)
            gap = nan & (dist <= half_width) & (thin_x | thin_y)
            n = int(gap.sum())
            if not n:
                break
            # a smooth estimate from the valid neighbours: the Gaussian sums of the values and of
            # the valid mask, so the cells of the hole see only the data around it
            w = ndimage.gaussian_filter(valid.astype(np.float32), sigma)
            v = ndimage.gaussian_filter(np.where(valid, a, 0).astype(np.float32), sigma)
            est = np.divide(v, w, out=np.zeros_like(v), where=w > 0)
            ok = gap & np.isfinite(est)
            a[ok] = est[ok]
            nan[ok] = False
            mask |= ok
            filled += int(ok.sum())
        # A systematic survey is run in parallel swaths and the strips between them were never
        # measured: the swaths carry the crests of the sand waves and the strips the mean
        # level, so at the grid of the mosaic the survey shows its swath pattern as a regular
        # structure. Such a survey is resampled over its own area (grid_survey), which merges
        # the swaths into one surface, and every cell of it counts as interpolated: no depth
        # point is placed on it. A survey with little missing data keeps its values: only its
        # thin gaps are closed.
        # the coverage is the measured cells against the area the survey reaches: a systematic
        # survey covers a small part of it, an ordinary survey nearly all of it
        dist = ndimage.distance_transform_edt(~valid0)
        footprint = dist <= wide_width
        coverage = valid0.sum() / max(1, footprint.sum())
        wide = nan & (both_sides(valid0, 1, int(wide_width)) | both_sides(valid0, 0, int(wide_width)))
        if coverage < 0.25 and wide.any():
            # the whole area of the survey is resampled, the data and the closed strips alike,
            # so that the swaths and the strips become one surface
            a = grid_survey(a, grid_sigma, valid0, np.isfinite(a) | wide)
            nan = ~np.isfinite(a)
            mask = np.isfinite(a) | wide
            filled += int(wide.sum())
        # a closed hole wider than the thin gaps (a gap between two swaths): filled by a Laplace
        # interpolation as long as it lies inside the footprint and is not wider than closed_width
        if closed_width > half_width and nan.any():
            lab, n = ndimage.label(nan)
            if n:
                dist = ndimage.distance_transform_edt(nan)
                wmax = np.zeros(n + 1)
                wmax[1:] = ndimage.labeled_comprehension(dist, lab, np.arange(1, n + 1),
                                                         np.max, np.float64, 0.0)
                edge = np.unique(np.concatenate((lab[0, :], lab[-1, :], lab[:, 0], lab[:, -1])))
                wmax[edge[edge > 0]] = np.inf  # touches the raster border: part of the background
                take = np.nonzero(wmax <= closed_width)[0]
                take = take[take > 0]  # label 0 is the data itself, not a hole
                objs = ndimage.find_objects(lab)
                for k in take:
                    sl = objs[k - 1] if k - 1 < len(objs) else None
                    if sl is None:
                        continue
                    hgt = sl[0].stop - sl[0].start
                    wid = sl[1].stop - sl[1].start
                    # a thin crack that crosses the whole raster is left to the thin passes: a
                    # Laplace fill of such a bounding box would take forever and change nothing
                    if hgt * wid > 200_000 or max(hgt, wid) > 400:
                        continue
                    r0, r1 = max(0, sl[0].start - 1), min(a.shape[0], sl[0].stop + 1)
                    c0, c1 = max(0, sl[1].start - 1), min(a.shape[1], sl[1].stop + 1)
                    if r0 >= r1 or c0 >= c1:
                        continue
                    sub = a[r0:r1, c0:c1]
                    hole = lab[r0:r1, c0:c1] == k
                    valid = np.isfinite(sub)  # other holes in the bbox must not feed the kernel
                    if not valid.any() or not hole.any():
                        continue
                    # the Gaussian sigma grows with the hole, so a 20 px gap is bridged smoothly
                    sig = max(sigma, closed_width / 2.0)
                    w = ndimage.gaussian_filter(valid.astype(np.float32), sig)
                    v = ndimage.gaussian_filter(np.where(valid, sub, 0).astype(np.float32), sig)
                    est = np.divide(v, w, out=np.zeros_like(v), where=w > 0)
                    put = hole & np.isfinite(est)
                    sub[put] = est[put]
                    mask[r0:r1, c0:c1] |= put
                    filled += int(put.sum())
                if len(take):
                    nan = np.isnan(a)
        bad = int((valid0 & ~np.isfinite(a)).sum())
        if bad:
            print(f"FAILED {os.path.basename(path)}: {bad} valid cells would turn NaN, "
                  f"nothing written", flush=True)
            return path, -1, nan0
        if write:
            tmp = path + ".tmp.tif"
            prof.update(count=2, dtype="float32", nodata=np.nan)
            with rasterio.open(tmp, "w", **prof) as dst:
                dst.write(a, 1)
                # the second band marks the cells whose value is interpolated
                dst.write(mask.astype(np.float32), 2)
            os.replace(tmp, path)
    except Exception as e:
        print(f"FAILED {os.path.basename(path)}: {type(e).__name__} {e}", flush=True)
        return path, -1, 0
    return path, filled, nan0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("uk")
    ap.add_argument("-j", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    ap.add_argument("--half-width", type=float, default=3.0,
                    help="greatest distance from a filled cell to valid data, pixels (default 3)")
    ap.add_argument("--closed-width", type=float, default=10.0,
                    help="closed holes up to this half width are interpolated too, pixels "
                         "(default 10; 0 keeps only the thin gaps)")
    ap.add_argument("--sigma", type=float, default=2.0,
                    help="Gaussian sigma of the interpolation, pixels (default 2)")
    ap.add_argument("--passes", type=int, default=3,
                    help="fill passes per tif: a diagonal chain needs its neighbours filled first "
                         "(default 3)")
    ap.add_argument("--wide-width", type=float, default=40.0,
                    help="the strips of a systematic survey are closed up to this width, cells "
                         "(default 40)")
    ap.add_argument("--grid-sigma", type=float, default=4.0,
                    help="the systematic surveys are resampled over their own area with a "
                         "Gaussian of this sigma, cells (default 4)")
    ap.add_argument("--report", action="store_true", help="count only, rewrite nothing")
    args = ap.parse_args()
    grid = os.path.join(args.uk, "grid")
    if not os.path.isdir(grid):
        sys.exit(f"{grid} does not exist")
    files = sorted(os.path.join(grid, n) for n in os.listdir(grid) if n.endswith(".tif"))
    print(f"{len(files)} tifs, half width {args.half_width} px, closed width "
          f"{args.closed_width} px, sigma {args.sigma}, {args.passes} passes"
          f"{', report only' if args.report else ''}", flush=True)
    n = len(files)
    done = touched = px = nanpx = failed = 0
    with ProcessPoolExecutor(max_workers=max(1, args.j)) as ex:
        it = ex.map(fill_one, files, itertools.repeat(args.half_width),
                    itertools.repeat(args.closed_width), itertools.repeat(args.sigma),
                    itertools.repeat(args.passes), itertools.repeat(not args.report),
                    itertools.repeat(args.wide_width), itertools.repeat(args.grid_sigma))
        for i, (path, filled, total) in enumerate(it, 1):
            if filled < 0:
                failed += 1
            elif filled:
                touched += 1
                px += filled
                print(f"ok {os.path.basename(path)[:70]} {filled} px", flush=True)
            nanpx += max(total, 0)
            if i % 500 == 0:
                print(f"[{i}/{n}] {touched} tifs, {px} px filled", flush=True)
    print(f"done: {touched} tifs, {px} px filled of {nanpx} NaN, {failed} failed", flush=True)


if __name__ == "__main__":
    main()
