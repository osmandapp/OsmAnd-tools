#!/usr/bin/env python3
"""Mask the interpolated cells out of a depth raster.

    depth_mask_raster.py RASTER OUTPUT MASK...

The cells where a mask raster is not zero become nodata. A mask on the grid of the source
(the whole UK mosaic) is resampled to the grid of the raster with the nearest neighbour, so
the masks of the mosaic and of the coastal fill can be used on a tile of a region.

Used for the depth points: a point is a sounding, so it must not stand on a value that was
interpolated - the gap closed inside a survey, the transition between two surveys or the
coastal fill.
"""
import sys

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject


def main():
    if len(sys.argv) < 4:
        sys.exit("usage: depth_mask_raster.py RASTER OUTPUT MASK...")
    raster, output, masks = sys.argv[1], sys.argv[2], sys.argv[3:]
    with rasterio.open(raster) as ds:
        profile = ds.profile
        data = ds.read(1)
        for path in masks:
            with rasterio.open(path) as ms:
                band = np.zeros(data.shape, dtype=np.float32)
                reproject(source=rasterio.band(ms, 1), destination=band,
                          src_transform=ms.transform, src_crs=ms.crs,
                          dst_transform=ds.transform, dst_crs=ds.crs,
                          resampling=Resampling.nearest, src_nodata=None, dst_nodata=0.0,
                          init_dest_nodata=True)
                data = np.where(band > 0, np.nan, data)
        profile.update(driver="GTiff", dtype="float32", nodata=np.nan,
                       compress="deflate", tiled=True, blockxsize=512, blockysize=512)
        with rasterio.open(output, "w", **profile) as dst:
            dst.write(data.astype("float32"), 1)
    print("masked raster: %s, %d nodata cells" % (output, int(np.isnan(data).sum())))


if __name__ == "__main__":
    main()
