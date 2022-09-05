import sys
from osgeo import gdal

def set_band_descriptions(filepath, bands):
    """
    filepath: path/virtual path/uri to raster
    bands:    ((band, description), (band, description),...)
    """
    ds = gdal.Open(filepath, gdal.GA_Update)
    for band, desc in bands:
        rb = ds.GetRasterBand(band)
        if rb is not None:
            rb.SetDescription(desc)
    del ds

if __name__ == '__main__':
    filepath = sys.argv[1]
    bands = [int(i) for i in sys.argv[2::2]]
    names = sys.argv[3::2]
    set_band_descriptions(filepath, zip(bands, names))