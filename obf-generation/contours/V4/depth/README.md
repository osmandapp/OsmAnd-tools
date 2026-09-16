# Depth source grids

Download scripts for the grids used to rebuild depth contours and depth points.
Both scripts resume after a dropped connection and check the final size, so just rerun them.

What the published depth OBFs are built from today (checked 2026-09-16 by sampling the grids at contour vertices):

| OBF | Source |
|---|---|
| `World_contours`, `World_*_hemisphere_points` | GEBCO One Minute Grid 2008 (1′) |
| `Europe_contours`, `Europe_points` | EMODnet DTM 2016 (1/8′) |

Current releases: GEBCO_2026 (15″) and EMODnet DTM 2024 (1/16′).

## Everything at once

`download_all.sh -o DIR` fetches the depth sources below into `DIR/src/<source>/` (`--data`) and the OSM land
polygons into `DIR/mask/` (`--mask`); without either flag it fetches both. Archives are unzipped as soon as they
are complete and deleted, an empty `<archive>.done` marks them, so a rerun resumes and skips what is done.
Jenkins: `SRTM_DownloadDepthSources` (parameters DOWNLOAD_DATA, DOWNLOAD_MASK).

```
./download_all.sh -o /data/depth --dry-run                    # sizes only
./download_all.sh -o /data/depth                              # everything
./download_all.sh -o /data/depth --mask                       # land polygons only, a fresh copy
./download_all.sh -o /data/depth --only emodnet,netherlands -j 4
```

`check_sources.sh DIR -j JOBS` checks the result: file counts, every raster opens, CRS, a known sea point is
negative, leftover archives; it also writes a VRT per gridded source (`gebco_2026.vrt`, `gebco_2026_tid.vrt`,
`emodnet_2024.vrt`, `cudem.vrt`). Needs GDAL; exits with 1 when a check fails.

| Source | What | Download (2026-09-16) |
|---|---|---|
| `gebco` | GEBCO_2026 elevation, 15″, 8 GeoTIFF tiles | 4.24 GB (7.5 GB unzipped) |
| `gebco_tid` | GEBCO_2026 type identifier grid | 0.10 GB (3.4 GB unzipped) |
| `emodnet` | EMODnet DTM 2024, 1/16′, 58 tiles | 11.43 GB |
| `noaa_enc` | NOAA ENC, all US charts, S-57 | 0.83 GB |
| `cudem` | NOAA CUDEM 1/3″ topobathy, 373 tiles | 8.25 GB |
| `cudem_ninth` | NOAA CUDEM 1/9″, 930 tiles, disabled in `download_all.sh` for now | 187.38 GB |
| `norway` | Kartverket "Sjøkart - Dybdedata", FGDB, open (Geonorge "no restrictions") | 2.33 GB |
| `netherlands` | Rijkswaterstaat bottom height 20 m 2024, Zeeland, NCP 2019, CC0 | 0.26 GB |
| `mask` | OSM land polygons (coastline only, rebuilt daily), osmdata.openstreetmap.de, ODbL | 0.92 GB |

Not scriptable: Kartverket ENC (sold via PRIMAR), BSH NAUTHIS (WFS download disabled).

## GEBCO

```
./download_gebco.sh -o /data/gebco --unzip                 # GEBCO_2026 elevation, 8 GeoTIFF tiles (~4.2 GB zip, deleted after unzip)
./download_gebco.sh -g tid -o /data/gebco --unzip          # type identifier grid (~92 MB zip)
./download_gebco.sh -y 2026 -f netcdf -j 8 -o /data/gebco  # single netCDF instead of tiles
```

## EMODnet DTM

```
./download_emodnet.sh --list                               # tiles with bounds and zip sizes
./download_emodnet.sh D5 D6 E6 -o /data/emodnet --unzip    # pilot: North Sea, Baltic, Adriatic
./download_emodnet.sh --bbox 3 51 9 56 -o /data/emodnet    # all tiles touching a box (W S E N)
./download_emodnet.sh --all -j 4 -o /data/emodnet          # all 58 tiles, ~11.4 GB zipped GeoTIFF
```

## Building depth contours of a region

`build_depth_region.sh` cuts a region out of a grid, sets land to 0 m by the land mask, contours it, drops short
closed rings, simplifies and writes `NAME.osm.gz` (tags by `../translations/contours_depth.py`) plus `NAME.gpkg`
for a quick look. The grid may be a `/vsicurl/` URL, only the region is read.

```
./build_depth_region.sh -D /data/depth -n Netherlands_contours -c /opt/OsmAndMapCreator -j 16   # a region from the script
./build_depth_region.sh -n Wadden -b "4.6 52.8 6.5 53.6" -i /data/depth/src/emodnet/emodnet_2024.vrt \
    -m /data/depth/mask/land_polygons.gpkg -o /data/depth/build                                  # any box
```

Regions (bounds, grid, levels, tile size) are listed in the script: `Netherlands_contours`, `Europe_contours`
(EMODnet 2024) and `World_contours` (GEBCO_2026, from 10 m down). `-c` adds `NAME.depth.obf` built by
OsmAndMapCreator. A region larger than `-t` degrees is split into tiles built `-j` at a time (each tile's Java
takes up to 2 GB), and their OBFs are merged with `merge-index`, one map section per tile.

Levels default to 2, 5, 10, 20, 30, 50, 100, 200, 500 m and every 1000 m (`-l`); `-u 2` upsamples a coarse grid
(GEBCO) before contouring for smoother lines.
