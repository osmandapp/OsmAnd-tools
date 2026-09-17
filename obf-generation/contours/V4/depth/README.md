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

Licences of all sources, used and rejected: [LICENSES.md](LICENSES.md).

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

## Building depth OBFs of a region

`build_depth_region.sh` cuts a region out of a grid and builds depth contours and depth points from it, clipped by
the land mask, as `.osm.gz` and, with `-c OsmAndMapCreator`, one `NAME.depth.obf`. The grid may be a `/vsicurl/` URL,
only the region is read.

```
./build_depth_region.sh -D /data/depth -n Netherlands_contours -c /opt/OsmAndMapCreator -j 16   # a region from the script
./build_depth_region.sh -n Wadden -b "4.6 52.8 6.5 53.6" -i /data/depth/src/emodnet/emodnet_2024.vrt \
    -m /data/depth/mask/land_polygons.gpkg -o /data/depth/build -p "0.01:11-12 0.005:13-"         # any box
```

Regions (bounds of the published OBFs, grid, levels, point tiers, tile size) are listed in the script:
`Netherlands_contours` (contours and points), `Europe_contours`, `Europe_points` (EMODnet 2024), `World_contours`,
`World_Northern_hemisphere_points`, `World_Southern_hemisphere_points` (GEBCO_2026) and
`Gulf_of_Mexico_north-west_contours` (NOAA CUDEM 1/3" near the coast over GEBCO_2026; `-i` takes several grids,
comma-separated, a later one wins where it has data).

`Norway_contours` is built from vectors, not a grid: `build_depth_kartverket.sh` (also called for that region name)
takes the charted contours (`dybdekurve`) of Kartverket "Sjøkart - Dybdedata" as they are and thins its soundings
(`dybdepunkt`) per zoom tier, keeping the shallowest one of every cell. `Europe_contours` and `Europe_points` leave
the Kartverket coverage (`datakvalitet`, `-x`/`-X`) out, so the fjords do not get two sets of lines.

- Contours: levels `-l` (2, 5, 10, 20, 30, 50, 100, 200, 500 m and every 1000 m by default; GEBCO from 10 m); levels
  from 20 m down are traced on a smoothed grid (`-s`, `-d`), otherwise a flat bottom with sand waves gives hundreds
  of zigzags; short closed rings are dropped.
- Points `-p "SPACING:ZOOMS ..."`: the shallowest water depth of every SPACING degree cell, placed where it is (as
  on charts, not a regular grid); every tier is its own map section shown from its zooms (OsmAndMapCreator
  `--map-zooms`).
- A region larger than `-t` degrees is split into tiles built `-j` at a time (each tile's Java takes up to 2 GB);
  the OBFs of contours, point tiers and tiles are merged with `merge-index`.
- Overview `-w CELL:ZOOMS` (World, Europe, Gulf: `0.02:5-8`): the contours start at zoom 9, so the levels of 200 m
  and deeper are traced again on the grid averaged to CELL degrees as a map section of their own for the lower zooms.
- `-k` skips a region whose `NAME.depth.obf` exists.

### All maps at once

```
./build_depth_all.sh -D /data/depth -c /opt/OsmAndMapCreator -j 4 [-P 6] [-k] Netherlands_contours Ireland_contours \
    France_contours Great_Britain_contours Norway_contours New-zealand_contours Gulf_of_Mexico_north-west_contours \
    Europe_contours Europe_points World_contours World_Northern_hemisphere_points World_Southern_hemisphere_points
```

The regions do not depend on each other, so they run at the same time (`-P` at most), each with `-j` tiles at a time
and a log in `build/logs/REGION.log`; the console gets a start, done or FAILED line (with the end of the log) per
region. Cores and memory grow with the number of regions: every region also runs its map sections in parallel, one
Java each (`JAVA_OPTS`, `-Xmx16g` by default).

### Maps without the detailed regions

A regional map (Netherlands, Ireland, France, Great Britain, Norway, New Zealand, Gulf) shown together with the whole
EMODnet or GEBCO doubles the contours, so the published `Europe_contours` and `World_contours` are the maps with the
coverage of the detailed regions cut out of the tiles' OSM (`-N`), and the whole grid goes beside them as
`Europe_full_coverage_contours` and `World_full_coverage_contours`. A regional map is meant to be used together with
`Europe_contours`: where a region has no data of its own (Brest, outside the SHOM zones), EMODnet fills in.

- `depth_coverage.py SOURCE... OUT.gpkg`: where a source has data - a grid reduced to 0.001 degree cells and
  polygonized, or vector layers (`--layer`, Kartverket `datakvalitet`, LINZ `area_1..5`), shrunk by a cell. Cached in
  `DIR/src/coverage/NAME.gpkg`, rebuilt when a source changes.
- `depth_osm_exclude.py IN.osm.gz OUT.osm.gz --exclude COVERAGE.gpkg...`: soundings inside dropped, contours cut at
  the coverage edge.
- `depth_exclude_check.py BEFORE_DIR AFTER_DIR --exclude ...`: nothing left inside, nothing lost outside.

`Europe_contours` leaves out Ireland, France, the Great Britain surveys, the Netherlands grids and Kartverket.
`World_contours` leaves out EMODnet (its overview too, Europe has its own), the same regions, New Zealand
(165..180 E) and the CUDEM of the Gulf box.
