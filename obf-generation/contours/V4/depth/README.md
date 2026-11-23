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
`Gulf_of_Mexico_north-west_contours` (NOAA CUDEM 1/3" near the coast over GEBCO_2026, with the NOAA ENC charts
replacing the grid where they exist; `-i` takes several grids, comma-separated, a later one wins where it has data).

`Norway_contours` and `New-zealand_contours` are built from vectors, not a grid: `build_depth_charts.sh` (also called
for `Norway_contours`) takes the charted contours (`dybdekurve`) of Kartverket "Sjøkart - Dybdedata", or of LINZ
hydrographic data merged into the same layers by `depth_bands_merge.py`, as they are and thins its soundings
(`dybdepunkt`) per zoom tier, keeping the shallowest one of every cell. `Europe_contours` and `Europe_points` leave
the Kartverket coverage (`datakvalitet`, `-x`/`-X`) out, so the fjords do not get two sets of lines.

The Gulf map also reads the NOAA ENC cells (`src/noaa_enc/ENC_ROOT`, S-57, usage bands 4, 5 and 6) when the folder
exists: `depth_enc_osm.py` turns `DEPCNT`, `SOUNDG`, `DEPARE`/`DRGARE` and the `M_COVR` coverage into charted
contours, soundings and areas, the most detailed band winning. The ENC coverage goes to `EXCLUDE`, so the GEBCO/CUDEM
contours, soundings and areas are not drawn inside it: the charts replace the grid there. The charted depths are in
meters (S-57), so the OBF holds `depth`/`name` in meters as well; the non-integer levels (1.8, 3.6, 5.4, 9.1, 18.2,
45.7 m ...) are the 6, 12, 18, 30, 60, 150 ft lines of the US charts.

- Contours: levels `-l` (2, 5, 10, 20, 30, 50, 100, 200, 500 m and every 1000 m by default; GEBCO from 10 m).
  Every grid-built map uses one unsmoothed grid and unsimplified geometry for all levels by default, so contours cannot
  cross at a change of grid and the 2/5/10 m lines stay exactly on their fill boundaries. `World_contours` is the
  exception: it keeps 4x4 smoothing and drops rings under 8 cells, because an unsmoothed 15" world section is 3.1 GB,
  over the 2 GB limit of one map index. `-s` optionally enables one
  common smoothed grid for every level; `-S` sets a common simplification tolerance, and `-g` filters short rings.
  Long ways are split at the 2000-node OSM limit. Vector chart contours and areas also keep every vertex by default.
- The EMODnet base of a regional map stops at the midpoint between its land and the land of the neighbouring
  countries (`uk_emodnet_fill.py --foreign`): the UK map must not cover the French coast with EMODnet, that ground
  belongs to the French and the Europe maps.
- Points `-p "SPACING:ZOOMS ..."`: the shallowest water depth of every SPACING degree cell, placed where it is (as
  on charts, not a regular grid); every tier is its own map section shown from its zooms (OsmAndMapCreator
  `--map-zooms`).
- A region larger than `-t` degrees is split into tiles built `-j` at a time (each tile's Java takes up to 2 GB);
  the OBFs of contours, point tiers and tiles are merged with `merge-index`.
- `World_contours` is too large for one map section even with the smoothing: its contour tiles are split into
  `CONTOUR_PARTS=4` groups of columns, each group becomes a map section of its own. The parts are built two at a
  time (`SECTION_HEAP=-Xmx6g` each, so at most 12 GB of heap), which keeps the machine from saturating, and are
  merged with `merge-index` at the end.
- Overview `-w CELL:ZOOMS` (World, Europe, Gulf: `0.02:5-8`): the contours start at zoom 9, so the levels of 200 m
  and deeper are traced again on the grid averaged to CELL degrees as a map section of their own for the lower zooms.
- Long segments: a straight segment of a contour or of a depth band is kept short, at most `MAX_SEGMENT`
  kilometres (50 by default). The converter (`ogr2osm.py --max-segment=50`) and the band writer
  (`depth_areas_osm.py --max-segment=50`) insert points on the segment, so the shape does not change, but the
  renderer no longer walks one segment over many tiles - the very long segments of the old world map made it slow
  to draw. Every build passes the limit: `build_depth_region.sh` (contours and ENC areas), `build_depth_charts.sh`,
  `build_depth_great_lakes.sh` and `build_depth_swiss_lakes.sh`.
- `-k` skips a region whose `NAME.depth.obf` exists.

### The Great Lakes

`build_depth_great_lakes.sh` builds one map of the Great Lakes out of the NOAA NCEI "Great Lakes Bathymetry"
collection: the charted contours of Erie, Michigan, Huron and Ontario (ESRI shapefiles, the depth in metres in
the `DEPTH` field) and the 3 arc-second grids of all five lakes. The soundings come from the grids: the negative
values are the depths below the low water datum of the lake, the positive ones are the land above it.

The contours and the fill areas are traced from the **same grid** by default (`CONTOURS=grid`): the charted
contours are a different, more detailed model, and a fill cut from the grid does not lie on them. On Lake Ontario
at z15 the charted 5 m and 10 m lines were 41 m and 65 m (median) away from the boundaries of the grid bands.
`CONTOURS=vector` keeps the charted lines, but then the fills do not match them; it is only for comparison. The
depth areas, the isobaths and the soundings of the default build all come from one grid. The rebuilt map
(173 MB, 8 sections, 5.71 M soundings, 2026-11-19) has 0.0 m median distance between the 2/5/10 m lines and
the band boundaries at the test point; the charted-line build is kept as
`preview/Great_Lakes_contours_vector.depth.obf`.

```
TIERS="0.05:9-10 0.02:11-12 0.01:13 0.005:14 0.0025:15 0.001:16-22" \
  ./build_depth_great_lakes.sh -D /data/depth -c /opt/OsmAndMapCreator -j 8
```

- The sources live in `src/great_lakes/`: `contours/<lake>/*.shp` and `grids/<lake>_lld/<lake>_lld.tif`
  (GeoTIFF, NAD83, Float32, NoData -9999). The result is `build/Great_Lakes_contours.depth.obf`: a map
  section of the contours, one of the depth areas (the bands 0-2, 2-5 and 5-10 m, as the other depth maps
  fill them) and one per tier of soundings.
- The bands are cut by the water outline the map draws (`osm_lake_outline.py`, cached in
  `src/great_lakes/water.gpkg`, passed as `depth_areas_osm.py --water`): a band traced from the grid ends on
  the edge of a 90 m cell, which is a staircase on the shore, and lies on the land. The outline puts the fill
  on the shoreline of the map instead. The same outline clips the isobaths (`ogr2ogr -clipsrc` before
  `ogr2osm.py`) and the soundings (`depth_points_osm.py --water`: the water is rasterized to the grid and
  only a water cell gives a point). A grid cell on the shore may hold a depth where the map has land, and
  that is where the points and the lines on the islands came from. Checked on the rebuilt map: in the
  Victoria Island and Thunder Bay windows 0 of 237 and 0 of 132 soundings lie outside the water, and the few
  contour and band vertices that overshoot stay within 1.1 m of the shore - the coordinate rounding of the OBF.
- The isobaths and the bands are traced from the same grid (`CONTOURS=grid`, the default): a band boundary is
  the same 2/5/10 m line the map draws, so the fill ends exactly on the isobath. The charted NOAA contours of
  Erie, Michigan, Huron and Ontario are a more detailed but different model; with `CONTOURS=vector` the fills
  (which still come from the grid) do not match them. This is the general rule for every depth map: one model
  gives both the lines and the fills; the regional maps already contour their areas from the same
  `CONTOUR_RASTER` as their lines.
- The class of a band face is not taken from the contour levels alone. A large deep face in a cell touches the
  outer rings of several shoals (the 2 m ring of one, the 5 m ring of another), and the levels used to put the
  whole face into the 2-5 m band, a rectangle the size of a band cell in the deep water. `depth_bands_from_contours.py`
  samples the raster on an 8x8 grid of the face's minimum rotated rectangle: if the median depth is outside the
  level class by more than 1 m, the class comes from the depth (both ways: a 2-5 m face with a 0.3 m median becomes
  0-2 m, and water deeper than 10 m gets no band). The same check is used by the Swiss lakes. The bands are then
  written with `depth_areas_osm.py --cumulative` (class 5 covers everything shallower than 10 m, class 2 everything
  shallower than 5 m), so a class has no hole at the inner isobath.
- An open contour that stops short of the OSM shore is extended to it: `depth_contours_filter.py --water WATER
  --lake NAME --snap 100` (the Swiss build uses `CONTOUR_SNAP=100`). Without it the 2/5 m line ended 15-60 m from
  the shore, the face went round its end and covered two bands.
- Every open line end is stretched a centimetre past its last vertex (`extend_line_ends` in
  `depth_bands_from_contours.py`): `intersection()` leaves the clipped end a few 1e-16 deg off the water
  outline, the exact predicates do not cover it, and the polygonizer demotes the whole shore-to-shore chain
  to a cut edge, so the face is not split and its band is lost (a 17 km2 face with a -60 m median swallowed
  the 2-5 m band at the Bodensee control point 9.615401/47.506687). A transversal crossing nodes reliably.
- Every face is cut by the data coverage before it is classified (`data_coverage`): a survey gap (a bay the
  source tiles do not cover, a hole between tiles) has no contour lines across it, so one face would merge
  shallow and deep water and paint its band colour over water that holds no data (Zürichsee Frauenwinkel,
  the seam at 47.232155/8.814527). Each surveyed piece is classified on its own and the gaps stay unpainted.
- Full Switzerland rebuild (20.11, run4): 22 lakes, 2 256 band polygons checked, 1 flag (a 0.06 km2 Leman
  shore patch, median -0.1 m); the Bodensee control point 9.615401/47.506687 is covered by the 2-5 m band;
  v2 renders confirm the seam fix (west 5-10 m, east plain water) and the empty Frauenwinkel bay. The dark
  water tint inside the Frauenwinkel reserve is the nature_reserve shader, not a band.
- A face that still spans several classes after the lines are noded (a 2-5 m pocket grown into deep water
  where a cut contour ring does not close) is split along the lines inside it (`--split`, on by default in
  both lake builds through `BAND_SPLIT=1`): dangling ring ends are connected to the nearest point of the
  face boundary and each part is classified on its own.
- Every band polygon is checked at many points of its area after the bands are built (`depth_fill_check.py`, the
  `BAND_CHECK` step of both lake builds): a 12x12 grid in the minimum rotated rectangle, samples on land and within
  the margin of a class boundary skipped, the class of every other sample compared with the polygon. A significant
  defect is reported (default) or fails the build (`BAND_CHECK_STRICT=1`). The check uses the filled raster; it
  also samples the raster water on a grid (`BAND_COVERAGE`, default 200) for points no band covers and pins
  control points (`BAND_REQUIRE="LON LAT CLASS ..."`, the Swiss build pins 9.615401 47.506687 2).
- The gap between the grid's zero line and the OSM shore has no depth: the grid keeps the land as a positive
  height, so its last negative cell is short of the shore and the shallow bands stopped there. `shore_fill.py`
  fills that strip (`SHORE_FILL=1`, only where the raster model is used: `CONTOURS=grid`, and Superior in
  either mode) by solving laplace(h)=0 with the land at 0 m and the grid's negative cells as boundaries. It
  fills only within `SHORE_FILL_LIMIT` of both the land and the data, no farther than `SHORE_FILL_DATA` from
  real data, and only near open water (`SHORE_FILL_OPEN_WIDTH` / `SHORE_FILL_OPEN_DISTANCE`), so narrow river
  beds and deltas are not interpolated. `SHORE_FILL_EDGE=200` (the Great Lakes) fills the strip within 200 m of
  land even where that guard fails: in a channel just narrow enough to fail it the guard left a cell-wide gap
  at the bank, and the shallow bands stopped short of the islands. The Swiss lakes use 50 m: their cells are two
  metres, so a smaller ring is enough. A positive grid cell inside the water
  outline is not land for the fill; treating it as land brings the same gap back, because a 3 arc-second land
  cell can cover water the map draws. For the same reason a cell whose centre falls on a small island keeps its
  negative survey depth in the output: the grid is coarser than the island, the cell still holds water, and
  without the value the cell becomes a NaN hole the size of a cell (the diamond gap at the island). The products
  are cut by the water outline anyway. The filled raster is used for the contours, the areas and the
  soundings; the interpolated cells are marked and `depth_points_osm.py --interp` places no soundings on them.
  The charted vector mode is not filled: its lines are another model.
- Only the standard levels are kept (2, 5, 10, 20, 50, 100, 200 m): the metre steps of the source have no
  `contourtype` of their own, so they would be drawn as an untyped line.
- `gdal_contour -fl` takes the levels from the deepest to the shallowest; any other order silently stops at
  the last level that followed it (`-fl -2 -5 -10` traces only the 2 m line). The script sorts them.
- The zoom range of a tier is a range (`15-22`), not `15+`.
- The lakes are not the sea: they are cut out of the world maps by the mask of the inland waters
  (`mask/inland_water.gpkg`), so the map overlaps nothing.

### The Swiss lakes

`build_depth_swiss_lakes.sh` builds one map of the Swiss lakes out of the swisstopo swissBATHY3D height model
(ESRI ASCII tiles, 1-3 m). The model is the height of the lake bed above sea level (LN02), so a lake becomes
depth only after its water level is subtracted; the levels are the long-term mean levels in the script.

```
./build_depth_swiss_lakes.sh -D /data/depth -c /opt/OsmAndMapCreator -j 8
# one lake for a check: LAKES="zugersee" ./build_depth_swiss_lakes.sh ...
```

- The tiles are unzipped to `src/swiss_lakes/asc/<lake>/*.asc` (no `.prj`: the product is CHLV95, so the
  warp states `-s_srs EPSG:2056`; the nodata value differs between the lakes, GDAL takes it from the header).
- Working grid 0.00002 degrees (about 2 m); the contours use the standard levels (2, 5, 10, 20, 50, 100,
  200 m - only those a lake reaches), the areas are the bands 0-2, 2-5 and 5-10 m, written cumulatively
  (`depth_areas_osm.py --cumulative`: class 5 covers everything shallower than 10 m, class 2 everything
  shallower than 5 m, so no class has a hole at its inner isobath), the soundings come from the same grid
  in six tiers.
- `AREA_SIMPLIFY` defaults to 0 as well: an area simplified by a few cells no longer follows the same
  vertices as its contour and the fill visibly drifts off the line at z20. Only set it to speed up a test
  build.
- The land of the grid is no data, and GDAL counts no data as shallow: `gdal_calc` turns it into -0.01
  (`numpy.where(A < 0, A, -0.01)`), so the water part of a shore cell stays in the 0-2 m band and the band,
  cut by the water outline, reaches the shore. A deep sentinel (-1000) falls into the 10+ class, which is not
  written, and leaves a cell-wide hole in the 5-10 m band at every island.
- The shore is still the edge of a two-metre cell, so the bands are cut by the water outline the map draws:
  `osm_lake_outline.py` fetches it from OSM (Nominatim, one request per lake, cached in
  `src/swiss_lakes/water.gpkg`) and `depth_areas_osm.py --water` clips the bands with it. Without the clip
  the shoreline of the fill is a staircase and lies on the land.
- The survey tiles stop at the shore cell, so a strip between the last depth and the OSM outline had no depth
  and the shallow bands stopped short of the land. `shore_fill.py` fills it with the same Laplace solve
  (`SHORE_FILL=1`, defaults `SHORE_FILL_LIMIT=500`, `SHORE_FILL_DATA=300`, `SHORE_FILL_OPEN_WIDTH=50`,
  `SHORE_FILL_OPEN_DISTANCE=100`, `SHORE_FILL_EDGE=50`): the land is the complement of the lake polygon, the
  islands are its holes
  and therefore land, and the fill stops within 500 m of the land and 300 m of real data. The open-water
  guard keeps it out of narrow river beds and deltas. The filled raster is used for the contours, the areas
  and the soundings; the interpolated cells are marked and `depth_points_osm.py --interp` puts no soundings
  there. `SHORE_FILL=0` restores the gaps. The full build of 2026-11-19 has 4.88 M filled cells, 10,868
  contours, 5,412 areas and 90,718 soundings (7.1 MB, 8 sections, 51 min).
- The largest depths of the map match the published ones (Geneva 310 m, Constance 251, Thun 217, Lucerne
  215, Zug 198, Zurich 136, Neuchatel 150, Brienz 257, Maggiore 301 in the Swiss part).
- No overlap with the world or Europe maps: GEBCO has no bathymetry of these lakes, so the inland-water mask
  does not list them and there is nothing to cut.
- Attribution: swisstopo (see `LICENSES.md`).

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
coverage of the detailed regions cut out of the tiles' OSM (`-N`). The map of the whole grid,
`Europe_full_coverage_contours` and `World_full_coverage_contours`, is not published and is built only with
`FULL_COVERAGE=1` in the environment - it doubles the map sections of the two largest regions. A regional map is meant to be used together with
`Europe_contours`: where a region has no data of its own (Brest, outside the SHOM zones), EMODnet fills in.

The published maps are one map each, split into files where a map index would be too big: `World_contours` and
`World_*_hemisphere_points` are the world, `Europe_contours` and `Europe_points` are Europe. **No two of them
cover the same water**: the world leaves out the EMODnet coverage of Europe and the coverage of every regional map,
Europe leaves out the regional maps, and every regional map leaves out the coverage of its neighbours. The
coverage of a detailed map is the area of *its own data* (`src/coverage/NAME.gpkg`), not the footprint of the
source: the INFOMAR grids reach far west of Ireland, where the Ireland map has no data, and that water belongs
to Europe (EMODnet), not to Ireland.

- `depth_coverage.py SOURCE... OUT.gpkg`: where a source has data - a grid reduced to 0.001 degree cells and
  polygonized, or vector layers (`--layer`, Kartverket `datakvalitet`, LINZ `area_1..5`), shrunk by a cell. Cached in
  `DIR/src/coverage/NAME.gpkg`, rebuilt when a source changes. `--below 0` keeps only water in a bathymetric grid that
  also holds land heights (GEBCO). `--max-extent DEG` skips a vector feature bigger than that: the LINZ depth area
  layer has one polygon around the globe, and it is broken. `--radius DEG` lets the lines and the points of a vector
  layer reach that far into the coverage, so that a wider map leaves them out as well.
  The source keeps the frame of every chart: the longitudes west of the antimeridian come as 180+, so the geometries
  are moved into -180..180 before the bbox is applied. Without it a third of the New Zealand chart data (the whole
  western Pacific: Samoa, Tonga, Fiji, the Cook islands) falls out of the coverage.
  `--hull CELL` replaces the data with the convex hull of every connected component of it (the gaps inside a
  continuous area are closed, remote islands keep their own hull). `--complement` gives the rest of the bbox
  instead (needs `--bbox`); build it with `--shrink 0`, so that it meets the coverage exactly.
  `--exclude FILE.gpkg` cuts the areas of a vector file out of the coverage (the inland water).
- `depth_inland_water.py GRID OUT.gpkg`: the inland water of a bathymetric grid, the water that is not connected
  with the ocean. GEBCO keeps the bottom of the lakes and the depressions of the dry land below the sea level, and a
  depth map built from it draws them as if they were the sea: the Caspian Sea, Lake Baikal, the parts of the Great
  Lakes with a bottom in GEBCO, the Dead Sea, Lake Eyre, the Turpan Depression. Only the water connected with the
  ocean is the sea, the rest is masked out as land (`mask/inland_water.gpkg`, used by the world maps through
  `LAND_EXTRA`).
  The components of the water are counted on a coarse grid (`--cell 0.01`, the mean of the cell, so that a narrow
  strait with land in it stays closed): the ones that touch the border of the world are the ocean. Every other
  component is checked again on the fine grid of the source: the water that reaches the border of its own box is
  connected with the sea through a strait (the Black Sea through the Bosporus), the water that does not reach it is
  inland.
- `depth_water_mask.py LAND OUT.gpkg --area SOURCE.gpkg [--layer NAME ...]`: the land polygons without the lakes
  that have charted depths. The OSM land polygons cover the inland lakes, so the soundings of a lake would be
  dropped as if they were on land and its depth areas cut away; this mask takes the charted depth areas out of the
  land. The New Zealand map is built with it (`src/nz/land_without_lakes.gpkg`).
- `maps_overview.py VIEW_JSON OUT.png`: the area of the data of the maps on an OSM background, or
  `--auto DIR` for one PNG per map; `--maps NAME,...` selects maps and `--frame W S E N` sets the view.
- `depth_osm_exclude.py IN.osm.gz OUT.osm.gz --exclude COVERAGE.gpkg...`: soundings inside dropped, contours cut at
  the coverage edge.
- `depth_exclude_check.py BEFORE_DIR AFTER_DIR --exclude ...`: nothing left inside, nothing lost outside.

`Europe_contours` leaves out Ireland, France, the Great Britain surveys, the Netherlands grids and Kartverket;
`Europe_points` is the same map with the points, so it leaves out the same. `World_contours` leaves out the EMODnet
coverage only within the `Europe_contours` bbox (`emodnet_europe`, -31.3 25.4 36 71.2; its overview too, Europe has
its own), the same regions, the Pacific map of New Zealand and the whole Gulf box (the Gulf map is GEBCO with the
CUDEM on top, so it covers its box). The rest of the EMODnet coverage (Cape Verde, the Atlantic
west of 31 W) is not covered by any other map, so it stays in the world on GEBCO instead of becoming a hole.

The New Zealand map is the whole LINZ hydrographic data: New Zealand with its lakes, the Chatham and the Kermadec
islands, the Ross Sea, Samoa, Tonga, Fiji and the Cook islands. Its box crosses the antimeridian, so the merge of
the scale bands, the land mask and the coverage walk over both halves of it. The LINZ data keeps the frame of every
chart (the western half of the Pacific comes as 180+), so the tools move the longitudes into -180..180: without it
the western Pacific is missing from the map and from its coverage.

The coverage of the New Zealand map (`new-zealand.gpkg`) is the convex hull of every connected component of its
data, the same rule that cuts the base of the detailed maps of Europe (`depth_emodnet_base.py`): the gaps inside the
chart data are closed, and the remote islands do not drag the empty water between them. It holds not only the depth
areas, but the contours and the soundings as well (`--radius 0.005`): the deep contours of the Ross Sea have no depth
area around them, and a sounding outside of every area would be drawn twice - by the New Zealand map and by the
world. The hulls are computed in a continuous longitude space (the parts west of the antimeridian are moved east
first) and cut at the antimeridian afterwards, so the chart data of the Pacific is one area and not two halves.

The world leaves the whole hull out, so the water inside it that the charts do not cover is filled by the New Zealand
map itself with GEBCO (`New-zealand_fill-east` and `New-zealand_fill-west`, cut at the antimeridian): the hull has no
empty gaps, and no water is left without a map. The fill covers the hull minus the chart data (the same
`new-zealand-charts` coverage with `--radius 0.005`), it is built as two extra map sections and merged into the chart
map (`merge-index`). The hull depends only on the chart data, not on the fill, so the world is built once and is not
rebuilt after the fill: it already leaves the whole hull out.
