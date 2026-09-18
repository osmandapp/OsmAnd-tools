
Used in combination with configuration project
- https://github.com/osmandapp/web-server-config (Provides content for Folder website/ on server)

Frontend is provided by
- https://github.com/osmandapp/web (maps/ and web/ built and copied to website/)

## Crash reports

`POST /api/crash-report?platform=android&version=5.4.5&osversion=14` with the report as the body (zip of tombstones and exception.log as `application/octet-stream` or multipart part `file`, max 50 MB).
`CrashReportController` stores one file per report in `CRASH_REPORTS_LOCATION` (the endpoint is disabled if it is not set), with no database and no personal data.
Copying to data.osmand.net, cleanup schedules (cron.d) and server setup: `servers/crash-reports/README.md` in web-server-config.

## Depth test tile server (this branch only, OsmAnd-Issues#3341)

`DepthTestMaps` registers the styles `depth-marine`, `depth-default` and `depth-nautical` with the vector tile
styles at start. They render the depth OBFs of the builder (`builder.osmand.net/depth-data/build/`) with the styles
of the running jar, so a new depth build or a new style can be looked at before anything is published. The maps are
downloaded into the temp folder (three at a time) and downloaded again when their size on the builder changed;
`/tile/depth-test/clear-cache` drops the rendered tiles and looks for new maps. `_full_coverage_` maps are left out:
they are `Europe_contours` and `World_contours` without the detailed regions cut out.

### Running it locally

    cd java-tools && ./gradlew :OsmAndServer:bootJar -x test -q     # needs ../../android and ../../resources
    java -Xmx8g -jar OsmAndServer/build/libs/osmand-server-boot-master-snapshot.jar \
        --server.port=8096 --osmand.files.location=<run dir> \
        --spring.datasource.url=jdbc:postgresql://localhost:5999/none \
        --spring.jpa.hibernate.ddl-auto=none \
        --spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false \
        --spring.jpa.properties.hibernate.boot.allow_jdbc_metadata_access=false

with the environment:

- `OBF_LOCATION` - folder of the base maps: a `*basemap*` OBF and the regions of the area to look at, plus
  `regions.ocbf` beside it. The depth styles close the `*.depth.obf` of this folder and open their own set instead.
- `NATIVE_LIBRARY_PATH` - the rendering library **file**, e.g. `core-legacy/binaries/darwin/arm64/libosmand.dylib`
  (without it every tile answers "Tile rendering engine is not initialized").
- `DEPTH_TEST_MAPS_DIR` - optional: a folder with `maps/NAME.depth.obf` built locally. Nothing is then downloaded or
  refreshed from the builder, so a locally built OBF stays; missing regions are simply not shown.
- `<run dir>` needs `web-server-config/sku-subscriptions.json` (copy it from the web-server-config repository).

`-Xmx8g`: the whole published set is about 2.6 GB of OBF, and a metatile of the World map does not fit in 3 GB.

Tiles are then at `http://127.0.0.1:8096/tile/depth-nautical/{z}/{x}/{y}.png` (512 px tiles, so a Leaflet layer uses
`tileSize: 512, zoomOffset: -1`). The styles switch the depth rendering on themselves - do not add
`?depthContours=true` to the URL, the server appends it and the extra parameter drops most of the drawing.

To compare two builds, run two servers on different ports, each with its own `DEPTH_TEST_MAPS_DIR`, and put two
Leaflet maps side by side. The map files are the only difference then, and a version in the tile URL
(`?v=...`) keeps the browser from showing the tiles of the previous build.

### Building a depth map to test

The generation scripts live on master, `obf-generation/contours/V4/depth` (see their own README). A region of one's
own, without the whole planet:

    BBOX="-97.6 25.8 -93.5 30.2" bash build_depth_region.sh \
        -D <data dir> -n Gulf_of_Mexico_north-west_contours -o <out dir> -c <OsmAndMapCreator> -j 4

`<data dir>` is the folder of `download_all.sh`; a VRT whose `SourceFilename`s are `/vsicurl/` URLs of the builder
reads the grids over the network instead of downloading them.
