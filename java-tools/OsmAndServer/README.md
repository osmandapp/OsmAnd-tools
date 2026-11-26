
Used in combination with configuration project
- https://github.com/osmandapp/web-server-config (Provides content for Folder website/ on server)

Frontend is provided by
- https://github.com/osmandapp/web (maps/ and web/ built and copied to website/)

## Crash reports

`POST /api/crash-report?platform=android&version=5.4.5&osversion=14` with the report as the body (zip of tombstones and exception.log as `application/octet-stream` or multipart part `file`, max 50 MB).
`CrashReportController` stores one file per report in `CRASH_REPORTS_LOCATION` (the endpoint is disabled if it is not set), with no database and no personal data.
Copying to data.osmand.net, cleanup schedules (cron.d) and server setup: `servers/crash-reports/README.md` in web-server-config.

## Depth test tile server (this branch only, OsmAnd-Issues#3341)

A depth map is looked at before anything is published: the server renders two sets of depth OBFs with the styles of
the running jar and shows them side by side at **`/tile/depth-test/compare`**.

Three folders:

- `OBF_LOCATION` - the ordinary maps: a `*basemap*` OBF and the regions of the area to look at, plus `regions.ocbf`
  beside them. Their own `*.depth.obf` are closed, the sets below are opened instead.
- `DEPTH_MAPS_DIR` - the depth maps of the builder (`builder.osmand.net/depth-data/build/`). They are downloaded
  into `<dir>/maps` at start, three at a time, and downloaded again when their size on the builder changed;
  `/tile/depth-test/clear-cache` drops the rendered tiles and looks for new ones. Default: a folder in the temp
  directory. The `_full_coverage_` maps are left out: they are `Europe_contours` and `World_contours` without the
  detailed regions cut out. Styles `depth-marine`, `depth-nautical`, `depth-default`.
- `DEPTH_WORK_DIR` - the OBFs built locally, under any file name: pieces of one region built for one place can lie
  beside each other. Nothing is downloaded there. Styles `work-marine`, `work-nautical`, `work-default`; without
  the variable there is only the set of the builder.

### Running it

    cd java-tools && ./gradlew :OsmAndServer:bootJar -x test -q     # needs ../../android and ../../resources
    OBF_LOCATION=<maps> DEPTH_MAPS_DIR=<server maps> DEPTH_WORK_DIR=<work maps> \
    NATIVE_LIBRARY_PATH=<core-legacy>/binaries/darwin/arm64/libosmand.dylib \
    java -Xmx8g -jar OsmAndServer/build/libs/osmand-server-boot-master-snapshot.jar \
        --server.port=8096 --osmand.files.location=<run dir> \
        --spring.datasource.url=jdbc:postgresql://localhost:5999/none \
        --spring.jpa.hibernate.ddl-auto=none \
        --spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false \
        --spring.jpa.properties.hibernate.boot.allow_jdbc_metadata_access=false

`NATIVE_LIBRARY_PATH` is the rendering library **file**; without it every tile answers "Tile rendering engine is not
initialized". `<run dir>` needs `web-server-config/sku-subscriptions.json` (copy it from the web-server-config
repository). `-Xmx8g`: the published set is about 2.6 GB of OBF and a metatile of the World map does not fit in 3 GB.

Then open `http://127.0.0.1:8096/tile/depth-test/compare`. The page has the places to look at and the style switch
on top, the two maps move together, and the address follows the view (`#zoom/lat/lon`), so a place is easy to pass
on. `?v=<anything>` gets past the browser's cache of the tiles of a previous build. The styles are not in
`/tile/styles`: they exist for this page.

Tiles on their own: `http://127.0.0.1:8096/tile/work-nautical/{z}/{x}/{y}.png` (512 px, so a Leaflet layer wants
`tileSize: 512, zoomOffset: -1`). Do not add `?depthContours=true` - the server appends it, and the extra parameter
drops most of the drawing.

### Building a depth map to test

The generation scripts live on master, `obf-generation/contours/V4/depth` (see their own README). A region of one's
own, without the whole planet:

    BBOX="-97.6 25.8 -93.5 30.2" bash build_depth_region.sh \
        -D <data dir> -n Gulf_of_Mexico_north-west_contours -o <out dir> -c <OsmAndMapCreator> -j 4

`<data dir>` is the folder of `download_all.sh`; a VRT whose `SourceFilename`s are `/vsicurl/` URLs of the builder
reads the grids over the network instead of downloading them. Copy the OBF into `DEPTH_WORK_DIR` and it shows up on
the right side of the page.
