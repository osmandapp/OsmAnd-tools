---
name: coastline-rendering-test
description: Run or debug the coastline rendering test (CoastlineRenderingTester / `utilities.sh test-coastline-rendering`) that compares locally rendered tiles against tile.osmand.net and reports water mask differences, with either the legacy (v1) renderer or the OpenGL core one (v2, `-renderer=opengl`). Use when working on coastline issues, on the Web_Ocean_Tiles_Test Jenkins job, on coastline-tests.json, when comparing the two rendering engines, or when the test is slow, crashes on a map, or reports unexpected failures.
---

# Coastline rendering test

Renders tiles with the legacy native renderer (v1) or with OsmAndCore (v2, `-renderer=opengl`) and
compares the water mask against the reference `https://tile.osmand.net/hd/{z}/{x}/{y}.png`. Exit
code 0 = clean, 2 = problems reproduced, 1 = could not run. Writes `index.html` + `summary.json`
into the output folder, images for failed tiles only.

- Utility: `java-tools/OsmAndMapCreatorUtilities/src/main/java/net/osmand/render/CoastlineRenderingTester.java`
  (all options are documented in its class javadoc - read it before guessing a flag).
- Fixed cases: `.../src/main/resources/net/osmand/render/coastline-tests.json`.
- Build server: Jenkins job `Web_Ocean_Tiles_Test`, maps in `/home/tileserver/maps`.

## Running it

```bash
OsmAndMapCreator/utilities.sh test-coastline-rendering -maps.dir=/home/tileserver/maps
```

A default run is the json cases **plus** 10 000 random tiles - no flag needed for the random part.
`-randomTilesK=N` changes the size in thousands, `-random` runs only that part, `-scan` walks every
tile of a zoom range instead.

## Running it locally

```bash
cd java-tools && ./gradlew :OsmAndMapCreator:buildDistribution   # tools only, never the android project
unzip -q OsmAndMapCreator/build/distributions/OsmAndMapCreator.zip -d /tmp/mc
/tmp/mc/utilities.sh test-coastline-rendering \
  -maps.dir=$HOME/osmand/maps -out=/tmp/report -random -minzoom=1 -maxzoom=7 \
  -native=<repo>/core-legacy/binaries/darwin/arm64/libosmand.dylib
```

`-native` is only needed when the bundled `osmand-<os>-<arch>.lib` is missing from the zip, which
happens with `--offline` because `downloadCoreJni` is skipped. On the build server pass nothing -
the bundled library is used, and `null` is the value that loads it.

## The OpenGL (v2) engine

`-renderer=opengl` renders every tile with OsmAndCore instead of the legacy library - the engine the
apps actually draw with - and changes nothing else: same cases, same water masks, same report, same
exit codes, so the two runs are directly comparable. The report and `summary.json` carry the
`renderer` they were produced with.

```bash
/tmp/mc/utilities.sh test-coastline-rendering -renderer=opengl -maps.dir=$HOME/osmand/maps \
  -out=/tmp/report-gl -eyepiece=<repo>/binaries/<platform>/Release/eyepiece
```

It drives the `eyepiece` tool of core as a co-process through its batch tile mode
(`-tiles=- -tilesOutputDir=...`, tiles as `z/x/y` lines on stdin, one `TILE z/x/y <file>` answer
per tile). Things worth knowing:

- **The binary needs the batch tile mode**, i.e. core with `-tiles=` (OsmAnd-core#1100). The tester
  checks it at start up and stops with an explanation (`-eyepieceCheck=false` skips the check);
  `strings eyepiece | grep tilesOutputDir` answers the same question by hand. The published
  `https://builder.osmand.net/binaries/amd64-linux-clang/eyepiece_standalone` is the build server's
  copy and is the one to use on Jenkins (it is linked with EGL - `EGL_PLATFORM_DEVICE_EXT` - so it
  renders headless, no X server and no `xvfb-run` needed), **but it is rebuilt by
  `OsmAndCoreAndTools-linux-clang-64bit` and lags a core merge until that job runs** - which is
  exactly how the first opengl run of the Jenkins job failed.
- `-eyepiece=` is autodetected from `binaries/` of a repository checkout and from the PATH.
  `-stylesPath=` defaults to the styles built into core, `-eyepieceLog=true` echoes everything
  eyepiece prints.
- **No legacy library, no fonts folder and no `indexes.cache`** are used in this mode - core does
  all of that itself. `-native=` is ignored.
- The set of maps is fixed when the process starts, so a case that opens or closes a map restarts
  eyepiece; the maps are handed over as a folder of symlinks in `<out>/opengl-maps`. Watch the
  `Started eyepiece #N` lines: with `-load=case` there is one per case, which is normal, but a
  restart with a thousand maps loaded costs the whole obf scan again.
- **It is ~20x slower per tile than the legacy renderer** - measured 0.7 s/tile against 25 ms/tile
  (276 fixed case tiles: 193 s against 12 s). A 10 000 tile run is hours of rendering, so on the
  build server keep the OpenGL run to the fixed cases or a small `-randomTilesK`.

Measured on the 276 fixed case tiles (September 2026): v2 fixes #25119 (Goa flooding: 4 failed
tiles against 0) and most of #24376, reproduces #25618 (St. Lawrence, 23 failed tiles) exactly like
v1, and fails one San Francisco tile that v1 draws correctly. The two `seamarksInland` cases fail
identically in both, which is the expected sanity check - they are a map data problem and have
nothing to do with the renderer.

## The one number that explains a slow run

The progress line reports where the wall clock went:

```
... 1742 of 10000 tiles, 10 failed, 4.4 tiles/s, 87% waiting for references, eta 8m
```

- **High "waiting for references"** - bound by `tile.osmand.net`, nothing on this side helps.
  mod_tile throttles per ip with a token bucket (`ModTileThrottlingTiles 10000 1`, i.e. a burst of
  10 000 tiles then 1 tile/s; `ModTileThrottlingRenders 500 0.5` for cache misses). So one run of
  ~10 000 tiles is the practical ceiling, and the bucket needs hours of idle to refill. Random
  tiles are the worst case for it: each one lands in its own metatile, so almost every tile is a
  render request rather than one render serving 64 tiles.
- **Low "waiting for references"** - bound by our own rendering, which is **single threaded**.
  `-threads` only sizes the reference download pool, so changing it will not move this number.
  Roughly 25 ms/tile on a laptop with ~126 maps, ~40 ms on the build server with ~1060.

What actually shortens a run: the reference cache (`coastline-reference` in the run folder, reused
by every run - keep it), and splitting by zoom.

## Splitting a run

`-minzoom`/`-maxzoom` do **not** change which tiles are drawn, they only skip zooms of the same
set, so the parts add up to exactly the whole run:

```
-randomTilesK=10                      10000 tiles, z1..17
-randomTilesK=10 -minzoom=1 -maxzoom=6  2367 tiles      # low zooms are cheap, they are cdn warm
-randomTilesK=10 -minzoom=7            7633 tiles
```

Do not split evenly by zoom number - z1 has 4 tiles in the whole world, so the low half of the zoom
range is a small minority of the tiles. Tiles are rendered from the highest zoom down.

The seed defaults to the calendar month, so the same tiles are checked all month long; pass `-seed`
to pin it.

## Gotchas

- A corrupt obf aborts the run with `Can't read <file>: Corrupt file` - **this is intentional**,
  fix or remove the map, do not make the tester skip it.
- Tiles are rendered from tile aligned 31 bit bounds, the way `VectorMetatile` does it. The lat/lon
  `RenderingImageContext` constructor lands a few units off the tile grid, which is enough to flip
  the ocean/land fill of a tile. Do not "simplify" it back.
- The last column and row of a zoom end at 2^31 and overflow a signed int - the bounds are clamped
  to `Integer.MAX_VALUE`, without which the edge tiles render as blank land.
- Glaciers (`#E4FDFF` here, `#ddecec` in the reference) and `landuse=salt_pond` are excluded from
  the water mask on purpose - they are water coloured but not water.
- `World_seamarks` and `basemap_mini` are excluded from `load=all`; an overlay and a second basemap
  distort the rendering.
- Jenkins serves the report under a CSP that drops inline `<style>`, so the css lives in a separate
  `styles.css`. Do not inline it.
