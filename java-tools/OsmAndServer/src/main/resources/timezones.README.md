# timezones.bin.gz

The time zone polygons used by `net.osmand.server.utils.TimezoneMapper` (the time zone of a place: the opening hours
status of a POI, conditional tags in routing).

- **Source:** [timezone-boundary-builder](https://github.com/evansiroky/timezone-boundary-builder) release **2026f**,
  file `timezones-with-oceans.geojson.zip`
  (sha256 `838b3c9161e09ee944edb64cc4d9e4d2eb3ad0a3ec3d8d6a1520a5f8f6e21ac9`). Data © OpenStreetMap contributors, ODbL.
- **Built with:** `net.osmand.server.utils.TimezoneMapGenerator`, simplification tolerance **0.001°** (about 110 m),
  JTS `TopologyPreservingSimplifier` per polygon.
- **Result:** 444 zones, 1356 polygons, 947,471 points, 3.3 MB; about 29 MB of heap once loaded, ~20 µs per lookup.

Only the borders come from this file. The clock rules (DST, offset changes) come from the tzdata of the JVM that runs the
server, so keep the JVM up to date. A zone newer than the JVM's tzdata is skipped and its area gets the neighbouring
zone.

Use the full `timezones-with-oceans` file, not the `-now` or `-1970` variants: those merge zones with the same clock
under one representative name by the rules of the release, and a JVM with older tzdata then gets the wrong clock for
them (with 2026f, Vancouver became America/Phoenix and Morocco Africa/Abidjan).

## How to rebuild

```sh
cd java-tools
mkdir -p /tmp/tz
./gradlew :OsmAndServer:compileJava
# the runtime classpath of OsmAndServer (JTS and Jackson are needed)
echo "allprojects { tasks.register('printCp') { doLast { if (project.name == 'OsmAndServer') println project.sourceSets.main.runtimeClasspath.asPath } } }" > /tmp/tz/cp.gradle
CP=$(./gradlew -q -I /tmp/tz/cp.gradle :OsmAndServer:printCp)

gh release download <tag> -R evansiroky/timezone-boundary-builder -p 'timezones-with-oceans.geojson.zip' -D /tmp/tz
java -Xmx6g -cp "$CP" net.osmand.server.utils.TimezoneMapGenerator /tmp/tz/timezones-with-oceans.geojson.zip \
    OsmAndServer/src/main/resources/timezones.bin.gz 0.001 \
    "timezone-boundary-builder <tag>, timezones-with-oceans, simplified 0.001 deg"
```

Then run `TimezoneMapperTest` (it checks every GeoNames city of 15000+ people) and update this file: release,
sha256, the numbers printed by the generator.

## File format

gzip of: int magic `OTZ1`, UTF source label, int zone count, UTF zone ids, int polygon count, then per polygon:
short zone index, varint ring count (outer ring first, then holes), per ring varint point count and zigzag varint
deltas of latitude, longitude in 1e-5 degrees.
