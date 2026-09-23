Turn lanes: OsmAnd vs Valhalla
==============================

Compares the turn lanes OsmAnd gives on a region with the lanes of a local Valhalla on the same OSM data.
Cases come from generate-turn-lanes-test (OsmAndMapCreator utilities), so every row is a real drive through a
junction, with OsmAnd's own answer recorded as the expectation.


1. Whole run

    ./auto_test.sh --pbf /path/to/region.osm.pbf [--obf /path/to/region.obf] --name test_name
                   [--junctions=1000] [--none-lanes] [--clear] [--port 8002]

   Paths are relative to the folder holding tools/ (e.g. ~/OsmAnd):
     OsmAndMapCreator/   downloaded from the night build when missing
                         (http://download.osmand.net/latest-night-build/OsmAndMapCreator-main.zip)
     tmp/<name>.obf      generated from the .pbf by `utilities.sh generate-obf` when --obf is not given;
                         reused by the next run (delete it to generate again)
     tmp/<name>.json     the cases, by `utilities.sh generate-turn-lanes-test`
     valhalla/           Valhalla, installed by ../valhalla/build_valhalla.py when missing; tiles are built
                         from the .pbf by ../valhalla/start_server.py and served on --port while the test runs
     tmp/<name>.csv      the result, by compare.sh
     tmp/<name>_valhalla.log

   --junctions=N    junctions to take cases from (default 1000)
   --none-lanes     only drives that read an unpainted lane ("||right", "none|through")
   --clear          rebuild the Valhalla tiles
   --port           Valhalla port, must be free (default 8002)

   The night build is made from master and may not have generate-turn-lanes-test yet; the script stops then.
   Point it to a build that has it:

    MAPCREATOR_DIR=/path/to/OsmAndMapCreator ./auto_test.sh ...

   The .pbf should be the one the .obf was made of (or close to it): Valhalla routes on the .pbf, and a
   different snapshot shows up as differences. The extract needs its country boundary for Valhalla to know the
   driving side.


2. Compare only

    ./compare.sh --json /path/to/test.json --valhalla http://localhost:8002 [--out test.csv]
                 [--turn-tolerance 2] [--all] [--costing auto] [--threads 8]

   Needs a running Valhalla (python3 ../valhalla/start_server.py region.osm.pbf). Writes <json name>.csv
   beside the json unless --out is given.

   Every case is routed by Valhalla in OSRM format (the only one with lanes); the route shape goes to
   /trace_attributes to get the OSM way of every edge. A Valhalla maneuver is keyed like an OsmAnd expectation:
   by the way it turns onto. Lanes on intersections Valhalla drives straight through are kept as "C:lanes" and
   used only when there is no maneuver on that way.

   Valhalla lanes are written the OsmAnd way: "TURN:lane|lane", "+" for an active lane, the used arrow of a
   lane first; a "none" lane with no used arrow becomes C.


3. CSV

    num,start,end,segment,osmand,valhalla,status,obf,link

   segment   the expectation key, way id (":start point" when the way is driven twice)
   link      osmand.net route between the case's points, zoomed on the Valhalla maneuver

   status
     ok        same lanes, turn types at most --turn-tolerance apart in TurnType.orderFromLeftToRight
               (TSLR vs KR is 1); "[MUTE] " is ignored, Valhalla has no such notion
     lanes-ok  same lanes, turn types further apart
     bug       as many lanes, but no active lane in common: TR:TL|TL|+C|C,TR vs TR:TL|TL|C|+TR,C
     diff      different lanes otherwise, e.g. KL:+C|+C|+C|TR vs C:+C|C|C|TR (an active lane in common)
     no-lanes  OsmAnd has lanes, Valhalla only the turn (mostly lanes OsmAnd made up for an untagged road)
     missing   Valhalla has nothing on that way (another route, or none)

   Only expectations with lanes on either side are written, --all writes every one.
