Local Valhalla router
=====================

Used to compare OsmAnd turn lanes with Valhalla on the same OSM data, without the rate limits of the public
FOSSGIS server (valhalla1.openstreetmap.de).

Valhalla comes from the pyvalhalla wheel (prebuilt executables for macOS arm64 and Linux), so nothing is
compiled. Requires python3 >= 3.9.


1. Install (once)

    python3 build_valhalla.py --dir ../valhalla/

   Creates <dir>/venv with pyvalhalla. --version 3.9.0 pins a version, otherwise the latest is installed.
   Without --dir it installs into valhalla/ next to the tools repo (e.g. ~/OsmAnd/valhalla).


2. Start the server for a region

    python3 start_server.py /path/to/region.osm.pbf [--dir ../valhalla/] [--port 8002]

   The .pbf is yours to supply (e.g. from download.geofabrik.de, or the one the .obf was generated from).
   The first start builds admin polygons and routing tiles into <dir>/data/<pbf name>/ (Prague: ~1 min,
   54 MB); later starts reuse them until the .pbf changes. --rebuild forces a new build.
   Build output goes to <dir>/data/<pbf name>/build.log. Ctrl+C stops the server.

    python3 start_server.py --clean /path/to/region.osm.pbf

   removes <dir>/data/<pbf name>/ (tiles and admins) and exits. The .pbf file is not touched and does not
   have to exist any more: only its name is used.

   The driving side comes from the country boundary inside the .pbf: an extract cut without it routes
   as right-hand traffic, which matters for Australia, UK, etc.


3. Query lanes

   Lanes are only returned in OSRM format:

    curl -s localhost:8002/route -d '{"locations":[{"lat":50.0755,"lon":14.4378},{"lat":50.0880,"lon":14.4208}],
                                      "costing":"auto","format":"osrm","banner_instructions":true}'

   routes[0].legs[].steps[].intersections[].lanes[]:
     indications       arrows of the lane: left, slight left, straight, right, uturn, none, ...
     valid             the maneuver can be made from this lane
     active            recommended lane (OsmAnd "+")
     valid_indication  which arrow of the lane is used (OsmAnd primary turn)
   Other costings: bus, taxi, truck, bicycle.
