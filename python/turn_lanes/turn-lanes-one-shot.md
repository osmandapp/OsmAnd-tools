
# Turn-lanes prediction — “one-shot junction” JSON (topology-first)

This document defines a **topology/graph-oriented** JSON payload for turn-lanes prediction.

Primary use:
- A model receives a **map crop image** centered on a decision point and a **single JSON object** describing that same decision point.
- The model predicts:
  - `maneuver.type`
  - `lanes` (typically `lane_guidance.lanes_string`)

The structure is designed to align *what you see on the map* (junction topology + lane layout) with *what OsmAnd knows* (route segments, turn instruction, lane guidance, attached alternatives).

## 1) Mental model

### 1.1 Route is an ordered walk through the road graph

`routeSegments` is an ordered list (travel order):

```text
routeSegments:  [0] -> [1] -> [2] -> [3] -> ...
                 |      |      |
                 |      |      +-- may have turnType != null (decision anchor)
```

The **order** in `routeSegments` is the “timeline” of navigation.

### 1.2 Each `RouteSegmentResult` is a slice of one road polyline

Each segment references a `RouteDataObject` polyline (`points[0..N]`).
The pair `startPointIndex`/`endPointIndex` selects the traversed slice:

```text
RouteDataObject points: 0---1---2---3---4---5---6---7---8---...

RouteSegmentResult slice:
  startPointIndex=2
  endPointIndex=7

  points: 0---1---[2---3---4---5---6---7]---8---...
                 ^ start               ^ end
```

This makes segments **comparable topologically** without requiring absolute coordinates:
- travel direction (forward/reverse) comes from the index ordering
- bearings represent local direction changes

### 1.3 A “decision point” is a local node topology event

Decision points are where the route must choose (or confirm) a branch:

```text
                      candidate A
                        /
approach (incoming) ---*-----> selected outgoing
                       \
                        \ candidate B
```

This is represented by:
- `maneuver` (`TurnType`) attached to the **approach** segment (`turnType != null`)
- `alternatives_geometry.attached_routes[]` gathered at the junction (topological adjacency)
- optionally: `intersection` (`RoadSplitStructure`) summarizing fork/keep-left/right logic

### 1.4 “Route fingerprint” (geometry-lite)

Even if you don’t use absolute coordinates, a junction can be described by:
- the approach bearing near the junction
- a set of **relative exit angles** for candidates
- which candidate is selected
- a short sequence of bearings/turn deviations around the junction

This is a robust join key between the map crop and the JSON.

## 2) Dataset structure

The dataset file is a list of test items. Each test item contains a list of junction shots.

```text
TestItem
  id / testName
  start_point / end_point
  segments: [JunctionShot, JunctionShot, ...]
```

## 3) Field-by-field target JSON

### 3.1 Top-level: `TestItem`

- `id` (string)
  - Stable identifier (e.g. `te.getTestName().split("\\.")[0]`).
- `testName` (string)
  - Human-readable test name.
- `start_point` ({lat, lon})
  - Route start.
- `end_point` ({lat, lon})
  - Route end.
- `segments` (array of `JunctionShot`)
  - One element for each maneuver anchor (each `RouteSegmentResult` where `turnType != null`).

`start_point`/`end_point` are for dataset context; the model typically focuses on a single `JunctionShot`.

### 3.2 Core unit: `JunctionShot`

#### 3.2.1 Identity and route indexing

- `shot_id` (string)
  - Recommended: `{testId}:{route_segment_index}`.

Indexing (flat form):
- `route_segment_index` (int)
  - Index of the approach segment in `routeSegments`.
- `route_total_segments` (int)
  - Total number of segments in the route.

Indexing (optional nested form):
- `route_context` (object)
  - `route_segment_index` (int)
  - `total_segments` (int)

Neighbor pointers (for local sequence context):
- `incoming_route_segment_index` (int|string)
  - Usually `route_segment_index - 1`, or empty string when absent.
- `outgoing_route_segment_index` (int|string)
  - Usually `route_segment_index + 1`, or empty string when absent.

#### 3.2.2 Junction anchor

- `junction` ({lat, lon})
  - The junction point for the shot.
  - In the current implementation this is typically `approach_segment.end_point`.

Mental model:

```text
approach_segment ends at junction
                     * (junction)
outgoing_segment starts at same point (or nearly)
                     *---->
```

#### 3.2.3 Labels (what you want the model to predict)

- `lanes` (string)
  - Training label for lane guidance (usually `TurnType.lanesToString(turnType.getLanes())`).
  - May be empty when no lane guidance is available.

- `maneuver` (object)
  - `type` (string)
    - Primary maneuver class (`turnType.toXmlString()`), e.g. `TL`, `TR`, `TSLR`, `C`, `KL`, `KR`, `RNDB3`.
  - `value` (int)
    - Raw integer code (`turnType.getValue()`).
  - `skip_to_speak` (bool)
    - `turnType.isSkipToSpeak()`.
  - `turn_angle` (number|string)
    - `turnType.getTurnAngle()` if non-zero, otherwise empty string.
  - `roundabout_exit` (int|string)
    - `turnType.getExitOut()` if roundabout, otherwise empty string.

Notes:
- `maneuver.type = C` still can be a real decision when the junction is a split/keep scenario; rely on topology fields below.

#### 3.2.4 Topological + directional cues

- `turn_deviation_deg` (number|string)
  - Signed angular difference between:
    - approach direction at the junction, and
    - selected outgoing direction after the junction.
  - Typically computed as:
    - `MapUtils.degreesDiff(approach_segment.bearing_end_deg, outgoing_segment.bearing_begin_deg)`

Interpretation:
- Small magnitude means “continue / slight bend”.
- Large magnitude indicates turn/exit.
- Sign is defined by `degreesDiff` and should be used consistently (don’t reinvent sign rules in the dataset).

#### 3.2.5 Approach segment (incoming edge into the node)

- `approach_segment` (object)
  - Represents the route segment that carries `turnType` (decision anchor).
  - Derived from `routeSegments[route_segment_index]`.

Fields:
- `osm_way_id` (number)
  - `ObfConstants.getOsmObjectId(segment.getObject())`.
- `start_point_index` / `end_point_index` (int)
  - Indices into this road’s polyline (`RouteDataObject`), defining the traversed slice.
- `is_forward` (bool)
  - `segment.isForwardDirection()`.
- `distance_m` / `speed_mps` / `time_s` (number)
  - Per-segment metrics.
- `bearing_begin_deg` / `bearing_end_deg` (number)
  - Local direction near start/end of this segment.
- `start_point` / `end_point` ({lat, lon})
  - Segment endpoints.
- `polyline` (array of `{lat, lon}`)
  - A bounded slice of the segment geometry.
- `turn_lanes_tag` (string)
  - Raw OSM `turn:lanes` string on the approach road, if available.
- `highway`, `oneway`, `lanes_tag`, `name`
  - Important OSM attributes often correlated with lane layout.

#### 3.2.6 Outgoing segment (selected exit edge)

- `outgoing_segment` (object)
  - The route segment immediately after the maneuver.
  - Typically `routeSegments[route_segment_index + 1]`.

Same field shape as `approach_segment` (but may omit `turn_lanes_tag` if not available).

Why it matters:
- It disambiguates `maneuver.type` (especially when it is `C`).
- It pins “selected exit” among alternatives.

#### 3.2.7 Lane guidance (normalized view)

- `lane_guidance` (object)
  - `lanes_string` (string)
    - Canonical label string (same as `lanes`, but kept here for grouping).
  - `lanes` (array)
    - Each lane is left-to-right:
      - `index` (int)
      - `active` (bool)
      - `primary` / `secondary` / `tertiary` (string)

Mental model sketch:

```text
Map shows lanes left-to-right:
  [lane0] [lane1] [lane2]

lane_guidance.lanes preserves this order.
"active" marks recommended lanes (the '+' markers in string form).
```

#### 3.2.8 Intersection summary (optional fork modeling)

- `intersection` (object)
  - Present when `turnType.getRoadSplitStructure() != null`.
  - Summarizes topology without enumerating full geometry.

Fields:
- `keep_left`, `keep_right`, `speak` (bool)
- `roads_on_left`, `roads_on_right` (int)
- `left_lanes`, `right_lanes` (int)
- `left_max_prio`, `right_max_prio` (int)
- `attached_roads_left`, `attached_roads_right` (array)

Each attached road item:
- `attached_angle_deg` (number)
  - Relative deviation angle from the approach direction.
- `attached_on_right` (bool)
- `lanes_count` (int)
- `speak_priority` (int)
- `synthetic_turn_value` (int)
- `turn_lanes_string` (string)

Interpretation sketch:

```text
approach -> junction -> candidates split into left/right sets

roads_on_left / roads_on_right: counts of candidate branches on each side
left_lanes / right_lanes: lane capacity attributed to each side
keep_left/keep_right: the high-level instruction derived from this topology
```

#### 3.2.9 Alternatives geometry (explicit adjacency list at the node)

- `alternatives_geometry` (object)
  - `attached_routes` (array)

Each attached route item:
- `osm_way_id` (number)
- `start_point_index`, `end_point_index`, `is_forward` (segment slice identity)
- `bearing_begin_deg` / `bearing_end_deg` (number)
Mental sketch:
  approach_segment                outgoing_segment
  (bearing_end)                   (bearing_begin)
----->  * junction  ----->
turn_deviation_deg = degreesDiff(approach.bearing_end, outgoing.bearing_begin)

- `attached_angle_deg` (number|string)
  - Relative deviation from the approach direction.
- `is_selected` (bool)
  - True for the actual outgoing route choice.
- `polyline` (array of `{lat, lon}`)
  - Short bounded geometry near the junction.
- `highway`, `oneway`, `lanes_tag`, `name`
  - Road attributes for candidate classification.

Topological statement you can derive:
- **how many exits** exist
- which are **left/right/straight** relative to the approach direction
- which exit is **selected**

## 5) Joining JSON with map image (practical)

Recommended overlay items:
- `approach_segment.polyline`
- the selected outgoing candidate polyline (`is_selected=true`)
- all other `attached_routes` polylines

What the model should visually recover from the image:
- number of exits + their relative placement
- lane markings and turn arrows
- whether exits are “distinct roads” vs a bend in the same road

What JSON provides as supervision:
- which exit is selected (`is_selected`)
- maneuver class (`maneuver.type`)
- lane label (`lanes` / `lane_guidance.lanes_string`)

