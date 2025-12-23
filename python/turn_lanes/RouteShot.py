from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, Field


class ManeuverType(str, Enum):
    C = "C"
    TL = "TL"
    TSLL = "TSLL"
    TSHL = "TSHL"
    TR = "TR"
    TSLR = "TSLR"
    TSHR = "TSHR"
    KL = "KL"
    KR = "KR"
    TU = "TU"
    TRU = "TRU"
    OFFR = "OFFR"
    RNDB = "RNDB"
    RNLB = "RNLB"


MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT: List[ManeuverType] = [
    ManeuverType.TU,
    ManeuverType.TSHL,
    ManeuverType.TL,
    ManeuverType.TSLL,
    ManeuverType.C,
    ManeuverType.TSLR,
    ManeuverType.TR,
    ManeuverType.TSHR,
    ManeuverType.TRU,
]


class LatLon(BaseModel):
    lat: float = Field(..., description=("Latitude in WGS84 degrees. Used for route context points (start/end) and for per-shot geometry: "
                                         "junction anchor, segment endpoints."), )
    lon: float = Field(..., description="Longitude in WGS84 degrees. ",)


class LaneItem(BaseModel):
    primary: str = Field(..., description="Primary allowed direction/arrow label for this lane (canonicalized).", )
    secondary: str = Field(default="", description="Optional secondary allowed direction/arrow label for this lane. Empty string when absent.", )
    tertiary: str = Field(default="", description="Optional tertiary allowed direction/arrow label for this lane. Empty string when absent.", )


class RouteSegmentGeometry(BaseModel):
    is_forward: bool = Field(..., description=("True when traversing the underlying road polyline in the forward (increasing point-index) direction; "
                                               "False when traversing it in reverse."), )

    distance_m: Optional[float] = Field(default=0.0, description="Segment distance in meters (per-segment metric).", )
    speed_mps: Optional[float] = Field(default=0.0, description="Segment speed in meters per second (per-segment metric).", )
    time_s: Optional[float] = Field(default=0.0, description="Segment time in seconds (per-segment metric).", )

    bearing_begin_deg: Optional[float] = Field(default=0.0,
                                               description="Local bearing (degrees) near the start of the traversed slice. Used to compute relative exit angles.", )
    bearing_end_deg: Optional[float] = Field(default=0.0,
        description=("Local bearing (degrees) near the end of the traversed slice (near the junction for approach segments). "
                     "Used with outgoing bearing_begin_deg to compute turn_deviation_deg."), )

    # Road attributes (OSM-derived)
    highway: Optional[str] = Field(default=None, description="OSM 'highway' tag value for this road.", )
    oneway: Optional[int] = Field(default=None, description="OSM 'oneway' tag value (commonly 0/1/-1).", )
    name: Optional[str] = Field(default=None, description="OSM road name.", )

    # Approach-only usually
    turn_lanes_tag_count: Optional[Union[int, str]] = Field(default=None, description="Count of tags in 'turn_lanes_tag' string.", )
    turn_lanes_tag: Optional[str] = Field(default=None,
                                          description="OSM 'turn:lanes' string on the approach road with '|' as separator. This is an input feature and not the label.", )


class AttachedRoadInfo(BaseModel):
    attached_angle_deg: Optional[float] = Field(...,
                                                description="Relative deviation angle (degrees) from the approach direction for this attached road candidate.", )
    lanes_count: int = Field(default=0, description="Number of lanes attributed to this attached road in the split model.", )
    attached_on_right: bool = Field(default=False, description="True when this attached road is on the right side of the approach direction.", )
    synthetic_turn_value: int = Field(default=0, description="Synthetic TurnType integer value used for split/keep modeling.", )

    turn_lanes_string: str = Field(default="", description="Turn lanes string associated with this attached road candidate in the split model.", )


class IntersectionSummary(BaseModel):
    keep_left: bool = Field(default=False, description="High-level instruction derived from fork/split topology: keep left.", )
    keep_right: bool = Field(default=False, description="High-level instruction derived from fork/split topology: keep right.", )

    roads_on_left: int = Field(default=0, description="Count of candidate branches on the left side of the approach direction.", )
    roads_on_right: int = Field(default=0, description="Count of candidate branches on the right side of the approach direction.", )

    left_lanes: int = Field(default=0, description="Lane capacity attributed to the left side branches.", )
    right_lanes: int = Field(default=0, description="Lane capacity attributed to the right side branches.", )

    left_max_prio: int = Field(default=0, description="Maximum priority among left-side attached roads.", )
    right_max_prio: int = Field(default=0, description="Maximum priority among right-side attached roads.", )

    attached_roads_left: List[AttachedRoadInfo] = Field(default_factory=list,
        description="Attached road candidates classified to the left side of the approach direction.", )
    attached_roads_right: List[AttachedRoadInfo] = Field(default_factory=list,
        description="Attached road candidates classified to the right side of the approach direction.", )


class AttachedRouteCandidate(RouteSegmentGeometry):
    attached_angle_deg: Optional[float] = Field(..., description="Relative deviation (degrees) from the approach direction for this candidate exit.", )
    is_selected: bool = Field(default=False, description="True for the actual outgoing route choice (selected exit) among attached routes.", )


class AlternativesGeometry(BaseModel):
    attached_routes: List[AttachedRouteCandidate] = Field(default_factory=list,
        description=("Explicit adjacency list of candidate exit routes at the junction, each with relative angle, geometry and road tags. "
                     "Used to derive number of exits, left/right/straight classification, and which exit is selected."), )

class Maneuver(BaseModel):
    turn_angle: Optional[float] = Field(default=None,
                                        description="Turn angle when non-zero. Can be omitted/null when not provided.", )
    roundabout_exit: Union[int, str] = Field(default="",
                                             description="Roundabout exit number if roundabout; otherwise empty string.", )



class Segment(BaseModel):
    segment_id: int = Field(..., description="Stable segment identifier.", )
    lanes_string: str = Field(default="", description=("Canonical lane guidance label string for the maneuver with '|' as separator and '+' as marker for active lanes. Typically the same training label as 'lanes'. "
                                                       "This is what the model should predict mainly."), )
    lanes: List[LaneItem] = Field(default_factory=list,
                                  description="Normalized lane guidance list (left-to-right). 'active' marks recommended lanes for the selected turn_type.", )
    turn_type: ManeuverType = Field(
        ...,
        description=(
            "Primary maneuver code (decision anchor). This is what the model should predict. Supported values: "
            "C (continue/straight), TL (turn left), TSLL (turn slightly left), TSHL (turn sharply left), "
            "TR (turn right), TSLR (turn slightly right), TSHR (turn sharply right), KL (keep left), KR (keep right), "
            "TU (U-turn), TRU (right U-turn), OFFR (off route), RNDB (round-about), RNLB (round-about left). "
            "For consistent left-to-right ordering use MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT."
        ),
    )
    maneuver: Maneuver = Field(...,
                               description="Maneuver attached to the approach segment.", )

    junction: Optional[LatLon] = Field(default=None,
                                       description="Junction point for the shot. In current data generation this is typically approach_segment.end_point.", )

    turn_deviation_deg: Optional[float] = Field(...,
                                                description="Signed angular difference (degrees) between approach direction at the junction and selected outgoing direction.", )

    approach_segment: RouteSegmentGeometry = Field(...,
                                                   description="Incoming route segment carrying the maneuver (decision anchor).", )
    outgoing_segment: Optional[RouteSegmentGeometry] = Field(default=None,
                                                             description="Selected outgoing route segment after the maneuver.", )

    intersection: IntersectionSummary = Field(default_factory=IntersectionSummary,
                                              description="Optional fork/split summary (RoadSplitStructure-derived). Present/meaningful when split modeling is available.", )

    alternatives_geometry: AlternativesGeometry = Field(default_factory=AlternativesGeometry,
                                                        description="Alternatives gathered at the junction (attached routes adjacency list), including the selected exit and other candidates.", )


class TestItem(BaseModel):
    id: str = Field(..., description="Stable identifier for the test item (often derived from testName). Used to group multiple junction shots.", )

    start_point: Optional[LatLon] = Field(default=None, description="Route start point (dataset context).", )
    end_point: Optional[LatLon] = Field(default=None, description="Route end point (dataset context).", )

    segments: List[Segment] = Field(default_factory=list,
                                    description="List of junction shots for this route. Typically one element for each maneuver anchor (each segment where turnType != null).", )
