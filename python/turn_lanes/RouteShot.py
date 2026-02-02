from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class ManeuverType(str, Enum):
    # Keep in mind this ordering to use a 1D scale with ManeuverType.C as a reference point "zero":
    # - Items closer to C (e.g. KL, KR) mean smaller deviation.
    # - Items farther from C (e.g. TL/TR, TSHL/TSHR) mean larger/steeper deviation.
    # The farther a maneuver is from C in this list, the larger its "difference" (deviation) from going straight.
    # Examples: KL is close to C (slight deviation) while TL/TSHL are farther (steeper left turn).
    # Similarly on the right side: KR is close to C, then TSLR, TR, and TSHR as deviation increases.
    # TL/TR and TSHL/TSHR is very close to each other, and sometimes it is hard to tell the difference.
    # There are some special maneuvers that are not geometric (e.g. U-turns, roundabouts, off-route, unknown):
    # - TU and TRU represent near-180° direction changes and should be treated as extreme deviations from C.
    # - RNLB/RNDB represent roundabout-type maneuvers; their placement is primarily for consistency, not strict turning angle.
    # - OFFR (off route) is a special non-geometric maneuver; it is intentionally placed at an extreme as a large mismatch.
    # - NONE is a temporarily unknown maneuver as placeholder which can be substituted by any suitable maneuver.
    RNLB = "RNLB"  # round-about left
    TU = "TU"  # U-turn
    TSHL = "TSHL"  # turn sharply left
    TL = "TL"  # turn left
    TSLL = "TSLL"  # turn slightly left
    KL = "KL"  # keep left
    C = "C"  # continue/straight as reference point
    KR = "KR"  # keep right
    TSLR = "TSLR"  # turn slightly right
    TR = "TR"  # turn right
    TSHR = "TSHR"  # turn sharply right
    TRU = "TRU"  # right U-turn
    RNDB = "RNDB"  # round about
    OFFR = "OFFR"  # off route
    NONE = "NONE"  # unknown maneuver


class LatLon(BaseModel):
    lat: float = Field(..., description=("Latitude in WGS84 degrees. Used for route context points (start/end) and for per-shot geometry: "
                                         "junction anchor, segment endpoints."), )
    lon: float = Field(..., description="Longitude in WGS84 degrees. ", )


class LaneItem(BaseModel):
    active: bool = Field(..., description="True when this lane is active.", )
    primary: ManeuverType = Field(..., description="Primary allowed maneuver code for this lane (canonicalized).", )
    secondary: ManeuverType = Field(default="", description="Optional secondary allowed maneuver code for this lane.", )
    tertiary: ManeuverType = Field(default="", description="Optional tertiary allowed maneuver code for this lane.", )


class Segment(BaseModel):
    point: int = Field(..., description="Stable point identifier.", )
    lanes: List[LaneItem] = Field(description="Normalized lanes (left-to-right) should be treated as ground-truth (except 'NONE').")
    turn_type: ManeuverType = Field(..., description=("Primary fixed maneuver code which should be treated as predefined decision anchor. Supported values: "
                                                      "C (continue/straight), TL (turn left), TSLL (turn slightly left), TSHL (turn sharply left), "
                                                      "TR (turn right), TSLR (turn slightly right), TSHR (turn sharply right), KL (keep left), KR (keep right), "
                                                      "TU (U-turn), TRU (right U-turn), OFFR (off route), RNDB (round-about), RNLB (round-about left). "), )
    distance_to_next_point_m: Optional[float] = Field(..., description="Distance to next maneuver point in meters (per-segment).", )


class TestItem(BaseModel):
    start_point: Optional[LatLon] = Field(default=None, description="Route start point.", )
    end_point: Optional[LatLon] = Field(default=None, description="Route end point.", )

    segments: List[Segment] = Field(default_factory=list,
                                    description="List of segments for this route. Each segment represents a maneuver anchor which is started at the point label on the image and completed at reaching the next point.", )
