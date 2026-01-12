from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, Field


class ManeuverType(str, Enum):
    RNLB = "RNLB"
    TU = "TU"
    TSHL = "TSHL"
    TL = "TL"
    TSLL = "TSLL"
    KL = "KL"
    C = "C"
    KR = "KR"
    TSLR = "TSLR"
    TR = "TR"
    TSHR = "TSHR"
    TRU = "TRU"
    RNDB = "RNDB"
    OFFR = "OFFR"


MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT: List[ManeuverType] = [
    ManeuverType.RNLB,
    ManeuverType.TU,
    ManeuverType.TSHL,
    ManeuverType.TL,
    ManeuverType.TSLL,
    ManeuverType.KL,
    ManeuverType.C,
    ManeuverType.KR,
    ManeuverType.TSLR,
    ManeuverType.TR,
    ManeuverType.TSHR,
    ManeuverType.TRU,
    ManeuverType.RNDB,
    ManeuverType.OFFR
]


class LatLon(BaseModel):
    lat: float = Field(..., description=("Latitude in WGS84 degrees. Used for route context points (start/end) and for per-shot geometry: "
                                         "junction anchor, segment endpoints."), )
    lon: float = Field(..., description="Longitude in WGS84 degrees. ",)


class LaneItem(BaseModel):
    active: bool = Field(..., description="True when this lane is active.", )
    primary: str = Field(..., description="Primary allowed direction/arrow label for this lane (canonicalized).", )
    secondary: str = Field(default="", description="Optional secondary allowed direction/arrow label for this lane. Empty string when absent.", )
    tertiary: str = Field(default="", description="Optional tertiary allowed direction/arrow label for this lane. Empty string when absent.", )


class Segment(BaseModel):
    segment_id: int = Field(..., description="Stable segment identifier.", )
    lanes: List[LaneItem] = Field(default_factory=list,
                                  description="Normalized lanes (left-to-right) should be treated as predefined structure.", )
    turn_type: ManeuverType = Field(
        ...,
        description=(
            "Primary maneuver code (decision anchor). This is what the model should predict also. Supported values: "
            "C (continue/straight), TL (turn left), TSLL (turn slightly left), TSHL (turn sharply left), "
            "TR (turn right), TSLR (turn slightly right), TSHR (turn sharply right), KL (keep left), KR (keep right), "
            "TU (U-turn), TRU (right U-turn), OFFR (off route), RNDB (round-about), RNLB (round-about left). "
            "For consistent left-to-right ordering use MANEUVER_TYPE_ORDER_LEFT_TO_RIGHT."
        ),
    )
    junction: Optional[LatLon] = Field(default=None,
                                       description="Junction point for the end of the segment.", )

    distance_to_next_point_m: Optional[float] = Field(default=0.0, description="Distance to next maneuver point in meters (per-segment).", )


class TestItem(BaseModel):
    id: str = Field(..., description="Stable identifier for the test item.", )

    start_point: Optional[LatLon] = Field(default=None, description="Route start point.", )
    end_point: Optional[LatLon] = Field(default=None, description="Route end point.", )

    segments: List[Segment] = Field(default_factory=list,
                                    description="List of segments for this route. "
                                    "Each segment represents a maneuver anchor which is started at the point with segment_id label on the image and completed at reaching the next point.", )
