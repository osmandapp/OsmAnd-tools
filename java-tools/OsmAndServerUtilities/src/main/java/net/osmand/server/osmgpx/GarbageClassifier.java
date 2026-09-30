package net.osmand.server.osmgpx;

import java.util.Set;

import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.MapUtils;

public final class GarbageClassifier {

	public static final String GARBAGE = "garbage";
	public static final String SHORT = "garbage_short";
	public static final String SPARSE = "garbage_sparse";
	public static final String TELEPORT = "garbage_teleport";
	// The "garbage" category and all its concrete subtypes.
	public static final Set<String> TYPES = Set.of(GARBAGE, SHORT, SPARSE, TELEPORT);

	private static final double GAP_MAX_SPEED_KMH = 1200; // above any airliner: crossing a gap faster = teleport
	private static final long MIN_SPEED_INTERVAL_MS = 500; // min elapsed time to trust a speed sample

	private GarbageClassifier() {
	}

	public static boolean isGarbage(String activity) {
		return activity != null && activity.startsWith(GARBAGE);
	}

	static boolean hasTeleportGap(GpxFile gpxFile) {
		for (Track track : gpxFile.getTracks(false)) {
			for (TrkSegment seg : track.getSegments()) {
				WptPt prev = null;
				for (WptPt p : seg.getPoints()) {
					if (prev != null) {
						long dtMs = p.getTime() - prev.getTime();
						if (dtMs >= MIN_SPEED_INTERVAL_MS) {
							double dist = MapUtils.getDistance(prev.getLat(), prev.getLon(), p.getLat(), p.getLon());
							if (dist * 3600d / dtMs > GAP_MAX_SPEED_KMH) { // m/ms -> km/h
								return true;
							}
						}
					}
					prev = p;
				}
			}
		}
		return false;
	}
}
