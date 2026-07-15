package net.osmand.server.osmgpx;

import java.util.Set;

import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.MapUtils;

final class GarbageClassifier {

	static final String GARBAGE = "garbage";
	static final String SHORT = "garbage_short";
	static final String SPARSE = "garbage_sparse";
	static final String TELEPORT = "garbage_teleport";
	static final Set<String> TYPES = Set.of(GARBAGE, SHORT, SPARSE, TELEPORT);

	private static final int MIN_POINTS = 10;
	private static final int MIN_DISTANCE = 200;
	private static final double GAP_MAX_SPEED_KMH = 1200; // above any airliner: crossing a gap faster = teleport = garbage
	private static final int TELEPORT_GAP_MIN_DISTANCE = 1000; // skip teleport check unless the largest gap exceeds this (m)
	private static final long MIN_SPEED_INTERVAL_MS = 500; // min elapsed time to trust a speed sample

	private GarbageClassifier() {
	}

	static boolean isGarbage(String activity) {
		return activity != null && activity.startsWith(GARBAGE);
	}

	static String classify(GpxFile gpxFile, GpxTrackAnalysis analysis) {
		if (gpxFile.getAllSegmentsPoints().size() < MIN_POINTS) {
			return SPARSE;
		}
		if (analysis.getTotalDistance() < MIN_DISTANCE) {
			return SHORT;
		}
		if (analysis.getMaxDistanceBetweenPoints() > TELEPORT_GAP_MIN_DISTANCE && hasTeleportGap(gpxFile)) {
			return TELEPORT;
		}
		return null;
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
