package net.osmand.server.api.searchtest;

import net.osmand.data.Building;
import net.osmand.data.LatLon;
import net.osmand.data.Street;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.util.MapUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SpatialResultActuator extends ResultActuator {
	protected final long osmId;

	public SpatialResultActuator(LatLon targetPoint, Map<String, Object> statMetrics, long osmId) {
		super(targetPoint, statMetrics);
		this.osmId = osmId;
		metrics.put("oid", osmId);
	}
	
	protected static final int DIST_PRECISE_THRESHOLD_M = 20;
	// the number interpolated on the line can be up to 150 m from the address point ("1831-1847" for 1833)
	protected static final int DIST_INTERPOLATION_THRESHOLD_M = 150;
	
	protected Result findActualResult(List<SearchResult> searchResults) throws IOException {
		// The first result that is the target: the same object or one at the point (deduplication may keep the id of the
		// building). Categories take no place; a street from the way with the same number is not the address.
		int resPlace = 0;
		for (SearchResult sr : searchResults) {
			if (sr.objectType == ObjectType.POI_TYPE) {
				continue;
			}
			resPlace++;
			if (!(sr.object instanceof Street) && osmId(sr) == osmId) {
				return new Result(ResultType.ById, resPlace, sr);
			}
			int threshold = sr.object instanceof Building b && b.getLatLon2() != null ? DIST_INTERPOLATION_THRESHOLD_M
					: DIST_PRECISE_THRESHOLD_M;
			if (sr.location != null && MapUtils.getDistance(sr.location, targetPoint) < threshold) {
				return new Result(ResultType.ByDist, resPlace, sr);
			}
		}
		return null;
	}
}
