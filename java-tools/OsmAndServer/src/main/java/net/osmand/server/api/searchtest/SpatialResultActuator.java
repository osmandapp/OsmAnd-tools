package net.osmand.server.api.searchtest;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.ObfConstants;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
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
	
	protected Result findActualResult(List<SearchResult> searchResults) throws IOException {
		// Find closest by distance by id & by tags 
		int resPlace = 1;
		double minDistance = Double.MAX_VALUE;
		for (SearchResult sr : searchResults) {
			if (sr.object instanceof MapObject mo && ObfConstants.getOsmObjectId(mo) == osmId) {
				actualResult = new Result(ResultType.ById, resPlace, sr);
				break;
			} else if (sr.object instanceof BinaryMapDataObject bo && ObfConstants.getOsmObjectId(bo) == osmId) {
				actualResult = new Result(ResultType.ById, resPlace, sr);
				break;
			} else if(sr.location != null) {
				double dist = MapUtils.getDistance(sr.location, targetPoint);
				if (dist < DIST_PRECISE_THRESHOLD_M && dist < minDistance) {
					minDistance = dist;
					actualResult = new Result(ResultType.ByDist, resPlace, sr);
				}
			}
			resPlace++;
		}
		return actualResult;
	}
}
