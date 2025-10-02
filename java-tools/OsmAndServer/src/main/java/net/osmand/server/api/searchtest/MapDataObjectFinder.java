package net.osmand.server.api.searchtest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Building;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.QuadRect;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class MapDataObjectFinder {
	public enum ResultType {
		Best,
		ById,
		ByTag,
		ByDist,
		Error
	}

	public record Result(ResultType type, BinaryMapDataObject exact, int place, SearchResult searchResult) {
		
		@NotNull
		public String toIdString() {
			if (exact != null) {
				return ObfConstants.getOsmObjectId(exact) + "";
			}
			if (searchResult.object instanceof MapObject) {
				MapObject mo = (MapObject) searchResult.object;
				return ObfConstants.getOsmObjectId(mo) + "";
			}
			return "";
		}
		
		public String toPlaceString() {
			return place + " - " + type();
		}
	}

	
	public Result[] find(List<SearchResult> searchResults, LatLon targetPoint, long datasetId, Map<String, Object> row) throws IOException {
		int DIST_THRESHOLD_M = 20;
		double closestDist = DIST_THRESHOLD_M;
		Result firstResult = null, actualResult = null, firstByDist = null, firstByTag = null;
		int resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.objectType != null && ObjectType.LOCATION != sr.objectType && firstResult == null) {
				firstResult = new Result(ResultType.Best, null, resPlace, sr);
				break;
			}
			resPlace++;
		}
		if (firstResult == null) {
			return new Result[] { firstResult, actualResult };
		}

		if (firstResult.searchResult().object instanceof Building b) {
			if (b.getInterpolationInterval() != 0 || b.getInterpolationType() != null) {
				row.put("interpolation", b.toString());
			}
		}
		// Retrieve target map binary object - unnecessary step if store all tags earlier
		QuadRect quad = MapUtils.calculateLatLonBbox(targetPoint.getLatitude(), targetPoint.getLongitude(), DIST_THRESHOLD_M);
		BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> request = BinaryMapIndexReader.buildSearchRequest(
				MapUtils.get31TileNumberX(quad.left), MapUtils.get31TileNumberX(quad.right),
				MapUtils.get31TileNumberY(quad.top), MapUtils.get31TileNumberY(quad.bottom), 16, null,
				new ResultMatcher<>() {

					@Override
					public boolean publish(BinaryMapDataObject obj) {
						if(ObfConstants.getOsmObjectId(obj) != datasetId) {
							return false;
						}
						return true;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});
		BinaryMapDataObject srcObj = null;
		List<BinaryMapDataObject> res = firstResult.searchResult.file.searchMapIndex(request);
		if (res.size() > 0) {
			srcObj = res.get(0);
		}
		
		// Find closest by distance by id & by tags 
		resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.object instanceof MapObject mo && ObfConstants.getOsmObjectId(mo) == datasetId) {
				actualResult = new Result(ResultType.ById, null, resPlace, sr);
				break;
			} else if (sr.object instanceof BinaryMapDataObject bo && actualResult == null && ObfConstants.getOsmObjectId(bo) == datasetId) {
				actualResult = new Result(ResultType.ById, bo, resPlace, sr);
				break;
			} else if (srcObj != null && firstByTag == null && sr.object instanceof Building b) {
				// only do matching by tags for object that we know don't store id like Building
				String hno = srcObj.getTagValue(OSMTagKey.ADDR_HOUSE_NUMBER.getValue());
				if (Algorithms.objectEquals(hno, b.getName())) {
					firstByTag = new Result(ResultType.ByTag, srcObj, resPlace, sr);
				}
			}
			if (MapUtils.getDistance(sr.location, targetPoint) < closestDist) {
				firstByDist = new Result(ResultType.ByDist, null, resPlace, sr);
				closestDist = MapUtils.getDistance(sr.location, targetPoint);
			}
			resPlace++;
		}
		
		if (actualResult == null) {
			actualResult = firstByTag;
		}
		if (actualResult == null) {
			actualResult = firstByDist;
		}
		return new Result[] {firstResult, actualResult};
	}

}
