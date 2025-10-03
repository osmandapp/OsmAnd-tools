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
			long id = -1;
			if (exact != null) {
				id = ObfConstants.getOsmObjectId(exact);
			} else if (searchResult.object instanceof MapObject mo) {
				id =  ObfConstants.getOsmObjectId(mo);
			}
			return id > 0 ? id + "" : "";
		}
		
		public String toPlaceString() {
			return place + " - " + type();
		}
	}

	
	public Result findFirstResult(List<SearchResult> searchResults, LatLon targetPoint, Map<String, Object> row) throws IOException {
		Result firstResult = null;
		int resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.objectType != null && ObjectType.LOCATION != sr.objectType) {
				firstResult = new Result(ResultType.Best, null, resPlace, sr);
				break;
			}
			resPlace++;
		}
		if (firstResult != null) {
			if (firstResult.searchResult().object instanceof Building b) {
				if (b.getInterpolationInterval() != 0 || b.getInterpolationType() != null) {
					row.put("interpolation", b.toString());
				}
				// try to calculate precise id for first result 
				QuadRect quad = MapUtils.calculateLatLonBbox(targetPoint.getLatitude(), targetPoint.getLongitude(), DIST_THRESHOLD_M);
				BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> request = BinaryMapIndexReader.buildSearchRequest(
						MapUtils.get31TileNumberX(quad.left), MapUtils.get31TileNumberX(quad.right),
						MapUtils.get31TileNumberY(quad.top), MapUtils.get31TileNumberY(quad.bottom), 16, null,
						new ResultMatcher<>() {

							@Override
							public boolean publish(BinaryMapDataObject obj) {
								return true;
							}

							@Override
							public boolean isCancelled() {
								return false;
							}
						});

				if (firstResult.searchResult().file != null) {
					List<BinaryMapDataObject> objects = firstResult.searchResult().file.searchMapIndex(request);
					for (BinaryMapDataObject o : objects) {
						String hno = o.getTagValue(OSMTagKey.ADDR_HOUSE_NUMBER.getValue());
						if (Algorithms.objectEquals(hno, b.getName())) {
							firstResult = new Result(ResultType.Best, o, firstResult.place, firstResult.searchResult);
							break;
						}
					}
				}
			}
		}
		return firstResult;
	}


	private static final int DIST_THRESHOLD_M = 20;

	public Result findActualResult(List<SearchResult> searchResults, LatLon targetPoint, long datasetId) throws IOException {
		Result actualResult = null, actualByDist = null, actualByTag = null;
		double closestDist = 100; // needed by deduplicate for interpolation 
		int resPlace;
		// Retrieve target map binary object - unnecessary step if store all tags earlier
		QuadRect quad = MapUtils.calculateLatLonBbox(targetPoint.getLatitude(), targetPoint.getLongitude(), DIST_THRESHOLD_M);
		BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> request = BinaryMapIndexReader.buildSearchRequest(
				MapUtils.get31TileNumberX(quad.left), MapUtils.get31TileNumberX(quad.right),
				MapUtils.get31TileNumberY(quad.top), MapUtils.get31TileNumberY(quad.bottom), 16, null,
				new ResultMatcher<>() {

					@Override
					public boolean publish(BinaryMapDataObject obj) {
						return ObfConstants.getOsmObjectId(obj) == datasetId;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});
		BinaryMapDataObject srcObj = null;
		for (SearchResult sr : searchResults) {
			if (sr.file != null) {
				List<BinaryMapDataObject> res = sr.file.searchMapIndex(request);
				if (!res.isEmpty()) {
					srcObj = res.get(0);
				}
				break;
			}
		}
		
		
		// Find closest by distance by id & by tags 
		resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.object instanceof MapObject mo && ObfConstants.getOsmObjectId(mo) == datasetId) {
				actualResult = new Result(ResultType.ById, null, resPlace, sr);
				break;
			} else if (sr.object instanceof BinaryMapDataObject bo && ObfConstants.getOsmObjectId(bo) == datasetId) {
				actualResult = new Result(ResultType.ById, bo, resPlace, sr);
				break;
			} else if (srcObj != null && sr.object instanceof Building b) {
				// only do matching by tags for object that we know don't store id like Building
				String hno = srcObj.getTagValue(OSMTagKey.ADDR_HOUSE_NUMBER.getValue());
				if (Algorithms.objectEquals(hno, b.getName())) {
					actualByTag = new Result(ResultType.ByTag, srcObj, resPlace, sr);
					break;
				}
			}
			if (MapUtils.getDistance(sr.location, targetPoint) < closestDist) {
				actualByDist = new Result(ResultType.ByDist, null, resPlace, sr);
				closestDist = MapUtils.getDistance(sr.location, targetPoint);
			}
			resPlace++;
		}
		
		if (actualResult == null) {
			actualResult = actualByTag;
		}
		if (actualResult == null) {
			actualResult = actualByDist;
		}
		return actualResult;
	}
}
