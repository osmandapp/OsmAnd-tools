package net.osmand.server.api.searchtest;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.ObfConstants;
import net.osmand.data.*;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.util.MapUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import static net.osmand.util.MapUtils.getDistance;

public class ResultActuator implements Consumer<List<SearchResult>> {
	public enum ResultType {
		Best,
		ById,
		ByTag,
		ByDist,
		Error
	}

	protected final LatLon targetPoint;
	protected final Map<String, Object> row;
	protected Result firstResult = null, actualResult = null;
	protected final long datasetId;

	public ResultActuator(LatLon targetPoint, Map<String, Object> row, long datasetId) {
		this.targetPoint = targetPoint;
		this.row = row;
		this.datasetId = datasetId;
	}

	public record Result(ResultType type, Object exact, int place, SearchResult searchResult) {
		@NotNull
		public String toIdString() {
			String idStr = "U";
			Object obj = exact == null ? searchResult.object : exact; 
			if (obj instanceof BinaryMapDataObject) {
				EntityType et = ObfConstants.getOsmEntityType((BinaryMapDataObject) obj);
				if (et == EntityType.RELATION) {
					idStr = "R";
				} else if (et == EntityType.NODE) {
					idStr = "N";
				} else {
					idStr = "W";
				}
				idStr += ObfConstants.getOsmObjectId((BinaryMapDataObject) obj);
			} else if (obj instanceof Street s) {
				idStr = "S" + ObfConstants.getOsmObjectId(s);
			} else if (obj instanceof MapObject mo && mo.getId() != null) {
				EntityType et = ObfConstants.getOsmEntityType(mo);
				if (et == EntityType.NODE) {
					idStr = "N";
				} else if (et == EntityType.WAY) {
					idStr = "W";
				} else {
					idStr = "R";
				}
				idStr += ObfConstants.getOsmObjectId(mo);
			} else if (searchResult.object != null) {
				idStr += searchResult.object.getClass().getSimpleName();
			}
			return idStr;
		}
		
		public String toPlaceString() {
			return place + " - " + type();
		}
		
		public String placeName() {
			return searchResult != null ? searchResult.toString() : "";
		}
	}

	protected Result findFirstResult(List<SearchResult> searchResults) throws IOException {
		Result firstResult = null;
		int resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.objectType != null && ObjectType.LOCATION != sr.objectType) {
				firstResult = new Result(ResultType.Best, null, resPlace, sr);
				break;
			}
			resPlace++;
		}
		return firstResult;
	}
	
	protected static final int DIST_PRECISE_THRESHOLD_M = 20;
	
	protected Result findActualResult(List<SearchResult> searchResults) throws IOException {
		// Find closest by distance by id & by tags 
		int resPlace = 1;
		double minDistance = Double.MAX_VALUE;
		for (SearchResult sr : searchResults) {
			if (sr.object instanceof MapObject mo && ObfConstants.getOsmObjectId(mo) == datasetId) {
				actualResult = new Result(ResultType.ById, mo, resPlace, sr);
				break;
			} else if (sr.object instanceof BinaryMapDataObject bo && ObfConstants.getOsmObjectId(bo) == datasetId) {
				actualResult = new Result(ResultType.ById, bo, resPlace, sr);
				break;
			} else if(sr.location != null) {
				double dist = MapUtils.getDistance(sr.location, targetPoint);
				if (dist < DIST_PRECISE_THRESHOLD_M && dist < minDistance) {
					minDistance = dist;
					actualResult = new Result(ResultType.ByDist, sr.object, resPlace, sr);
				}
			}
			resPlace++;
		}
		return actualResult;
	}

	public void accept(List<SearchResult> searchResults) {
		try {
			firstResult = findFirstResult(searchResults);
			actualResult = findActualResult(searchResults);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Result getFirstResult() {
		return firstResult;
	}

	public Result getActualResult() {
		return actualResult;
	}

	public Map<String, Object> getRow() {
		return row;
	}
}
