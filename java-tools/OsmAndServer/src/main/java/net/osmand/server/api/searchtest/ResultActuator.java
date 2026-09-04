package net.osmand.server.api.searchtest;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.ObfConstants;
import net.osmand.data.*;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.spatial.SpatialResultFormatter;
import net.osmand.util.MapUtils;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

public abstract class ResultActuator implements Consumer<List<SearchResult>> {
	public enum ResultType {
		Best,
		ById,
		ByName,
		ByTag,
		ByDist
	}

	public record Result(ResultType type, String entityId, int place, String name, LatLon location, String entityType) {
		public Result(ResultType type, int place, SearchResult result) {
			this(type, result == null ? null : getEntityId(result.object), place, 
					result == null ? null : result.toString(), 
					result == null ? null : result.location, 
					result == null ? null : getEntityType(result.object));
		}

		public Result(ResultType type, Result result) {
			this(type, result.entityId, result.place, result.name, result.location, result.entityType);
		}
		
		public String toPlaceString() {
			return place + " - " + type;
		}
		
		public String getName() {
			if (name == null)
				return null;
			return formatName(name);
		}
	}

	protected final LatLon targetPoint;
	protected final Map<String, Object> metrics;
	protected Result firstResult = null, actualResult = null;

	protected String error = null, resultPoint = null;
	protected Integer resultPlace = null, distance = null;

	public ResultActuator(LatLon targetPoint, Map<String, Object> statMetrics) {
		this.targetPoint = targetPoint;
		this.metrics = statMetrics;
	}
	
	public final Map<String, Object> getMetrics() {
		return metrics;
	}

	protected Result findFirstResult(List<SearchResult> searchResults) throws IOException {
		Result firstResult = null;
		int resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.objectType != null && ObjectType.LOCATION != sr.objectType) {
				firstResult = new Result(ResultType.Best, resPlace, sr);
				break;
			}
			resPlace++;
		}
		return firstResult;
	}

	protected abstract Result findActualResult(List<SearchResult> searchResults) throws IOException;

	public final void accept(List<SearchResult> searchResults) {
		try {
			firstResult = findFirstResult(searchResults);
			actualResult = findActualResult(searchResults);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static final int SEARCH_DUPLICATE_NAME_RADIUS = 5000;
	private static final int FOUND_DEDUPLICATE_RADIUS = 100;

	public boolean isFound(List<SearchResult> searchResults) {
		if (firstResult == null) {
			error = searchResults.isEmpty() ? "Search result is empty" : "First search result is missing";
			return false;
		}

		LatLon resPoint = firstResult.location;
		boolean found = false;
		if (resPoint != null) {
			int dupCount = 0;
			double closestDuplicate = MapUtils.getDistance(targetPoint, resPoint);
			int dupInd = firstResult.place() - 1;
			String resName = firstResult.name; // to do check to string is not too much
			for (int i = firstResult.place(); i < searchResults.size(); i++) {
				SearchResult sr = searchResults.get(i);
				double dist = MapUtils.getDistance(resPoint, sr.location);
				if (resName.equals(sr.toString()) && dist < SEARCH_DUPLICATE_NAME_RADIUS) {
					dupCount++;
				} else {
					break;
				}
				if (MapUtils.getDistance(targetPoint, sr.location) < closestDuplicate) {
					closestDuplicate = MapUtils.getDistance(targetPoint, sr.location);
					dupInd = i;
				}
			}
			resultPlace = firstResult.place();

			resultPoint = String.format(Locale.US, "%f, %f", resPoint.getLatitude(), resPoint.getLongitude());
			distance = ((int) MapUtils.getDistance(targetPoint, resPoint) / 10) * 10;

			if (dupCount > 0) {
				metrics.put("dup_count", dupCount);
			}

			metrics.put("web_type", firstResult.entityType);
			setResult("res", firstResult);
			if (actualResult == null && closestDuplicate < FOUND_DEDUPLICATE_RADIUS) {
				SearchResult sr = searchResults.get(dupInd);
				actualResult = new Result(ResultActuator.ResultType.ByDist, dupInd + 1, sr);
			}
			if (actualResult != null) {
				setResult("actual", actualResult);
				found = actualResult.place() <= dupCount + firstResult.place();
			}
			found |= closestDuplicate < FOUND_DEDUPLICATE_RADIUS; // deduplication also count as found
		} else {
			error = "Result point location is null";
		}
		return found;
	}

	public Integer getResultPlace() {
		return resultPlace;
	}

	public String getResultPoint() {
		return resultPoint;
	}

	public String getError() {
		return error;
	}

	public Integer getResultDistance() {
		return distance;
	}

	public void setFormatter(SpatialResultFormatter formatter) {}
	
	public void setResult(String prefix, Result res) {
		metrics.put(prefix + "_place", res.toPlaceString());
		metrics.put(prefix + "_id", res.entityId);
		metrics.put(prefix + "_name", res.getName());
		if (res.location != null) {
			metrics.put(prefix + "_dist", ((int) MapUtils.getDistance(targetPoint, res.location) / 10) * 10);
			metrics.put(prefix + "_lat_lon", toString(res.location));
		}
	}

	protected static long osmId(SearchResult result) {
		Object object = result.spatialResult == null ? result.object : result.spatialResult.getMainObject();
		if (object instanceof MapObject mapObject && mapObject.getId() != null) {
			return ObfConstants.getOsmObjectId(mapObject);
		}
		if (object instanceof BinaryMapDataObject binaryObject) {
			return ObfConstants.getOsmObjectId(binaryObject);
		}
		return -1;
	}

	public static String toString(LatLon point) {
		if (point == null) 
			return null;
		return String.format(Locale.US, "%.5f, %.5f", point.getLatitude(), point.getLongitude());
	}
	
	public static String getEntityId(Object object) {
		String entityType = getEntityType(object);
		if ("U".equals(entityType)) {
			return entityType;
		}
		if (object instanceof MapObject mo) {
			return entityType + ObfConstants.getOsmObjectId(mo);
		}
		return entityType + ObfConstants.getOsmObjectId((BinaryMapDataObject)object);
	}
	
	public static String getEntityType(Object object) {
		if (object == null) {
			return "U";
		}
		EntityType et;
		if (object instanceof MapObject mo) {
			et = ObfConstants.getOsmEntityType(mo);
		} else if (object instanceof BinaryMapDataObject bmo) {
			et = ObfConstants.getOsmEntityType(bmo);
		} else {
			throw new IllegalArgumentException("Unexpected class: " + object.getClass().getName());
		}
		
		if (et == null) {
			return "U";
		}
		if (object instanceof Street) {
			return "S";
		}
		if (et == EntityType.NODE) {
			return "N";
		} 
		if (et == EntityType.WAY) {
			return "W";
		} 
		return "R";
	}

	public static String formatName(String name) {
		if (name == null) {
			return null;
		}
		int detailsIndex = name.indexOf('[');
		return (detailsIndex == -1 ? name : name.substring(0, detailsIndex)).replaceAll("\\s*\\([^)]*\\)", "").trim();
	}
}
