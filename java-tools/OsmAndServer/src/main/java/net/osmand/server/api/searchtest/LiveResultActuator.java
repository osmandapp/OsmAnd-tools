package net.osmand.server.api.searchtest;

import net.osmand.data.LatLon;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.spatial.SpatialResultFormatter;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.util.*;

public class LiveResultActuator extends ResultActuator {
	private static final double MATCH_RADIUS_METERS = 20;
	private static final int MAX_TOOLTIP_RESULTS = 10;

	private record ExpectedResult(long osmId, LatLon point, String result, int place, String entityType) {
		public String entityId() {
			if (entityType == null || "U".equals(entityType))
				return "U";
			return entityType + osmId;
		}
		public String getName() {
			return formatName(result);
		}
	}
	public record ResultInfo(String name, Integer distance, String location, String entityId) {}

	private final List<ExpectedResult> expectedResults;
	private final List<ResultInfo> actualResults = new ArrayList<>();
	private ExpectedResult matched;
	private SpatialResultFormatter formatter;
	private String unitTest;

	public LiveResultActuator(LatLon targetPoint, Map<String, Object> metrics, List<Map<String, Object>> objects) {
		super(targetPoint, metrics);
		
		expectedResults = new ArrayList<>(objects.size());
		for (int i = 0; i < objects.size(); i++) {
			Map<String, Object> object = objects.get(i);
			Object id = object.get("id");
			if (!(id instanceof Number) && !(id instanceof String)) {
				throw new IllegalArgumentException("Expected result id is missing at place " + (i + 1));
			}
			long osmId = id instanceof Number number ? number.longValue() : Long.parseLong((String) id);
			String pointText = Objects.toString(object.get("point"), null);
			LatLon point = Algorithms.parseLatLon(pointText);
			String entityType = (String)object.get("entityType");
			unitTest = (String)object.get("unitTest");
			expectedResults.add(new ExpectedResult(osmId, point, 
					Objects.toString(object.get("result"), null), i + 1, entityType));
		}
	}

	@Override
	public void setFormatter(SpatialResultFormatter formatter) {
		this.formatter = formatter;
	}

	@Override
	protected Result findActualResult(List<SearchResult> searchResults) {
		actualResults.clear();
		matched = null;
		for (SearchResult actual : searchResults) {
			String name = formatter != null && actual.spatialResult != null
					? formatter.format(actual.spatialResult) : actual.toString();

			Object object = actual.spatialResult == null ? actual.object : actual.spatialResult.getMainObject();
			actualResults.add(toResultInfo(name, actual.location, getEntityId(object)));
		}
		
		for (int actualIndex = 0; actualIndex < searchResults.size(); actualIndex++) {
			SearchResult actual = searchResults.get(actualIndex);
			
			long actualId = osmId(actual);
			String actualResultText = actualResults.get(actualIndex).name();
			
			for (ExpectedResult expected : expectedResults) {
				if (expected.point() == null || actual.location == null
						|| MapUtils.getDistance(expected.point(), actual.location) > MATCH_RADIUS_METERS) {
					continue;
				}
				ResultType matchType = null;
				if (expected.osmId() != -1 && expected.osmId() == actualId) {
					matchType = ResultType.ById;
				} else if (expected.osmId() == -1 && actualId == -1 && formatter != null && actual.spatialResult != null) {
					String expectedResult = formatName(expected.result());
					if (Objects.equals(expectedResult, actualResultText)) {
						matchType = ResultType.ByName;
					}
				}
				
				if (matchType != null) {
					matched = expected;
					trimActualResults(actualIndex + 1);
					String name = formatter != null && actual.spatialResult != null
							? formatter.format(actual.spatialResult) : actual.toString();
					return new Result(matchType, getEntityId(actual.object), actualIndex + 1, name,
							actual.location, getEntityType(actual.object));
				}
			}
		}
		trimActualResults(firstResult == null ? 0 : firstResult.place());
		return null;
	}

	private void trimActualResults(int selectedPlace) {
		int size = Math.max(MAX_TOOLTIP_RESULTS, Math.max(expectedResults.size(), selectedPlace));
		if (actualResults.size() > size) {
			actualResults.subList(size, actualResults.size()).clear();
		}
	}

	private ResultInfo toResultInfo(String name, LatLon point, String entityId) {
		Integer distance = point == null ? null : ((int) MapUtils.getDistance(targetPoint, point) / 10) * 10;
		return new ResultInfo(formatName(name), distance, toString(point), entityId);
	}

	@Override
	public boolean isFound(List<SearchResult> searchResults) {
		if (expectedResults.isEmpty()) {
			error = "Expected result is empty";
			return false;
		}
		
		metrics.put("web_type", unitTest);
		metrics.put("actual_count", expectedResults.size());
		if (searchResults.isEmpty()) {
			error = "Search result is empty";
			return false;
		}

		if (matched == null || actualResult == null) {
			if (firstResult != null) {
				setResult("res", new Result(ResultType.Best, firstResult.entityId(), 1, actualResults.get(firstResult.place() - 1).name(),
						firstResult.location(), firstResult.entityType()));
			}
			ExpectedResult firstExpected = expectedResults.get(0);
			setResult("actual", new Result(ResultType.Best, firstExpected.entityId(), 1, firstExpected.result, firstExpected.point, firstExpected.entityType));
			return false;
		}

		distance = matched.point() == null ? null : ((int) MapUtils.getDistance(matched.point(), targetPoint) / 10) * 10;
		resultPlace = matched.place();
		resultPoint = toString(matched.point);

		setResult("res", actualResult);
		setResult("actual", new Result(ResultType.ByName, matched.entityId(), resultPlace, matched.result, 
				matched.point, matched.entityType));

		return true;
	}

	public void setResult(String prefix, Result res) {
		super.setResult(prefix, res);
		
		String name = res.getName();
		if (name != null) {
			List<ResultInfo> results = "res".equals(prefix) ? actualResults
					: expectedResults.stream()
							.map(result -> toResultInfo(result.result(), result.point(), result.entityId()))
							.toList();
			metrics.put(prefix + "_name", new Object[] {name, results});
		}
	}
}
