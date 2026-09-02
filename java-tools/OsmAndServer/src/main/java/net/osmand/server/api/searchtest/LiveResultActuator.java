package net.osmand.server.api.searchtest;

import net.osmand.data.LatLon;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.spatial.SpatialResultFormatter;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.util.*;

public class LiveResultActuator extends ResultActuator {
	private record ExpectedResult(long osmId, LatLon point, String result, int place, String entityType) {
		public String entityId() {
			if (entityType == null)
				return "U";
			return entityType + ("U".equals(entityType) ? "" : osmId);
		}
	}

	private final List<ExpectedResult> expectedResults;
	private ExpectedResult matched;
	private SpatialResultFormatter formatter;

	public LiveResultActuator(LatLon targetPoint, Map<String, Object> statMetrics, List<Map<String, Object>> objects) {
		super(targetPoint, statMetrics);
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
		for (int actualIndex = 0; actualIndex < searchResults.size(); actualIndex++) {
			SearchResult actual = searchResults.get(actualIndex);
			long actualId = osmId(actual);
			String actualResultText = null;
			for (ExpectedResult expected : expectedResults) {
				ResultType matchType = null;
				if (expected.osmId() != -1 && expected.osmId() == actualId) {
					matchType = ResultType.ById;
				} else if (expected.osmId() == -1 && actualId == -1 && formatter != null && actual.spatialResult != null) {
					actualResultText = actualResultText == null ? formatter.format(actual.spatialResult) : actualResultText;
					String expectedResult = expected.result();
					if (expectedResult.indexOf('[') != -1) {
						expectedResult = expectedResult.substring(0, expectedResult.indexOf('[') + 4).trim();
					}
					if (actualResultText != null && actualResultText.indexOf('[') != -1) {
						actualResultText = actualResultText.substring(0, actualResultText.indexOf('[') + 4).trim();
					}
					if (Objects.equals(expectedResult, actualResultText)) {
						matchType = ResultType.ByResult;
					}
				}
				
				if (matchType != null) {
					matched = expected;
					return new Result(matchType, actualIndex + 1, actual);
				}
			}
		}
		return null;
	}

	@Override
	public boolean isFound(List<SearchResult> searchResults) {
		if (expectedResults.isEmpty()) {
			error = "Expected result is empty";
			return false;
		}
		
		ExpectedResult first = expectedResults.get(0);
		String entityId = first.entityType + first.osmId;
		setActual(new Result(ResultType.Best, entityId, 1, first.result, first.point, first.entityType));
		if (searchResults.isEmpty()) {
			error = "Search result is empty";
			return false;
		}
		
		if (matched == null || actualResult == null) {
			setFirst(firstResult);
			return false;
		}

		distance = matched.point() == null ? null
				: ((int) MapUtils.getDistance(targetPoint, matched.point()) / 10) * 10;
		resultPlace = matched.place();
		resultPoint = toString(matched.point);

		setFirst(actualResult);
		setActual(new Result(ResultType.ByResult, matched.entityId(), resultPlace, matched.result, matched.point, matched.entityType));

		return true;
	}
}
