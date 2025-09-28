package net.osmand.server.api.searchtest;

import gnu.trove.map.hash.TIntObjectHashMap;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.Building;
import net.osmand.data.QuadRect;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.util.MapUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapDataObjectFinder {
	public enum ResultType {
		ById,
		ByTag,
		ByDist,
		Error
	}

	public record Result(ResultType type, BinaryMapDataObject object, int place, SearchResult searchResult) {
		@NotNull
		public String toIdString() {
			long id = object == null ? -1 : object.getId();
			if (id > 0)
				return String.valueOf(id / 128);
			return "";
		}
		public String toPlaceString() {
			return place + " - " + type();
		}
	}

	private Map<String, String> getTags(BinaryMapDataObject obj) {
		Map<String, String> tags = new HashMap<>();

		TIntObjectHashMap<String> names = obj.getObjectNames();
		int[] keys = names.keys();
		for (int key : keys) {
			BinaryMapIndexReader.TagValuePair pair = obj.getMapIndex().decodeType(key);
			if (pair == null)
				continue;

			String v = names.get(key);
			if (v != null) {
				tags.put(pair.tag, v);
			}

		}
		return tags;
	}

	public Result[] find(List<SearchResult> searchResults, long datasetId, Map<String, Object> row) {
		int resPlace = 1;
		Result firstResult = null, firstByTag = null, firstByDist = null;
		for (SearchResult res : searchResults) {
			Object wt = res.objectType;
			if (wt != null && !"LOCATION".equals(wt.toString())) {
				Result currResult = getMapDataObject(res, datasetId, resPlace, row);

				if (firstResult == null)
					firstResult = currResult;
				if (currResult.type() == MapDataObjectFinder.ResultType.ById)
					return new Result[] {firstResult, currResult};
				if (currResult.type() == MapDataObjectFinder.ResultType.ByTag && firstByTag == null)
					firstByTag = currResult;
				if (currResult.type() == MapDataObjectFinder.ResultType.ByDist && firstByDist == null)
					firstByDist = currResult;
			}
			resPlace++;
		}
		if (firstResult != null && firstResult.searchResult() != null && firstResult.searchResult().object instanceof Building b) {
			if (b.getInterpolationInterval() != 0 || b.getInterpolationType() != null)
				row.put("interpolation", b.toString());
		}
		if (firstByTag != null) {
			return new Result[] {firstResult, firstByTag};
		}
		if (firstByDist != null) {
			return new Result[] {firstResult, firstByDist};
		}
		return new Result[] {firstResult, null};
	}

	public Result getMapDataObject(SearchResult result, long expectedOsmId, int place, Map<String, Object> row) {
		if (result.location == null || result.localeName == null || result.file == null)
			return new Result(ResultType.Error, null, place, result);

		final BinaryMapDataObject[] byDist = {null}, byId = {null};
		QuadRect quad = MapUtils.calculateLatLonBbox(result.location.getLatitude(), result.location.getLongitude(), 100);
		BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> request = BinaryMapIndexReader.buildSearchRequest(
				MapUtils.get31TileNumberX(quad.left),
				MapUtils.get31TileNumberX(quad.right),
				MapUtils.get31TileNumberY(quad.top),
				MapUtils.get31TileNumberY(quad.bottom), 16, null, new ResultMatcher<>() {

					@Override
					public boolean publish(BinaryMapDataObject obj) {
						if (obj.getId() != -1 && byDist[0] == null)
							byDist[0] = obj;

						if (obj.getId() / 128 == expectedOsmId) {
							byId[0] = obj;
							return true;
						}

						// Use address tags to match SearchResult types against BinaryMapDataObject names
						String tag = ObjectType.HOUSE.equals(result.objectType) ? "addr:housenumber" : "name";
						Map<String, String> tags = getTags(obj);
						if (tags.isEmpty())
							return false;

						String value = tags.get(tag);
						boolean matches = value != null && value.startsWith(result.localeName);
						if (matches)
							row.put("by_tag", tag + "=" + value);
						return matches;
					}

					@Override
					public boolean isCancelled() {
						return byId[0] != null;
					}
				});

		List<BinaryMapDataObject> found;
		try {
			found = result.file.searchMapIndex(request);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (!found.isEmpty()) {
			return byId[0] != null ?
					new Result(ResultType.ById, byId[0], place, result) :
					new Result(ResultType.ByTag, found.get(0), place, result);
		}
		return new Result(ResultType.ByDist, byDist[0], place, result);
	}
}
