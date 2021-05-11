package net.osmand.obf.preparation;

import net.osmand.osm.edit.Relation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TransportTags {
	private final Map<Long, Map<String, String>> tags = new HashMap<>();
	private static final Set<String> tagsFilter = new HashSet<>();
	private static final Map<String, Integer> tagsCount = new HashMap<>();

	static {
		tagsFilter.add("interval");
		tagsFilter.add("opening_hours");
		tagsFilter.add("duration");
	}

	public TransportTags() {
	}

	public TransportTags(Long routeId, Map<String, String> transportTags) {
		tags.put(routeId, transportTags);
	}

	public Map<String, String> get(long idRoute) {
		return tags.get(idRoute);
	}

	public void putFilteredTags(Relation rel, long routeId) {
		Map<String, String> filteredTags = new HashMap<>();
		Map<String, String> relTags = rel.getTags();
		for (String tagKey : relTags.keySet()) {
			for (String neededTag : tagsFilter) {
				if (tagKey.startsWith(neededTag)) {
					filteredTags.put(tagKey, relTags.get(tagKey));
					tagsCount.put(tagKey, tagsCount.getOrDefault(tagKey, 0) + 1);
					break;
				}
			}
		}
		if (!filteredTags.isEmpty()) {
			tags.put(routeId, filteredTags);
		}
	}

	int getCount(String tag) {
		return tagsCount.get(tag);
	}
}
