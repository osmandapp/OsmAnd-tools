package net.osmand.obf.preparation;

import net.osmand.osm.edit.Relation;

import java.util.*;

public class TransportTags {
	public static final int POPULARITY_THRESHOLD = 3;
	private final Map<Long, List<TransportTagValue>> tags = new HashMap<>();
	Map<String, TransportTagValue> allTags = new HashMap<>();
	private static final Set<String> tagsFilter = new HashSet<>();

	static {
		tagsFilter.add("interval");
		tagsFilter.add("opening_hours");
		tagsFilter.add("duration");
	}

	public void add(Long routeId, List<TransportTagValue> transportTags) {
		tags.put(routeId, transportTags);
	}

	public List<TransportTagValue> get(long idRoute) {
		List<TransportTagValue> res = tags.get(idRoute);
		if (res != null) {
			return res;
		} else {
			return Collections.emptyList();
		}
	}

	public void registerTagValues(Relation rel, long routeId) {
		Map<String, String> relTags = rel.getTags();
		registerTagValues(routeId, relTags);
	}

	public void registerTagValues(long routeId, Map<String, String> relTags) {
		List<TransportTagValue> filteredTags = new ArrayList<>();
		for (String tagKey : relTags.keySet()) {
			for (String neededTag : tagsFilter) {
				if (tagKey.startsWith(neededTag)) {
					TransportTagValue tagValue = allTags.get(TransportTagValue.createKey(tagKey, relTags.get(tagKey)));
					if (tagValue != null) {
						tagValue.count++;
					} else {
						tagValue = new TransportTagValue(tagKey, relTags.get(tagKey));
						allTags.put(tagValue.getKey(), tagValue);
					}
					filteredTags.add(tagValue);
					break;
				}
			}
		}
		if (!filteredTags.isEmpty()) {
			tags.put(routeId, filteredTags);
		}
	}

	public static class TransportTagValue {
		int id;
		String tag;
		String value;
		String key; // tag+'/'+value
		int count;

		public TransportTagValue(String tag, String value) {
			this.tag = tag;
			this.value = value;
			key = createKey(tag, value);
		}

		private static String createKey(String tag, String value) {
			return tag + "/" + value;
		}

		public boolean isPopular() {
			return count > POPULARITY_THRESHOLD;
		}

		public String getTag() {
			return tag;
		}

		public String getKey() {
			return key;
		}

		public String getValue() {
			return value;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((tag == null) ? 0 : tag.hashCode());
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null || getClass() != obj.getClass())
				return false;
			TransportTagValue other = (TransportTagValue) obj;
			if (tag == null) {
				if (other.tag != null)
					return false;
			} else if (!tag.equals(other.tag))
				return false;
			if (value == null) {
				return other.value == null;
			} else {
				return value.equals(other.value);
			}
		}
	}
}
