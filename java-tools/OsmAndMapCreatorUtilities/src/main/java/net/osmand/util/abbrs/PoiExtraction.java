package net.osmand.util.abbrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.osmand.util.MapUtils;

/**
 * Named POI of the maps as the generation extracted them ({@link net.osmand.obf.preparation.PoiNamesExtractor}),
 * duplicates of maps removed ({@link PoiExtractionReader}): OSM entities with their raw name tags and the POI the OBF
 * holds for them.
 */
public class PoiExtraction {

	/** an OSM entity, the unit of the thresholds */
	public static class ExtractedEntity {
		public final String osmType;
		public final long osmId;
		public final Map<String, String> tags;
		/** map of the entity: of its POI with center 0 */
		public String map;
		public final List<ExtractedPoi> pois = new ArrayList<>();

		ExtractedEntity(String osmType, long osmId, Map<String, String> tags) {
			this.osmType = osmType;
			this.osmId = osmId;
			this.tags = tags;
		}

		public String key() {
			return osmType + "/" + osmId;
		}

		public String tag(String tag) {
			return tags.get(tag);
		}

		/** the POI of center 0, the point and the map of the entity */
		public ExtractedPoi mainPoi() {
			ExtractedPoi main = null;
			for (ExtractedPoi p : pois) {
				if (main == null || p.center < main.center) {
					main = p;
				}
			}
			return main;
		}

		/** subtypes and additional types of every POI of the entity */
		public Set<String> types() {
			Set<String> types = new TreeSet<>();
			for (ExtractedPoi p : pois) {
				types.add(p.subtype);
				types.addAll(p.additional);
			}
			return types;
		}

		/** tile of the main point at {@code zoom} as one number */
		public long tile(int zoom) {
			ExtractedPoi p = mainPoi();
			long x = (long) MapUtils.getTileNumberX(zoom, p.lon);
			long y = (long) MapUtils.getTileNumberY(zoom, p.lat);
			return (x << 32) | y;
		}

		@Override
		public String toString() {
			return key() + " " + tags.getOrDefault("name", tags.getOrDefault("operator", ""));
		}
	}

	/** a POI of the OBF: the unit of application and coverage */
	public static class ExtractedPoi {
		public final ExtractedEntity entity;
		public final int center;
		public final long obfId;
		public final String type;
		public final String subtype;
		public final Set<String> additional;
		public final double lat;
		public final double lon;
		public final String map;

		ExtractedPoi(ExtractedEntity entity, int center, long obfId, String type, String subtype, Set<String> additional,
				double lat, double lon, String map) {
			this.entity = entity;
			this.center = center;
			this.obfId = obfId;
			this.type = type;
			this.subtype = subtype;
			this.additional = additional;
			this.lat = lat;
			this.lon = lon;
			this.map = map;
		}

		/** POI identity (osm_type, osm_id, center, type, subtype) */
		public String identity() {
			return entity.key() + "/" + center + "/" + type + "/" + subtype;
		}

		public boolean hasType(Collection<String> types) {
			if (types.contains(subtype)) {
				return true;
			}
			for (String a : additional) {
				if (types.contains(a)) {
					return true;
				}
			}
			return false;
		}
	}

	/** a map of the extraction: the generation date decides between copies of one POI */
	public static class ExtractedMap {
		public final String name;
		public final long generated;
		public final int depth;
		public int entities;
		public int pois;
		public boolean used = true;
		public String skipReason;

		ExtractedMap(String name, long generated, int depth) {
			this.name = name;
			this.generated = generated;
			this.depth = depth;
		}
	}

	final Map<String, ExtractedEntity> entities = new LinkedHashMap<>();
	final List<ExtractedMap> maps = new ArrayList<>();
	int duplicatePois;

	public Collection<ExtractedEntity> entities() {
		return entities.values();
	}

	public ExtractedEntity entity(String osmType, long osmId) {
		return entities.get(osmType + "/" + osmId);
	}

	public List<ExtractedMap> maps() {
		return maps;
	}

	public int duplicatePois() {
		return duplicatePois;
	}

	public int poiCount() {
		int n = 0;
		for (ExtractedEntity e : entities.values()) {
			n += e.pois.size();
		}
		return n;
	}
}
