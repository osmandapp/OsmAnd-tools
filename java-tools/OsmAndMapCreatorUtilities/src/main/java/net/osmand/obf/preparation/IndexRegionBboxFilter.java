package net.osmand.obf.preparation;

import net.osmand.data.Amenity;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.map.WorldRegion;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IndexRegionBboxFilter {
	private static final double INFLATE_REGION_BBOX_KM = 20;

	// https://www.openstreetmap.org/relation/13218198 overinflates maps
	private static final Map<String, String> LOFAR_TELESCOPE_SYMPTOMS = Map.of(
			"name", "LOFAR",
			"barrier", "gate",
			"landuse", "grass",
			"man_made", "telescope"
	);

	private List<QuadRect> inflatedRegionQuads = null;

	private static final Log log = LogFactory.getLog(IndexRegionBboxFilter.class);

	public void initRegionQuads(WorldRegion region) {
		inflatedRegionQuads = region.getAllPolygonsBounds();
		double inflate = INFLATE_REGION_BBOX_KM * 1000 / MapUtils.METERS_IN_DEGREE;
		for (QuadRect rect : inflatedRegionQuads) {
			MapUtils.inflateBBoxLatLon(rect, inflate, inflate);
		}
	}

	public boolean shouldFilterPoiEntity(Entity entity) {
		return !isInsideRegionBbox(entity);
	}

	public boolean shouldFilterPoiAmenity(Amenity amenity) {
		return !isInsideRegionBbox(amenity);
	}

	public boolean shouldFilterMapEntity(Entity entity) {
		Map<String, String> tags = entity.getTags();
		if (!tags.isEmpty()) {
			for (Map.Entry<String, String> filter : LOFAR_TELESCOPE_SYMPTOMS.entrySet()) {
				if (filter.getValue().equals(tags.get(filter.getKey()))) {
					return !isInsideRegionBbox(entity);
				}
			}
		}
		return false;
	}

	public void logEntity(Entity e) {
		log.warn(String.format("MAP out-of-bbox %s [%s]", e.getOsmUrl(), e.getTags()));
	}

	public void logEntityWithAmenity(Entity entity, Amenity amenity) {
		LatLon ll = entity.getLatLon();
		if (entity instanceof Way way && !way.getNodes().isEmpty()) {
			ll = way.getFirstNode().getLatLon();
		}
		if (entity instanceof Relation && amenity.getLocation() != null) {
			ll = amenity.getLocation();
		}
		if (ll != null) {
			String type = amenity.getType().getKeyName();
			String subtype = amenity.getSubType();
			String name = amenity.getName();
			log.warn(String.format(Locale.US, "POI out-of-bbox %s %.4f %.4f %s %s %s",
					entity.getOsmUrl(), ll.getLatitude(), ll.getLongitude(), type, subtype, name));
		}
	}

	private boolean isInsideRegionBbox(double lat, double lon) {
		if (inflatedRegionQuads == null) {
			return true;
		}
		for (QuadRect quad : inflatedRegionQuads) {
			if (quad.contains(lon, lat, lon, lat)) {
				return true;
			}
		}
		return false;
	}

	private boolean isInsideRegionBbox(Amenity amenity) {
		if (amenity == null || amenity.getLocation() == null) {
			return true;
		}
		return isInsideRegionBbox(amenity.getLocation().getLatitude(), amenity.getLocation().getLongitude());
	}

	private boolean isInsideRegionBbox(Entity entity) {
		if (inflatedRegionQuads == null) {
			return true; // region might have no bbox
		} else if (entity instanceof Node node) {
			return isInsideRegionBbox(node.getLatitude(), node.getLongitude()); // Node
		} else if (entity instanceof Way way && way.getFirstNode() != null) {
			List<LatLon> latLons = new ArrayList<>();
			latLons.add(way.getFirstNode().getLatLon());
			latLons.add(way.getLastNode().getLatLon());
			if (way.getNodes().size() > 2) {
				latLons.add(way.getNodes().get(way.getNodes().size() / 2).getLatLon());
			}
			for (LatLon l : latLons) {
				if (isInsideRegionBbox(l.getLatitude(), l.getLongitude())) {
					return true;
				}
			}
			return false; // Way is outside
		} else if (entity instanceof Relation relation) {
			for (Relation.RelationMember member : relation.getMembers()) {
				if (isInsideRegionBbox(member.getEntity())) {
					return true; // recursive
				}
			}
			return false; // Relation is outside
		} else {
			return true; // default
		}
	}
}
